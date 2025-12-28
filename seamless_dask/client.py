"""Seamless Dask client that drives transformation execution."""

from __future__ import annotations

import time
import traceback
import uuid
import asyncio
import sys
import threading
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Callable, Coroutine

import dask.config
from distributed import Client, Future
from distributed.worker import get_worker

from seamless import Buffer, Checksum, CacheMissError
from seamless_transformer import worker as transformer_worker
from seamless_transformer.transformation_utils import tf_get_buffer

from .permissions import ensure_configured
from .types import (
    TransformationFutures,
    TransformationInputSpec,
    TransformationSubmission,
)
from .worker_setup import SeamlessWorkerPlugin

# Apply the requested global Dask defaults up front.
dask.config.set({"distributed.worker.daemon": False})
dask.config.set({"distributed.scheduler.unknown-task-duration": "1m"})
dask.config.set({"distributed.scheduler.target-duration": "10m"})


def _run_on_worker_loop(coro_factory: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    loop = _get_worker_loop()
    if loop is not None and not loop.is_closed():
        try:
            coro = coro_factory()
            return asyncio.run_coroutine_threadsafe(coro, loop).result()
        except Exception:
            pass
    # Fallback: run in a fresh loop with a fresh coroutine
    return asyncio.run(coro_factory())


def _run_coro_blocking(coro: Coroutine[Any, Any, Any]) -> Any:
    """Run a coroutine to completion in a fresh event loop (avoids nested-loop issues)."""

    try:
        return asyncio.run(coro)
    except RuntimeError:
        # Fall back to a dedicated thread if we're already inside a running loop.
        result: dict[str, Any] = {}
        error: dict[str, BaseException] = {}

        def _runner():
            try:
                result["value"] = asyncio.run(coro)
            except BaseException as exc:
                error["exc"] = exc

        t = threading.Thread(target=_runner, name="promise-blocking-runner")
        t.start()
        t.join()
        if error:
            raise error["exc"]
        return result.get("value")


async def _resolve_buffer_async(checksum: Checksum) -> Buffer | None:
    buffer_obj = await checksum.resolution()
    if isinstance(buffer_obj, Buffer):
        buffer_obj.tempref()
        return buffer_obj
    return None


async def _fetch_cached_result_async(
    tf_checksum: Checksum, require_value: bool
) -> Checksum | None:
    """Fetch a cached transformation result from the remote database, if any."""
    try:
        from seamless_remote import database_remote
    except Exception:
        return None

    try:
        result_checksum = await database_remote.get_transformation_result(tf_checksum)
        if result_checksum is None:
            return None
        if require_value:
            try:
                await result_checksum.resolution()
            except CacheMissError:
                return None
            except Exception:
                return None
        return result_checksum
    except Exception:
        return None


async def _promise_and_write_result_async(
    tf_checksum: Checksum, result_checksum: Checksum
) -> None:
    """Promise the result checksum and persist it to the remote database."""
    try:
        from seamless_remote import buffer_remote, database_remote
    except Exception:
        return
    try:
        await buffer_remote.promise(result_checksum)
        await database_remote.set_transformation_result(tf_checksum, result_checksum)
    except Exception:
        # Best-effort; failures are swallowed to avoid task crashes.
        return


def _fat_checksum_task(checksum_hex: str) -> Tuple[str, Buffer | None, str | None]:
    """Resolve a checksum into a buffer on a worker."""
    try:
        checksum = Checksum(checksum_hex)
        buffer_obj = _run_on_worker_loop(lambda: _resolve_buffer_async(checksum))
        return (
            checksum.hex(),
            buffer_obj if isinstance(buffer_obj, Buffer) else None,
            None,
        )
    except Exception:
        return checksum_hex, None, traceback.format_exc()


def _run_base(
    payload: Dict[str, Any], input_results: Mapping[str, Tuple[Any, ...]]
) -> Tuple[str | None, str | None, Buffer | None, str | None]:
    """Worker task that performs the transformation itself."""

    from seamless_dask.client import SeamlessDaskClient
    from seamless_dask.transformer_client import (
        get_seamless_dask_client,
        set_seamless_dask_client,
    )
    import distributed

    client = get_seamless_dask_client()
    if client is None:
        dask_client = distributed.get_client()
        assert dask_client is not None
        client = SeamlessDaskClient(dask_client)
        set_seamless_dask_client(client)

    tf_checksum_hex = payload.get("tf_checksum")
    transformation_dict = dict(payload.get("transformation_dict") or {})
    inputs = payload.get("inputs") or []
    scratch = bool(payload.get("scratch", False))
    tf_dunder = payload.get("tf_dunder", {}) or {}
    require_value = bool(payload.get("require_value", False))

    try:
        for raw_spec in inputs:
            spec = TransformationInputSpec(**raw_spec)
            input_value = input_results.get(spec.name)
            if input_value is None:
                raise RuntimeError(f"Missing input for pin '{spec.name}'")

            if spec.kind == "checksum":
                checksum_hex, _buf, exc = input_value
                if exc:
                    return tf_checksum_hex, None, None, exc
                transformation_dict[spec.name] = (
                    spec.celltype,
                    spec.subcelltype,
                    checksum_hex,
                )
            elif spec.kind == "transformation":
                result_checksum_hex, _buf, exc = input_value
                if exc:
                    return tf_checksum_hex, None, None, exc
                transformation_dict[spec.name] = (
                    spec.celltype,
                    spec.subcelltype,
                    result_checksum_hex,
                )
            else:  # pragma: no cover - defensive guard
                raise ValueError(f"Unknown input kind '{spec.kind}'")

        if tf_checksum_hex is None:
            tf_buffer = tf_get_buffer(transformation_dict)
            tf_buffer.tempref()
            tf_checksum_hex = tf_buffer.get_checksum().hex()
        tf_checksum = Checksum(tf_checksum_hex)

        # Fast path: check remote database on the worker before recomputing.
        cached_checksum = _run_on_worker_loop(
            lambda: _fetch_cached_result_async(tf_checksum, require_value)
        )
        if isinstance(cached_checksum, Checksum):
            result_checksum = cached_checksum
            result_checksum_hex = result_checksum.hex()
            result_buffer = None
            if require_value:
                try:
                    result_buffer = _run_on_worker_loop(
                        lambda: _resolve_buffer_async(result_checksum)
                    )
                    if not isinstance(result_buffer, Buffer):
                        result_buffer = None
                except Exception:
                    result_buffer = None
                if result_buffer is None:
                    # fall back to full execution if value unavailable
                    pass
                else:
                    _run_on_worker_loop(
                        lambda: _promise_and_write_result_async(
                            tf_checksum, result_checksum
                        )
                    )
                    return tf_checksum_hex, result_checksum_hex, result_buffer, None
            else:
                _run_on_worker_loop(
                    lambda: _promise_and_write_result_async(
                        tf_checksum, result_checksum
                    )
                )
                return tf_checksum_hex, result_checksum_hex, None, None

        result_checksum = _run_on_worker_loop(
            lambda: transformer_worker.dispatch_to_workers(
                transformation_dict,
                tf_checksum=tf_checksum,
                tf_dunder=tf_dunder,
                scratch=scratch,
            )
        )
        if isinstance(result_checksum, str):
            return tf_checksum_hex, None, None, result_checksum
        result_checksum = Checksum(result_checksum)

        result_checksum_hex = result_checksum.hex()

        result_buffer: Buffer | None
        try:
            result_buffer = _run_on_worker_loop(
                lambda: _resolve_buffer_async(result_checksum)
            )
            if not isinstance(result_buffer, Buffer):
                result_buffer = None
        except Exception:
            result_buffer = None
        if require_value and result_buffer is None:
            return (
                tf_checksum_hex,
                result_checksum_hex,
                None,
                "Result value unavailable",
            )

        try:
            tf_checksum_obj = Checksum(tf_checksum_hex)
            _run_on_worker_loop(
                lambda: _promise_and_write_result_async(
                    tf_checksum_obj, result_checksum
                )
            )
        except Exception:
            pass

        return tf_checksum_hex, result_checksum_hex, result_buffer, None
    except Exception:
        return tf_checksum_hex, None, None, traceback.format_exc()


def _run_fat(
    base_result: Tuple[str | None, str | None, Buffer | None, str | None],
) -> Tuple[str | None, Buffer | None, str | None]:
    """Worker task that ensures a buffer is available for dependents."""
    tf_checksum_hex, result_checksum_hex, result_buffer, exc = base_result
    if exc:
        return result_checksum_hex, result_buffer, exc
    if result_buffer is not None:
        return result_checksum_hex, result_buffer, None
    if result_checksum_hex is None:
        return None, None, "Result checksum unavailable"
    return _fat_checksum_task(result_checksum_hex)


def _run_thin(
    base_result: Tuple[str | None, str | None, Buffer | None, str | None],
) -> Tuple[str | None, str | None, str | None]:
    """Worker task that only needs the checksums."""
    tf_checksum_hex, result_checksum_hex, _result_buffer, exc = base_result
    return tf_checksum_hex, result_checksum_hex, exc


class SeamlessDaskClient:
    """Coordinate Seamless transformations running on a Dask cluster."""

    def __init__(
        self,
        client: Client,
        *,
        fat_future_ttl: float = 10.0,
        worker_plugin_workers: int = 3,
        remote_clients: dict | None = None,
        is_local_cluster: bool | None = None,
        interactive: bool = False,
    ) -> None:
        self._client = client
        self._fat_future_ttl = fat_future_ttl
        self._fat_checksum_cache: dict[str, tuple[Future, float]] = {}
        self._transformation_cache: dict[str, tuple[TransformationFutures, float]] = {}
        self._is_local_cluster = bool(is_local_cluster)
        self._interactive = bool(interactive)
        self._promised_targets: dict[str, set[str]] = {}
        ensure_configured(workers=worker_plugin_workers)
        self._register_worker_plugin(worker_plugin_workers, remote_clients)

        self._client.submit(lambda: 42)
        print("Wait for workers to connect...")
        for _ in range(20):
            if self._client.scheduler_info().get("workers"):
                break
            time.sleep(1)

        self._warn_if_no_workers()

    # --- public API -----------------------------------------------------
    @property
    def client(self) -> Client:
        return self._client

    def get_fat_checksum_future(self, checksum: Checksum | str) -> Future:
        """Return (or build) a fat-checksum future for the checksum."""
        self._prune_caches()
        checksum_hex = (
            checksum.hex() if isinstance(checksum, Checksum) else str(checksum)
        )
        self._ensure_promised(checksum_hex)
        cached = self._fat_checksum_cache.get(checksum_hex)
        if cached is not None and not cached[0].cancelled():
            self._fat_checksum_cache[checksum_hex] = (cached[0], self._expiry())
            return cached[0]

        future = self._client.submit(
            _fat_checksum_task,
            checksum_hex,
            pure=False,
            key="fat_checksum-" + checksum_hex,
            resources={"S": 1},
            priority=-1,
        )
        self._fat_checksum_cache[checksum_hex] = (future, self._expiry())
        return future

    def submit_transformation(
        self, submission: TransformationSubmission, *, need_fat: bool = False
    ) -> TransformationFutures:
        """Submit a transformation to Dask and return its futures."""
        self._prune_caches()
        tf_checksum_hex = submission.tf_checksum

        if tf_checksum_hex:
            cached = self._transformation_cache.get(tf_checksum_hex)
            if cached is not None and not cached[0].base.cancelled():
                self._transformation_cache[tf_checksum_hex] = (
                    cached[0],
                    self._expiry(),
                )
                return cached[0]

        payload = {
            "transformation_dict": submission.transformation_dict,
            "inputs": [spec.__dict__ for spec in submission.inputs.values()],
            "tf_checksum": submission.tf_checksum,
            "tf_dunder": dict(submission.tf_dunder),
            "scratch": submission.scratch,
            "require_value": submission.require_value,
        }
        input_futures = dict(submission.input_futures)
        resource_string = None  # TODO: get from tf_dunder
        base_key = self._build_key("base", resource_string, tf_checksum_hex)
        thin_key = self._build_key("thin", resource_string, tf_checksum_hex)

        base_future = self._client.submit(
            _run_base,
            payload,
            input_futures,
            pure=False,
            key=base_key,
            resources={"S": 1},
            priority=10,
            retries=3,
        )
        thin_future = self._client.submit(
            _run_thin,
            base_future,
            pure=False,
            key=thin_key,
            resources={"S": 1},
            priority=20,
        )

        fat_future = None
        if need_fat:
            fat_key = self._build_key("fat", resource_string, tf_checksum_hex)
            fat_future = self._client.submit(
                _run_fat,
                base_future,
                pure=False,
                key=fat_key,
                resources={"S": 1},
                priority=-1,
            )

        futures = TransformationFutures(
            base=base_future,
            fat=fat_future,
            thin=thin_future,
            tf_checksum=tf_checksum_hex,
        )

        thin_future.add_done_callback(
            lambda fut: self._register_transformation(tf_checksum_hex, futures, fut)
        )
        if tf_checksum_hex:
            self._store_transformation(tf_checksum_hex, futures)

        return futures

    def ensure_fat_future(self, futures: TransformationFutures) -> Future:
        """Create or return the fat future for a transformation."""
        if futures.fat is not None and not futures.fat.cancelled():
            return futures.fat
        tf_hex = futures.tf_checksum
        resource_string = None  # TODO: extract from tf_dunder
        fat_key = self._build_key("fat", resource_string, tf_hex)
        futures.fat = self._client.submit(
            _run_fat,
            futures.base,
            pure=False,
            key=fat_key,
            resources={"S": 1},
            priority=-1,
        )
        if futures.result_checksum:
            self._fat_checksum_cache[futures.result_checksum] = (
                futures.fat,
                self._expiry(),
            )
        if tf_hex:
            self._store_transformation(tf_hex, futures)
        return futures.fat

    # --- internals ------------------------------------------------------
    def _register_worker_plugin(
        self, worker_count: int, remote_clients: dict | None
    ) -> None:
        try:
            # Avoid re-registering if scheduler already has the plugin.
            already_registered = self._client.run_on_scheduler(
                lambda dask_scheduler: "seamless-worker-setup"
                in getattr(dask_scheduler, "worker_plugins", {})
            )
            if already_registered:
                return
            plugin = SeamlessWorkerPlugin(
                num_workers=worker_count, remote_clients=remote_clients
            )
            self._client.register_plugin(plugin, name="seamless-worker-setup")
        except Exception:
            # Best-effort: worker might already have the plugin registered or scheduler unreachable.
            pass

    def _warn_if_no_workers(self) -> None:
        """Emit a warning if the scheduler reports zero connected workers."""

        if not (self._is_local_cluster or self._interactive):
            return
        try:
            info = self._client.scheduler_info()
            if info.get("workers"):
                return
        except Exception:
            return

        print("[seamless-dask] Scheduler reports 0 workers attached", file=sys.stderr)

    def _ensure_promised(self, checksum_hex: str) -> None:
        """Best-effort: make sure write servers have been promised this checksum."""

        has_local_buffer = False
        try:
            from seamless.caching.buffer_cache import get_buffer_cache

            cache = get_buffer_cache()
            cs_obj = Checksum(checksum_hex)
            with cache.lock:
                if cs_obj in cache.strong_cache or cs_obj in cache.weak_cache:
                    has_local_buffer = True
        except Exception:
            pass
        if not has_local_buffer:
            return

        try:
            from seamless_remote import buffer_remote
        except Exception:
            return

        clients = getattr(buffer_remote, "_write_server_clients", []) or []
        for client in clients:
            target = getattr(client, "url", None) or getattr(client, "directory", None)
            if target is None:
                target = f"client-{id(client)}"
            promised = self._promised_targets.setdefault(target, set())
            if checksum_hex in promised:
                continue
            try:
                _run_coro_blocking(client.promise(Checksum(checksum_hex)))
                promised.add(checksum_hex)
            except Exception:
                continue

    def _build_key(
        self, prefix: str, resource_string: str | None, checksum_hex: str | None
    ) -> str:
        if checksum_hex is None:
            checksum_hex = uuid.uuid4().hex
        if not resource_string:
            return f"{prefix}-{checksum_hex}"
        else:
            return f"{prefix}-{resource_string}-{checksum_hex}"

    def _expiry(self) -> float:
        return time.monotonic() + self._fat_future_ttl

    def _prune_caches(self) -> None:
        now = time.monotonic()
        for cache in (self._fat_checksum_cache, self._transformation_cache):
            for key in list(cache.keys()):
                _, expiry = cache[key]
                if expiry < now:
                    cache.pop(key, None)

    def _store_transformation(
        self, tf_checksum_hex: str, futures: TransformationFutures
    ) -> None:
        self._transformation_cache[tf_checksum_hex] = (futures, self._expiry())

    def _register_transformation(
        self,
        tf_checksum_hint: str | None,
        futures: TransformationFutures,
        thin_future: Future,
    ) -> None:
        try:
            tf_checksum_hex, result_checksum_hex, exc = thin_future.result()
        except Exception:
            return

        if tf_checksum_hex:
            futures.tf_checksum = tf_checksum_hex
            self._store_transformation(tf_checksum_hex, futures)
        if result_checksum_hex:
            futures.result_checksum = result_checksum_hex
            if futures.fat is not None:
                if result_checksum_hex not in self._fat_checksum_cache:
                    self._fat_checksum_cache[result_checksum_hex] = (
                        futures.fat,
                        self._expiry(),
                    )


__all__ = ["SeamlessDaskClient", "_fat_checksum_task"]


def _get_worker_loop() -> asyncio.AbstractEventLoop | None:
    try:
        worker = get_worker()
    except ValueError:
        return None
    return getattr(worker, "seamless_loop", None)
