"""Seamless Dask client that drives transformation execution."""

from __future__ import annotations

import time
import traceback
import uuid
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

import dask.config
from distributed import Client, Future

from seamless import Buffer, Checksum
from seamless_transformer.transformation_cache import run_sync
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


def _fat_checksum_task(checksum_hex: str) -> Tuple[str, Buffer | None, str | None]:
    """Resolve a checksum into a buffer on a worker."""
    try:
        checksum = Checksum(checksum_hex)
        buffer_obj = checksum.resolve()
        if isinstance(buffer_obj, Buffer):
            buffer_obj.tempref()
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

        result_checksum = run_sync(
            transformation_dict,
            tf_checksum=tf_checksum,
            tf_dunder=tf_dunder,
            scratch=scratch,
            require_fingertip=require_value,
        )
        result_checksum_hex = Checksum(result_checksum).hex()

        result_buffer: Buffer | None
        try:
            result_buffer = result_checksum.resolve()
            if not isinstance(result_buffer, Buffer):
                result_buffer = None
        except Exception:
            result_buffer = None

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
    ) -> None:
        self._client = client
        self._fat_future_ttl = fat_future_ttl
        self._fat_checksum_cache: dict[str, tuple[Future, float]] = {}
        self._transformation_cache: dict[str, tuple[TransformationFutures, float]] = {}
        ensure_configured(workers=worker_plugin_workers)
        self._register_worker_plugin(worker_plugin_workers)

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
        cached = self._fat_checksum_cache.get(checksum_hex)
        if cached is not None and not cached[0].cancelled():
            self._fat_checksum_cache[checksum_hex] = (cached[0], self._expiry())
            return cached[0]

        future = self._client.submit(
            _fat_checksum_task,
            checksum_hex,
            pure=False,
            key="fat-checksum-" + checksum_hex,
            resources={"S": 1},
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
        )
        thin_future = self._client.submit(
            _run_thin, base_future, pure=False, key=thin_key, resources={"S": 1}
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
            _run_fat, futures.base, pure=False, key=fat_key, resources={"S": 1}
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
    def _register_worker_plugin(self, worker_count: int) -> None:
        try:
            plugin = SeamlessWorkerPlugin(num_workers=worker_count)
            self._client.register_plugin(plugin, name="seamless-worker-spawn")
        except Exception:
            # Best-effort: worker might already have the plugin registered.
            pass

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
