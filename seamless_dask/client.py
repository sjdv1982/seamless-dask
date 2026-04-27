"""Seamless Dask client that drives transformation execution."""

from __future__ import annotations

import os
import logging
import time
import traceback
import uuid
import asyncio
import sys
import threading
from datetime import datetime, timezone
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

_SEAMLESS_DASK_CLIENT_LOCK = threading.Lock()
_LOGGER = logging.getLogger(__name__)
_BASE_DASK_PRIORITY = 10
_ENSURE_RESULT_UPLOAD_ENV = "SEAMLESS_DASK_ENSURE_RESULT_UPLOAD"
_QUEUE_EXCLUSIVE_ENV = "SEAMLESS_DASK_QUEUE_EXCLUSIVE"


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    return default


def _should_ensure_result_upload() -> bool:
    # Manual override has priority when explicitly set.
    if _ENSURE_RESULT_UPLOAD_ENV in os.environ:
        return _parse_bool_env(_ENSURE_RESULT_UPLOAD_ENV, default=False)
    # Default policy: enforce on exclusive-queue workers only.
    return _parse_bool_env(_QUEUE_EXCLUSIVE_ENV, default=False)


_ENSURE_RESULT_UPLOAD = _should_ensure_result_upload()


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _scheduler_has_task_key(dask_scheduler, key: str) -> bool:
    try:
        return key in getattr(dask_scheduler, "tasks", {})
    except Exception:
        return False


def _dask_key_exists(dask_client: Client, key: str) -> bool:
    try:
        return bool(dask_client.run_on_scheduler(_scheduler_has_task_key, key=key))
    except Exception:
        pass
    try:
        get_future = getattr(dask_client, "get_future", None)
        if callable(get_future):
            try:
                future = get_future(key)
            except Exception:
                return False
            return future is not None
    except Exception:
        return False
    return False


def _resolve_driver_key(dask_client: Client, base_key: str) -> str:
    if not _dask_key_exists(dask_client, base_key):
        return base_key
    counter = 1
    while True:
        suffix = "-copy" if counter == 1 else f"-{counter}-copy"
        candidate = base_key + suffix
        if not _dask_key_exists(dask_client, candidate):
            return candidate
        counter += 1


def _driver_from_meta(meta: Any) -> bool:
    return isinstance(meta, dict) and bool(meta.get("driver"))


def _is_driver_submission(submission: "TransformationSubmission") -> bool:
    if _driver_from_meta(submission.meta):
        return True
    tf_dunder = submission.tf_dunder
    if isinstance(tf_dunder, dict) and _driver_from_meta(tf_dunder.get("__meta__")):
        return True
    if _driver_from_meta(submission.transformation_dict.get("__meta__")):
        return True
    return False


def _is_driver_payload(payload: Dict[str, Any]) -> bool:
    tf_dunder = payload.get("tf_dunder")
    if isinstance(tf_dunder, dict) and _driver_from_meta(tf_dunder.get("__meta__")):
        return True
    transformation_dict = payload.get("transformation_dict")
    if isinstance(transformation_dict, dict) and _driver_from_meta(
        transformation_dict.get("__meta__")
    ):
        return True
    return False


def _base_prefix_for_transformation(transformation_dict: Mapping[str, Any]) -> str:
    base_prefix = "base"
    ccs = transformation_dict.get("__code_checksum__")
    if ccs is not None:
        base_prefix += "_" + str(ccs)
    return base_prefix


def _output_celltype_from_transformation(
    transformation_dict: Mapping[str, Any] | None,
) -> str | None:
    if not isinstance(transformation_dict, Mapping):
        return None
    output = transformation_dict.get("__output__")
    if not isinstance(output, (list, tuple)) or len(output) < 2:
        return None
    try:
        if len(output) == 3:
            _output_name, output_celltype, _subcelltype = output
        else:
            _output_name, output_celltype, _subcelltype, hash_pattern = output[:4]
            if hash_pattern == {"*": "#"}:
                output_celltype = "deepcell"
            elif hash_pattern == {"*": "##"}:
                output_celltype = "deepfolder"
    except Exception:
        return None
    return output_celltype if isinstance(output_celltype, str) else None


def _parse_cache_prune_interval() -> float:
    raw = os.environ.get("SEAMLESS_DASK_CACHE_PRUNE_INTERVAL", "10")
    try:
        value = float(raw)
    except Exception:
        return 10.0
    return max(0.0, value)


def _describe_dask_client(dask_client: Client | None) -> str:
    if dask_client is None:
        return "dask_client=None"
    scheduler_addr = None
    client_id = None
    status = None
    try:
        client_id = getattr(dask_client, "id", None)
    except Exception:
        client_id = None
    try:
        status = getattr(dask_client, "status", None)
    except Exception:
        status = None
    try:
        scheduler = getattr(dask_client, "scheduler", None)
        scheduler_addr = getattr(scheduler, "address", None)
    except Exception:
        scheduler_addr = None
    return (
        "dask_client_id="
        + str(id(dask_client))
        + " client_id="
        + str(client_id)
        + " status="
        + str(status)
        + " scheduler="
        + str(scheduler_addr)
    )


def _resolve_scheduler_address() -> str | None:
    addr = os.environ.get("SEAMLESS_DASK_SCHEDULER")
    if addr:
        return addr
    try:
        worker = get_worker()
    except Exception:
        return None
    addr = getattr(worker, "scheduler_address", None)
    if isinstance(addr, str) and addr:
        return addr
    scheduler = getattr(worker, "scheduler", None)
    addr = getattr(scheduler, "address", None) if scheduler is not None else None
    if isinstance(addr, str) and addr:
        return addr
    return None


def _noop_42() -> int:
    return 42


def _has_worker_plugin(dask_scheduler) -> bool:
    return "seamless-worker-setup" in getattr(dask_scheduler, "worker_plugins", {})


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


async def _fingertip_buffer_async(checksum: Checksum) -> Buffer | None:
    buffer_obj = await checksum.fingertip()
    if isinstance(buffer_obj, Buffer):
        buffer_obj.tempref(scratch=True)
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
    tf_checksum: Checksum,
    result_checksum: Checksum,
    *,
    write_buffer: bool,
    output_celltype: str | None = None,
    result_buffer: Buffer | None = None,
    transformation_dict: Mapping[str, Any] | None = None,
    tf_dunder: Mapping[str, Any] | None = None,
    scratch: bool = False,
    execution: str = "remote",
    started_at: str | None = None,
    finished_at: str | None = None,
    wall_time_seconds: float | None = None,
    cpu_user_seconds: float | None = None,
    cpu_system_seconds: float | None = None,
    gpu_memory_peak_bytes: int | None = None,
) -> None:
    """Persist a transformation result after ensuring buffer availability."""
    try:
        from seamless_remote import buffer_remote, database_remote
    except Exception:
        return
    try:
        from seamless_config.select import get_record
    except Exception:
        def get_record():
            return False

    async def _ensure_uploaded(
        checksum: Checksum, *, preferred_buffer: Buffer | None = None
    ) -> bool:
        # First wait for any in-flight background writer task for this checksum.
        try:
            from seamless.caching import buffer_writer
        except Exception:
            buffer_writer = None
        if buffer_writer is not None:
            try:
                existing = await buffer_writer.await_existing_task(checksum)
            except Exception:
                existing = None
            if existing is not None:
                return bool(existing)

        # Fallback: do a direct upload from local cache/content if available.
        buffer_obj: Buffer | None = preferred_buffer
        if not isinstance(buffer_obj, Buffer):
            try:
                from seamless.caching.buffer_cache import get_buffer_cache

                buffer_obj = get_buffer_cache().get(checksum)
            except Exception:
                buffer_obj = None
        if not isinstance(buffer_obj, Buffer):
            try:
                resolved = await checksum.resolution()
            except Exception:
                resolved = None
            if isinstance(resolved, Buffer):
                buffer_obj = resolved
        if not isinstance(buffer_obj, Buffer):
            return False
        try:
            wrote = await buffer_remote.write_buffer(checksum, buffer_obj)
        except Exception:
            return False
        return bool(wrote)

    def _collect_deep_member_checksums(buffer_obj: Buffer) -> list[Checksum]:
        try:
            structure = buffer_obj.get_value("plain")
        except Exception:
            return []
        seen: dict[str, Checksum] = {}

        def _walk(value: Any) -> None:
            if isinstance(value, dict):
                for child in value.values():
                    _walk(child)
                return
            if isinstance(value, list):
                for child in value:
                    _walk(child)
                return
            if not isinstance(value, str):
                return
            try:
                checksum = Checksum(value)
            except Exception:
                return
            if checksum:
                seen[checksum.hex()] = checksum

        _walk(structure)
        return list(seen.values())

    try:
        if write_buffer:
            await buffer_remote.promise(result_checksum)
            if _ENSURE_RESULT_UPLOAD:
                root_buffer = result_buffer
                uploaded = await _ensure_uploaded(
                    result_checksum, preferred_buffer=root_buffer
                )
                if not uploaded:
                    _LOGGER.warning(
                        "[seamless-dask] result buffer upload failed checksum=%s",
                        result_checksum.hex(),
                    )
                    return
                if output_celltype in {"deepcell", "deepfolder"}:
                    if not isinstance(root_buffer, Buffer):
                        try:
                            resolved = await result_checksum.resolution()
                        except Exception:
                            resolved = None
                        root_buffer = resolved if isinstance(resolved, Buffer) else None
                    if isinstance(root_buffer, Buffer):
                        member_checksums = _collect_deep_member_checksums(root_buffer)
                        for member_checksum in member_checksums:
                            if member_checksum == result_checksum:
                                continue
                            try:
                                await buffer_remote.promise(member_checksum)
                            except Exception:
                                pass
                            uploaded = await _ensure_uploaded(member_checksum)
                            if not uploaded:
                                _LOGGER.warning(
                                    "[seamless-dask] deep member upload failed root=%s member=%s",
                                    result_checksum.hex(),
                                    member_checksum.hex(),
                                )
                                return
        await database_remote.set_transformation_result(tf_checksum, result_checksum)
        if get_record():
            try:
                from seamless_transformer.transformation_cache import (
                    _memory_peak_bytes,
                    build_compilation_context_checksum,
                    build_execution_record,
                    build_validation_snapshot_checksum,
                    collect_compilation_runtime_metadata,
                    collect_job_validation,
                    compute_record_io_bytes,
                    load_bucket_contract_violations,
                    _normalize_job_validation_payload,
                )
                from seamless_transformer.probe_index import (
                    ensure_record_bucket_preconditions,
                    is_record_probe,
                )

                if not is_record_probe(
                    dict(transformation_dict or {}),
                    dict(tf_dunder or {}),
                ):
                    probe_context = await ensure_record_bucket_preconditions(
                        dict(transformation_dict or {}),
                        dict(tf_dunder or {}),
                        execution=execution,
                    )
                    bucket_contract_violations = (
                        await load_bucket_contract_violations(probe_context)
                    )
                    compilation_context = await build_compilation_context_checksum(
                        dict(transformation_dict or {}),
                        dict(tf_dunder or {}),
                    )
                    job_validation = _normalize_job_validation_payload(
                        await collect_job_validation(
                            dict(transformation_dict or {}),
                            dict(tf_dunder or {}),
                            compilation_context=compilation_context,
                        )
                    )
                    job_contract_violations = job_validation[
                        "job_contract_violations"
                    ]
                    input_total_bytes, output_total_bytes = (
                        await compute_record_io_bytes(
                            dict(transformation_dict or {}), result_checksum
                        )
                    )
                    validation_snapshot = await build_validation_snapshot_checksum(
                        dict(transformation_dict or {}),
                        dict(tf_dunder or {}),
                        execution=execution,
                        probe_context=probe_context,
                        compilation_context=compilation_context,
                        bucket_contract_violations=bucket_contract_violations,
                        job_contract_violations=job_contract_violations,
                        job_validation_diagnostics=job_validation["diagnostics"],
                    )
                    record_runtime_metadata = {
                        "memory_peak_bytes": _memory_peak_bytes()
                    }
                    if gpu_memory_peak_bytes is not None:
                        record_runtime_metadata["gpu_memory_peak_bytes"] = (
                            gpu_memory_peak_bytes
                        )
                    record_runtime_metadata.update(
                        await collect_compilation_runtime_metadata(
                            dict(transformation_dict or {}),
                            dict(tf_dunder or {}),
                        )
                    )
                    record = build_execution_record(
                        dict(transformation_dict or {}),
                        tf_checksum=tf_checksum,
                        result_checksum=result_checksum,
                        tf_dunder=dict(tf_dunder or {}),
                        execution=execution,
                        started_at=started_at or _utcnow_iso(),
                        finished_at=finished_at or _utcnow_iso(),
                        wall_time_seconds=wall_time_seconds or 0.0,
                        cpu_user_seconds=cpu_user_seconds or 0.0,
                        cpu_system_seconds=cpu_system_seconds or 0.0,
                        probe_context=probe_context,
                        bucket_contract_violations=bucket_contract_violations,
                        job_contract_violations=job_contract_violations,
                        compilation_context=compilation_context,
                        validation_snapshot=validation_snapshot,
                        runtime_metadata=record_runtime_metadata,
                    )
                    record["input_total_bytes"] = input_total_bytes
                    record["output_total_bytes"] = output_total_bytes
                    record["execution_envelope"]["scratch"] = bool(scratch)
                    await database_remote.set_execution_record(
                        tf_checksum, result_checksum, record
                    )
            except Exception:
                return
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


def _fat_finger_checksum_task(
    checksum_hex: str,
) -> Tuple[str, Buffer | None, str | None]:
    """Resolve or fingertip a checksum into a buffer on a worker."""
    try:
        checksum = Checksum(checksum_hex)
        buffer_obj = _run_on_worker_loop(lambda: _fingertip_buffer_async(checksum))
        return (
            checksum.hex(),
            buffer_obj if isinstance(buffer_obj, Buffer) else None,
            None,
        )
    except Exception:
        return checksum_hex, None, traceback.format_exc()


def _run_base(
    payload: Dict[str, Any],
    input_results: Mapping[str, Tuple[Any, ...]],
    priority: int | None = None,
) -> Tuple[str | None, str | None, Buffer | None, str | None]:
    """Worker task that performs the transformation itself."""

    from seamless_dask.client import SeamlessDaskClient
    from seamless_dask.transformer_client import (
        get_seamless_dask_client,
        set_seamless_dask_client,
    )
    import distributed

    try:
        priority_value = int(priority) if priority is not None else None
    except Exception:
        priority_value = None
    if priority_value is not None and payload.get("owner_dask_priority") is None:
        payload = dict(payload)
        payload["owner_dask_priority"] = priority_value

    owner_dask_key = payload.get("owner_dask_key")
    if not isinstance(owner_dask_key, str) or not owner_dask_key:
        try:
            from distributed.worker import thread_state as dask_thread_state

            owner_dask_key = getattr(dask_thread_state, "key", None)
        except Exception:
            owner_dask_key = None

    client = get_seamless_dask_client()
    if client is None:
        # Avoid racing multiple base tasks creating/replacing the global client.
        with _SEAMLESS_DASK_CLIENT_LOCK:
            client = get_seamless_dask_client()
            if client is None:
                scheduler_address = _resolve_scheduler_address()
                if scheduler_address:
                    _LOGGER.info(
                        "[seamless-dask] _run_base creating client pid=%s thread=%s scheduler=%s",
                        os.getpid(),
                        threading.current_thread().name,
                        scheduler_address,
                    )
                    dask_client = Client(
                        scheduler_address, timeout="10s", set_as_default=False
                    )
                else:
                    _LOGGER.warning(
                        "[seamless-dask] _run_base fallback to distributed.get_client pid=%s thread=%s",
                        os.getpid(),
                        threading.current_thread().name,
                    )
                    dask_client = distributed.get_client()
                    assert dask_client is not None
                client = SeamlessDaskClient(dask_client)
                set_seamless_dask_client(client)

    tf_checksum_hex = payload.get("tf_checksum")
    tf_checksum_missing = tf_checksum_hex is None
    transformation_dict = dict(payload.get("transformation_dict") or {})
    inputs = payload.get("inputs") or []
    scratch = bool(payload.get("scratch", False))
    owner_dask_priority = payload.get("owner_dask_priority")
    try:
        owner_dask_priority = (
            int(owner_dask_priority) if owner_dask_priority is not None else None
        )
    except Exception:
        owner_dask_priority = None
    tf_dunder = payload.get("tf_dunder", {}) or {}
    require_value = bool(payload.get("require_value", False))
    started_at = _utcnow_iso()
    wall_start = time.perf_counter()
    cpu_start = os.times()
    gpu_memory_peak_bytes = None

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
        if tf_checksum_missing:
            # Redirect tasks submitted without a checksum to the deterministic key.
            base_prefix = _base_prefix_for_transformation(transformation_dict)
            base_key = client._build_key(base_prefix, None, tf_checksum_hex)
            if _is_driver_payload(payload):
                base_key = _resolve_driver_key(client.client, base_key)
            dedupe_payload = dict(payload)
            dedupe_payload["tf_checksum"] = tf_checksum_hex
            dedupe_payload["owner_dask_key"] = base_key
            try:
                from distributed import worker_client
            except Exception:
                worker_client = None
            if worker_client is None:
                future = client.client.submit(
                    _run_base,
                    dedupe_payload,
                    input_results,
                    priority_value,
                    pure=False,
                    key=base_key,
                    resources={"S": 1},
                    priority=priority_value or _BASE_DASK_PRIORITY,
                    retries=3,
                )
                return future.result()
            with worker_client() as wc:
                future = wc.submit(
                    _run_base,
                    dedupe_payload,
                    input_results,
                    priority_value,
                    pure=False,
                    key=base_key,
                    resources={"S": 1},
                    priority=priority_value or _BASE_DASK_PRIORITY,
                    retries=3,
                )
                return future.result()
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
                    return tf_checksum_hex, result_checksum_hex, result_buffer, None
            else:
                return tf_checksum_hex, result_checksum_hex, None, None

        from seamless_transformer.transformation_cache import (
            start_gpu_memory_sampler,
            stop_gpu_memory_sampler,
        )

        gpu_sampler = start_gpu_memory_sampler()
        try:
            result_checksum = _run_on_worker_loop(
                lambda: transformer_worker.dispatch_to_workers(
                    transformation_dict,
                    tf_checksum=tf_checksum,
                    tf_dunder=tf_dunder,
                    scratch=scratch,
                    owner_dask_key=owner_dask_key,
                    owner_dask_priority=owner_dask_priority,
                )
            )
        finally:
            gpu_memory_peak_bytes = stop_gpu_memory_sampler(gpu_sampler)
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
            output_celltype = _output_celltype_from_transformation(transformation_dict)
            finished_at = _utcnow_iso()
            wall_time_seconds = round(time.perf_counter() - wall_start, 6)
            cpu_end = os.times()
            cpu_user_seconds = round(cpu_end.user - cpu_start.user, 6)
            cpu_system_seconds = round(cpu_end.system - cpu_start.system, 6)
            _run_on_worker_loop(
                lambda: _promise_and_write_result_async(
                    tf_checksum_obj,
                    result_checksum,
                    write_buffer=not scratch,
                    output_celltype=output_celltype,
                    result_buffer=result_buffer,
                    transformation_dict=transformation_dict,
                    tf_dunder=tf_dunder,
                    scratch=scratch,
                    execution="remote",
                    started_at=started_at,
                    finished_at=finished_at,
                    wall_time_seconds=wall_time_seconds,
                    cpu_user_seconds=cpu_user_seconds,
                    cpu_system_seconds=cpu_system_seconds,
                    gpu_memory_peak_bytes=gpu_memory_peak_bytes,
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


def _run_fat_finger(
    base_result: Tuple[str | None, str | None, Buffer | None, str | None],
) -> Tuple[str | None, Buffer | None, str | None]:
    """Worker task that allows fingering inputs when buffers are missing."""
    tf_checksum_hex, result_checksum_hex, result_buffer, exc = base_result
    if exc:
        return result_checksum_hex, result_buffer, exc
    if result_buffer is not None:
        return result_checksum_hex, result_buffer, None
    if result_checksum_hex is None:
        return None, None, "Result checksum unavailable"
    checksum_hex, buffer_obj, err = _fat_checksum_task(result_checksum_hex)
    if buffer_obj is not None and err is None:
        return checksum_hex, buffer_obj, None
    return _fat_finger_checksum_task(result_checksum_hex)


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
        self._fat_checksum_cache: dict[str, tuple[Future, float | None]] = {}
        self._fat_finger_checksum_cache: dict[str, tuple[Future, float | None]] = {}
        self._transformation_cache: dict[
            str, tuple[TransformationFutures, float | None]
        ] = {}
        self._cache_lock = threading.RLock()
        self._prune_stop = threading.Event()
        self._prune_interval = _parse_cache_prune_interval()
        self._prune_thread: threading.Thread | None = None
        self._is_local_cluster = bool(is_local_cluster)
        self._interactive = bool(interactive)
        self._promised_targets: dict[str, set[str]] = {}
        ensure_configured(workers=worker_plugin_workers)
        self._register_worker_plugin(worker_plugin_workers, remote_clients)
        _LOGGER.info(
            "[seamless-dask] SeamlessDaskClient created pid=%s thread=%s %s",
            os.getpid(),
            threading.current_thread().name,
            _describe_dask_client(self._client),
        )

        self._client.submit(_noop_42)
        if self._interactive:
            print("Wait for workers to connect...")
            for _ in range(20):
                if self._client.scheduler_info().get("workers"):
                    break
                time.sleep(1)

        self._warn_if_no_workers()
        self._start_prune_thread()

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
        with self._cache_lock:
            cached = self._fat_checksum_cache.get(checksum_hex)
        if cached is not None and not cached[0].cancelled():
            self._touch_fat_checksum_cache(checksum_hex, cached[0])
            return cached[0]

        future = self._client.submit(
            _fat_checksum_task,
            checksum_hex,
            pure=False,
            key="fat_checksum-" + checksum_hex,
            priority=-1,
        )
        self._cache_fat_checksum_future(checksum_hex, future)
        return future

    def get_fat_finger_checksum_future(self, checksum: Checksum | str) -> Future:
        """Return (or build) a fat-finger-checksum future for the checksum."""
        self._prune_caches()
        checksum_hex = (
            checksum.hex() if isinstance(checksum, Checksum) else str(checksum)
        )
        self._ensure_promised(checksum_hex)
        with self._cache_lock:
            cached = self._fat_finger_checksum_cache.get(checksum_hex)
        if cached is not None and not cached[0].cancelled():
            self._touch_fat_finger_checksum_cache(checksum_hex, cached[0])
            return cached[0]

        future = self._client.submit(
            _fat_finger_checksum_task,
            checksum_hex,
            pure=False,
            key="fat_finger_checksum-" + checksum_hex,
            priority=-1,
        )
        self._cache_fat_finger_checksum_future(checksum_hex, future)
        return future

    def get_transformation_futures(
        self, tf_checksum: Checksum | str
    ) -> TransformationFutures | None:
        """Return cached transformation futures if they exist."""
        tf_checksum_hex = (
            tf_checksum.hex() if isinstance(tf_checksum, Checksum) else str(tf_checksum)
        )
        with self._cache_lock:
            cached = self._transformation_cache.get(tf_checksum_hex)
        if cached is None:
            return None
        futures = cached[0]
        if futures.base is not None and futures.base.cancelled():
            return None
        return futures

    def submit_transformation(
        self,
        submission: TransformationSubmission,
        *,
        need_fat: bool = False,
        priority_boost: int = 0,
    ) -> TransformationFutures:
        """Submit a transformation to Dask and return its futures."""
        self._prune_caches()
        tf_checksum_hex = submission.tf_checksum
        is_driver = _is_driver_submission(submission)
        try:
            boost = int(priority_boost)
        except Exception:
            boost = 0
        base_priority = _BASE_DASK_PRIORITY + boost
        thin_priority = _BASE_DASK_PRIORITY + 10 + boost

        if tf_checksum_hex and not is_driver:
            with self._cache_lock:
                cached = self._transformation_cache.get(tf_checksum_hex)
            if cached is not None and not cached[0].base.cancelled():
                self._touch_transformation_cache(tf_checksum_hex, cached[0])
                return cached[0]

        payload = {
            "transformation_dict": submission.transformation_dict,
            "inputs": [spec.__dict__ for spec in submission.inputs.values()],
            "tf_checksum": submission.tf_checksum,
            "tf_dunder": dict(submission.tf_dunder),
            "scratch": submission.scratch,
            "require_value": submission.require_value,
            "owner_dask_priority": base_priority,
        }
        input_futures = dict(submission.input_futures)
        resource_string = None  # TODO: get from tf_dunder
        base_prefix = _base_prefix_for_transformation(submission.transformation_dict)
        base_key = self._build_key(base_prefix, resource_string, tf_checksum_hex)
        thin_key = self._build_key("thin", resource_string, tf_checksum_hex)

        key_suffix = ""
        if is_driver:
            base_key_original = base_key
            base_key = _resolve_driver_key(self._client, base_key)
            if base_key != base_key_original:
                key_suffix = base_key[len(base_key_original) :]
                thin_key += key_suffix

        payload["owner_dask_key"] = base_key

        base_future = self._client.submit(
            _run_base,
            payload,
            input_futures,
            base_priority,
            pure=False,
            key=base_key,
            resources={"S": 1},
            priority=base_priority,
            retries=3,
        )
        thin_future = self._client.submit(
            _run_thin,
            base_future,
            pure=False,
            key=thin_key,
            priority=thin_priority,
        )

        fat_future = None
        if need_fat:
            fat_key = self._build_key("fat", resource_string, tf_checksum_hex)
            if key_suffix:
                fat_key += key_suffix
            fat_future = self._client.submit(
                _run_fat,
                base_future,
                pure=False,
                key=fat_key,
                priority=-1,
            )

        futures = TransformationFutures(
            base=base_future,
            fat=fat_future,
            thin=thin_future,
            tf_checksum=tf_checksum_hex,
        )

        thin_future.add_done_callback(
            lambda fut, store_cache=not is_driver: self._register_transformation(
                tf_checksum_hex, futures, fut, store_cache=store_cache
            )
        )
        if tf_checksum_hex and not is_driver:
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
            priority=-1,
        )
        if futures.result_checksum:
            self._cache_fat_checksum_future(futures.result_checksum, futures.fat)
        if tf_hex:
            self._store_transformation(tf_hex, futures)
        return futures.fat

    def ensure_fat_finger_future(self, futures: TransformationFutures) -> Future:
        """Create or return the fat-finger future for a transformation."""
        if futures.fat is not None and not futures.fat.cancelled():
            return futures.fat
        tf_hex = futures.tf_checksum
        resource_string = None  # TODO: extract from tf_dunder
        fat_key = self._build_key("fat-finger", resource_string, tf_hex)
        futures.fat = self._client.submit(
            _run_fat_finger,
            futures.base,
            pure=False,
            key=fat_key,
            priority=-1,
        )
        if futures.result_checksum:
            self._cache_fat_finger_checksum_future(futures.result_checksum, futures.fat)
        if tf_hex:
            self._store_transformation(tf_hex, futures)
        return futures.fat

    # --- internals ------------------------------------------------------
    def _register_worker_plugin(
        self, worker_count: int, remote_clients: dict | None
    ) -> None:
        try:
            # Avoid re-registering if scheduler already has the plugin.
            already_registered = self._client.run_on_scheduler(_has_worker_plugin)
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

    def _start_prune_thread(self) -> None:
        if self._prune_interval <= 0:
            return
        if self._prune_thread is not None:
            return

        def _loop() -> None:
            while not self._prune_stop.wait(self._prune_interval):
                try:
                    self._prune_caches()
                except Exception:
                    pass

        thread = threading.Thread(
            target=_loop,
            name="seamless-dask-cache-prune",
            daemon=True,
        )
        self._prune_thread = thread
        thread.start()

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

    def _transformation_done(self, futures: TransformationFutures) -> bool:
        if not futures.base.done():
            return False
        if not futures.thin.done():
            return False
        if futures.fat is not None and not futures.fat.done():
            return False
        return True

    def _touch_fat_checksum_cache(self, checksum_hex: str, future: Future) -> None:
        if not future.done():
            return
        with self._cache_lock:
            cached = self._fat_checksum_cache.get(checksum_hex)
            if cached is None or cached[0] is not future:
                return
            self._fat_checksum_cache[checksum_hex] = (future, self._expiry())

    def _touch_fat_finger_checksum_cache(
        self, checksum_hex: str, future: Future
    ) -> None:
        if not future.done():
            return
        with self._cache_lock:
            cached = self._fat_finger_checksum_cache.get(checksum_hex)
            if cached is None or cached[0] is not future:
                return
            self._fat_finger_checksum_cache[checksum_hex] = (future, self._expiry())

    def _touch_transformation_cache(
        self, tf_checksum_hex: str, futures: TransformationFutures
    ) -> None:
        if not self._transformation_done(futures):
            return
        with self._cache_lock:
            cached = self._transformation_cache.get(tf_checksum_hex)
            if cached is None or cached[0] is not futures:
                return
            self._transformation_cache[tf_checksum_hex] = (futures, self._expiry())

    def _cache_fat_checksum_future(self, checksum_hex: str, future: Future) -> None:
        with self._cache_lock:
            cached = self._fat_checksum_cache.get(checksum_hex)
            if cached is None or cached[0] is not future:
                self._fat_checksum_cache[checksum_hex] = (future, None)
                future.add_done_callback(
                    lambda fut, checksum_hex=checksum_hex: self._touch_fat_checksum_cache(
                        checksum_hex, fut
                    )
                )
        self._touch_fat_checksum_cache(checksum_hex, future)

    def _cache_fat_finger_checksum_future(
        self, checksum_hex: str, future: Future
    ) -> None:
        with self._cache_lock:
            cached = self._fat_finger_checksum_cache.get(checksum_hex)
            if cached is None or cached[0] is not future:
                self._fat_finger_checksum_cache[checksum_hex] = (future, None)
                future.add_done_callback(
                    lambda fut, checksum_hex=checksum_hex: self._touch_fat_finger_checksum_cache(
                        checksum_hex, fut
                    )
                )
        self._touch_fat_finger_checksum_cache(checksum_hex, future)

    def _cache_transformation_futures(
        self, tf_checksum_hex: str, futures: TransformationFutures
    ) -> None:
        with self._cache_lock:
            cached = self._transformation_cache.get(tf_checksum_hex)
            if cached is None or cached[0] is not futures:
                self._transformation_cache[tf_checksum_hex] = (futures, None)

        def _on_done(_fut: Future) -> None:
            self._touch_transformation_cache(tf_checksum_hex, futures)

        futures.base.add_done_callback(_on_done)
        futures.thin.add_done_callback(_on_done)
        if futures.fat is not None:
            futures.fat.add_done_callback(_on_done)
        self._touch_transformation_cache(tf_checksum_hex, futures)

    def release_transformation_futures(
        self, futures: TransformationFutures, *, cancel: bool = True
    ) -> None:
        """Release and optionally cancel a transformation's futures and cache entries."""

        tf_checksum_hex = futures.tf_checksum
        with self._cache_lock:
            if tf_checksum_hex:
                cached = self._transformation_cache.get(tf_checksum_hex)
                if cached is not None and cached[0] is futures:
                    self._transformation_cache.pop(tf_checksum_hex, None)
            if futures.result_checksum and futures.fat is not None:
                cached = self._fat_checksum_cache.get(futures.result_checksum)
                if cached is not None and cached[0] is futures.fat:
                    self._fat_checksum_cache.pop(futures.result_checksum, None)
                cached = self._fat_finger_checksum_cache.get(futures.result_checksum)
                if cached is not None and cached[0] is futures.fat:
                    self._fat_finger_checksum_cache.pop(futures.result_checksum, None)

        for future in (futures.base, futures.thin, futures.fat):
            if future is None:
                continue
            if cancel:
                try:
                    if not future.cancelled() and not future.done():
                        self._client.cancel(future, force=True)
                except Exception:
                    pass
            try:
                future.release()
            except Exception:
                pass

    def _prune_caches(self) -> None:
        now = time.monotonic()
        fat_release: list[Future] = []
        trans_release: list[Future] = []
        with self._cache_lock:
            for key in list(self._fat_checksum_cache.keys()):
                future, expiry = self._fat_checksum_cache[key]
                if expiry is None or expiry >= now or not future.done():
                    continue
                self._fat_checksum_cache.pop(key, None)
                fat_release.append(future)
            for key in list(self._fat_finger_checksum_cache.keys()):
                future, expiry = self._fat_finger_checksum_cache[key]
                if expiry is None or expiry >= now or not future.done():
                    continue
                self._fat_finger_checksum_cache.pop(key, None)
                fat_release.append(future)
            for key in list(self._transformation_cache.keys()):
                futures, expiry = self._transformation_cache[key]
                if (
                    expiry is None
                    or expiry >= now
                    or not self._transformation_done(futures)
                ):
                    continue
                self._transformation_cache.pop(key, None)
                for future in (futures.base, futures.thin, futures.fat):
                    if future is None:
                        continue
                    trans_release.append(future)
        for future in fat_release:
            try:
                future.release()
            except Exception:
                pass
        for future in trans_release:
            try:
                future.release()
            except Exception:
                pass

    def _store_transformation(
        self, tf_checksum_hex: str, futures: TransformationFutures
    ) -> None:
        self._cache_transformation_futures(tf_checksum_hex, futures)

    def _register_transformation(
        self,
        tf_checksum_hint: str | None,
        futures: TransformationFutures,
        thin_future: Future,
        *,
        store_cache: bool = True,
    ) -> None:
        try:
            tf_checksum_hex, result_checksum_hex, exc = thin_future.result()
        except Exception:
            return

        if tf_checksum_hex:
            futures.tf_checksum = tf_checksum_hex
            if store_cache:
                self._store_transformation(tf_checksum_hex, futures)
        if result_checksum_hex:
            futures.result_checksum = result_checksum_hex
            if futures.fat is not None:
                fat_key = getattr(futures.fat, "key", "") or ""
                if fat_key.startswith("fat-finger-"):
                    self._cache_fat_finger_checksum_future(
                        result_checksum_hex, futures.fat
                    )
                else:
                    self._cache_fat_checksum_future(result_checksum_hex, futures.fat)


__all__ = ["SeamlessDaskClient", "_fat_checksum_task"]


def _get_worker_loop() -> asyncio.AbstractEventLoop | None:
    try:
        worker = get_worker()
    except ValueError:
        return None
    return getattr(worker, "seamless_loop", None)
