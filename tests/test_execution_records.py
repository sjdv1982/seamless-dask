import asyncio
import threading

import pytest

from seamless import Buffer, Checksum

import seamless_config.select as select
import seamless_dask.client as dask_client
from seamless_dask.wrapper import build_wrapper_configuration
from seamless_dask.transformation_mixin import TransformationDaskMixin
from seamless_dask.types import TransformationSubmission
import seamless_remote
import seamless_transformer.probe_index as probe_index
import seamless_transformer.record_assembly as record_assembly
import seamless_transformer.record_runtime as record_runtime
import seamless_transformer.transformation_cache as transformation_cache


class _FakeBufferRemote:
    def __init__(self):
        self.promised = []

    async def promise(self, checksum):
        self.promised.append(checksum.hex())
        return True

    def has_write_server(self):
        return True


class _FakeDatabaseRemote:
    def __init__(self, *, write_server=True):
        self.write_server = write_server
        self.transformation_results = []
        self.execution_records = []

    async def set_transformation_result(self, tf_checksum, result_checksum):
        self.transformation_results.append((tf_checksum.hex(), result_checksum.hex()))
        return True

    async def set_execution_record(self, tf_checksum, result_checksum, record):
        self.execution_records.append(
            (tf_checksum.hex(), result_checksum.hex(), record)
        )
        return True

    def has_write_server(self):
        return self.write_server


class _RaisingExecutionRecordDatabaseRemote(_FakeDatabaseRemote):
    def __init__(self, exc):
        super().__init__()
        self.exc = exc

    async def set_execution_record(self, tf_checksum, result_checksum, record):
        del tf_checksum, result_checksum, record
        raise self.exc


class _DummyMixin(TransformationDaskMixin):
    pass


class _FakePreTransformation:
    def __init__(self, transformation_dict):
        self.transformation_dict = dict(transformation_dict)

    def build_partial_transformation(self, upstream_dependencies):
        assert upstream_dependencies == {}
        return dict(self.transformation_dict), {}


def _make_checksum(value, celltype="plain") -> Checksum:
    buf = Buffer(value, celltype)
    checksum = buf.get_checksum()
    if len(buf):
        buf.tempref()
    return checksum


def test_record_mode_requires_database_write_server(monkeypatch):
    dummy = _DummyMixin()

    monkeypatch.setattr(select, "get_execution", lambda: "process")
    monkeypatch.setattr(record_runtime, "get_record_mode", lambda: True)
    monkeypatch.setattr(seamless_remote, "buffer_remote", _FakeBufferRemote())
    monkeypatch.setattr(seamless_remote, "database_remote", None)

    assert (
        dummy._remote_storage_error()
        == "Record mode requires an active database write server"
    )


def test_dask_record_mode_missing_probe_blocks_before_submit(monkeypatch):
    dummy = _DummyMixin()
    dummy._pretransformation = _FakePreTransformation(
        {"__language__": "python", "__output__": ("result", "mixed", None)}
    )
    dummy._upstream_dependencies = {}
    dummy._meta = {}
    dummy._tf_dunder = {}
    dummy._scratch = False
    dummy._constructed = False
    dummy._transformation_checksum = None
    dummy._result_checksum = None
    dummy._evaluated = False
    dummy._exception = None
    dummy._dask_futures = None

    def _unexpected_submit(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Dask submit should not happen before record probes exist")

    monkeypatch.setattr(dummy, "_dask_client", lambda: object())
    monkeypatch.setattr(dummy, "_remote_storage_error", lambda: None)
    monkeypatch.setattr(dummy, "_compute_tf_checksum_no_deps", lambda: None)
    monkeypatch.setattr(dummy, "_skip_permission_gate", lambda: True)
    monkeypatch.setattr(dummy, "_ensure_dask_futures", _unexpected_submit)
    monkeypatch.setattr(record_runtime, "get_record_mode", lambda: True)
    monkeypatch.setattr(
        probe_index,
        "ensure_record_bucket_preconditions_sync",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            probe_index.RecordBucketError("missing bucket probe")
        ),
    )

    with pytest.raises(probe_index.RecordBucketError, match="missing bucket probe"):
        dummy._compute_with_dask(require_value=False)


def test_dask_record_mode_cache_hit_bypasses_probe_preflight(monkeypatch):
    dummy = _DummyMixin()
    cached_checksum = _make_checksum(12, "mixed")
    dummy._pretransformation = _FakePreTransformation(
        {"__language__": "python", "__output__": ("result", "mixed", None)}
    )
    dummy._upstream_dependencies = {}
    dummy._meta = {}
    dummy._tf_dunder = {}
    dummy._scratch = False
    dummy._constructed = False
    dummy._transformation_checksum = None
    dummy._result_checksum = None
    dummy._evaluated = False
    dummy._exception = None
    dummy._dask_futures = None

    def _unexpected_probe(*args, **kwargs):
        del args, kwargs
        raise AssertionError("cache hits should not require record probes")

    monkeypatch.setattr(dummy, "_dask_client", lambda: object())
    monkeypatch.setattr(dummy, "_remote_storage_error", lambda: None)
    monkeypatch.setattr(dummy, "_compute_tf_checksum_no_deps", lambda: "1" * 64)
    monkeypatch.setattr(
        dummy,
        "_try_database_cache_sync",
        lambda *args, **kwargs: cached_checksum,
    )
    monkeypatch.setattr(record_runtime, "get_record_mode", lambda: True)
    monkeypatch.setattr(
        probe_index,
        "ensure_record_bucket_preconditions_sync",
        _unexpected_probe,
    )

    assert dummy._compute_with_dask(require_value=False) == cached_checksum


def test_submit_transformation_sends_record_mode(monkeypatch):
    submitted = []

    class _FakeFuture:
        def cancelled(self):
            return False

        def add_done_callback(self, callback):
            del callback

    class _FakeDistributedClient:
        def submit(self, func, *args, **kwargs):
            submitted.append((func, args, kwargs))
            return _FakeFuture()

    client = dask_client.SeamlessDaskClient.__new__(dask_client.SeamlessDaskClient)
    client._client = _FakeDistributedClient()
    client._cache_lock = threading.RLock()
    client._transformation_cache = {}
    client._prune_caches = lambda: None
    client._touch_transformation_cache = lambda *args, **kwargs: None

    monkeypatch.setattr(dask_client, "get_record_mode", lambda: True)

    client.submit_transformation(
        TransformationSubmission(
            transformation_dict={
                "__language__": "python",
                "__output__": ("result", "mixed", None),
            },
            inputs={},
            input_futures={},
            tf_checksum="1" * 64,
            tf_dunder={},
            scratch=False,
        )
    )

    base_payload = submitted[0][1][0]
    assert base_payload["record"] is True


def test_run_base_rejects_record_mode_mismatch(monkeypatch):
    tf_checksum = _make_checksum({"kind": "dask-record-mismatch"})

    monkeypatch.setenv("SEAMLESS_DASK_RECORD_MODE", "1")
    result = dask_client._run_base(
        {
            "tf_checksum": tf_checksum.hex(),
            "transformation_dict": {
                "__language__": "python",
                "__output__": ("result", "mixed", None),
            },
            "inputs": [],
            "tf_dunder": {},
            "scratch": False,
            "require_value": False,
            "record": False,
        },
        {},
    )

    assert result[0] == tf_checksum.hex()
    assert result[1] is None
    assert "Dask server record mode mismatch" in result[3]


def test_wrapper_configuration_exports_startup_record_mode():
    config = build_wrapper_configuration(
        host="127.0.0.1",
        port_range=(20000, 29999),
        parameters={
            "walltime": "00:20:00",
            "cores": 1,
            "memory": "1GiB",
            "record": True,
        },
    )

    assert config.record is True
    assert "export SEAMLESS_DASK_RECORD_MODE=1" in config.env_exports


def test_promise_and_write_result_writes_execution_record(monkeypatch):
    fake_database_remote = _FakeDatabaseRemote()
    fake_buffer_remote = _FakeBufferRemote()
    tf_checksum = _make_checksum({"kind": "dask-record-test"})
    result_checksum = _make_checksum(7, "mixed")
    node_checksum = _make_checksum(
        {
            "bucket_kind": "node",
            "contract_violations": ["node.contract"],
        }
    )
    environment_checksum = _make_checksum(
        {
            "bucket_kind": "environment",
            "contract_violations": ["environment.contract"],
        }
    )
    node_env_checksum = _make_checksum(
        {
            "bucket_kind": "node_env",
            "contract_violations": ["node_env.contract"],
        }
    )
    queue_checksum = _make_checksum(
        {
            "bucket_kind": "queue",
            "contract_violations": ["queue.contract"],
        }
    )
    queue_node_checksum = _make_checksum(
        {
            "bucket_kind": "queue_node",
            "contract_violations": ["queue_node.contract"],
        }
    )
    probe_context = {
        "required_bucket_labels": {
            "node": "worker-1",
            "environment": "conda:/envs/seamless1",
                "node_env": node_checksum.hex() + ":" + environment_checksum.hex(),
                "queue": "demo/gpu/daskserver",
                "queue_node": queue_checksum.hex() + ":worker-1",
            },
            "required_bucket_checksums": {
                "node": node_checksum.hex(),
                "environment": environment_checksum.hex(),
                "node_env": node_env_checksum.hex(),
                "queue": queue_checksum.hex(),
                "queue_node": queue_node_checksum.hex(),
            },
        "live_tokens": {
            "node": {"hostname": "worker-1", "boot_id": "boot-1"},
            "environment": {"sys_prefix": "/envs/seamless1"},
            "node_env": {
                "node": {"hostname": "worker-1", "boot_id": "boot-1"},
                "environment": {"sys_prefix": "/envs/seamless1"},
            },
            "queue": {"queue_config_sha256": "queue-sha"},
            "queue_node": {
                "queue": {"queue_config_sha256": "queue-sha"},
                "node": {"hostname": "worker-1", "boot_id": "boot-1"},
            },
        },
        "bucket_tokens": {
            "node": {"hostname": "worker-1", "boot_id": "boot-1"},
            "environment": {"sys_prefix": "/envs/seamless1"},
            "node_env": {
                "node": {"hostname": "worker-1", "boot_id": "boot-1"},
                "environment": {"sys_prefix": "/envs/seamless1"},
            },
            "queue": {"queue_config_sha256": "queue-sha"},
            "queue_node": {
                "queue": {"queue_config_sha256": "queue-sha"},
                "node": {"hostname": "worker-1", "boot_id": "boot-1"},
            },
        },
    }
    validation_snapshot = "6" * 64

    monkeypatch.setattr(dask_client, "get_record_mode", lambda: True)
    monkeypatch.setattr(seamless_remote, "database_remote", fake_database_remote)
    monkeypatch.setattr(seamless_remote, "buffer_remote", fake_buffer_remote)
    monkeypatch.setattr(dask_client, "_ENSURE_RESULT_UPLOAD", False)
    monkeypatch.setattr(transformation_cache, "get_remote", lambda: "daskserver")
    monkeypatch.setattr(transformation_cache, "get_selected_cluster", lambda: None)
    monkeypatch.setattr(transformation_cache, "get_queue", lambda cluster=None: None)
    monkeypatch.setattr(transformation_cache, "get_node", lambda: None)
    monkeypatch.setattr(record_assembly, "get_remote", lambda: "daskserver")
    monkeypatch.setattr(record_assembly, "get_selected_cluster", lambda: None)
    monkeypatch.setattr(record_assembly, "get_queue", lambda cluster=None: None)
    monkeypatch.setattr(record_assembly, "get_node", lambda: None)
    monkeypatch.setattr(transformation_cache, "_memory_peak_bytes", lambda: 987654)
    monkeypatch.setattr(
        transformation_cache, "_process_create_time_epoch", lambda: 2468.0
    )
    monkeypatch.setattr(
        probe_index,
        "ensure_record_bucket_preconditions",
        lambda *args, **kwargs: asyncio.sleep(0, result=probe_context),
    )
    monkeypatch.setattr(
        transformation_cache,
        "build_validation_snapshot_checksum",
        lambda *args, **kwargs: asyncio.sleep(0, result=validation_snapshot),
    )

    asyncio.run(
        dask_client._promise_and_write_result_async(
            tf_checksum,
            result_checksum,
            write_buffer=False,
            transformation_dict={
                "__language__": "python",
                "__output__": ("result", "mixed", None),
            },
            tf_dunder={},
            scratch=False,
            execution="remote",
            started_at="2026-04-26T12:00:00Z",
            finished_at="2026-04-26T12:00:01Z",
            wall_time_seconds=1.0,
            cpu_user_seconds=0.4,
            cpu_system_seconds=0.1,
            gpu_memory_peak_bytes=333222111,
        )
    )

    assert fake_database_remote.transformation_results == [
        (tf_checksum.hex(), result_checksum.hex())
    ]
    assert len(fake_database_remote.execution_records) == 1
    record = fake_database_remote.execution_records[0][2]
    assert record["tf_checksum"] == tf_checksum.hex()
    assert record["result_checksum"] == result_checksum.hex()
    assert record["execution_mode"] == "remote"
    assert record["remote_target"] == "daskserver"
    assert record["node"] == node_checksum.hex()
    assert record["environment"] == environment_checksum.hex()
    assert record["node_env"] == node_env_checksum.hex()
    assert record["queue"] == queue_checksum.hex()
    assert record["queue_node"] == queue_node_checksum.hex()
    assert record["bucket_contract_violations"] == [
        "environment.contract",
        "node.contract",
        "node_env.contract",
        "queue.contract",
        "queue_node.contract",
    ]
    assert record["freshness"] == probe_context
    assert record["validation_snapshot"] == validation_snapshot
    assert record["memory_peak_bytes"] == 987654
    assert record["gpu_memory_peak_bytes"] == 333222111
    assert record["process_create_time_epoch"] == 2468.0
    assert record["input_total_bytes"] == 0
    assert isinstance(record["output_total_bytes"], int)
    assert record["output_total_bytes"] > 0
    assert record["execution_envelope"]["scratch"] is False


def test_probe_jobs_skip_execution_record_write(monkeypatch):
    fake_database_remote = _FakeDatabaseRemote()
    fake_buffer_remote = _FakeBufferRemote()
    tf_checksum = _make_checksum({"kind": "dask-probe-skip"})
    result_checksum = _make_checksum(11, "mixed")

    async def _unexpected_probe_context(*args, **kwargs):
        del args, kwargs
        raise AssertionError("record probe should skip execution-record preflight")

    monkeypatch.setattr(dask_client, "get_record_mode", lambda: True)
    monkeypatch.setattr(seamless_remote, "database_remote", fake_database_remote)
    monkeypatch.setattr(seamless_remote, "buffer_remote", fake_buffer_remote)
    monkeypatch.setattr(dask_client, "_ENSURE_RESULT_UPLOAD", False)
    monkeypatch.setattr(
        probe_index,
        "ensure_record_bucket_preconditions",
        _unexpected_probe_context,
    )

    asyncio.run(
        dask_client._promise_and_write_result_async(
            tf_checksum,
            result_checksum,
            write_buffer=False,
            transformation_dict={
                "__language__": "python",
                "__output__": ("result", "mixed", None),
            },
            tf_dunder={"__record_probe__": {"mode": "capture"}},
            scratch=False,
            execution="remote",
        )
    )

    assert fake_database_remote.transformation_results == [
        (tf_checksum.hex(), result_checksum.hex())
    ]
    assert fake_database_remote.execution_records == []


def test_promise_and_write_result_writes_minimal_record_when_record_mode_false(
    monkeypatch,
):
    fake_database_remote = _FakeDatabaseRemote()
    fake_buffer_remote = _FakeBufferRemote()
    tf_checksum = _make_checksum({"kind": "dask-minimal-record-test"})
    result_checksum = _make_checksum(13, "mixed")

    async def _unexpected_probe_context(*args, **kwargs):
        del args, kwargs
        raise AssertionError("record: false should not probe execution-record buckets")

    monkeypatch.setattr(dask_client, "get_record_mode", lambda: False)
    monkeypatch.setattr(seamless_remote, "database_remote", fake_database_remote)
    monkeypatch.setattr(seamless_remote, "buffer_remote", fake_buffer_remote)
    monkeypatch.setattr(dask_client, "_ENSURE_RESULT_UPLOAD", False)
    monkeypatch.setattr(
        probe_index,
        "ensure_record_bucket_preconditions",
        _unexpected_probe_context,
    )
    monkeypatch.setattr(transformation_cache, "_memory_peak_bytes", lambda: 456789)
    monkeypatch.setattr(record_assembly, "get_remote", lambda: "daskserver")
    monkeypatch.setattr(record_assembly, "get_selected_cluster", lambda: None)
    monkeypatch.setattr(record_assembly, "get_queue", lambda cluster=None: None)
    monkeypatch.setattr(record_assembly, "get_node", lambda: None)

    asyncio.run(
        dask_client._promise_and_write_result_async(
            tf_checksum,
            result_checksum,
            write_buffer=False,
            transformation_dict={
                "__language__": "python",
                "__output__": ("result", "mixed", None),
            },
            tf_dunder={},
            scratch=False,
            execution="remote",
            wall_time_seconds=1.25,
            cpu_user_seconds=0.5,
            cpu_system_seconds=0.2,
            gpu_memory_peak_bytes=654321,
        )
    )

    assert fake_database_remote.transformation_results == [
        (tf_checksum.hex(), result_checksum.hex())
    ]
    assert len(fake_database_remote.execution_records) == 1
    record = fake_database_remote.execution_records[0][2]
    assert set(record) == {
        "schema_version",
        "tf_checksum",
        "result_checksum",
        "seamless_version",
        "execution_mode",
        "remote_target",
        "wall_time_seconds",
        "cpu_time_user_seconds",
        "cpu_time_system_seconds",
        "memory_peak_bytes",
        "gpu_memory_peak_bytes",
    }
    assert record["execution_mode"] == "remote"
    assert record["remote_target"] == "daskserver"
    assert record["memory_peak_bytes"] == 456789
    assert record["gpu_memory_peak_bytes"] == 654321


def test_compiled_dask_record_writes_compilation_context(monkeypatch):
    fake_database_remote = _FakeDatabaseRemote()
    fake_buffer_remote = _FakeBufferRemote()
    tf_checksum = _make_checksum({"kind": "dask-compiled-record-test"})
    result_checksum = _make_checksum(17, "mixed")
    compilation_context = "f" * 64
    compilation_time_seconds = 3.25
    process_create_time_epoch = 1357.0
    validation_snapshot = "5" * 64
    job_contract_violations = ["ld_preload_outside_conda_prefix"]
    captured_snapshot_kwargs = {}

    monkeypatch.setattr(dask_client, "get_record_mode", lambda: True)
    monkeypatch.setattr(seamless_remote, "database_remote", fake_database_remote)
    monkeypatch.setattr(seamless_remote, "buffer_remote", fake_buffer_remote)
    monkeypatch.setattr(dask_client, "_ENSURE_RESULT_UPLOAD", False)
    monkeypatch.setattr(
        probe_index,
        "ensure_record_bucket_preconditions",
        lambda *args, **kwargs: asyncio.sleep(
            0,
            result={
                "required_bucket_labels": {},
                "required_bucket_checksums": {},
                "live_tokens": {},
                "bucket_tokens": {},
            },
        ),
    )
    monkeypatch.setattr(
        transformation_cache,
        "build_compilation_context_checksum",
        lambda *args, **kwargs: asyncio.sleep(0, result=compilation_context),
    )
    async def _fake_validation_snapshot(*args, **kwargs):
        del args
        captured_snapshot_kwargs.update(kwargs)
        return validation_snapshot

    monkeypatch.setattr(
        transformation_cache,
        "build_validation_snapshot_checksum",
        _fake_validation_snapshot,
    )
    monkeypatch.setattr(
        transformation_cache,
        "collect_job_validation",
        lambda *args, **kwargs: asyncio.sleep(
            0,
            result={
                "job_contract_violations": job_contract_violations,
                "diagnostics": {"compiled": True, "origin": "dask-worker"},
            },
        ),
    )
    monkeypatch.setattr(
        transformation_cache,
        "collect_compilation_runtime_metadata",
        lambda *args, **kwargs: asyncio.sleep(
            0, result={"compilation_time_seconds": compilation_time_seconds}
        ),
    )
    monkeypatch.setattr(
        transformation_cache,
        "_process_create_time_epoch",
        lambda: process_create_time_epoch,
    )

    asyncio.run(
        dask_client._promise_and_write_result_async(
            tf_checksum,
            result_checksum,
            write_buffer=False,
            transformation_dict={
                "__language__": "c",
                "__compiled__": True,
                "__output__": ("result", "mixed", None),
            },
            tf_dunder={},
            scratch=False,
            execution="remote",
        )
    )

    record = fake_database_remote.execution_records[0][2]
    assert record["compilation_context"] == compilation_context
    assert record["job_contract_violations"] == job_contract_violations
    assert record["contract_violations"] == job_contract_violations
    assert record["validation_snapshot"] == validation_snapshot
    assert record["compilation_time_seconds"] == compilation_time_seconds
    assert record["process_create_time_epoch"] == process_create_time_epoch
    assert captured_snapshot_kwargs["job_contract_violations"] == job_contract_violations
    assert captured_snapshot_kwargs["job_validation_diagnostics"] == {
        "compiled": True,
        "origin": "dask-worker",
    }
    assert captured_snapshot_kwargs["runtime_metadata"]["process_create_time_epoch"] == (
        process_create_time_epoch
    )
    assert record["input_total_bytes"] == 0
    assert isinstance(record["output_total_bytes"], int)
    assert record["output_total_bytes"] > 0


def test_strict_record_write_failure_propagates(monkeypatch):
    fake_database_remote = _RaisingExecutionRecordDatabaseRemote(
        ValueError("broken execution record")
    )
    fake_buffer_remote = _FakeBufferRemote()
    tf_checksum = _make_checksum({"kind": "dask-strict-record-failure"})
    result_checksum = _make_checksum(19, "mixed")

    monkeypatch.setattr(dask_client, "get_record_mode", lambda: True)
    monkeypatch.setattr(seamless_remote, "database_remote", fake_database_remote)
    monkeypatch.setattr(seamless_remote, "buffer_remote", fake_buffer_remote)
    monkeypatch.setattr(dask_client, "_ENSURE_RESULT_UPLOAD", False)
    monkeypatch.setattr(
        probe_index,
        "ensure_record_bucket_preconditions",
        lambda *args, **kwargs: asyncio.sleep(
            0,
            result={
                "required_bucket_labels": {},
                "required_bucket_checksums": {},
                "live_tokens": {},
                "bucket_tokens": {},
            },
        ),
    )

    with pytest.raises(ValueError, match="broken execution record"):
        asyncio.run(
            dask_client._promise_and_write_result_async(
                tf_checksum,
                result_checksum,
                write_buffer=False,
                transformation_dict={
                    "__language__": "python",
                    "__output__": ("result", "mixed", None),
                },
                tf_dunder={},
                scratch=False,
                execution="remote",
            )
        )


def test_run_base_surfaces_record_persistence_failure(monkeypatch):
    tf_checksum = _make_checksum({"kind": "dask-run-base-record-failure"})
    result_checksum = _make_checksum(21, "mixed")
    calls = {"count": 0}

    def _fake_run_on_worker_loop(coro_factory):
        del coro_factory
        calls["count"] += 1
        if calls["count"] == 1:
            return None
        if calls["count"] == 2:
            return result_checksum
        if calls["count"] == 3:
            return None
        raise probe_index.RecordBucketError("missing bucket probe")

    monkeypatch.setattr(dask_client, "_run_on_worker_loop", _fake_run_on_worker_loop)
    monkeypatch.setattr(
        transformation_cache, "start_gpu_memory_sampler", lambda pid=None: object()
    )
    monkeypatch.setattr(
        transformation_cache, "stop_gpu_memory_sampler", lambda sampler: None
    )
    monkeypatch.setattr(
        dask_client.transformer_worker,
        "dispatch_to_workers",
        lambda *args, **kwargs: asyncio.sleep(0, result=result_checksum),
    )
    import seamless_dask.transformer_client as transformer_client

    monkeypatch.setattr(transformer_client, "get_seamless_dask_client", lambda: object())

    result = dask_client._run_base(
        {
            "tf_checksum": tf_checksum.hex(),
            "transformation_dict": {
                "__language__": "python",
                "__output__": ("result", "mixed", None),
            },
            "inputs": [],
            "tf_dunder": {},
            "scratch": False,
            "require_value": False,
        },
        {},
    )

    assert result[0] == tf_checksum.hex()
    assert result[1] is None
    assert "missing bucket probe" in result[3]


def test_minimal_record_write_storage_failure_logs_and_continues(
    monkeypatch, caplog
):
    fake_database_remote = _RaisingExecutionRecordDatabaseRemote(
        OSError("database temporarily unavailable")
    )
    fake_buffer_remote = _FakeBufferRemote()
    tf_checksum = _make_checksum({"kind": "dask-minimal-record-warning"})
    result_checksum = _make_checksum(23, "mixed")

    monkeypatch.setattr(dask_client, "get_record_mode", lambda: False)
    monkeypatch.setattr(seamless_remote, "database_remote", fake_database_remote)
    monkeypatch.setattr(seamless_remote, "buffer_remote", fake_buffer_remote)
    monkeypatch.setattr(dask_client, "_ENSURE_RESULT_UPLOAD", False)
    async def _unexpected_probe_context(*args, **kwargs):
        del args, kwargs
        raise AssertionError("record: false should not probe")

    monkeypatch.setattr(
        probe_index,
        "ensure_record_bucket_preconditions",
        _unexpected_probe_context,
    )

    with caplog.at_level("WARNING"):
        asyncio.run(
            dask_client._promise_and_write_result_async(
                tf_checksum,
                result_checksum,
                write_buffer=False,
                transformation_dict={
                    "__language__": "python",
                    "__output__": ("result", "mixed", None),
                },
                tf_dunder={},
                scratch=False,
                execution="remote",
            )
        )

    assert fake_database_remote.transformation_results == [
        (tf_checksum.hex(), result_checksum.hex())
    ]
    assert "best-effort minimal execution-record write failed" in caplog.text
