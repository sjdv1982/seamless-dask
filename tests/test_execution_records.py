import asyncio

from seamless import Buffer, Checksum

import seamless_config.select as select
import seamless_dask.client as dask_client
from seamless_dask.transformation_mixin import TransformationDaskMixin
import seamless_remote
import seamless_transformer.probe_index as probe_index
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


class _DummyMixin(TransformationDaskMixin):
    pass


def _make_checksum(value, celltype="plain") -> Checksum:
    buf = Buffer(value, celltype)
    checksum = buf.get_checksum()
    if len(buf):
        buf.tempref()
    return checksum


def test_record_mode_requires_database_write_server(monkeypatch):
    dummy = _DummyMixin()

    monkeypatch.setattr(select, "get_execution", lambda: "process")
    monkeypatch.setattr(select, "get_record", lambda: True)
    monkeypatch.setattr(seamless_remote, "buffer_remote", _FakeBufferRemote())
    monkeypatch.setattr(seamless_remote, "database_remote", None)

    assert (
        dummy._remote_storage_error()
        == "Record mode requires an active database write server"
    )


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

    monkeypatch.setattr(select, "get_record", lambda: True)
    monkeypatch.setattr(seamless_remote, "database_remote", fake_database_remote)
    monkeypatch.setattr(seamless_remote, "buffer_remote", fake_buffer_remote)
    monkeypatch.setattr(dask_client, "_ENSURE_RESULT_UPLOAD", False)
    monkeypatch.setattr(transformation_cache, "get_remote", lambda: "daskserver")
    monkeypatch.setattr(transformation_cache, "get_selected_cluster", lambda: None)
    monkeypatch.setattr(transformation_cache, "get_queue", lambda cluster=None: None)
    monkeypatch.setattr(transformation_cache, "get_node", lambda: None)
    monkeypatch.setattr(transformation_cache, "_memory_peak_bytes", lambda: 987654)
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

    monkeypatch.setattr(select, "get_record", lambda: True)
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


def test_compiled_dask_record_writes_compilation_context(monkeypatch):
    fake_database_remote = _FakeDatabaseRemote()
    fake_buffer_remote = _FakeBufferRemote()
    tf_checksum = _make_checksum({"kind": "dask-compiled-record-test"})
    result_checksum = _make_checksum(17, "mixed")
    compilation_context = "f" * 64
    compilation_time_seconds = 3.25
    validation_snapshot = "5" * 64
    job_contract_violations = ["ld_preload_outside_conda_prefix"]
    captured_snapshot_kwargs = {}

    monkeypatch.setattr(select, "get_record", lambda: True)
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
    assert captured_snapshot_kwargs["job_contract_violations"] == job_contract_violations
    assert captured_snapshot_kwargs["job_validation_diagnostics"] == {
        "compiled": True,
        "origin": "dask-worker",
    }
    assert record["input_total_bytes"] == 0
    assert isinstance(record["output_total_bytes"], int)
    assert record["output_total_bytes"] > 0
