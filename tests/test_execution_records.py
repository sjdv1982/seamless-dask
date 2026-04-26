import asyncio

from seamless import Buffer, Checksum

import seamless_config.select as select
import seamless_dask.client as dask_client
from seamless_dask.transformation_mixin import TransformationDaskMixin
import seamless_remote
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

    monkeypatch.setattr(select, "get_record", lambda: True)
    monkeypatch.setattr(seamless_remote, "database_remote", fake_database_remote)
    monkeypatch.setattr(seamless_remote, "buffer_remote", fake_buffer_remote)
    monkeypatch.setattr(dask_client, "_ENSURE_RESULT_UPLOAD", False)
    monkeypatch.setattr(transformation_cache, "get_remote", lambda: "daskserver")
    monkeypatch.setattr(transformation_cache, "get_selected_cluster", lambda: None)
    monkeypatch.setattr(transformation_cache, "get_queue", lambda cluster=None: None)
    monkeypatch.setattr(transformation_cache, "get_node", lambda: None)

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
    assert record["execution_envelope"]["scratch"] is False
