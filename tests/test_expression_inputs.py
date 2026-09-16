from seamless import Buffer, CacheMissError, Checksum
from seamless.checksum import expression as expression_mod
from seamless_dask import client as dask_client
from seamless_dask import transformer_client


def test_expression_task_returns_fat_input_tuple():
    source = Buffer({"value": 42}, "plain")
    source_checksum = source.get_checksum()
    source.tempref()

    result_checksum, result_buffer, error = dask_client._expression_task(
        {
            "path": "value",
            'input_celltype': "plain",
            'celltype': "int",
            "validator": None,
            "validator_language": None,
        },
        (source_checksum.hex(), source, None),
    )

    assert error is None
    assert result_checksum is not None
    assert isinstance(result_buffer, Buffer)
    assert result_buffer.get_value("int") == 42


def test_expression_task_requests_auto_location(monkeypatch):
    source = Buffer({"value": 42}, "plain")
    source_checksum = source.get_checksum()
    source.tempref()
    expected = Buffer(42, "int")
    expected_checksum = expected.get_checksum()
    expected.tempref()
    executions = []

    async def evaluate_expression(*args, **kwargs):
        executions.append(kwargs["execution"])
        return expected_checksum

    monkeypatch.setattr(
        expression_mod,
        "evaluate_expression_remote",
        evaluate_expression,
    )

    result_checksum, result_buffer, error = dask_client._expression_task(
        {
            "path": "value",
            "input_celltype": "plain",
            "celltype": "int",
            "validator": None,
            "validator_language": None,
        },
        (source_checksum.hex(), source, None),
    )

    assert error is None
    assert result_checksum == expected_checksum.hex()
    assert isinstance(result_buffer, Buffer)
    assert result_buffer.get_value("int") == 42
    assert executions == ["auto"]


def test_expression_task_reports_structured_cache_miss(monkeypatch):
    missing = Checksum("f" * 64)

    async def evaluate_expression(*args, **kwargs):
        raise CacheMissError(missing)

    monkeypatch.setattr(
        expression_mod,
        "evaluate_expression_remote",
        evaluate_expression,
    )

    result_checksum, result_buffer, error = dask_client._expression_task(
        {
            "path": "value",
            "input_celltype": "plain",
            "celltype": "int",
            "validator": None,
            "validator_language": None,
        },
        (missing.hex(), None, None),
    )

    assert result_checksum == missing.hex()
    assert result_buffer is None
    assert error["error"]["kind"] == "cache_miss"
    assert error["error"]["checksum"] == missing.hex()


def test_transformation_input_cache_miss_keeps_its_type(monkeypatch):
    missing = Checksum("e" * 64)
    envelope = {
        "kind": "cache_miss",
        "message": missing.hex(),
        "checksum": missing.hex(),
    }
    monkeypatch.setattr(
        transformer_client,
        "get_seamless_dask_client",
        lambda: object(),
    )
    payload = {
        "tf_checksum": "d" * 64,
        "record": False,
        "inputs": [
            {
                "name": "projected",
                "celltype": "int",
                "subcelltype": None,
                "checksum": None,
                "kind": "expression",
            }
        ],
    }

    result = dask_client._run_base(
        payload,
        {"projected": (missing.hex(), None, envelope)},
    )

    assert result == ("d" * 64, None, None, envelope)


def test_dask_api_records_typed_expression_input_error():
    import asyncio
    import concurrent.futures
    from types import SimpleNamespace
    from seamless.error_envelope import encode_error
    from seamless_dask.transformation_mixin import TransformationDaskMixin

    missing = Checksum("c" * 64)
    future = concurrent.futures.Future()
    future.set_result(("d" * 64, None, encode_error(CacheMissError(missing))))

    class Subject(TransformationDaskMixin):
        _constructed = False
        _dask_client = lambda self: object()
        _remote_storage_error = lambda self: None
        _compute_tf_checksum_no_deps = lambda self: None
        _skip_permission_gate = lambda self: True
        _ensure_record_bucket_preflight_sync = lambda self: None
        _build_dask_submission = lambda self, *args, **kwargs: None
        _ensure_dask_futures = lambda self, *args, **kwargs: SimpleNamespace(thin=future)

        async def _ensure_record_bucket_preflight_async(self):
            pass

    for asynchronous in (False, True):
        subject = Subject()
        if asynchronous:
            result = asyncio.run(subject._compute_with_dask_async(False))
        else:
            result = subject._compute_with_dask(False)
        assert result is None
        assert isinstance(subject._exception, CacheMissError)
        assert isinstance(subject._exception.args[0], Checksum)
        assert subject._exception.args[0] == missing
        assert subject._exception.__traceback__ is None


def test_checksum_only_reference_does_not_promise_an_upload(monkeypatch):
    from types import SimpleNamespace
    from seamless_remote import buffer_remote

    calls = []
    async def promise(checksum):
        calls.append(checksum)

    remote = SimpleNamespace(url="http://unused", promise=promise)
    monkeypatch.setattr(buffer_remote, "_write_server_clients", [remote])
    client = SimpleNamespace(_promised_targets={})
    missing = Checksum("a" * 64)
    missing.incref_refholder()
    try:
        dask_client.SeamlessDaskClient._ensure_promised(client, missing.hex())
        assert calls == []
        buffer = Buffer(b"an uploadable local buffer")
        dask_client.SeamlessDaskClient._ensure_promised(client, buffer.get_checksum().hex())
        assert calls == [buffer.get_checksum()]
    finally:
        missing.decref_refholder()
