from seamless import Buffer
from seamless_dask import client as dask_client


def test_expression_task_returns_fat_input_tuple():
    source = Buffer({"value": 42}, "plain")
    source_checksum = source.get_checksum()
    source.tempref()

    result_checksum, result_buffer, error = dask_client._expression_task(
        {
            "path": "value",
            "celltype": "plain",
            "target_celltype": "int",
            "validator": None,
            "validator_language": None,
        },
        (source_checksum.hex(), source, None),
    )

    assert error is None
    assert result_checksum is not None
    assert isinstance(result_buffer, Buffer)
    assert result_buffer.get_value("int") == 42
