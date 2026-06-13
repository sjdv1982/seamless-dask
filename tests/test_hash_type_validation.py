from seamless import Buffer
from seamless_dask import client as dask_client
import seamless_dask.transformer_client as transformer_client


def test_run_base_rejects_incompatible_input_hash_type_before_dispatch(monkeypatch):
    monkeypatch.setattr(
        transformer_client, "get_seamless_dask_client", lambda: object()
    )
    input_buffer = Buffer(b"\xff\xfe\x00")
    input_checksum = input_buffer.get_checksum()

    result = dask_client._run_base(
        {
            "tf_checksum": None,
            "transformation_dict": {
                "__language__": "python",
                "__output__": ("result", "int", None),
                "value": ("text", None, None),
            },
            "inputs": [
                {
                    "name": "value",
                    "celltype": "text",
                    "subcelltype": None,
                    "checksum": None,
                    "kind": "checksum",
                }
            ],
            "tf_dunder": {},
            "scratch": False,
            "require_value": False,
        },
        {"value": (input_checksum.hex(), input_buffer, None)},
    )

    assert result[1] is None
    assert "Cannot deserialize" in result[3]
