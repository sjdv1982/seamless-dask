"""Converted pin dependencies through the Dask scheduling and worker paths."""
import seamless
from seamless import Buffer, Cell
from seamless.transformer import delayed
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client


def test_converted_pin_dependencies_and_null_boundaries():
    client = create_dummy_client(workers=1, worker_threads=2, spawn_workers=2)
    set_seamless_dask_client(client)
    try:
        @delayed
        def source(value):
            return value

        @delayed
        def consume(value):
            return value

        source.celltypes.value = source.celltypes.result = 'str'
        consume.celltypes.value = consume.celltypes.result = 'text'
        converted = consume(source('hello dask'))
        assert converted.run() == 'hello dask'
        payload = converted.construct().resolve('plain')
        assert payload['value'][2] == Buffer('hello dask', 'text').get_checksum().hex()

        consume.celltypes.value = consume.celltypes.result = 'int'
        converted = consume(source('42'))
        assert converted.run() == 42
        payload = converted.construct().resolve('plain')
        # str -> int keeps the checksum: the int pin holds the str's checksum.
        assert payload['value'][2] == Buffer('42', 'str').get_checksum().hex()

        cell = Cell('str')
        cell.set('43')
        assert consume(cell.build()).run() == 43

        @delayed
        def null_source():
            return None

        @delayed
        def optional(value=None):
            return value is None

        null_source.celltypes.result = 'plain'
        optional.celltypes.value = 'binary'
        optional.optional_pins.value.enable()
        absent = optional()
        connected = optional(null_source())
        assert connected.run() is True
        assert connected.construct() == absent.construct()
        invalid = consume(null_source())
        assert invalid.construct() is None
        assert "Required pin 'value'" in invalid.exception
    finally:
        set_seamless_dask_client(None)
        seamless.close()
