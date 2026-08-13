from __future__ import annotations

from seamless import Buffer
from seamless.caching.buffer_cache import get_buffer_cache
from seamless.transformer import delayed
from seamless_dask.transformation_mixin import (
    _publish_definition_for_dask,
    _publish_result_for_dask,
)


def identity(value):
    return value


def test_dask_publication_helpers_route_through_owner_lifecycle():
    class Owner:
        def __init__(self):
            self.definition = None
            self.result = None

        def _publish_definition(self, checksum):
            self.definition = checksum
            return checksum

        def _publish_result(self, checksum):
            self.result = checksum
            return checksum

    owner = Owner()
    definition = Buffer(b"dask-definition").get_checksum()
    result = Buffer(b"dask-result").get_checksum()
    assert _publish_definition_for_dask(owner, definition) == definition
    assert _publish_result_for_dask(owner, result) == result
    assert owner.definition == definition
    assert owner.result == result


def test_dask_neutral_publication_then_public_access_has_one_result_claim():
    transformation = delayed(identity)(1)
    result_buffer = Buffer(23, "int")
    result = result_buffer.get_checksum()
    _publish_result_for_dask(transformation, result)
    assert get_buffer_cache().reference_snapshot().get(result, (0, 0, False))[0] == 0
    assert transformation.buffer.get_value("int") == 23
    assert get_buffer_cache().reference_snapshot()[result][0] == 1
    transformation._release_refholds()
