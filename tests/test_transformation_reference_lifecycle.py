from __future__ import annotations

import asyncio
from concurrent.futures import Future
import gc
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from seamless import Buffer, CacheMissError
from seamless.caching.buffer_cache import get_buffer_cache
from seamless.transformer import delayed
from seamless_dask.transformation_mixin import (
    _publish_definition_for_dask,
    _publish_result_for_dask,
)
from seamless_dask.types import TransformationFutures
from seamless_transformer.transformation_cache import TransformationCancelledError

sys.path.insert(0, str(Path(__file__).parent / "helpers"))
from reference_lifecycle import force_expiry  # noqa: E402


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


class _ImmediateFuture:
    """Small Future-shaped object used by the actual mixin execution paths."""

    def __init__(self, value):
        self._value = value
        self._released = False

    def result(self):
        return self._value

    def add_done_callback(self, callback):
        # The callback is intentionally retained as an execution event; the
        # mixin still receives a real future-shaped object, not a helper spy.
        self._callback = callback

    def done(self):
        return True

    def cancelled(self):
        return False

    def release(self):
        self._released = True


class _FakeDaskClient:
    def __init__(self, result_checksum=None, error=None):
        self.result_checksum = result_checksum
        self.error = error
        self.submissions = []
        self.fat_requests = 0
        self.released = []

    def _cached_transformation_for_submission(self, submission, member_id=None):
        return None

    def get_fat_checksum_future(self, checksum):
        return _ImmediateFuture(checksum)

    def get_fat_finger_checksum_future(self, checksum):
        return _ImmediateFuture(checksum)

    def ensure_fat_future(self, futures):
        self.fat_requests += 1
        futures.fat = _ImmediateFuture((futures.tf_checksum, futures.result_checksum))
        return futures.fat

    def ensure_fat_finger_future(self, futures):
        self.fat_requests += 1
        futures.fat = _ImmediateFuture((futures.tf_checksum, futures.result_checksum))
        return futures.fat

    def submit_transformation(self, submission, *, need_fat=False, member_id=None):
        self.submissions.append(submission)
        base = _ImmediateFuture(None)
        thin = _ImmediateFuture(
            (submission.tf_checksum, self.result_checksum, self.error)
        )
        # Let the mixin exercise its real ensure_fat_future branch even when
        # the submission itself requested a fat value.
        fat = None
        return TransformationFutures(
            base=base,
            fat=fat,
            thin=thin,
            tf_checksum=submission.tf_checksum,
            result_checksum=self.result_checksum,
        )

    def release_transformation_futures(self, futures, *, cancel, member_id=None):
        self.released.append((futures, cancel, member_id))


def _fake_transformation(monkeypatch, client, *, value=None, scratch=False):
    builder = delayed(identity)
    builder.scratch = scratch
    transformation = builder(value if value is not None else f"dask-{uuid4().hex}")
    monkeypatch.setattr(transformation, "_dask_client", lambda: client)
    monkeypatch.setattr(transformation, "_skip_permission_gate", lambda: True)
    monkeypatch.setattr(
        transformation,
        "_try_database_cache_sync",
        lambda tf_checksum, require_value: None,
    )
    return transformation


def test_actual_cached_result_branch_publishes_definition_and_result(monkeypatch):
    result_buffer = Buffer(f"cached-{uuid4().hex}", "text")
    result = result_buffer.get_checksum()
    client = _FakeDaskClient(result_checksum=result)
    transformation = _fake_transformation(monkeypatch, client)
    monkeypatch.setattr(
        transformation,
        "_try_database_cache_sync",
        lambda tf_checksum, require_value: result,
    )
    assert transformation._compute_with_dask(require_value=False) == result
    assert transformation._transformation_checksum is not None
    assert transformation._result_checksum_internal() == result
    assert transformation._evaluated is True
    assert not client.submissions
    assert get_buffer_cache().reference_snapshot()[transformation._transformation_checksum][0] == 1
    assert get_buffer_cache().reference_snapshot().get(result, (0, 0, False))[0] == 0
    transformation.buffer
    assert get_buffer_cache().reference_snapshot()[result][0] == 1
    transformation._release_refholds()


def test_actual_thin_success_uses_owner_publication_and_neutral_result(monkeypatch):
    result_buffer = Buffer(f"thin-{uuid4().hex}", "text")
    result = result_buffer.get_checksum()
    client = _FakeDaskClient(result_checksum=result)
    transformation = _fake_transformation(monkeypatch, client)
    definition_calls = []
    result_calls = []
    original_definition = transformation._publish_definition
    original_result = transformation._publish_result
    monkeypatch.setattr(
        transformation,
        "_publish_definition",
        lambda checksum: (definition_calls.append(checksum), original_definition(checksum))[1],
    )
    monkeypatch.setattr(
        transformation,
        "_publish_result",
        lambda checksum: (result_calls.append(checksum), original_result(checksum))[1],
    )
    assert transformation._compute_with_dask(require_value=False) == result
    assert definition_calls
    assert len(set(definition_calls)) == 1
    assert result_calls == [result]
    assert get_buffer_cache().reference_snapshot().get(result, (0, 0, False))[0] == 0
    transformation.buffer
    assert get_buffer_cache().reference_snapshot()[result][0] == 1
    transformation._release_refholds()


def test_actual_fat_acquisition_and_scratch_publication_paths(monkeypatch):
    result_buffer = Buffer(f"fat-{uuid4().hex}", "text")
    result = result_buffer.get_checksum()
    client = _FakeDaskClient(result_checksum=result)
    transformation = _fake_transformation(monkeypatch, client, scratch=True)
    futures = transformation._ensure_dask_futures(client, require_value=True, need_fat=True)
    assert futures.fat is not None
    assert client.fat_requests == 1
    assert client.submissions

    transformation._publish_definition(futures.tf_checksum)
    transformation._publish_result(result)
    definition = transformation._transformation_checksum_internal()
    assert definition is not None
    cache = get_buffer_cache()
    assert cache.is_scratch_ref(definition) is True
    assert cache.is_scratch_ref(result) is True
    assert cache.reference_snapshot().get(result, (0, 0, False))[0] == 0
    assert cache.purge_scratch(result) == 1
    transformation._release_refholds()


def test_public_after_neutral_dask_publication_acquires_once(monkeypatch):
    result_buffer = Buffer(f"public-after-dask-{uuid4().hex}", "text")
    result = result_buffer.get_checksum()
    client = _FakeDaskClient(result_checksum=result)
    transformation = _fake_transformation(monkeypatch, client)
    assert transformation._compute_with_dask(require_value=False) == result
    assert get_buffer_cache().reference_snapshot().get(result, (0, 0, False))[0] == 0
    transformation.buffer
    transformation.buffer
    assert get_buffer_cache().reference_snapshot()[result][0] == 1
    transformation._release_refholds()


def test_dask_failure_publishes_no_result_role(monkeypatch):
    client = _FakeDaskClient(error="worker failed")
    transformation = _fake_transformation(monkeypatch, client)
    assert transformation._compute_with_dask(require_value=False) is None
    assert transformation._result_checksum_internal() is None
    assert not any(role == "result" for _, role in transformation._refheld_checksums())
    transformation._release_refholds()


def test_dask_cancellation_blocks_late_completion_and_releases_roles(monkeypatch):
    result_buffer = Buffer(f"late-dask-{uuid4().hex}", "text")
    late = result_buffer.get_checksum()
    client = _FakeDaskClient(error="Transformation was canceled")
    transformation = _fake_transformation(monkeypatch, client)
    with pytest.raises(TransformationCancelledError):
        transformation._compute_with_dask(require_value=False)
    assert transformation._cancelled is True
    assert not transformation._refheld_checksums()
    assert transformation._publish_result(late) is None
    assert get_buffer_cache().reference_snapshot().get(late, (0, 0, False))[0] == 0


def test_dependency_evaluation_path_remains_neutral(monkeypatch):
    result_buffer = Buffer(f"dependency-dask-{uuid4().hex}", "text")
    result = result_buffer.get_checksum()
    client = _FakeDaskClient(result_checksum=result)
    transformation = _fake_transformation(monkeypatch, client)
    assert transformation._compute_with_dask(require_value=False) == result
    assert not any(role == "result" for _, role in transformation._refheld_checksums())
    assert get_buffer_cache().reference_snapshot().get(result, (0, 0, False))[0] == 0
    transformation._release_refholds()


def test_actual_dask_result_survives_forced_expiry_with_unowned_control(monkeypatch):
    result_buffer = Buffer(f"forced-dask-{uuid4().hex}", "text")
    result = result_buffer.get_checksum()
    client = _FakeDaskClient(result_checksum=result)
    transformation = _fake_transformation(monkeypatch, client)
    assert transformation._compute_with_dask(require_value=False) == result
    transformation.buffer
    del result_buffer
    gc.collect()

    evidence = force_expiry(result)
    assert evidence.after["accounting"] == (1, 0, True)
    assert result.resolve("text").startswith("forced-dask-")

    transformation._release_refholds()
    force_expiry(result)
    try:
        import seamless_remote.buffer_remote as buffer_remote
    except ImportError:
        buffer_remote = None
    if buffer_remote is not None:
        async def missing(checksum):
            return None

        monkeypatch.setattr(buffer_remote, "get_buffer", missing)
    with pytest.raises(CacheMissError):
        result.resolve("text")
