import threading
import sys

import pytest

pytest.importorskip("dask")
pytest.importorskip("distributed")

from seamless import Checksum

from seamless_dask.client import (
    SeamlessDaskClient,
    _is_submission_cancelled,
    _mark_cancelled_submission,
    _normalized_dunder_envelope_checksum,
)
from seamless_dask.types import TransformationFutures, TransformationSubmission

dask_client_module = sys.modules["seamless_dask.client"]


class FakeFuture:
    def __init__(self, *, done=False, cancelled=False, result=None):
        self._done = done
        self._cancelled = cancelled
        self._result = result

    def done(self):
        return self._done

    def cancelled(self):
        return self._cancelled

    def result(self):
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result

    def add_done_callback(self, callback):
        if self._done:
            callback(self)

    def release(self):
        pass


class FakeDaskClient:
    def __init__(self):
        self.cancelled = []
        self.scheduler = type("_FakeScheduler", (), {})()

    def cancel(self, future, force=False):
        future._cancelled = True
        self.cancelled.append((future, force))

    def run_on_scheduler(self, fn, **kwargs):
        return fn(self.scheduler, **kwargs)


def _client():
    client = SeamlessDaskClient.__new__(SeamlessDaskClient)
    client._client = FakeDaskClient()
    client._cache_lock = threading.RLock()
    client._transformation_cache = {}
    client._active_transformation_envelopes = {}
    client._released_transformation_futures = set()
    client._fat_future_ttl = 10.0
    return client


def _submission(tf_checksum, *, meta=None, strict=False):
    return TransformationSubmission(
        transformation_dict={"__output__": ("result", "mixed", None)},
        inputs={},
        input_futures={},
        tf_checksum=tf_checksum,
        tf_dunder={"__meta__": meta or {}},
        scratch=False,
        meta=meta or {},
        strict_dunder=strict,
    )


def test_dask_same_checksum_different_dunder_latches_by_default():
    client = _client()
    active = _submission("a" * 64, meta={"local": False})
    futures = TransformationFutures(
        base=FakeFuture(done=False),
        thin=FakeFuture(done=False),
        fat=None,
        tf_checksum=active.tf_checksum,
    )
    client._store_transformation(
        active.tf_checksum,
        futures,
        envelope_checksum=_normalized_dunder_envelope_checksum(active),
    )

    latcher = _submission("a" * 64, meta={"local": True})

    assert client._cached_transformation_for_submission(latcher) is futures


def test_dask_cached_transformation_adds_latcher_member():
    client = _client()
    active = _submission("1" * 64, meta={"local": False})
    futures = TransformationFutures(
        base=FakeFuture(done=False),
        thin=FakeFuture(done=False),
        fat=None,
        tf_checksum=active.tf_checksum,
    )
    futures.members.add("first")
    client._store_transformation(
        active.tf_checksum,
        futures,
        envelope_checksum=_normalized_dunder_envelope_checksum(active),
    )

    latcher = _submission("1" * 64, meta={"local": True})

    assert (
        client._cached_transformation_for_submission(latcher, member_id="second")
        is futures
    )
    assert futures.members == {"first", "second"}


def test_dask_softcancel_one_member_leaves_futures_alive():
    client = _client()
    active = _submission("2" * 64, meta={"local": False})
    futures = TransformationFutures(
        base=FakeFuture(done=False),
        thin=FakeFuture(done=False),
        fat=None,
        tf_checksum=active.tf_checksum,
        submission_id="soft-token",
    )
    futures.members.update({"first", "second"})
    client._store_transformation(
        active.tf_checksum,
        futures,
        envelope_checksum=_normalized_dunder_envelope_checksum(active),
    )

    assert client.softcancel_by_checksum(active.tf_checksum, "first") is True
    assert active.tf_checksum in client._transformation_cache
    assert futures.members == {"second"}
    assert not _is_submission_cancelled(client._client, futures.submission_id)


def test_dask_softcancel_last_member_detaches_without_scheduler_cancel():
    client = _client()
    active = _submission("3" * 64, meta={"local": False})
    futures = TransformationFutures(
        base=FakeFuture(done=False),
        thin=FakeFuture(done=False),
        fat=None,
        tf_checksum=active.tf_checksum,
        submission_id="last-token",
    )
    futures.members.add("last")
    client._store_transformation(
        active.tf_checksum,
        futures,
        envelope_checksum=_normalized_dunder_envelope_checksum(active),
    )

    assert client.softcancel_by_checksum(active.tf_checksum, "last") is True
    assert active.tf_checksum not in client._transformation_cache
    # Soft cancellation only drops this process's interest.  Hard cancellation
    # is the operation that marks a shared scheduler submission canceled.
    assert not _is_submission_cancelled(client._client, futures.submission_id)


def test_dask_strict_different_dunder_rejects_only_while_active():
    client = _client()
    active = _submission("b" * 64, meta={"local": False})
    futures = TransformationFutures(
        base=FakeFuture(done=False),
        thin=FakeFuture(done=False),
        fat=None,
        tf_checksum=active.tf_checksum,
    )
    client._store_transformation(
        active.tf_checksum,
        futures,
        envelope_checksum=_normalized_dunder_envelope_checksum(active),
    )

    strict_latcher = _submission("b" * 64, meta={"local": True}, strict=True)
    with pytest.raises(RuntimeError, match="different dunder envelope"):
        client._cached_transformation_for_submission(strict_latcher)

    futures.base._done = True
    futures.thin._done = True

    assert client._cached_transformation_for_submission(strict_latcher) is futures


def test_dask_cancel_by_checksum_marks_and_releases_active_submission():
    client = _client()
    active = _submission("c" * 64, meta={"local": False})
    futures = TransformationFutures(
        base=FakeFuture(done=False),
        thin=FakeFuture(done=False),
        fat=None,
        tf_checksum=active.tf_checksum,
        submission_id="cancel-by-checksum-token",
    )
    client._store_transformation(
        active.tf_checksum,
        futures,
        envelope_checksum=_normalized_dunder_envelope_checksum(active),
    )

    assert client.cancel_by_checksum(active.tf_checksum) is True
    assert client.cancel_by_checksum(active.tf_checksum) is False
    assert active.tf_checksum not in client._transformation_cache
    assert active.tf_checksum not in client._active_transformation_envelopes
    assert futures.members == set()
    assert not client._client.cancelled
    assert _is_submission_cancelled(client._client, futures.submission_id)


def test_dask_late_completion_after_cancel_does_not_restore_cache_entry():
    client = _client()
    active = _submission("d" * 64, meta={"local": False})
    futures = TransformationFutures(
        base=FakeFuture(done=False),
        thin=FakeFuture(done=False),
        fat=None,
        tf_checksum=active.tf_checksum,
    )
    envelope_checksum = _normalized_dunder_envelope_checksum(active)
    client._store_transformation(
        active.tf_checksum,
        futures,
        envelope_checksum=envelope_checksum,
    )

    assert client.cancel_by_checksum(active.tf_checksum) is True

    late_thin = FakeFuture(
        done=True,
        result=(active.tf_checksum, "e" * 64, None),
    )
    client._register_transformation(
        active.tf_checksum,
        futures,
        late_thin,
        store_cache=True,
        envelope_checksum=envelope_checksum,
    )

    assert active.tf_checksum not in client._transformation_cache
    assert client._cached_transformation_for_submission(active) is None


def test_dask_base_skips_database_write_after_submission_cancel(monkeypatch):
    writes = []
    submission_id = "cancel-token"
    tf_checksum = Checksum("f" * 64)
    result_checksum = Checksum("e" * 64)

    async def _no_cached_result(*_args, **_kwargs):
        return None

    async def _dispatch_result(*_args, **_kwargs):
        return result_checksum

    async def _no_buffer(*_args, **_kwargs):
        return None

    async def _write_result(*args, **kwargs):
        writes.append((args, kwargs))

    class _FakeInnerClient:
        scheduler = type("_FakeScheduler", (), {})()

        def run_on_scheduler(self, fn, **kwargs):
            return fn(self.scheduler, **kwargs)

    _mark_cancelled_submission(
        _FakeInnerClient.scheduler,
        submission_id=submission_id,
    )

    class _FakeSeamlessDaskClient:
        client = _FakeInnerClient()

    monkeypatch.setattr(
        dask_client_module,
        "_fetch_cached_result_async",
        _no_cached_result,
    )
    monkeypatch.setattr(
        dask_client_module.transformer_worker,
        "dispatch_to_workers",
        _dispatch_result,
    )
    monkeypatch.setattr(
        dask_client_module,
        "_resolve_buffer_async",
        _no_buffer,
    )
    monkeypatch.setattr(
        dask_client_module,
        "_promise_and_write_result_async",
        _write_result,
    )
    monkeypatch.setattr(
        "seamless_dask.transformer_client.get_seamless_dask_client",
        lambda: _FakeSeamlessDaskClient(),
    )

    tf_hex, result_hex, _buffer, exc = dask_client_module._run_base(
        {
            "transformation_dict": {
                "__language__": "bash",
                "__output__": ("result", "bytes", None),
                "code": ("text", None, "1" * 64),
            },
            "inputs": [],
            "tf_checksum": tf_checksum.hex(),
            "tf_dunder": {},
            "scratch": False,
            "require_value": False,
            "record": False,
            "submission_id": submission_id,
        },
        {},
    )

    assert tf_hex == tf_checksum.hex()
    assert result_hex is None
    assert exc == "Transformation was canceled"
    assert writes == []
