import threading

import pytest

pytest.importorskip("dask")
pytest.importorskip("distributed")

from seamless_dask.client import (
    SeamlessDaskClient,
    _normalized_dunder_envelope_checksum,
)
from seamless_dask.types import TransformationFutures, TransformationSubmission


class FakeFuture:
    def __init__(self, *, done=False, cancelled=False):
        self._done = done
        self._cancelled = cancelled

    def done(self):
        return self._done

    def cancelled(self):
        return self._cancelled

    def add_done_callback(self, callback):
        if self._done:
            callback(self)

    def release(self):
        pass


class FakeDaskClient:
    def __init__(self):
        self.cancelled = []

    def cancel(self, future, force=False):
        future._cancelled = True
        self.cancelled.append((future, force))


def _client():
    client = SeamlessDaskClient.__new__(SeamlessDaskClient)
    client._client = FakeDaskClient()
    client._cache_lock = threading.RLock()
    client._transformation_cache = {}
    client._active_transformation_envelopes = {}
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


def test_dask_cancel_by_checksum_releases_active_submission():
    client = _client()
    active = _submission("c" * 64, meta={"local": False})
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

    assert client.cancel_by_checksum(active.tf_checksum) is True
    assert client.cancel_by_checksum(active.tf_checksum) is False
    assert active.tf_checksum not in client._transformation_cache
    assert active.tf_checksum not in client._active_transformation_envelopes
    assert client._client.cancelled
