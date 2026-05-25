from __future__ import annotations

import threading

from seamless_dask.client import SeamlessDaskClient
from seamless_dask.types import TransformationFutures


class _FakeTopicClient:
    def __init__(self) -> None:
        self.subscriptions = {}
        self.unsubscribed = []
        self.cancelled = []

    def subscribe_topic(self, topic, handler):
        self.subscriptions[topic] = handler

    def unsubscribe_topic(self, topic):
        self.unsubscribed.append(topic)

    def cancel(self, future, force=True):
        self.cancelled.append((future, force))


class _FakeFuture:
    def __init__(self, *, done=False) -> None:
        self._done = done
        self.released = False

    def cancelled(self):
        return False

    def done(self):
        return self._done

    def release(self):
        self.released = True


def _make_client() -> SeamlessDaskClient:
    client = SeamlessDaskClient.__new__(SeamlessDaskClient)
    client._client = _FakeTopicClient()
    client._stream_lock = threading.RLock()
    client._active_stream_topics = set()
    client._stream_topic_cleanups = {}
    client._cache_lock = threading.RLock()
    client._transformation_cache = {}
    client._fat_checksum_cache = {}
    client._fat_finger_checksum_cache = {}
    return client


def test_stream_topic_handler_prints_prefixed_stdout_and_stderr(capsys) -> None:
    client = _make_client()

    topic = client._subscribe_stream_topic("base-abcdef123456")
    handler = client._client.subscriptions[topic]
    handler((1.0, {"kind": "stream", "stream": "stdout", "text": "hello\n"}))
    handler((1.0, {"kind": "stream", "stream": "stderr", "text": "oops"}))

    captured = capsys.readouterr()
    assert "[base-abcdef1] hello\n" in captured.out
    assert "[base-abcdef1] oops" in captured.err


def test_stream_topic_handler_reports_truncation(capsys) -> None:
    client = _make_client()

    topic = client._subscribe_stream_topic("base-abcdef123456")
    handler = client._client.subscriptions[topic]
    handler(
        (
            1.0,
            {
                "kind": "stream",
                "stream": "stdout",
                "text": "tail",
                "truncated_head_bytes": 12,
            },
        )
    )

    captured = capsys.readouterr()
    assert "<truncated 12 bytes>" in captured.out
    assert "tail" in captured.out


def test_release_transformation_futures_unsubscribes_stream_topic() -> None:
    client = _make_client()
    topic = client._subscribe_stream_topic("base-abcdef123456")
    future = _FakeFuture(done=True)
    futures = TransformationFutures(
        base=future,
        fat=None,
        thin=future,
        tf_checksum="1" * 64,
        stream_topic=topic,
    )

    client.release_transformation_futures(futures, cancel=True)

    assert client._client.unsubscribed == [topic]
    assert topic not in client._active_stream_topics
    assert future.released
