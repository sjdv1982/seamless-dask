"""Phase-one acceptance tests derived from streaming-plan.md and its contract.

These exercise transport and public submission behavior as well as bounded
capture. No external Dask/database services are required.
"""

import asyncio
import io
import multiprocessing
import threading
import time
import uuid
from types import SimpleNamespace

import pytest

import seamless
from seamless import Buffer
from seamless.transformer import delayed
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client


@pytest.fixture(scope="session", autouse=True)
def _close_seamless_session():
    """Seamless cannot be reopened; close it only after all selected tests."""
    yield
    seamless.close()


def test_channel_events_are_one_way_and_missing_handlers_are_harmless():
    from seamless_transformer.process.channel import Endpoint

    async def exercise():
        left_conn, right_conn = multiprocessing.Pipe()
        left, right = Endpoint(left_conn), Endpoint(right_conn)
        received = []
        ready = asyncio.Event()

        def handler(payload):
            received.append(payload)
            ready.set()

        right.add_event_handler("stream_chunk", handler)
        right.add_request_handler("ping", lambda payload: payload)
        try:
            await left.notify("stream_chunk", {"text": "hello"})
            await asyncio.wait_for(ready.wait(), 3)
            assert received == [{"text": "hello"}]
            assert not left._pending, "notifications must not allocate reply futures"
            await left.notify("unregistered", {"late": True})
            assert await asyncio.wait_for(left.request("ping", "alive"), 3) == "alive"
        finally:
            left.close()
            right.close()
            await asyncio.gather(left.wait_closed(), right.wait_closed())

    asyncio.run(exercise())


@pytest.mark.parametrize("text", ["HEAD-" + "x" * 200_000 + "-TAIL", "HEAD-" + "é🙂" * 20_000 + "-TAIL"])
def test_capture_keeps_full_exception_sink_but_only_streams_bounded_tail(text):
    from seamless_transformer.stream_capture import _StreamingTap

    sink = io.BytesIO()
    chunks = []
    tap = _StreamingTap(stream_name="stdout", sink=sink, notifier=chunks.append,
                        max_payload=8192, min_interval=2.0)
    assert tap.write(text) == len(text)
    tap.close()  # final output must never wait for another cadence tick
    assert sink.getvalue().decode("utf-8") == text
    assert chunks
    assert all(len(chunk["text"].encode("utf-8")) <= 8192 for chunk in chunks)
    assert chunks[-1]["text"].endswith("-TAIL")
    assert sum(chunk["truncated_head_bytes"] for chunk in chunks) > 0
    assert all(chunk["stream"] == "stdout" for chunk in chunks)
    assert [chunk["seq"] for chunk in chunks] == sorted({chunk["seq"] for chunk in chunks})
    assert all(isinstance(chunk["ts"], (int, float)) for chunk in chunks)


def test_capture_coalesces_fast_writes_and_forces_final_stderr():
    from seamless_transformer.stream_capture import _StreamingTap

    chunks = []
    tap = _StreamingTap(stream_name="stderr", sink=io.BytesIO(),
                        notifier=chunks.append, max_payload=8192, min_interval=2.0)
    for _ in range(100):
        tap.write("x")
    tap._maybe_flush()
    tap._maybe_flush()
    assert len(chunks) <= 1
    tap.write("last line\n")
    tap.close()
    assert "".join(chunk["text"] for chunk in chunks) == "x" * 100 + "last line\n"


def test_capture_pushed_throttle_updates_active_tap():
    from seamless_transformer.stream_capture import _StreamingTap

    chunks = []
    sink = io.BytesIO()
    tap = _StreamingTap(stream_name="stdout", sink=sink, notifier=chunks.append,
                        max_payload=8192, min_interval=2.0)
    tap.write("first")
    tap._maybe_flush()
    chunks.clear()
    tap.write("x" * 7000 + "-TAIL")
    tap.update_throttle(max_payload=1024, min_interval=30.0)
    tap.close()
    assert chunks
    assert all(len(chunk["text"].encode("utf-8")) <= 1024 for chunk in chunks)
    assert chunks[-1]["text"].endswith("-TAIL")
    assert sum(chunk["truncated_head_bytes"] for chunk in chunks) > 0
    assert sink.getvalue().decode("utf-8").endswith("x" * 7000 + "-TAIL")


def test_capture_close_cannot_overtake_an_inflight_cadence_notification():
    from seamless_transformer.stream_capture import _StreamingTap

    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    chunks, errors = [], []

    def notify(chunk):
        if not chunks:
            entered.set()
            assert release.wait(3), "test did not release the first notification"
        chunks.append(chunk)

    tap = _StreamingTap(stream_name="stdout", sink=io.BytesIO(), notifier=notify,
                        max_payload=8192, min_interval=2.0)
    tap.write("first")

    def cadence():
        try:
            tap._maybe_flush()
        except BaseException as error:
            errors.append(error)

    def finish():
        try:
            tap.write("last")
            tap.close()
            closed.set()
        except BaseException as error:
            errors.append(error)

    flush_thread = threading.Thread(target=cadence)
    close_thread = threading.Thread(target=finish)
    flush_thread.start()
    try:
        assert entered.wait(3), "cadence notification did not start"
        close_thread.start()
        assert not closed.wait(0.05), "close returned before the earlier notification"
    finally:
        release.set()
        flush_thread.join(3)
        if close_thread.ident is not None:
            close_thread.join(3)
    assert not flush_thread.is_alive() and not close_thread.is_alive()
    assert errors == []
    assert closed.is_set()
    assert "".join(chunk["text"] for chunk in chunks) == "firstlast"
    assert [chunk["seq"] for chunk in chunks] == sorted({chunk["seq"] for chunk in chunks})


def test_streaming_api_and_submission_do_not_change_identity():
    @delayed
    def identity(value):
        return value

    tf = identity(123)
    assert tf.streaming is False
    client = SimpleNamespace(get_fat_checksum_future=lambda checksum: object())
    off = tf._build_dask_submission(client, require_value=False, need_fat=False)
    tf.streaming = 1
    assert tf.streaming is True
    on = tf._build_dask_submission(client, require_value=False, need_fat=False)
    assert off.streaming is False and on.streaming is True
    assert off.tf_checksum == on.tf_checksum
    assert off.transformation_dict == on.transformation_dict
    assert off.tf_dunder == on.tf_dunder
    assert "streaming" not in on.transformation_dict
    assert "streaming" not in on.tf_dunder
    assert "streaming" not in on.meta
    tf.streaming = 0
    assert tf.streaming is False


def test_streaming_database_cache_fast_path_never_submits(monkeypatch):
    @delayed
    def identity(value):
        return value

    tf = identity(456)
    tf.streaming = True
    result = Buffer(456, "mixed").get_checksum()
    monkeypatch.setattr(tf, "_dask_client", lambda: object())
    monkeypatch.setattr(tf, "_remote_storage_error", lambda: None)
    monkeypatch.setattr(tf, "_try_database_cache_sync", lambda *args, **kwargs: result)

    def forbidden(*args, **kwargs):
        raise AssertionError("a cache hit must not create Dask futures or streaming topics")

    monkeypatch.setattr(tf, "_ensure_dask_futures", forbidden)
    assert tf._compute_with_dask(require_value=False) == result
    assert tf._dask_futures is None


@pytest.fixture
def stream_client(monkeypatch):
    client = create_dummy_client(workers=1, worker_threads=4, spawn_workers=2)
    set_seamless_dask_client(client)
    events, subscribed, unsubscribed = [], [], []
    subscribe, unsubscribe = client.client.subscribe_topic, client.client.unsubscribe_topic

    def record_subscribe(topic, handler):
        if topic.startswith("seamless-stream-") and topic != "seamless-stream-throttle":
            subscribed.append(topic)

            def wrapped(event):
                events.append((topic, event[1], time.monotonic()))
                return handler(event)

            return subscribe(topic, wrapped)
        return subscribe(topic, handler)

    def record_unsubscribe(topic):
        unsubscribed.append(topic)
        return unsubscribe(topic)

    monkeypatch.setattr(client.client, "subscribe_topic", record_subscribe)
    monkeypatch.setattr(client.client, "unsubscribe_topic", record_unsubscribe)
    try:
        yield client, events, subscribed, unsubscribed
    finally:
        set_seamless_dask_client(None)


def _wait_for(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    assert predicate(), "expected streaming event or cleanup did not arrive"


def test_live_stdout_stderr_precede_completion_and_keep_prefix(stream_client, capsys):
    client, events, subscribed, unsubscribed = stream_client

    @delayed
    def talk(token):
        import sys
        import time

        print("hello-" + token, flush=True)
        print("error-" + token, file=sys.stderr, flush=True)
        time.sleep(3)
        print("world-" + token, flush=True)
        return token

    token = uuid.uuid4().hex
    tf = talk(token)
    tf.streaming = True
    finished = threading.Event()
    before_completion = []
    subscribe = client.client.subscribe_topic

    def watch(topic, handler):
        def wrapped(event):
            before_completion.append(not finished.is_set())
            return handler(event)
        return subscribe(topic, wrapped)

    client.client.subscribe_topic = watch
    try:
        assert tf.run() == token
    finally:
        finished.set()
    completed_at = time.monotonic()
    _wait_for(lambda: any("world-" + token in e[1].get("text", "") for e in events))
    assert any(before_completion), "all output arrived only after completion"
    assert any("hello-" + token in e[1].get("text", "") for e in events)
    assert any("hello-" + token in e[1].get("text", "") and e[2] < completed_at - 0.5 for e in events), "hello must be delivered during the three-second sleep"
    assert any(e[1].get("stream") == "stderr" for e in events)
    assert len(subscribed) == 1
    _wait_for(lambda: subscribed[0] in unsubscribed)
    out, err = capsys.readouterr()
    prefix = "[" + tf._dask_futures.base.key[:12] + "] "
    assert prefix in out and "hello-" + token in out and "world-" + token in out
    assert prefix in err and "error-" + token in err


def test_non_streaming_submission_never_subscribes(stream_client):
    _, events, subscribed, _ = stream_client

    @delayed
    def silent(token):
        print("off-" + token)
        return token

    token = uuid.uuid4().hex
    tf = silent(token)
    assert tf.streaming is False
    assert tf.run() == token
    assert subscribed == []
    assert events == []


def test_streaming_flag_is_frozen_at_submission_time(stream_client):
    _, events, subscribed, _ = stream_client

    @delayed
    def wait_and_print(token):
        import time

        time.sleep(1)
        print("late-" + token)
        return token

    token = uuid.uuid4().hex
    tf = wait_and_print(token)

    async def run():
        task = asyncio.ensure_future(tf.task())
        for _ in range(200):
            if tf._dask_futures is not None:
                break
            await asyncio.sleep(0.05)
        assert tf._dask_futures is not None, "submission never started"
        tf.streaming = True
        return await task

    assert asyncio.run(run()) == token
    assert tf.streaming is True
    assert subscribed == [] and events == []


def test_parallel_transformations_have_separate_stream_topics(stream_client):
    _, events, subscribed, unsubscribed = stream_client

    @delayed
    def talk(token):
        import time

        print(token, flush=True)
        time.sleep(0.5)
        print("tail-" + token, flush=True)
        return token

    tokens = [uuid.uuid4().hex, uuid.uuid4().hex]
    transformations = [talk(token) for token in tokens]
    for tf in transformations:
        tf.streaming = True

    async def run():
        return await asyncio.gather(*(tf.task() for tf in transformations))

    assert asyncio.run(run()) == tokens
    _wait_for(lambda: all(any("tail-" + token in e[1].get("text", "") for e in events) for token in tokens))
    assert len(set(subscribed)) == 2
    for tf, token, other in zip(transformations, tokens, reversed(tokens)):
        topic = "seamless-stream-" + tf._dask_futures.base.key
        chunks = [e[1].get("text", "") for e in events if e[0] == topic]
        assert token in "".join(chunks)
        assert other not in "".join(chunks)
    _wait_for(lambda: all(topic in unsubscribed for topic in subscribed))


def test_scheduler_throttle_escalates_and_obeys_hysteresis():
    from seamless_dask.stream_throttle import SeamlessStreamThrottlePlugin

    plugin = SeamlessStreamThrottlePlugin()
    tighter = plugin.update_rates(100.0, {}, now=100.0)
    assert tighter is not None
    assert 1024 <= tighter["max_payload"] < 8192
    assert 2.0 < tighter["min_interval"] <= 30.0
    assert plugin.update_rates(100.0, {}, now=100.1) is None
    assert plugin.update_rates(400.0, {}, now=101.0) is None
    stricter = plugin.update_rates(400.0, {}, now=104.0)
    assert stricter is not None
    assert stricter["max_payload"] <= tighter["max_payload"]
    assert stricter["min_interval"] >= tighter["min_interval"]
    assert plugin.update_rates(0.0, {}, now=105.0) is None
    relaxed = plugin.update_rates(0.0, {}, now=115.0)
    assert relaxed is not None
    assert relaxed["max_payload"] == 8192
    assert relaxed["min_interval"] == 2.0
    assert plugin.update_rates(0.0, {}, now=116.0) is None


def test_scheduler_worker_hotspot_is_targeted_after_sustained_overload():
    from seamless_dask.stream_throttle import SeamlessStreamThrottlePlugin

    plugin = SeamlessStreamThrottlePlugin()
    first = plugin.update_rates(10.0, {"hot-worker": 8.0, "cool-worker": 2.0}, now=100.0)
    assert first is None or not first.get("per_worker")
    targeted = plugin.update_rates(10.0, {"hot-worker": 8.0, "cool-worker": 2.0}, now=106.0)
    assert targeted is not None
    assert "hot-worker" in targeted["per_worker"]
    assert "cool-worker" not in targeted["per_worker"]
    assert targeted["per_worker"]["hot-worker"]["min_interval"] > 2.0


def test_streamed_exception_preserves_full_output_trailer(stream_client):
    _, events, subscribed, unsubscribed = stream_client

    @delayed
    def fail(token):
        import sys

        print("stdout-" + token)
        print("stderr-" + token, file=sys.stderr)
        raise RuntimeError("intentional-" + token)

    token = uuid.uuid4().hex
    tf = fail(token)
    tf.streaming = True
    tf.compute()
    assert tf.exception is not None
    assert "intentional-" + token in tf.exception
    assert "stdout-" + token in tf.exception and "stderr-" + token in tf.exception
    assert "Standard output" in tf.exception
    _wait_for(lambda: any("stdout-" + token in e[1].get("text", "") for e in events))
    _wait_for(lambda: all(topic in unsubscribed for topic in subscribed))
