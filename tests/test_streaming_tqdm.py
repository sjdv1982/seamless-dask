"""Phase-two requirements: remote tqdm events and local proxy lifecycles."""

import builtins
import io
import sys
import threading
import time
import uuid

import pytest
import tqdm
import tqdm.auto
import tqdm.std

from seamless.transformer import delayed
from seamless_dask.client import SeamlessDaskClient
from test_streaming import _close_seamless_session, _wait_for, stream_client


def _kind(chunk):
    return chunk.get("kind", chunk.get("type"))


def test_patch_covers_main_std_auto_and_restores_on_exception():
    from seamless_transformer.stream_tqdm import install_tqdm_patch

    original = (tqdm.tqdm, tqdm.std.tqdm, tqdm.auto.tqdm)
    finders = list(sys.meta_path)
    chunks = []
    with pytest.raises(RuntimeError, match="intentional"):
        with install_tqdm_patch(chunks.append):
            assert tqdm.tqdm is not original[0]
            assert tqdm.std.tqdm is not original[1]
            assert tqdm.auto.tqdm is not original[2]
            bars = [cls(total=2, desc="alias", file=io.StringIO()) for cls in (tqdm.tqdm, tqdm.std.tqdm, tqdm.auto.tqdm)]
            for bar in bars:
                bar.update(2)
                bar.close()
            raise RuntimeError("intentional")
    assert (tqdm.tqdm, tqdm.std.tqdm, tqdm.auto.tqdm) == original
    assert sys.meta_path == finders
    opened = [c for c in chunks if _kind(c) == "tqdm_open"]
    closed = [c for c in chunks if _kind(c) == "tqdm_close"]
    assert len(opened) == len(closed) == 3
    assert len({c["bar_id"] for c in opened}) == 3
    assert all(c["n"] == 2 for c in closed)


def test_patch_handles_auto_imported_after_context_entry(monkeypatch):
    from seamless_transformer.stream_tqdm import install_tqdm_patch

    monkeypatch.delitem(sys.modules, "tqdm.auto")
    monkeypatch.delattr(tqdm, "auto")
    chunks = []
    finders = list(sys.meta_path)
    with install_tqdm_patch(chunks.append):
        import tqdm.auto as late_auto

        with late_auto.tqdm(total=1, file=io.StringIO()) as bar:
            bar.update(1)
    assert any(_kind(c) == "tqdm_open" for c in chunks)
    assert any(_kind(c) == "tqdm_close" and c["n"] == 1 for c in chunks)
    assert sys.meta_path == finders
    assert late_auto.tqdm.__module__.startswith("tqdm")


def test_nested_and_indeterminate_bars_close_without_terminal_duplicates():
    from seamless_transformer.stream_tqdm import install_tqdm_patch

    chunks = []
    terminal = io.StringIO()
    with install_tqdm_patch(chunks.append):
        with tqdm.tqdm(total=2, desc="outer", unit="item", unit_scale=True, file=terminal) as outer:
            with tqdm.tqdm(total=None, desc="inner", file=terminal) as inner:
                inner.update(3)
                outer.update(2)
    assert terminal.getvalue() == "", "server bars must not also stream terminal escapes as text"
    opened = [c for c in chunks if _kind(c) == "tqdm_open"]
    closed = [c for c in chunks if _kind(c) == "tqdm_close"]
    assert len(opened) == len(closed) == 2
    by_desc = {c["desc"]: c for c in opened}
    assert by_desc["outer"]["unit"] == "item"
    assert by_desc["outer"]["unit_scale"] is True
    assert by_desc["inner"]["total"] is None
    finals = {c["bar_id"]: c for c in closed}
    assert finals[by_desc["outer"]["bar_id"]]["n"] == 2
    assert finals[by_desc["inner"]["bar_id"]]["n"] == 3
    for opened_chunk in opened:
        identifier = opened_chunk["bar_id"]
        kinds = [_kind(c) for c in chunks if c["bar_id"] == identifier]
        assert kinds[0] == "tqdm_open" and kinds[-1] == "tqdm_close"


def test_fast_progress_coalesces_but_close_carries_latest_state():
    from seamless_transformer.stream_tqdm import install_tqdm_patch

    chunks = []
    with install_tqdm_patch(chunks.append):
        with tqdm.tqdm(total=100, mininterval=0, file=io.StringIO()) as bar:
            for _ in range(100):
                bar.update(1)
                bar.refresh()
    updates = [c for c in chunks if _kind(c) == "tqdm_update"]
    assert len(updates) <= 2, "100 fast updates must coalesce at the default two-second cadence"
    assert [_kind(c) for c in chunks][0] == "tqdm_open"
    assert _kind(chunks[-1]) == "tqdm_close"
    assert chunks[-1]["n"] == 100


def test_context_exit_closes_unfinished_bars_and_restores_patch():
    from seamless_transformer.stream_tqdm import install_tqdm_patch

    original = tqdm.tqdm
    chunks = []
    with install_tqdm_patch(chunks.append):
        bar = tqdm.tqdm(total=10, file=io.StringIO())
        bar.update(4)
    assert tqdm.tqdm is original
    final = [c for c in chunks if _kind(c) == "tqdm_close"]
    assert len(final) == 1 and final[0]["n"] == 4
    bar.close()
    assert len([c for c in chunks if _kind(c) == "tqdm_close"]) == 1


def test_progress_throttle_can_tighten_active_bars():
    from seamless_transformer.stream_tqdm import install_tqdm_patch

    chunks = []
    with install_tqdm_patch(chunks.append, min_interval=0.01) as controller:
        with tqdm.tqdm(total=5, file=io.StringIO()) as bar:
            bar.update(1)
            bar.refresh()
            time.sleep(0.03)
            bar.update(1)
            bar.refresh()
            controller.update_throttle(min_interval=30.0)
            before = len([c for c in chunks if _kind(c) == "tqdm_update"])
            for _ in range(3):
                bar.update(1)
                bar.refresh()
            after = len([c for c in chunks if _kind(c) == "tqdm_update"])
            assert after == before
    assert _kind(chunks[-1]) == "tqdm_close" and chunks[-1]["n"] == 5


class _FakeBar:
    instances = []

    def __init__(self, *args, **kwargs):
        self.options = kwargs
        self.n = 0
        self.closed = False
        self.refreshed = []
        self.__class__.instances.append(self)

    def refresh(self):
        self.refreshed.append(self.n)

    def set_postfix(self, value, refresh=True):
        self.postfix = value

    def set_postfix_str(self, value, refresh=True):
        self.postfix = value

    def close(self):
        self.closed = True


def _renderer():
    subject = SeamlessDaskClient.__new__(SeamlessDaskClient)
    subject._stream_topic_lock = threading.RLock()
    subject._stream_topics = {}
    subject._client = type("Client", (), {
        "subscribe_topic": lambda self, topic, handler: None,
        "unsubscribe_topic": lambda self, topic: None,
    })()
    return subject


def _emit(subject, key, kind, **fields):
    subject._render_stream_event(key, (time.time(), {"kind": kind, **fields}))


def test_client_proxy_reaches_final_state_and_closes(monkeypatch):
    _FakeBar.instances = []
    monkeypatch.setattr(tqdm, "tqdm", _FakeBar)
    subject = _renderer()
    subject._subscribe_stream_topic("seamless-stream-base-one", "base-one")
    _emit(subject, "base-one", "tqdm_open", bar_id="bar", desc="work", total=10, unit="it")
    _emit(subject, "base-one", "tqdm_update", bar_id="bar", n=7, total=10, elapsed=2, rate=3.5, postfix="ok")
    _emit(subject, "base-one", "tqdm_close", bar_id="bar", n=10, total=10)
    assert len(_FakeBar.instances) == 1
    bar = _FakeBar.instances[0]
    assert bar.options["desc"] == "work" and bar.options["total"] == 10
    assert bar.options["file"] is sys.stderr
    assert 7 in bar.refreshed
    assert bar.n == 10 and bar.closed


def test_client_topic_cleanup_closes_interrupted_bars(monkeypatch):
    _FakeBar.instances = []
    monkeypatch.setattr(tqdm, "tqdm", _FakeBar)
    subject = _renderer()
    topic = "seamless-stream-base-one"
    subject._subscribe_stream_topic(topic, "base-one")
    _emit(subject, "base-one", "tqdm_open", bar_id="unfinished", desc="work", total=None, unit="it")
    subject._release_stream_topic(topic)
    assert len(_FakeBar.instances) == 1
    assert _FakeBar.instances[0].closed
    assert not subject._stream_topics


def test_client_keeps_identical_bar_ids_in_separate_topics(monkeypatch):
    _FakeBar.instances = []
    monkeypatch.setattr(tqdm, "tqdm", _FakeBar)
    subject = _renderer()
    for key in ("base-one", "base-two"):
        subject._subscribe_stream_topic("seamless-stream-" + key, key)
        _emit(subject, key, "tqdm_open", bar_id="same-id", desc=key, total=10, unit="it")
    _emit(subject, "base-one", "tqdm_update", bar_id="same-id", n=3, total=10)
    _emit(subject, "base-two", "tqdm_update", bar_id="same-id", n=7, total=10)
    assert len(_FakeBar.instances) == 2
    by_desc = {bar.options["desc"]: bar for bar in _FakeBar.instances}
    assert by_desc["base-one"].n == 3 and by_desc["base-two"].n == 7
    subject._release_stream_topic("seamless-stream-base-one")
    assert by_desc["base-one"].closed and not by_desc["base-two"].closed
    subject._release_stream_topic("seamless-stream-base-two")
    assert by_desc["base-two"].closed


def test_client_without_tqdm_prints_fallback(monkeypatch, capsys):
    original_import = builtins.__import__

    def without_tqdm(name, *args, **kwargs):
        if name == "tqdm" or name.startswith("tqdm."):
            raise ImportError("tqdm unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_tqdm)
    subject = _renderer()
    subject._subscribe_stream_topic("seamless-stream-base-one", "base-one")
    _emit(subject, "base-one", "tqdm_open", bar_id="bar", desc="work", total=100, unit="it")
    _emit(subject, "base-one", "tqdm_update", bar_id="bar", n=42, total=100)
    _emit(subject, "base-one", "tqdm_close", bar_id="bar", n=100, total=100)
    out, err = capsys.readouterr()
    assert "42/100" in out + err
    assert "100/100" in out + err


def test_real_dask_progress_mixes_text_and_reaches_ten(stream_client, monkeypatch):
    _, events, subscribed, unsubscribed = stream_client
    _FakeBar.instances = []
    monkeypatch.setattr(tqdm, "tqdm", _FakeBar)

    @delayed
    def progress(token):
        import time
        from tqdm.auto import tqdm

        print("before-" + token, flush=True)
        with tqdm(total=10, desc=token) as bar:
            for _ in range(10):
                time.sleep(0.45)
                bar.update(1)
        print("after-" + token, flush=True)
        return token

    token = uuid.uuid4().hex
    tf = progress(token)
    tf.streaming = True
    assert tf.run() == token
    _wait_for(lambda: any(_kind(c) == "tqdm_close" for _, c, _ in events))
    chunks = [c for _, c, _ in events]
    updates = [c for c in chunks if _kind(c) == "tqdm_update"]
    assert len(updates) >= 2
    assert any(_kind(c) == "tqdm_close" and c["n"] == 10 for c in chunks)
    assert any("before-" + token in c.get("text", "") for c in chunks)
    assert any("after-" + token in c.get("text", "") for c in chunks)
    before_index = next(i for i, c in enumerate(chunks) if "before-" + token in c.get("text", ""))
    after_index = next(i for i, c in enumerate(chunks) if "after-" + token in c.get("text", ""))
    assert before_index <= after_index
    progress_kinds = [_kind(c) for c in chunks if _kind(c) in ("tqdm_open", "tqdm_update", "tqdm_close")]
    assert progress_kinds[0] == "tqdm_open" and progress_kinds[-1] == "tqdm_close"
    assert all("\r" not in c.get("text", "") for c in chunks), "remote tqdm must not duplicate terminal redraws via stderr"
    _wait_for(lambda: all(topic in unsubscribed for topic in subscribed))
    bars = [bar for bar in _FakeBar.instances if bar.options.get("desc") == token]
    assert len(bars) == 1 and bars[0].n == 10 and bars[0].closed
