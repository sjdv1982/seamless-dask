"""Small transport tests without a live Dask cluster."""
import asyncio
import concurrent.futures
from types import SimpleNamespace

import pytest
from seamless import Checksum
import seamless_dask.client as dc
import seamless_dask.transformer_client as transformer_client
import seamless_dask.transformation_mixin as tm
import seamless_transformer.transformation_cache as tc


@pytest.mark.parametrize('cached', [False, True])
def test_base_success_fifth_slot_uses_worker_clock_cache_has_none(monkeypatch, cached):
    checksum = Checksum('e' * 64)
    logs = {'stdout': 'worker stdout', 'stderr': 'worker stderr'}
    monkeypatch.setattr(dc, '_startup_record_mode', lambda: False)
    monkeypatch.setattr(transformer_client, 'get_seamless_dask_client', lambda: SimpleNamespace(client=None))
    monkeypatch.setattr(dc, '_run_on_worker_loop', lambda factory: asyncio.run(factory()))
    monkeypatch.setattr(dc, '_fetch_cached_result_async', lambda *a: asyncio.sleep(0, result=checksum if cached else None))
    monkeypatch.setattr(dc, '_resolve_buffer_async', lambda *a: asyncio.sleep(0, result=None))
    dispatched = []
    events = []
    monkeypatch.setattr(dc, 'get_worker', lambda: SimpleNamespace(log_event=lambda topic, message: events.append((topic, message))))
    async def dispatch(*a, **k):
        assert events == [('seamless-transformation-log-dispatch', {'tf_checksum': 'f' * 64, 'submission_id': 'test-submission'})]
        dispatched.append(k)
        return checksum, logs
    monkeypatch.setattr(dc, '_dispatch_with_cancel_watch', dispatch)
    monkeypatch.setattr(tc, 'start_gpu_memory_sampler', lambda: None)
    monkeypatch.setattr(tc, 'stop_gpu_memory_sampler', lambda sampler: None)
    persisted = []
    async def persist(*a, **k):
        persisted.append(k)
    monkeypatch.setattr(dc, '_promise_and_write_result_async', persist)
    clock = iter([20.0, 23.125])
    monkeypatch.setattr(dc.time, 'perf_counter', lambda: next(clock))
    result = dc._run_base({'submission_id': 'test-submission', 'tf_checksum': 'f' * 64, 'transformation_dict': {'__language__': 'python', '__output__': ('result', 'mixed', None)}, 'inputs': [], 'tf_dunder': {}, 'scratch': True, 'require_value': False}, {})
    assert len(result) == 5
    assert result[:4] == ('f' * 64, checksum.hex(), None, None)
    if cached:
        assert result[4] is None
        assert not dispatched and not persisted and not events
    else:
        assert dispatched[0]['with_logs'] is True
        assert result[4] == {**logs, 'wall_time_seconds': 3.125}
        assert result[4]['wall_time_seconds'] == persisted[0]['wall_time_seconds']


@pytest.mark.parametrize('logs', [None, {'stdout': 'out', 'stderr': 'err', 'wall_time_seconds': 1.5}])
def test_thin_retains_fourth_log_slot_fat_stays_compatible(logs):
    buffer = object()
    base = ('a' * 64, 'b' * 64, buffer, None, logs)
    assert dc._run_thin(base) == ('a' * 64, 'b' * 64, None, logs)
    assert dc._run_fat(base) == ('b' * 64, buffer, None)


class Handle(tm.TransformationDaskMixin):
    def __init__(self, futures):
        self._constructed = False
        self._evaluated = False
        self._transformation_checksum = None
        self._result_checksum = None
        self._exception = None
        self.futures = futures
    def _dask_client(self):
        return object()
    def _remote_storage_error(self):
        return None
    def _compute_tf_checksum_no_deps(self):
        return None
    def _skip_permission_gate(self):
        return True
    def _ensure_record_bucket_preflight_sync(self):
        pass
    async def _ensure_record_bucket_preflight_async(self):
        pass
    def _build_dask_submission(self, *a, **k):
        return object()
    def _ensure_dask_futures(self, *a, **k):
        return self.futures
    def _publish_definition(self, checksum):
        self._transformation_checksum = checksum
        return checksum
    def _publish_result(self, checksum):
        self._result_checksum = checksum
        return checksum


@pytest.mark.parametrize('async_mode', [False, True])
def test_joining_handles_consume_shared_thin_future_and_register_log(monkeypatch, async_mode):
    logs = {'stdout': 'shared output', 'stderr': 'shared error', 'wall_time_seconds': 4.5}
    future = concurrent.futures.Future()
    future.set_result(('a' * 64, 'b' * 64, None, logs))
    futures = SimpleNamespace(thin=future)
    cache = tc.TransformationCache()
    monkeypatch.setattr(tc, '_transformation_cache_instance', cache)
    one, two = Handle(futures), Handle(futures)
    if async_mode:
        async def compute():
            return await asyncio.gather(one._compute_with_dask_async(False), two._compute_with_dask_async(False))
        results = asyncio.run(compute())
    else:
        results = [one._compute_with_dask(False), two._compute_with_dask(False)]
    assert results == [Checksum('b' * 64)] * 2
    assert one.futures.thin is two.futures.thin
    assert one._exception is None and two._exception is None
    assert cache.get_transformation_log(Checksum('a' * 64)) == logs


class EventClient:
    """Dask's event API plus manually completed task futures."""
    def __init__(self):
        self.handlers = {}
        self.submissions = []
    def subscribe_topic(self, topic, handler):
        self.handlers[topic] = handler
    def unsubscribe_topic(self, topic):
        self.handlers.pop(topic, None)
    def submit(self, function, *args, **kwargs):
        future = concurrent.futures.Future()
        future.key = kwargs.get('key', 'initialization')
        future.released = False
        future.release = lambda: setattr(future, 'released', True)
        self.submissions.append((function, args, kwargs, future))
        return future
    def deliver(self, topic, message):
        handler = self.handlers[topic]
        result = handler((0.0, message))
        if asyncio.iscoroutine(result):
            asyncio.run(result)


@pytest.fixture
def event_client(monkeypatch):
    raw = EventClient()
    monkeypatch.setattr(dc, 'ensure_configured', lambda **kwargs: None)
    monkeypatch.setattr(dc, 'fire_and_forget', lambda future: None)
    monkeypatch.setattr(dc.SeamlessDaskClient, '_register_worker_plugin', lambda *a, **k: None)
    monkeypatch.setattr(dc.SeamlessDaskClient, '_register_stream_throttle_plugin', lambda *a: None)
    monkeypatch.setattr(dc.SeamlessDaskClient, '_warn_if_no_workers', lambda *a: None)
    monkeypatch.setattr(dc.SeamlessDaskClient, '_start_prune_thread', lambda *a: None)
    cache = tc.TransformationCache()
    monkeypatch.setattr(tc, '_transformation_cache_instance', cache)
    client = dc.SeamlessDaskClient(raw)
    raw.submissions.clear()
    return client, raw, cache


def _log_submission():
    from seamless_dask.types import TransformationSubmission
    return TransformationSubmission(transformation_dict={'__language__': 'python', '__output__': ('result', 'mixed', None)}, inputs={}, input_futures={}, tf_checksum='a' * 64, tf_dunder={}, scratch=True)


def test_dask_submit_and_shared_join_preserve_old_log_pending_cache_decision(event_client):
    client, raw, cache = event_client
    prior = {'stdout': 'old', 'stderr': '', 'wall_time_seconds': 1}
    checksum = Checksum('a' * 64)
    cache.set_transformation_log(checksum, prior)
    first = client.submit_transformation(_log_submission(), member_id='first')
    assert cache.get_transformation_log(checksum) == prior
    second = client.submit_transformation(_log_submission(), member_id='second')
    assert second is first
    assert len(raw.submissions) == 2
    assert cache.get_transformation_log(checksum) == prior
    first.base.set_result(('a' * 64, 'b' * 64, None, None, None))
    first.thin.set_result(('a' * 64, 'b' * 64, None, None))
    assert cache.get_transformation_log(checksum) == prior


@pytest.mark.parametrize('outcome', ['success', 'failure', 'cancel'])
def test_dask_dispatch_event_clears_prior_log_before_completion_and_join(event_client, outcome):
    client, raw, cache = event_client
    checksum = Checksum('a' * 64)
    prior = {'stdout': 'prior output', 'stderr': '', 'wall_time_seconds': 1}
    cache.set_transformation_log(checksum, prior)
    futures = client.submit_transformation(_log_submission(), member_id='first')
    assert cache.get_transformation_log(checksum) == prior
    raw.deliver('seamless-transformation-log-dispatch', {'tf_checksum': checksum.hex(), 'submission_id': futures.submission_id})
    assert not futures.thin.done()
    assert cache.get_transformation_log(checksum) is None
    joined = client.submit_transformation(_log_submission(), member_id='joined')
    assert joined is futures
    assert cache.get_transformation_log(checksum) is None
    if outcome == 'success':
        logs = {'stdout': 'fresh output', 'stderr': 'fresh error', 'wall_time_seconds': 2}
        futures.base.set_result(('a' * 64, 'b' * 64, None, None, logs))
        futures.thin.set_result(('a' * 64, 'b' * 64, None, logs))
        handle = Handle(futures)
        assert handle._compute_with_dask(False) == Checksum('b' * 64)
        assert cache.get_transformation_log(checksum) == logs
    else:
        error = 'failure output' if outcome == 'failure' else 'Transformation was canceled'
        futures.base.set_result(('a' * 64, None, None, error, None))
        futures.thin.set_result(('a' * 64, None, error, None))
        assert cache.get_transformation_log(checksum) is None


def test_stale_dispatch_event_cannot_clear_log_of_another_submission(event_client):
    client, raw, cache = event_client
    checksum = Checksum('a' * 64)
    prior = {'stdout': 'kept', 'stderr': '', 'wall_time_seconds': 3}
    cache.set_transformation_log(checksum, prior)
    client.submit_transformation(_log_submission())
    raw.deliver('seamless-transformation-log-dispatch', {'tf_checksum': checksum.hex(), 'submission_id': 'obsolete-submission'})
    assert cache.get_transformation_log(checksum) == prior


def test_late_same_submission_dispatch_event_after_success_preserves_fresh_log(event_client):
    client, raw, cache = event_client
    futures = client.submit_transformation(_log_submission())
    fresh = {'stdout': 'newly committed', 'stderr': '', 'wall_time_seconds': 3}
    futures.base.set_result(('a' * 64, 'b' * 64, None, None, fresh))
    futures.thin.set_result(('a' * 64, 'b' * 64, None, fresh))
    Handle(futures)._compute_with_dask(False)
    raw.deliver('seamless-transformation-log-dispatch', {'tf_checksum': 'a' * 64, 'submission_id': futures.submission_id})
    assert cache.get_transformation_log(Checksum('a' * 64)) == fresh
    assert futures.submission_id not in client._transformation_log_submissions


def test_dispatch_clear_and_completion_commit_are_serialized(monkeypatch, event_client):
    import threading
    client, raw, cache = event_client
    futures = client.submit_transformation(_log_submission())
    checksum = Checksum('a' * 64)
    cache.set_transformation_log(checksum, {'stdout': 'old', 'stderr': '', 'wall_time_seconds': 1})
    entered_clear, release_clear, committed = threading.Event(), threading.Event(), threading.Event()
    original_clear = tc.clear_transformation_log
    fresh = {'stdout': 'concurrent fresh', 'stderr': '', 'wall_time_seconds': 4}
    errors = []
    def paused_clear(checksum):
        entered_clear.set()
        assert release_clear.wait(5)
        original_clear(checksum)
    monkeypatch.setattr(tc, 'clear_transformation_log', paused_clear)
    def deliver():
        try:
            raw.deliver('seamless-transformation-log-dispatch', {'tf_checksum': 'a' * 64, 'submission_id': futures.submission_id})
        except BaseException as exc:
            errors.append(exc)
    def finish():
        try:
            futures.base.set_result(('a' * 64, 'b' * 64, None, None, fresh))
            futures.thin.set_result(('a' * 64, 'b' * 64, None, fresh))
            Handle(futures)._compute_with_dask(False)
            committed.set()
        except BaseException as exc:
            errors.append(exc)
    event_thread = threading.Thread(target=deliver)
    completion_thread = threading.Thread(target=finish)
    event_thread.start()
    try:
        assert entered_clear.wait(5)
        completion_thread.start()
        # An unsafe handler lets commit win while it is paused, then erases it.
        committed.wait(0.2)
    finally:
        release_clear.set()
        event_thread.join(5)
        if completion_thread.ident is not None:
            completion_thread.join(5)
    assert not event_thread.is_alive() and not completion_thread.is_alive()
    assert not errors
    assert cache.get_transformation_log(checksum) == fresh


def test_thin_submit_failure_cleans_association_and_releases_base(monkeypatch, event_client):
    client, raw, cache = event_client
    original_submit = raw.submit
    bases = []
    def submit(function, *args, **kwargs):
        if function is dc._run_thin:
            raise RuntimeError('thin submission refused')
        future = original_submit(function, *args, **kwargs)
        bases.append(future)
        return future
    monkeypatch.setattr(raw, 'submit', submit)
    with pytest.raises(RuntimeError, match='thin submission refused'):
        client.submit_transformation(_log_submission())
    assert client._transformation_log_submissions == {}
    assert len(bases) == 1 and bases[0].released


def test_release_before_delayed_dispatch_event_cannot_erase_later_run(event_client):
    client, raw, cache = event_client
    first = client.submit_transformation(_log_submission())
    client.release_transformation_futures(first, cancel=False)
    assert first.submission_id not in client._transformation_log_submissions
    later = client.submit_transformation(_log_submission())
    assert later is not first
    fresh = {'stdout': 'later run', 'stderr': '', 'wall_time_seconds': 5}
    later.base.set_result(('a' * 64, 'b' * 64, None, None, fresh))
    later.thin.set_result(('a' * 64, 'b' * 64, None, fresh))
    Handle(later)._compute_with_dask(False)
    raw.deliver('seamless-transformation-log-dispatch', {'tf_checksum': 'a' * 64, 'submission_id': first.submission_id})
    assert cache.get_transformation_log(Checksum('a' * 64)) == fresh


@pytest.mark.parametrize('error', ['failed transformation', 'Transformation was canceled'])
@pytest.mark.parametrize('dispatched', [False, True])
def test_completion_before_event_distinguishes_dispatch_failure_from_preflight(event_client, error, dispatched):
    client, raw, cache = event_client
    checksum = Checksum('a' * 64)
    prior = {'stdout': 'prior successful run', 'stderr': '', 'wall_time_seconds': 1}
    cache.set_transformation_log(checksum, prior)
    futures = client.submit_transformation(_log_submission())
    marker = {'_seamless_dispatch_started': True} if dispatched else None
    # Scheduler completion can beat delivery of the earlier dispatch event.
    futures.base.set_result(('a' * 64, None, None, error, marker))
    futures.thin.set_result(('a' * 64, None, error, marker))
    assert cache.get_transformation_log(checksum) == (None if dispatched else prior)
    assert futures.submission_id not in client._transformation_log_submissions
    if dispatched:
        raw.deliver('seamless-transformation-log-dispatch', {'tf_checksum': 'a' * 64, 'submission_id': futures.submission_id})
        assert cache.get_transformation_log(checksum) is None


@pytest.mark.parametrize('error', ['failed transformation', 'Transformation was canceled'])
def test_worker_dispatched_failure_marks_decision_for_completion_transport(monkeypatch, error):
    monkeypatch.setattr(dc, '_startup_record_mode', lambda: False)
    monkeypatch.setattr(transformer_client, 'get_seamless_dask_client', lambda: SimpleNamespace(client=None))
    monkeypatch.setattr(dc, '_run_on_worker_loop', lambda factory: asyncio.run(factory()))
    monkeypatch.setattr(dc, '_fetch_cached_result_async', lambda *a: asyncio.sleep(0, result=None))
    monkeypatch.setattr(dc, '_dispatch_with_cancel_watch', lambda *a, **k: asyncio.sleep(0, result=(error, None)))
    monkeypatch.setattr(tc, 'start_gpu_memory_sampler', lambda: None)
    monkeypatch.setattr(tc, 'stop_gpu_memory_sampler', lambda sampler: None)
    monkeypatch.setattr(dc, 'get_worker', lambda: SimpleNamespace(log_event=lambda *a: None))
    result = dc._run_base({'submission_id': 'failure-submission', 'tf_checksum': 'a' * 64, 'transformation_dict': {'__language__': 'python', '__output__': ('result', 'mixed', None)}, 'inputs': [], 'tf_dunder': {}, 'scratch': True}, {})
    assert result[1] is None and result[3] is not None
    assert result[4] == {'_seamless_dispatch_started': True}
    thin = dc._run_thin(result)
    assert thin[2] == result[3] and thin[3] == result[4]


@pytest.mark.parametrize('error', ['old run failed', 'Transformation was canceled'])
def test_released_old_dispatch_failure_completion_preserves_new_success(event_client, error):
    client, raw, cache = event_client
    old = client.submit_transformation(_log_submission())
    client.release_transformation_futures(old, cancel=False)
    newer = client.submit_transformation(_log_submission())
    fresh = {'stdout': 'new successful run', 'stderr': '', 'wall_time_seconds': 6}
    newer.base.set_result(('a' * 64, 'b' * 64, None, None, fresh))
    newer.thin.set_result(('a' * 64, 'b' * 64, None, fresh))
    Handle(newer)._compute_with_dask(False)
    assert cache.get_transformation_log(Checksum('a' * 64)) == fresh
    marker = {'_seamless_dispatch_started': True}
    old.base.set_result(('a' * 64, None, None, error, marker))
    # The already-attached callback runs _register_transformation on completion.
    old.thin.set_result(('a' * 64, None, error, marker))
    assert cache.get_transformation_log(Checksum('a' * 64)) == fresh
    assert client.get_transformation_futures(Checksum('a' * 64)) is newer
