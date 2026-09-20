"""Live-Dask evidence for known-issues section 3.4, item 3.

These characterize the documented limitation: abandoning dispatch_expression
does not interrupt remote materialization or reclaim its default-executor slot.
The dummy-scheduler helper starts real Dask workers; no external cluster is needed.
Run this file on its own, in the seamless1 conda environment.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import uuid

import pytest
from distributed import Event, Future

from seamless import Buffer
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client
from seamless_transformer.worker import dispatch_expression


def _slow_materialization(value, entered_name, release_name, completed_name):
    """A bounded, controllable remote input fetch, followed by real evaluation."""
    from distributed import Event
    from seamless import Buffer

    Event(entered_name).set()
    if not Event(release_name).wait(timeout=30):
        raise TimeoutError("Test did not release remote materialization")
    buffer = Buffer(value, "plain")
    Event(completed_name).set()
    return buffer.get_checksum().hex(), buffer, None


async def _until(predicate, message, timeout=10):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, message
        await asyncio.sleep(0.01)


def _observe_cancel(monkeypatch):
    client = create_dummy_client(workers=1, worker_threads=2, spawn_workers=1)
    set_seamless_dask_client(client)
    token = uuid.uuid4().hex
    entered = Event("expression-entered-" + token, client=client.client)
    release = Event("expression-release-" + token, client=client.client)
    completed = Event("expression-completed-" + token, client=client.client)
    result_entered = threading.Event()
    result_left = threading.Event()
    original_result = Future.result
    input_future = None

    def observed_result(future, *args, **kwargs):
        if future.key.startswith("expression-") and future.key.endswith("-checksum"):
            result_entered.set()
            try:
                return original_result(future, *args, **kwargs)
            finally:
                result_left.set()
        return original_result(future, *args, **kwargs)

    monkeypatch.setattr(Future, "result", observed_result)
    value = {"answer": 42, "nonce": token}
    checksum = Buffer(value, "plain").get_checksum()

    def materialize(requested_checksum):
        nonlocal input_future
        assert requested_checksum == checksum
        input_future = client.client.submit(
            _slow_materialization,
            value,
            entered.name,
            release.name,
            completed.name,
            pure=False,
        )
        return input_future

    # Substitute only the slow input source. Expression submission, evaluation,
    # thin-result waiting, and dispatch cancellation all use production code.
    monkeypatch.setattr(client, "get_fat_checksum_future", materialize)

    async def drive():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        task = asyncio.create_task(
            dispatch_expression(checksum, ("answer",), "plain", "int")
        )
        probe = None
        try:
            await _until(entered.is_set, "Dask input materialization never started")
            await _until(result_entered.is_set, "dispatch never entered Future.result")
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=5)

            remote_still_running = not completed.is_set() and not input_future.done()
            probe = loop.run_in_executor(None, lambda: "executor available")
            # Bounded observation only; shield keeps the probe available for cleanup.
            try:
                await asyncio.wait_for(asyncio.shield(probe), timeout=0.5)
                executor_blocked = False
            except asyncio.TimeoutError:
                executor_blocked = True
            waiter_still_blocked = not result_left.is_set()
            return remote_still_running, executor_blocked, waiter_still_blocked
        finally:
            # Release before asyncio.run shuts down its executor, even on failure.
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if probe is not None:
                assert await asyncio.wait_for(probe, timeout=15) == "executor available"
            if result_entered.is_set():
                await _until(result_left.is_set, "Future.result did not leave after release")
            if input_future is not None:
                await _until(input_future.done, "Remote materialization did not finish")
                assert input_future.result(timeout=1)[2] is None
                assert completed.is_set()

    try:
        return asyncio.run(drive())
    finally:
        release.set()
        if input_future is not None:
            input_future.release()
        set_seamless_dask_client(None)


def test_cancelled_expression_dispatch_leaves_remote_materialization_running(monkeypatch):
    remote_running, _, _ = _observe_cancel(monkeypatch)
    assert remote_running, "Update this characterization if Dask cancellation is fixed"


def test_cancelled_expression_dispatch_retains_default_executor_thread(monkeypatch):
    _, executor_blocked, waiter_blocked = _observe_cancel(monkeypatch)
    assert waiter_blocked, "Future.result should still be waiting on the remote result"
    assert executor_blocked, "Known limitation: cancellation does not reclaim the thread"
