"""Regression test for local in-process cancellation of a Dask-backed transformation.

A bare ``CancelledError`` on the async computation path (e.g. ``task().cancel()``)
used to only mark the local promise canceled while orphaning the running Dask
submission. It must instead converge on the authoritative scheduler-flag
mechanism (``SeamlessDaskClient.cancel_by_checksum``), matching the other
cancel-signal producers.
"""

import asyncio

import pytest

import seamless
from seamless.transformer import Transformation, delayed
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client


def test_task_cancel_propagates_to_dask_scheduler():
    sd_client = create_dummy_client(workers=1, worker_threads=2, spawn_workers=2)
    set_seamless_dask_client(sd_client)

    cancel_calls: list[str] = []
    orig_cancel = sd_client.cancel_by_checksum

    def _spy(tf_checksum):
        cancel_calls.append(str(tf_checksum))
        return orig_cancel(tf_checksum)

    # Shadow the bound method so the patched handler's
    # ``client.cancel_by_checksum(...)`` call is observed.
    sd_client.cancel_by_checksum = _spy

    try:

        @delayed
        def slow(x: int) -> int:
            import time

            time.sleep(4)
            return x + 1

        tf: Transformation = slow(1)

        async def _drive():
            task = asyncio.ensure_future(tf.computation(require_value=False))
            # Wait until the Dask submission is actually in-flight.
            for _ in range(200):
                if getattr(tf, "_dask_futures", None) is not None:
                    break
                await asyncio.sleep(0.05)
            assert (
                getattr(tf, "_dask_futures", None) is not None
            ), "Dask submission never started"
            # Let the coroutine reach the await on futures.thin.result.
            await asyncio.sleep(0.2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(_drive())

        assert (
            cancel_calls
        ), "cancel_by_checksum was not invoked when the Dask-backed task was canceled"
    finally:
        sd_client.cancel_by_checksum = orig_cancel
        set_seamless_dask_client(None)
        seamless.close()
