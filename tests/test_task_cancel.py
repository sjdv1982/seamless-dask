"""Regression test for local in-process cancellation of a Dask-backed transformation.

A bare ``CancelledError`` on the async computation path (e.g. ``task().cancel()``)
used to only mark the local promise canceled while orphaning the running Dask
submission. It now detaches the local member with
``SeamlessDaskClient.softcancel_by_checksum``; if that was the last member, the
authoritative scheduler flag is set.
"""

import asyncio

import pytest

from seamless.transformer import Transformation, delayed
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client


def test_task_cancel_propagates_to_dask_scheduler():
    sd_client = create_dummy_client(workers=1, worker_threads=2, spawn_workers=2)
    set_seamless_dask_client(sd_client)

    softcancel_calls: list[tuple[str, str | None]] = []
    orig_softcancel = sd_client.softcancel_by_checksum

    def _spy(tf_checksum, member_id=None):
        softcancel_calls.append((str(tf_checksum), member_id))
        return orig_softcancel(tf_checksum, member_id)

    # Shadow the bound method so the patched handler's softcancel call is observed.
    sd_client.softcancel_by_checksum = _spy

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

        assert softcancel_calls, (
            "softcancel_by_checksum was not invoked when the Dask-backed task "
            "was canceled"
        )
        assert softcancel_calls[0][1] is not None
    finally:
        sd_client.softcancel_by_checksum = orig_softcancel
        set_seamless_dask_client(None)
