"""Canceling a Dask-backed transformation reclaims the inner worker slot.

The driver-side cancel sets the scheduler flag; a watcher running alongside the
worker dispatch observes it and terminates the seamless spawn subprocess executing
the checksum inside the Dask worker, instead of letting it run to completion. This
reuses the spawn reclaim primitive and never restarts the Dask worker itself.
"""

import threading
import time
import uuid

import seamless
from seamless.transformer import delayed
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client


def test_dask_cancel_reclaims_inner_subprocess(monkeypatch):
    # Poll quickly so the test does not need to wait a full second for the watcher.
    import seamless_dask.client as dask_client_mod

    monkeypatch.setattr(dask_client_mod, "_CANCEL_WATCH_INTERVAL", 0.2)

    sd_client = create_dummy_client(workers=1, worker_threads=2, spawn_workers=2)
    set_seamless_dask_client(sd_client)
    try:
        nonce = str(uuid.uuid4())

        @delayed
        def slow(token):
            import os
            import time

            time.sleep(20)
            return os.getpid(), token

        tf = slow(nonce)

        outcome = {}

        def _run():
            try:
                outcome["value"] = tf.compute()
            except BaseException as exc:  # noqa: BLE001
                outcome["exc"] = repr(exc)

        thread = threading.Thread(target=_run, name="dask-slow-compute")
        thread.start()

        # Wait until the submission is in flight on the cluster.
        deadline = time.time() + 20
        while time.time() < deadline:
            if getattr(tf, "_dask_futures", None) is not None:
                break
            time.sleep(0.05)
        assert getattr(tf, "_dask_futures", None) is not None, "submission never started"
        # Give the worker a moment to actually start computing.
        time.sleep(1.5)

        canceled = sd_client.cancel_by_checksum(tf._dask_futures.tf_checksum)
        assert canceled is True

        # The watcher (polling every 0.2s) must terminate the inner subprocess so the
        # blocked compute unblocks well before the 20s sleep elapses.
        thread.join(timeout=15)
        assert not thread.is_alive(), "compute did not unblock after cancel"
        assert outcome.get("value") in (None,), f"unexpected result: {outcome!r}"
    finally:
        set_seamless_dask_client(None)
        seamless.close()
