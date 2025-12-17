"""Worker utilities for Seamless Dask integration."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any

from distributed.diagnostics.plugin import WorkerPlugin


LOGGER = logging.getLogger(__name__)


def _run_loop_forever(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _ensure_worker_loop(worker) -> asyncio.AbstractEventLoop | None:
    loop = getattr(worker, "seamless_loop", None)
    if loop is not None and not loop.is_closed():
        return loop

    loop = asyncio.new_event_loop()
    thread = threading.Thread(
        target=_run_loop_forever, args=(loop,), name="seamless-worker-loop", daemon=True
    )
    thread.start()
    worker.seamless_loop = loop
    worker.seamless_loop_thread = thread
    return loop


def _stop_worker_loop(worker) -> None:
    loop = getattr(worker, "seamless_loop", None)
    thread = getattr(worker, "seamless_loop_thread", None)
    if loop is None or loop.is_closed():
        return
    try:
        loop.call_soon_threadsafe(loop.stop)
    except Exception:
        LOGGER.debug("Failed to stop worker loop cleanly", exc_info=True)
    if thread is not None:
        try:
            thread.join(timeout=1.0)
        except Exception:
            LOGGER.debug("Failed to join worker loop thread", exc_info=True)
    try:
        loop.close()
    except Exception:
        LOGGER.debug("Failed to close worker loop", exc_info=True)
    for attr in ("seamless_loop", "seamless_loop_thread"):
        if hasattr(worker, attr):
            try:
                delattr(worker, attr)
            except Exception:
                pass


class SeamlessWorkerPlugin(WorkerPlugin):
    """Ensure a Seamless worker pool is available inside every Dask worker."""

    def __init__(self, *, num_workers: int, remote_clients: dict | None) -> None:
        self.num_workers = num_workers
        self.remote_clients = remote_clients

    def setup(self, worker) -> None:  # type: ignore[override]
        try:
            from seamless.transformer import has_spawned, spawn
            from seamless.config import set_remote_clients
            from .permissions import configure as configure_permissions
            import os

            threads = getattr(worker.state, "nthreads", None)
            if threads is not None and threads < self.num_workers:
                raise RuntimeError(
                    f"Dask worker threads ({threads}) "
                    f"must be at least Seamless worker count ({self.num_workers})"
                )

            def _update_resources(value: int) -> None:
                """Push the current permission counter to the scheduler."""

                try:
                    loop = getattr(worker.loop, "asyncio_loop", None) or worker.loop
                    coro = worker.set_resources(S=float(value))
                    asyncio.run_coroutine_threadsafe(coro, loop)
                except Exception:
                    LOGGER.debug("Failed to update worker resources", exc_info=True)

            try:
                configure_permissions(
                    workers=self.num_workers, resource_updater=_update_resources
                )
            except Exception:
                LOGGER.debug("Failed to configure permission manager", exc_info=True)
            if not has_spawned():
                # Dask worker processes do not run as MainProcess; allow spawning anyway.
                os.environ.setdefault("SEAMLESS_ALLOW_CHILD_SPAWN", "1")
                spawn(self.num_workers)
                LOGGER.info(
                    "Spawned Seamless workers inside Dask worker %s (count=%d, threads=%s)",
                    worker.name,
                    self.num_workers,
                    threads,
                )

            if self.remote_clients is not None:
                set_remote_clients(self.remote_clients)
                LOGGER.info(
                    "Set remote clients inside Dask worker %s: %s",
                    worker.name,
                    json.dumps(self.remote_clients, indent=4),
                )

            _ensure_worker_loop(worker)

        except Exception as exc:  # pragma: no cover - best-effort safety
            LOGGER.error(
                "Failed to spawn Seamless workers inside Dask worker %s: %s",
                getattr(worker, "name", "<unknown>"),
                exc,
            )

    def teardown(self, worker) -> None:  # type: ignore[override]
        _stop_worker_loop(worker)
        try:
            from seamless_transformer import worker as seamless_worker

            seamless_worker.shutdown_workers(wait=False)
        except Exception:  # pragma: no cover - best-effort safety
            LOGGER.debug("Failed to shut down Seamless workers cleanly", exc_info=True)
        try:
            from .permissions import shutdown_permission_manager

            shutdown_permission_manager()
        except Exception:  # pragma: no cover - best-effort safety
            LOGGER.debug(
                "Failed to shut down permission manager cleanly", exc_info=True
            )


__all__ = ["SeamlessWorkerPlugin"]
