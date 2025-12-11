"""Worker utilities for Seamless Dask integration."""

from __future__ import annotations

import logging
from typing import Any

from distributed.diagnostics.plugin import WorkerPlugin


LOGGER = logging.getLogger(__name__)


class SeamlessWorkerPlugin(WorkerPlugin):
    """Ensure a Seamless worker pool is available inside every Dask worker."""

    def __init__(self, *, num_workers: int = 3) -> None:
        self.num_workers = num_workers

    def setup(self, worker) -> None:  # type: ignore[override]
        try:
            from seamless.transformer import has_spawned, spawn

            threads = getattr(worker.state, "nthreads", None)
            if threads is not None and threads < self.num_workers:
                raise RuntimeError(
                    f"Dask worker threads ({threads}) "
                    f"must be at least Seamless worker count ({self.num_workers})"
                )
            if not has_spawned():
                spawn(self.num_workers)
                LOGGER.info(
                    "Spawned Seamless workers inside Dask worker %s (count=%d, threads=%s)",
                    worker.name,
                    self.num_workers,
                    threads,
                )
        except Exception as exc:  # pragma: no cover - best-effort safety
            LOGGER.error(
                "Failed to spawn Seamless workers inside Dask worker %s: %s",
                getattr(worker, "name", "<unknown>"),
                exc,
            )

    def teardown(self, worker) -> None:  # type: ignore[override]
        try:
            from seamless_transformer import worker as seamless_worker

            seamless_worker.shutdown_workers()
        except Exception:  # pragma: no cover - best-effort safety
            LOGGER.debug("Failed to shut down Seamless workers cleanly", exc_info=True)


__all__ = ["SeamlessWorkerPlugin"]
