"""Small convenience helpers to spin up a Seamless Dask client."""

from __future__ import annotations

import contextlib
from typing import Iterator, Tuple

from distributed import Client

from .client import SeamlessDaskClient
from .dummy_scheduler import DummySchedulerHandle, start_dummy_scheduler


def create_default_client(
    *, workers: int = 1, worker_threads: int = 3, spawn_workers: int = 3
) -> Tuple[SeamlessDaskClient, DummySchedulerHandle, Client]:
    """Start a dummy scheduler and return a connected Seamless client."""
    scheduler = start_dummy_scheduler(
        workers=workers, worker_threads=worker_threads, register_at_exit=False
    )
    dask_client = Client(scheduler.address, timeout="10s")
    return (
        SeamlessDaskClient(dask_client, worker_plugin_workers=spawn_workers),
        scheduler,
        dask_client,
    )


@contextlib.contextmanager
def default_client(
    *, workers: int = 1, worker_threads: int = 3, spawn_workers: int = 3
) -> Iterator[SeamlessDaskClient]:
    """Context manager that yields a configured Seamless Dask client."""
    client: SeamlessDaskClient | None = None
    scheduler: DummySchedulerHandle | None = None
    dask_client: Client | None = None
    try:
        client, scheduler, dask_client = create_default_client(
            workers=workers, worker_threads=worker_threads, spawn_workers=spawn_workers
        )
        yield client
    finally:
        if dask_client is not None:
            try:
                dask_client.close()
            except Exception:
                pass
        if scheduler is not None:
            try:
                scheduler.stop()
            except Exception:
                pass


__all__ = ["create_default_client", "default_client"]
