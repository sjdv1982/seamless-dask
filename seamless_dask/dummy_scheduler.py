"""Helper to start a tiny in-process Dask scheduler for development and tests."""

from __future__ import annotations

import argparse
import asyncio
import atexit
import contextlib
import logging
import threading
from concurrent.futures import Future
from typing import Tuple

from distributed import Scheduler, Worker

LOGGER = logging.getLogger(__name__)


async def create_dummy_scheduler(
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    workers: int = 1,
    worker_threads: int = 1,
) -> Tuple[Scheduler, list[Worker]]:
    """Start a Dask scheduler with a handful of local workers."""
    if workers < 0:
        raise ValueError("workers must be zero or a positive integer")
    if worker_threads < 1:
        raise ValueError("worker_threads must be at least 1")

    scheduler = Scheduler(host=host, port=port)
    await scheduler

    worker_handles: list[Worker] = []
    for idx in range(workers):
        worker = Worker(
            scheduler.address,
            nthreads=worker_threads,
            name=f"dummy-worker-{idx}",
        )
        await worker
        worker_handles.append(worker)

    return scheduler, worker_handles


async def serve_dummy_scheduler(
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    workers: int = 1,
    worker_threads: int = 1,
) -> str:
    """
    Start the dummy scheduler and keep it alive until interrupted.

    Returns the scheduler address.
    """
    scheduler, worker_handles = await create_dummy_scheduler(
        host=host,
        port=port,
        workers=workers,
        worker_threads=worker_threads,
    )
    LOGGER.info("Dummy scheduler listening at %s", scheduler.address)
    print(f"Dummy Dask scheduler available at {scheduler.address}")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        LOGGER.info("Stopping dummy scheduler")
    finally:
        await asyncio.gather(
            *(worker.close() for worker in worker_handles), return_exceptions=True
        )
        await scheduler.close()
        LOGGER.info("Dummy scheduler stopped")

    return scheduler.address


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a minimal Dask scheduler with in-process workers."
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Scheduler host/IP to bind to (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        default=0,
        type=int,
        help="Scheduler port, set to 0 to auto-select a free port.",
    )
    parser.add_argument(
        "--workers",
        default=1,
        type=int,
        help="How many single-threaded workers to attach to the scheduler.",
    )
    parser.add_argument(
        "--worker-threads",
        dest="worker_threads",
        default=1,
        type=int,
        help="Number of threads per worker (default: 1).",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _build_parser().parse_args()

    asyncio.run(
        serve_dummy_scheduler(
            host=args.host,
            port=args.port,
            workers=args.workers,
            worker_threads=args.worker_threads,
        )
    )


class DummySchedulerHandle:
    """Background dummy scheduler that cleans up on process exit."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        workers: int = 1,
        worker_threads: int = 1,
        register_at_exit: bool = True,
    ):
        self.host = host
        self.port = port
        self.workers = workers
        self.worker_threads = worker_threads
        self._register_at_exit = register_at_exit

        self.address: str | None = None
        self._scheduler: Scheduler | None = None
        self._workers: list[Worker] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._started = False

    def start(self) -> str:
        """Start the scheduler in a background thread and return its address."""
        if self._started:
            if self.address is None:
                raise RuntimeError("Dummy scheduler failed to start")
            return self.address

        loop = asyncio.new_event_loop()
        self._loop = loop
        started_future: Future[Tuple[Scheduler, list[Worker]]] = Future()

        async def _bootstrap():
            try:
                scheduler, workers = await create_dummy_scheduler(
                    host=self.host,
                    port=self.port,
                    workers=self.workers,
                    worker_threads=self.worker_threads,
                )
                started_future.set_result((scheduler, workers))
            except Exception as exc:  # pragma: no cover - best-effort safety
                started_future.set_exception(exc)
                loop.call_soon(loop.stop)
                raise

        def _runner():
            asyncio.set_event_loop(loop)
            loop.create_task(_bootstrap())
            try:
                loop.run_forever()
            finally:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                with contextlib.suppress(Exception):
                    loop.run_until_complete(asyncio.gather(*pending))
                with contextlib.suppress(Exception):
                    loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
                self._stop_event.set()

        thread = threading.Thread(
            target=_runner, name="dummy-scheduler", daemon=True
        )
        thread.start()
        self._thread = thread

        scheduler, workers = started_future.result()  # Propagate startup errors
        self._scheduler = scheduler
        self._workers = workers
        self.address = scheduler.address
        self._started = True
        if self._register_at_exit:
            atexit.register(self.stop)
        return self.address

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the scheduler and workers gracefully (idempotent)."""
        if not self._started:
            return
        self._started = False

        if self._loop is None:
            return

        async def _shutdown():
            await asyncio.gather(
                *(worker.close() for worker in self._workers),
                return_exceptions=True,
            )
            if self._scheduler is not None:
                await self._scheduler.close()

        if self._loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(_shutdown(), self._loop)
            with contextlib.suppress(Exception):
                fut.result(timeout=timeout)
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._stop_event.wait(timeout=timeout)

    def __enter__(self) -> DummySchedulerHandle:
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


def start_dummy_scheduler(
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    workers: int = 1,
    worker_threads: int = 1,
    register_at_exit: bool = True,
) -> DummySchedulerHandle:
    """
    Start a dummy scheduler in the background and return a handle.

    The scheduler is shut down automatically on process exit when
    ``register_at_exit`` is True.
    """
    handle = DummySchedulerHandle(
        host=host,
        port=port,
        workers=workers,
        worker_threads=worker_threads,
        register_at_exit=register_at_exit,
    )
    handle.start()
    return handle


def get_dummy_scheduler_address(
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    workers: int = 1,
    worker_threads: int = 1,
) -> str:
    """
    Start a dummy scheduler and return its address for ``dask.distributed.Client``.

    The scheduler automatically stops when the Python process exits.
    """
    handle = start_dummy_scheduler(
        host=host,
        port=port,
        workers=workers,
        worker_threads=worker_threads,
        register_at_exit=True,
    )
    return handle.address


if __name__ == "__main__":
    main()
