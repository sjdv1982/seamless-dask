"""Integration check for the in-process dummy scheduler."""

from distributed import Client

from seamless_dask.dummy_scheduler import start_dummy_scheduler


def _square(x: int) -> int:
    return x * x


def _add(x: int, y: int) -> int:
    return x + y


def test_dummy_scheduler_executes_jobs() -> None:
    handle = start_dummy_scheduler(
        workers=2, worker_threads=1, register_at_exit=False
    )
    try:
        assert handle.address

        with Client(handle.address, timeout="10s") as client:
            squares = [client.submit(_square, i) for i in range(3)]
            summed = client.submit(_add, squares[0], squares[1])

            assert client.gather(squares) == [0, 1, 4]
            assert summed.result(timeout=10) == 1
    finally:
        handle.stop()
