import seamless
from seamless import Buffer

from seamless.transformer import delayed
from seamless_dask.transformer_client import set_seamless_dask_client
from seamless_dask.dummy_scheduler import create_dummy_client


def _get_nthreads():
    from distributed.worker import get_worker

    return get_worker().state.nthreads


def test_seamless_client_executes_transformation() -> None:
    from seamless.transformer import Transformation

    sd_client = create_dummy_client(workers=1, worker_threads=3, spawn_workers=3)
    set_seamless_dask_client(sd_client)
    try:
        info = sd_client.client.scheduler_info()
        worker_threads = [w.get("nthreads") for w in info.get("workers", {}).values()]
        assert worker_threads, "No workers reported"
        assert all(v == 3 for v in worker_threads)

        buf = Buffer(5, "plain")
        checksum = buf.get_checksum()

        cs_hex, buffer_obj, exc = sd_client.get_fat_checksum_future(checksum).result(
            timeout=10
        )

        assert exc is None
        assert cs_hex == checksum.hex()
        assert buffer_obj is not None
        assert buffer_obj.get_value("plain") == 5

        @delayed
        def add_one(x: int) -> int:
            return x + 1

        tf: Transformation = add_one(1)
        assert isinstance(tf, Transformation)
        result_checksum = tf.compute()
        assert tf.exception is None, tf.exception
        assert result_checksum is not None
        assert tf.result_checksum == result_checksum
        assert tf.run() == 2
        futures = tf._dask_futures  # type: ignore[attr-defined]
        assert futures is not None
        base_key = futures.base.key
        locations = sd_client.client.who_has(futures.base)
        assert locations.get(base_key), "Base task was not executed on a worker"
    finally:
        set_seamless_dask_client(None)
        seamless.close()
