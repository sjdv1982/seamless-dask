import os
import time

import seamless

from seamless.transformer import delayed
from seamless_transformer import worker
from seamless_dask.default import default_client
from seamless_dask.transformer_client import set_dask_client


def _measure_run(tf, expected: int) -> float:
    start = time.perf_counter()
    result = tf.run()
    duration = time.perf_counter() - start
    assert result == expected, result
    return duration


def test_dependencies():
    main_pid = os.getpid()
    assert not worker.has_spawned()

    with default_client(workers=1, worker_threads=3) as sd_client:
        set_dask_client(sd_client)
        try:

            @delayed
            def slow_add(a, b, c=None, d=None, e=None):
                import time
                from seamless.transformer import global_lock

                with global_lock:
                    time.sleep(0.2)
                return sum(v for v in (a, b, c, d, e) if v is not None)

            @delayed
            def fast_add(a, b, c=None, d=None, e=None):
                return sum(v for v in (a, b, c, d, e) if v is not None)

            # Two independent slow adds should run in parallel on the Dask worker pool.
            duration = _measure_run(
                fast_add(slow_add(2, 3), slow_add(4, 5)),
                expected=14,
            )
            assert 0.1 < duration < 1.0

            duration = _measure_run(
                fast_add(slow_add(2, 13), slow_add(4, 15)),
                expected=34,
            )
            assert 0.1 < duration < 1.0

            # Nested slow_add depends on the inner ones; expect roughly two sleeps.
            duration = _measure_run(
                slow_add(slow_add(5, 6), fast_add(1, 2)),
                expected=14,
            )
            assert 0.3 < duration < 1.2

            # A handful of slow adds should still complete within a couple seconds
            # if the worker-side pool is providing parallelism.
            duration = _measure_run(
                fast_add(
                    slow_add(9, -1, -2, -3),
                    slow_add(10, -1, -2, -3),
                    slow_add(11, -1, -2, -3),
                    slow_add(12, -1, -2, -3),
                    slow_add(13, -1, -2, -3),
                ),
                expected=3 + 4 + 5 + 6 + 7,
            )
            assert 0.2 < duration < 1.5
        finally:
            set_dask_client(None)
            seamless.close()
