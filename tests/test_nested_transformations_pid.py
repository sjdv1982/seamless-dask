import os

import seamless

from seamless_transformer import delayed, worker
from seamless_dask.default import default_client
from seamless_dask.transformer_client import set_dask_client


def _canonical_result(value):
    """Convert transformation results to hashable structures."""

    if isinstance(value, dict):
        return tuple(sorted((k, _canonical_result(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_canonical_result(v) for v in value)
    return value


def test_nested_transformations_pid_and_spawn_expect_failures():
    main_pid = os.getpid()
    assert not worker.has_spawned()

    with default_client(workers=1, worker_threads=3) as sd_client:
        set_dask_client(sd_client)
        try:
            assert not worker.has_spawned()

            @delayed
            def inner(label):
                import datetime
                import os
                import time

                return (
                    label,
                    os.getpid(),
                    datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    time.perf_counter_ns(),
                )

            @delayed
            def outer(first, second):
                import os

                return {"outer_pid": os.getpid(), "results": (first, second)}

            first_tf = inner("beta")
            second_tf = inner("beta")
            combined = outer(first_tf, second_tf)

            first_run = combined.run()
            first_result, second_result = first_run["results"]

            assert first_run["outer_pid"] != main_pid
            assert first_result == second_result
            assert first_result[1] != main_pid
            assert first_result[1] != first_run["outer_pid"]

            repeat_run = outer(inner("beta"), inner("beta")).run()
            all_results = {
                _canonical_result(first_result),
                _canonical_result(second_result),
                *(_canonical_result(r) for r in repeat_run["results"]),
            }
            assert len(all_results) == 1
            assert not worker.has_spawned()
        finally:
            set_dask_client(None)
            worker.shutdown_workers()
            seamless.close()
