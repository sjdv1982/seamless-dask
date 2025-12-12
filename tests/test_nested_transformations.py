import os

import pytest
from seamless.transformer import delayed
from seamless_transformer import worker
from seamless_dask.default import default_client
from seamless_dask.transformer_client import set_dask_client


@pytest.fixture(scope="session", autouse=True)
def _close_seamless_session():
    """Ensure Seamless shuts down once after the full test session."""
    import seamless

    yield
    seamless.close()


def _canonical_result(value):
    """Convert transformation results to hashable structures."""

    if isinstance(value, dict):
        return tuple(sorted((k, _canonical_result(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_canonical_result(v) for v in value)
    return value


def _test_nested_transformations(local, OFFSET):
    main_pid = os.getpid()
    assert not worker.has_spawned()

    with default_client(workers=1, worker_threads=3) as sd_client:
        set_dask_client(sd_client)
        try:

            @delayed
            def outer(label, local):
                import os
                from seamless.transformer import delayed

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

                inner.local = local

                tf_first = inner(label + "-1").start()
                tf_second = inner(label + "-2").start()
                first = tf_first.run()
                second = tf_second.run()

                return {"outer_pid": os.getpid(), "results": (first, second)}

            combined = outer("beta" + str(OFFSET), local)

            first_run = combined.run()
            first_result, second_result = first_run["results"]

            futures = combined._dask_futures  # type: ignore[attr-defined]
            assert futures is not None
            base_key = futures.base.key
            locations = sd_client.client.who_has(futures.base)
            assert locations.get(
                base_key
            ), "Outer transformation did not execute on a worker"

            assert first_run["outer_pid"] != main_pid
            assert first_result[0] == "beta" + str(OFFSET) + "-1", first_result
            assert second_result[0] == "beta" + str(OFFSET) + "-2", second_result
            assert first_result[1] != main_pid

            repeat_tf = outer("beta" + str(OFFSET), local)
            repeat_run = repeat_tf.run()
            all_results = {
                _canonical_result(first_result),
                _canonical_result(second_result),
                *(_canonical_result(r) for r in repeat_run["results"]),
            }
            assert len(all_results) == 2
        finally:
            set_dask_client(None)


def test_nested_transformations_local():
    return _test_nested_transformations(local=True, OFFSET=1000)


def test_nested_transformations_nested_client():
    return _test_nested_transformations(local=False, OFFSET=2000)
