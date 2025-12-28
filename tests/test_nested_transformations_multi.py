import os
import time

import seamless
import pytest
from seamless.transformer import delayed, spawn
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client
from tqdm import tqdm


@pytest.fixture(scope="session", autouse=True)
def _close_seamless_session():
    """Ensure Seamless shuts down once after the full test session."""
    import seamless

    yield
    seamless.close()


def test_nested_transformations_multi():
    """Stress nested + nested-nested execution with many small jobs."""

    main_pid = os.getpid()
    job_count = 1
    spawn_workers = 5

    sd_client = create_dummy_client(
        workers=1, spawn_workers=spawn_workers, worker_threads=3 * spawn_workers
    )
    set_seamless_dask_client(sd_client)
    try:

        @delayed
        def outer(label: str):
            from seamless.transformer import delayed, direct

            @direct
            def middle(label: str):
                from seamless.transformer import delayed, direct

                def leaf(label: str):
                    import time
                    import os
                    from seamless.transformer import global_lock

                    time.sleep(0.5)
                    return label, os.getpid()

                leaf = delayed(leaf)

                left = leaf(f"{label}-a").start()

                right = leaf(f"{label}-b").start()
                return left.run(), right.run()

            first = middle(f"{label}-1")
            second = middle(f"{label}-2")
            return first, second

        start = time.perf_counter()
        tasks = [outer(f"job-{idx}").start() for idx in range(job_count)]
        results = [tf.run() for tf in tqdm(tasks)]
        duration = time.perf_counter() - start

        expected_labels = {
            f"job-{idx}-{suffix}"
            for idx in range(job_count)
            for suffix in ("1-a", "1-b", "2-a", "2-b")
        }

        seen_labels = set()
        seen_pids = set()
        for outer_res in results:
            for mid in outer_res:
                for leaf_res in mid:
                    label, pid = leaf_res
                    seen_labels.add(label)
                    seen_pids.add(pid)
                    assert pid != main_pid

        assert expected_labels == seen_labels
        assert len(seen_pids) >= 2  # should run on spawned workers
        import math

        if job_count <= 50:
            assert duration * 0.9 - 2 < job_count / (
                spawn_workers * 0.5
            )  # should complete with reasonable (half of spawn workers) concurrency, and 2 + 10 % overhead
            # An outer job should be 1 sec: 4 half-second jobs of which 2 are in parallel
        else:
            # with more than 50 jobs for 5 cores, with 2 levels of nesting, we're flooded.
            # we're happy to be out of deadlock
            pass

    finally:
        set_seamless_dask_client(None)
        try:
            seamless.close()
        except Exception:
            pass
