import os
import time

import seamless
from seamless.transformer import delayed
from seamless_transformer import worker
from seamless_dask.default import default_client
from seamless_dask.transformer_client import set_dask_client
from tqdm import tqdm

tqdm = lambda x: x  ### debug


def test_nested_transformations_stress():
    """Stress nested + nested-nested execution with many small jobs."""

    main_pid = os.getpid()
    job_count = 1

    with default_client(workers=1, spawn_workers=5, worker_threads=10) as sd_client:
        set_dask_client(sd_client)
        try:

            @delayed
            def outer(label: str):
                from seamless.transformer import delayed, direct

                ###@direct
                def middle(label: str):
                    from seamless.transformer import delayed, direct

                    def leaf(label: str):
                        import time
                        import os

                        time.sleep(0.1)
                        return label, os.getpid()

                    leaf = delayed(leaf)

                    left = leaf(f"{label}-a").start()

                    right = leaf(f"{label}-b").start()
                    return left.run(), right.run()

                first = middle(f"{label}-1")
                second = middle(f"{label}-2")
                # return first, second
                return first, first

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
            assert duration < 30.0  # should complete with reasonable concurrency
        finally:
            set_dask_client(None)
            worker.shutdown_workers()
            seamless.close()
