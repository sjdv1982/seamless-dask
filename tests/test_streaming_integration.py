from __future__ import annotations

import uuid

import seamless
from seamless.transformer import delayed
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client


def test_streaming_transformation_prints_to_client_stdout(capsys) -> None:
    sd_client = create_dummy_client(workers=1, worker_threads=3, spawn_workers=1)
    set_seamless_dask_client(sd_client)
    token = uuid.uuid4().hex
    try:

        @delayed
        def noisy(value: str) -> str:
            print("stream hello " + value, flush=True)
            return value.upper()

        tf = noisy(token)
        tf.streaming = True
        assert tf.run() == token.upper()
        captured = capsys.readouterr()
        assert "stream hello " + token in captured.out
        assert "[base" in captured.out
    finally:
        set_seamless_dask_client(None)
        seamless.close()
