import time

import pytest
import seamless.config
from seamless.transformer import delayed
from seamless_config import select as config_select
from seamless_dask.dummy_scheduler import create_dummy_client
from seamless_dask.transformer_client import set_seamless_dask_client


@pytest.fixture(scope="session", autouse=True)
def _close_seamless_session():
    """Ensure Seamless shuts down once after the full test session."""
    import seamless

    yield
    seamless.close()


@delayed
def outer(nonce, nested_nparallel):
    import seamless.config
    from seamless.transformer import delayed

    @delayed
    def inner(nonce):
        import seamless.config

        try:
            return seamless.config.get_nparallel()
        except RuntimeError:
            return "unset"

    inner.local = False

    try:
        seen = seamless.config.get_nparallel()
    except RuntimeError:
        seen = "unset"
    if nested_nparallel is not None:
        seamless.config.set_nparallel(nested_nparallel)
    return [seen, inner(nonce).run()]


def _run_outer(nested_nparallel):
    sd_client = create_dummy_client(workers=1, worker_threads=3, spawn_workers=3)
    set_seamless_dask_client(sd_client)
    try:
        return list(outer(time.time_ns(), nested_nparallel).run())
    finally:
        set_seamless_dask_client(None)


def test_nparallel_propagates_into_dask_transformation(monkeypatch):
    monkeypatch.setattr(config_select, "_current_nparallel", None)
    seamless.config.set_nparallel(7)
    assert _run_outer(None) == [7, 7]


def test_nparallel_propagates_from_nested_submitter(monkeypatch):
    monkeypatch.setattr(config_select, "_current_nparallel", None)
    seamless.config.set_nparallel(7)
    # The nested transformation follows its submitter, not the original client.
    assert _run_outer(5) == [7, 5]


def test_unset_nparallel_stays_unset(monkeypatch):
    monkeypatch.setattr(config_select, "_current_nparallel", None)
    assert _run_outer(None) == ["unset", "unset"]
