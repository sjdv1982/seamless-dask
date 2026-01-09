"""Global Seamless Dask client handle used by seamless-transformer."""

from __future__ import annotations

import logging
import os
import threading
import traceback
from typing import Optional

try:  # pragma: no cover - optional dependency wiring
    from .client import SeamlessDaskClient
except Exception:  # pragma: no cover - allow import without full install
    SeamlessDaskClient = None  # type: ignore

from .permissions import shutdown_permission_manager

_current_client: Optional["SeamlessDaskClient"] = None
_close_hook_registered = False
_LOGGER = logging.getLogger(__name__)


def _describe_client(client: Optional["SeamlessDaskClient"]) -> str:
    if client is None:
        return "client=None"
    dask_client = getattr(client, "_dummy_dask_client", None) or getattr(
        client, "client", None
    )
    scheduler_addr = None
    try:
        scheduler = getattr(dask_client, "scheduler", None)
        scheduler_addr = getattr(scheduler, "address", None)
    except Exception:
        scheduler_addr = None
    return (
        "client_id="
        + str(id(client))
        + " dask_client_id="
        + str(id(dask_client))
        + " scheduler="
        + str(scheduler_addr)
    )


def _shutdown_client(client: "SeamlessDaskClient") -> None:
    try:
        dask_client = getattr(client, "_dummy_dask_client", None) or getattr(
            client, "client", None
        )
        if dask_client is not None:
            dask_client.close(timeout="2s")
    except Exception:
        pass

    try:
        scheduler = getattr(client, "_dummy_scheduler_handle", None)
        if scheduler is not None:
            scheduler.stop(timeout=2.0)
    except Exception:
        pass

    try:
        shutdown_permission_manager()
    except Exception:
        pass


def set_seamless_dask_client(client: Optional["SeamlessDaskClient"]) -> None:
    """Register (and optionally tear down) the global Seamless Dask client."""

    global _current_client, _close_hook_registered
    if client is _current_client:
        return

    if client is None:
        if _current_client is not None:
            _LOGGER.info(
                "[seamless-dask] Clearing SeamlessDaskClient pid=%s thread=%s %s",
                os.getpid(),
                threading.current_thread().name,
                _describe_client(_current_client),
            )
            _LOGGER.debug(
                "[seamless-dask] Clear stack:\n%s",
                "".join(traceback.format_stack(limit=8)),
            )
            _shutdown_client(_current_client)
        _current_client = None
        return

    if not _close_hook_registered:
        try:
            import seamless

            seamless.register_close_hook(lambda: set_seamless_dask_client(None))
            _close_hook_registered = True
        except Exception:
            pass

    if _current_client is not None:
        _LOGGER.info(
            "[seamless-dask] Replacing SeamlessDaskClient pid=%s thread=%s old=%s new=%s",
            os.getpid(),
            threading.current_thread().name,
            _describe_client(_current_client),
            _describe_client(client),
        )
        _shutdown_client(_current_client)
    else:
        _LOGGER.info(
            "[seamless-dask] Registering SeamlessDaskClient pid=%s thread=%s %s",
            os.getpid(),
            threading.current_thread().name,
            _describe_client(client),
        )

    _current_client = client


def get_seamless_dask_client() -> Optional["SeamlessDaskClient"]:
    """Return the globally configured Seamless Dask client, if any."""

    return _current_client


__all__ = ["get_seamless_dask_client", "set_seamless_dask_client", "SeamlessDaskClient"]
