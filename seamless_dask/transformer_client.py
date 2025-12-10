"""Global Dask client handle used by seamless-transformer when present."""

from __future__ import annotations

from typing import Optional

try:  # pragma: no cover - optional dependency wiring
    from .client import SeamlessDaskClient
except Exception:  # pragma: no cover - allow import without full install
    SeamlessDaskClient = None  # type: ignore

_default_client: Optional["SeamlessDaskClient"] = None


def set_dask_client(client: Optional["SeamlessDaskClient"]) -> None:
    """Register the Dask client used by seamless-transformer."""
    global _default_client
    _default_client = client


def get_dask_client() -> Optional["SeamlessDaskClient"]:
    """Return the globally configured Seamless Dask client, if any."""
    return _default_client


__all__ = ["get_dask_client", "set_dask_client", "SeamlessDaskClient"]

