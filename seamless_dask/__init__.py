"""Seamless utilities for working with Dask."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = [
    "SeamlessDaskClient",
    "get_seamless_dask_client",
    "set_seamless_dask_client",
    "TransformationDaskMixin",
    "DummySchedulerHandle",
    "create_dummy_client",
    "create_dummy_scheduler",
    "serve_dummy_scheduler",
    "start_dummy_scheduler",
    "get_dummy_scheduler_address",
]

# Map exported symbols to (module, attribute) for lazy loading. This prevents
# heavyweight dependencies such as ``distributed`` from being imported when the
# package is merely used as a namespace (e.g. ``import seamless_dask.wrapper``).
_LAZY_EXPORTS = {
    "SeamlessDaskClient": (".client", "SeamlessDaskClient"),
    "get_seamless_dask_client": (
        ".transformer_client",
        "get_seamless_dask_client",
    ),
    "set_seamless_dask_client": (
        ".transformer_client",
        "set_seamless_dask_client",
    ),
    "TransformationDaskMixin": (".transformation_mixin", "TransformationDaskMixin"),
    "DummySchedulerHandle": (".dummy_scheduler", "DummySchedulerHandle"),
    "create_dummy_client": (".dummy_scheduler", "create_dummy_client"),
    "create_dummy_scheduler": (".dummy_scheduler", "create_dummy_scheduler"),
    "serve_dummy_scheduler": (".dummy_scheduler", "serve_dummy_scheduler"),
    "start_dummy_scheduler": (".dummy_scheduler", "start_dummy_scheduler"),
    "get_dummy_scheduler_address": (
        ".dummy_scheduler",
        "get_dummy_scheduler_address",
    ),
}

if TYPE_CHECKING:
    from .client import SeamlessDaskClient
    from .dummy_scheduler import (
        DummySchedulerHandle,
        create_dummy_client,
        create_dummy_scheduler,
        get_dummy_scheduler_address,
        serve_dummy_scheduler,
        start_dummy_scheduler,
    )
    from .transformer_client import (
        get_seamless_dask_client,
        set_seamless_dask_client,
    )
    from .transformation_mixin import TransformationDaskMixin


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(target[0], __name__)
    value = getattr(module, target[1])
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + __all__))
