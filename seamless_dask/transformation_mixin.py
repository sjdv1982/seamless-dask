"""Mixin that wires Seamless transformations into Dask."""

from __future__ import annotations

import asyncio
import traceback
from typing import Any, Dict, Optional, TYPE_CHECKING

from seamless import Checksum
from seamless_transformer.transformation_utils import tf_get_buffer

from .transformer_client import get_dask_client
from .types import (
    TransformationFutures,
    TransformationInputSpec,
    TransformationSubmission,
)

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from .client import SeamlessDaskClient


class TransformationDaskMixin:
    """Shared Dask helpers for seamless-transformer Transformation."""

    _dask_futures: TransformationFutures | None = None

    # ---- public helpers used by Transformation ----------------------------
    def _dask_client(self) -> Optional["SeamlessDaskClient"]:
        return get_dask_client()

    def _compute_with_dask(self, require_value: bool) -> Checksum | None:
        client = self._dask_client()
        if client is None:
            return None
        try:
            futures = self._ensure_dask_futures(
                client, require_value=require_value, need_fat=False
            )
            tf_checksum_hex, result_checksum_hex, exc = futures.thin.result()
        except Exception:
            self._exception = traceback.format_exc().strip("\n") + "\n"
            self._constructed = True
            self._evaluated = True
            return None
        if exc:
            self._exception = exc if exc.endswith("\n") else exc + "\n"
            self._constructed = True
            self._evaluated = True
            return None
        if tf_checksum_hex:
            self._transformation_checksum = Checksum(tf_checksum_hex)
            self._constructed = True
        if result_checksum_hex:
            self._result_checksum = Checksum(result_checksum_hex)
            self._evaluated = True
        return self._result_checksum

    async def _compute_with_dask_async(self, require_value: bool) -> Checksum | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._compute_with_dask, require_value)

    # ---- internals --------------------------------------------------------
    def _get_resource(self) -> str:
        meta = getattr(self, "_meta", None)
        if isinstance(meta, dict):
            return meta.get("resource", "default")
        return "default"

    def _build_dask_submission(
        self, client: "SeamlessDaskClient", *, require_value: bool, need_fat: bool
    ) -> TransformationSubmission:
        pretransformation = getattr(self, "_pretransformation", None)
        if pretransformation is None:
            raise RuntimeError("No pre-transformation available for Dask")

        upstream_dependencies = getattr(self, "_upstream_dependencies", {}) or {}
        transformation_dict, dependencies = (
            pretransformation.build_partial_transformation(upstream_dependencies)
        )
        tf_checksum_hex: str | None = None
        if not dependencies:
            tf_buffer = tf_get_buffer(transformation_dict)
            tf_buffer.tempref()
            tf_checksum = tf_buffer.get_checksum()
            tf_checksum_hex = tf_checksum.hex()
            self._transformation_checksum = tf_checksum
            self._constructed = True

        inputs: dict[str, TransformationInputSpec] = {}
        input_futures: dict[str, Any] = {}
        resource = self._get_resource()
        for pinname, value in transformation_dict.items():
            if pinname.startswith("__"):
                continue
            if not isinstance(value, tuple) or len(value) < 3:
                continue
            celltype, subcelltype, checksum_hex = value
            dependency = dependencies.get(pinname)
            if dependency is not None:
                if dependency.exception is not None:
                    msg = f"Dependency '{pinname}' has an exception."
                    raise RuntimeError(msg)
                dep_futures = dependency._ensure_dask_futures(  # type: ignore[attr-defined]
                    client, require_value=require_value, need_fat=True
                )
                inputs[pinname] = TransformationInputSpec(
                    name=pinname,
                    celltype=celltype,
                    subcelltype=subcelltype,
                    checksum=None,
                    kind="transformation",
                )
                input_futures[pinname] = dep_futures.fat
                continue

                # noqa: B007 (false positive)
            if checksum_hex is None:
                raise RuntimeError(f"Input '{pinname}' has no checksum")
            if isinstance(checksum_hex, Checksum):
                checksum_hex = checksum_hex.hex()
            inputs[pinname] = TransformationInputSpec(
                name=pinname,
                celltype=celltype,
                subcelltype=subcelltype,
                checksum=checksum_hex,
                kind="checksum",
            )
            input_futures[pinname] = client.get_fat_checksum_future(
                checksum_hex, resource=resource
            )

        return TransformationSubmission(
            transformation_dict=transformation_dict,
            inputs=inputs,
            input_futures=input_futures,
            tf_checksum=tf_checksum_hex,
            tf_dunder=getattr(self, "_tf_dunder", {}),
            scratch=getattr(self, "_scratch", False),
            resource=resource,
            meta=getattr(self, "_meta", {}) or {},
            require_value=require_value,
        )

    def _ensure_dask_futures(
        self,
        client: "SeamlessDaskClient",
        *,
        require_value: bool,
        need_fat: bool = False,
    ) -> TransformationFutures:
        if self._dask_futures is not None:
            futures = self._dask_futures
        else:
            submission = self._build_dask_submission(
                client, require_value=require_value, need_fat=need_fat
            )
            futures = client.submit_transformation(submission, need_fat=need_fat)
            self._dask_futures = futures
        if need_fat and futures.fat is None:
            futures.fat = client.ensure_fat_future(
                futures, resource=self._get_resource()
            )
        return futures


__all__ = ["TransformationDaskMixin"]
