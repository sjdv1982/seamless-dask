"""Mixin that wires Seamless transformations into Dask."""

from __future__ import annotations

import asyncio
import random
import time
import traceback
from typing import Any, Dict, Optional, TYPE_CHECKING

from seamless import Checksum, CacheMissError
from seamless_transformer.transformation_utils import tf_get_buffer

from .permissions import release_permission, request_permission
from .transformer_client import get_seamless_dask_client
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
        return get_seamless_dask_client()

    def _compute_with_dask(self, require_value: bool) -> Checksum | None:
        client = self._dask_client()
        if client is None:
            return None

        # Build submission up front to obtain tf_checksum for DB cache checks.
        submission = self._build_dask_submission(
            client, require_value=require_value, need_fat=False
        )

        # Fast path: try remote DB before acquiring permissions.
        cached = self._try_database_cache_sync(
            submission.tf_checksum, require_value=require_value
        )
        if cached is not None:
            if submission.tf_checksum:
                self._transformation_checksum = Checksum(submission.tf_checksum)
                self._constructed = True
            self._result_checksum = cached
            self._evaluated = True
            self._exception = None
            return self._result_checksum

        # When execution is 'remote', Dask is mandatory; do not fall back locally.
        try:
            from seamless_config.select import get_execution

            env_requires_remote = get_execution() == "remote"
        except Exception:
            env_requires_remote = False

        permission_acquired = self._wait_for_permission(
            env_requires_remote=env_requires_remote
        )
        if not permission_acquired:
            return self._run_local_fallback_sync(require_value=require_value)

        try:
            futures = self._ensure_dask_futures(
                client,
                require_value=require_value,
                need_fat=False,
                permission_granted=True,
            )
            tf_checksum_hex, result_checksum_hex, exc = futures.thin.result()
        except Exception:
            if permission_acquired:
                release_permission()
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
        client = self._dask_client()
        if client is None:
            return None

        submission = self._build_dask_submission(
            client, require_value=require_value, need_fat=False
        )
        cached = await self._try_database_cache_async(
            submission.tf_checksum, require_value=require_value
        )
        if cached is not None:
            if submission.tf_checksum:
                self._transformation_checksum = Checksum(submission.tf_checksum)
                self._constructed = True
            self._result_checksum = cached
            self._evaluated = True
            self._exception = None
            return self._result_checksum

        try:
            from seamless_config.select import get_execution

            env_requires_remote = get_execution() == "remote"
        except Exception:
            env_requires_remote = False

        permission_acquired = await self._wait_for_permission_async(
            env_requires_remote=env_requires_remote
        )
        if not permission_acquired:
            return await self._run_local_fallback_async(require_value=require_value)

        submission = self._build_dask_submission(
            client, require_value=require_value, need_fat=False
        )
        cached = await self._try_database_cache_async(
            submission.tf_checksum, require_value=require_value
        )
        if cached is not None:
            release_permission()
            if submission.tf_checksum:
                self._transformation_checksum = Checksum(submission.tf_checksum)
                self._constructed = True
            self._result_checksum = cached
            self._evaluated = True
            self._exception = None
            return self._result_checksum

        try:
            futures = self._ensure_dask_futures(
                client,
                require_value=require_value,
                need_fat=False,
                permission_granted=True,
            )
            tf_checksum_hex, result_checksum_hex, exc = await asyncio.wrap_future(
                asyncio.get_running_loop().run_in_executor(None, futures.thin.result)
            )
        except Exception:
            if permission_acquired:
                release_permission()
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

    # ---- internals --------------------------------------------------------
    def _env_allows_local(self) -> bool:
        """Stub for environment analysis; currently always returns True."""

        return True

    def _wait_for_permission(self, *, env_requires_remote: bool) -> bool:
        """Blocking permission acquisition with 10% local fallback on denial."""

        while True:
            granted = request_permission()
            if granted:
                return True
            if env_requires_remote:
                time.sleep(0.05)
                continue
            if random.random() < 0.1:
                return False
            time.sleep(0.05)

    async def _wait_for_permission_async(self, *, env_requires_remote: bool) -> bool:
        while True:
            granted = await asyncio.get_running_loop().run_in_executor(
                None, request_permission
            )
            if granted:
                return True
            if env_requires_remote:
                await asyncio.sleep(0.05)
                continue
            if random.random() < 0.1:
                return False
            await asyncio.sleep(0.05)

    def _run_local_fallback_sync(self, require_value: bool) -> Checksum | None:
        """Execute the transformation locally (no Dask) when permission is denied."""

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)

            async def _run():
                await self._run_dependencies_async(require_value=require_value)
                await self._evaluation(require_value=require_value)
                return self._result_checksum

            return loop.run_until_complete(_run())
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            asyncio.set_event_loop(None)
            loop.close()

    async def _run_local_fallback_async(self, require_value: bool) -> Checksum | None:
        await self._run_dependencies_async(require_value=require_value)
        await self._evaluation(require_value=require_value)
        return self._result_checksum

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
            input_futures[pinname] = client.get_fat_checksum_future(checksum_hex)

        return TransformationSubmission(
            transformation_dict=transformation_dict,
            inputs=inputs,
            input_futures=input_futures,
            tf_checksum=tf_checksum_hex,
            tf_dunder=getattr(self, "_tf_dunder", {}),
            scratch=getattr(self, "_scratch", False),
            meta=getattr(self, "_meta", {}) or {},
            require_value=require_value,
        )

    def _ensure_dask_futures(
        self,
        client: "SeamlessDaskClient",
        *,
        require_value: bool,
        need_fat: bool = False,
        permission_granted: bool = False,
    ) -> TransformationFutures:
        permission_released = False
        if self._dask_futures is not None:
            futures = self._dask_futures
        else:
            submission = self._build_dask_submission(
                client, require_value=require_value, need_fat=need_fat
            )
            if submission.tf_checksum:
                cached = client._transformation_cache.get(submission.tf_checksum)  # type: ignore[attr-defined]
                if cached is not None and not cached[0].base.cancelled():
                    if permission_granted:
                        release_permission()
                        permission_released = True
                    futures = cached[0]
                    self._dask_futures = futures
                    if need_fat and futures.fat is None:
                        futures.fat = client.ensure_fat_future(
                            futures,
                        )
                    return futures
            try:
                futures = client.submit_transformation(submission, need_fat=need_fat)
                self._dask_futures = futures
                if permission_granted and futures.base is not None:
                    futures.base.add_done_callback(lambda _f: release_permission())
                    permission_released = True
            finally:
                if permission_granted and not permission_released:
                    release_permission()
        if need_fat and futures.fat is None:
            futures.fat = client.ensure_fat_future(futures)
        return futures


    # ---- remote cache helpers -----------------------------------------------
    @classmethod
    def _try_database_cache_sync(
        cls, tf_checksum_hex: str | None, *, require_value: bool
    ) -> Checksum | None:
        if not tf_checksum_hex:
            return None
        try:
            from seamless_remote import database_remote
        except Exception:
            return None

        tf_checksum = Checksum(tf_checksum_hex)

        async def _fetch():
            result = await database_remote.get_transformation_result(tf_checksum)
            if result is None:
                return None
            if require_value:
                try:
                    await result.resolution()
                except CacheMissError:
                    return None
                except Exception:
                    return None
            return result

        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_fetch())
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()

    @classmethod
    async def _try_database_cache_async(
        cls, tf_checksum_hex: str | None, *, require_value: bool
    ) -> Checksum | None:
        if not tf_checksum_hex:
            return None
        try:
            from seamless_remote import database_remote
        except Exception:
            return None

        tf_checksum = Checksum(tf_checksum_hex)
        result = await database_remote.get_transformation_result(tf_checksum)
        if result is None:
            return None
        if require_value:
            try:
                await result.resolution()
            except CacheMissError:
                return None
            except Exception:
                return None
        return result


__all__ = ["TransformationDaskMixin"]
