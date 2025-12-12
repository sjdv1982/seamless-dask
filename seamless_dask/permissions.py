"""Permission gate to throttle Dask submissions and avoid deadlocks.

Implements the policy described in anti-deadlock.txt. The manager is
process-local (driver or worker) and keeps track of a resource counter ``S``
and the throttle capacity ``T * N``. Requests that would exceed the capacity
are batched per 10s epoch and granted/denied according to the load and
random chance; denied requests can trigger a caller-side local fallback.
"""

from __future__ import annotations

import math
import random
import threading
import time
from typing import Callable, Optional

DefaultLoadProvider = Callable[[], float]
ResourceUpdater = Callable[[int], None]


class PermissionManager:
    """Synchronous permission gate with epoch-based batching."""

    def __init__(
        self,
        *,
        workers: int,
        throttle: int,
        load_provider: DefaultLoadProvider,
        resource_updater: ResourceUpdater | None = None,
    ) -> None:
        self.workers = max(1, int(workers))
        self.throttle = max(1, int(throttle))
        # S starts at the number of Seamless workers on this Dask worker.
        self._s_counter = float(self.workers)
        self._load_provider = load_provider
        self._resource_updater = resource_updater
        self._lock = threading.Lock()
        self._pending: list[dict] = []
        self._epoch_thread: Optional[threading.Thread] = None
        self._epoch_deadline: float | None = None
        self._sync_resources()

    @property
    def capacity(self) -> int:
        return self.throttle * self.workers

    def _sync_resources(self) -> None:
        if self._resource_updater is None:
            return
        try:
            self._resource_updater(int(math.ceil(self._s_counter)))
        except Exception:
            pass

    def _schedule_epoch_if_needed(self, deadline: float) -> None:
        if self._epoch_thread is not None and self._epoch_thread.is_alive():
            return
        self._epoch_deadline = deadline
        self._epoch_thread = threading.Thread(
            target=self._process_epoch, args=(deadline,), daemon=True
        )
        self._epoch_thread.start()

    def _process_epoch(self, deadline: float) -> None:
        sleep_for = max(0.0, deadline - time.time())
        if sleep_for:
            time.sleep(sleep_for)
        with self._lock:
            pending = self._pending
            self._pending = []
            self._epoch_thread = None
            self._epoch_deadline = None

            free_ratio = self._safe_load_ratio()
            if free_ratio >= 0.85:
                grant_count = 0
            elif free_ratio >= 0.5:
                grant_count = 1
            else:
                grant_count = max(1, math.ceil(0.05 * self.capacity))

            random.shuffle(pending)
            for entry in pending:
                granted = False
                if grant_count > 0:
                    self._s_counter += 1.0
                    grant_count -= 1
                    granted = True
                    self._sync_resources()
                entry["result"] = granted
                entry["condition"].notify()

    def _safe_load_ratio(self) -> float:
        try:
            ratio = float(self._load_provider())
            if math.isnan(ratio) or math.isinf(ratio):
                return 1.0
            return max(0.0, min(1.0, ratio))
        except Exception:
            return 1.0

    def request(self) -> bool:
        """Request permission; may block until the end of the current epoch."""

        with self._lock:
            if self._s_counter < self.capacity:
                self._s_counter += 1.0
                self._sync_resources()
                return True

            free_ratio = self._safe_load_ratio()
            if free_ratio >= 0.85:
                return False

            now = time.time()
            next_epoch = math.floor(now / 10.0) * 10.0 + 10.0
            cond = threading.Condition(self._lock)
            entry = {"result": None, "condition": cond}
            self._pending.append(entry)
            self._schedule_epoch_if_needed(next_epoch)
            while entry["result"] is None:
                cond.wait()
            return bool(entry["result"])

    def release(self) -> None:
        """Release a previously granted permission."""

        with self._lock:
            baseline = float(self.workers)
            self._s_counter = max(baseline, self._s_counter - 1.0)
            self._sync_resources()

    def shutdown(self) -> None:
        """Best-effort shutdown to prevent lingering epoch threads."""

        with self._lock:
            pending = self._pending
            self._pending = []
            thread = self._epoch_thread
            self._epoch_thread = None
            self._epoch_deadline = None
            for entry in pending:
                entry["result"] = False
                entry["condition"].notify()
        if thread is not None and thread.is_alive():
            try:
                thread.join(timeout=0.1)
            except Exception:
                pass


_manager: PermissionManager | None = None
_load_provider: DefaultLoadProvider | None = None


def _default_load_provider() -> float:
    """Return free-throttle ratio based on Seamless worker semaphores."""
    try:
        from seamless_transformer import worker as st_worker

        used, total = st_worker.get_throttle_load()
        if total == 0:
            return 1.0
        free = max(0, total - used)
        return free / total
    except Exception:
        return 1.0


def configure(
    workers: int,
    throttle: int,
    load_provider: DefaultLoadProvider | None = None,
    resource_updater: ResourceUpdater | None = None,
) -> None:
    """Configure the global permission manager."""
    global _manager, _load_provider
    _load_provider = load_provider or _default_load_provider
    _manager = PermissionManager(
        workers=workers,
        throttle=throttle,
        load_provider=_load_provider,
        resource_updater=resource_updater,
    )


def ensure_configured(
    workers: int, throttle: int, *, resource_updater: ResourceUpdater | None = None
) -> None:
    """Ensure a manager exists; if not, configure with the provided defaults."""
    global _manager
    if _manager is None:
        configure(workers, throttle, resource_updater=resource_updater)


def request_permission() -> bool:
    """Blocking permission request."""
    if _manager is None:
        configure(workers=1, throttle=3)
    return _manager.request()  # type: ignore[union-attr]


def release_permission() -> None:
    """Release a previously granted permission token."""
    if _manager is None:
        return
    _manager.release()


def shutdown_permission_manager() -> None:
    """Stop any background epoch thread and clear the manager."""

    global _manager
    if _manager is None:
        return
    try:
        _manager.shutdown()
    finally:
        _manager = None


__all__ = [
    "configure",
    "ensure_configured",
    "shutdown_permission_manager",
    "release_permission",
    "request_permission",
]
