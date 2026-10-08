"""Scheduler-side rate control for Seamless streaming events."""

from __future__ import annotations

import asyncio
import math
import os
import threading
import time
import uuid
from collections.abc import Mapping
from typing import Any

from distributed.diagnostics.plugin import SchedulerPlugin

DEFAULT_MAX_PAYLOAD = 8192
DEFAULT_MIN_INTERVAL = 2.0
DEFAULT_AGGREGATE_RATE = 50.0
DEFAULT_WORKER_RATE = 4.0
STREAM_THROTTLE_TOPIC = "seamless-stream-throttle"
_WORKER_STATE_LOCK = threading.Lock()
_WORKER_STATE_EPOCH: str | None = None
_WORKER_STATE_REVISION = -1


def _positive_int_env(name: str, default: int, *, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return min(max(value, 1), maximum)


def _positive_float_env(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    if not math.isfinite(value):
        value = default
    return min(max(value, 0.01), 3600.0)


def default_stream_throttle() -> dict[str, float | int]:
    return {
        "max_payload": _positive_int_env(
            "SEAMLESS_STREAM_MAX_PAYLOAD_BYTES", DEFAULT_MAX_PAYLOAD, maximum=10_240
        ),
        "min_interval": _positive_float_env(
            "SEAMLESS_STREAM_MIN_INTERVAL_SECONDS", DEFAULT_MIN_INTERVAL
        ),
    }


def update_worker_state(
    payload: dict[str, Any], dask_worker=None
) -> None:
    """Apply a scheduler throttle event on the current Dask worker.

    Client.run injects the reserved dask_worker argument on each worker.
    """
    from distributed.worker import get_worker
    from seamless_transformer import worker as transformer_worker

    worker = dask_worker
    if worker is None:
        worker = get_worker()
    epoch = payload.get("epoch")
    try:
        revision = int(payload.get("revision", 0))
    except (TypeError, ValueError, OverflowError):
        revision = 0
    selected = None
    per_worker = payload.get("per_worker")
    if isinstance(per_worker, dict):
        selected = per_worker.get(getattr(worker, "address", None))
    if not isinstance(selected, dict):
        selected = payload
    try:
        max_payload = min(
            int(payload.get("max_payload", DEFAULT_MAX_PAYLOAD)),
            int(selected.get("max_payload", DEFAULT_MAX_PAYLOAD)),
        )
    except (TypeError, ValueError, OverflowError):
        max_payload = DEFAULT_MAX_PAYLOAD
    try:
        min_interval = max(
            float(payload.get("min_interval", DEFAULT_MIN_INTERVAL)),
            float(selected.get("min_interval", DEFAULT_MIN_INTERVAL)),
        )
    except (TypeError, ValueError, OverflowError):
        min_interval = DEFAULT_MIN_INTERVAL
    global _WORKER_STATE_EPOCH, _WORKER_STATE_REVISION
    with _WORKER_STATE_LOCK:
        if epoch is not None and epoch == _WORKER_STATE_EPOCH:
            if revision < _WORKER_STATE_REVISION:
                return
            if revision == _WORKER_STATE_REVISION:
                return
        transformer_worker.set_stream_throttle(
            max_payload=max_payload,
            min_interval=min_interval,
        )
        if epoch is not None:
            _WORKER_STATE_EPOCH = str(epoch)
            _WORKER_STATE_REVISION = revision


def _params_for_rate(rate: float, target: float, base: dict[str, float | int]) -> dict[str, float | int]:
    if rate < target or target <= 0:
        return dict(base)
    factor = max(1, math.ceil(math.log2(rate / target)))
    return {
        "max_payload": max(1024, int(base["max_payload"]) // (2**factor)),
        "min_interval": min(30.0, float(base["min_interval"]) * (2**factor)),
    }


class SeamlessStreamThrottlePlugin(SchedulerPlugin):
    """Observe stream event rates and broadcast changed limits to Dask workers.

    ``update_rates`` is intentionally deterministic and independent of a live
    scheduler, so hysteresis and hotspot policy can be checked directly.
    """

    idempotent = True

    def __init__(self) -> None:
        self.base = default_stream_throttle()
        self.aggregate_target = _positive_float_env(
            "SEAMLESS_STREAM_AGGREGATE_THROTTLE_RATE", DEFAULT_AGGREGATE_RATE
        )
        self.worker_target = DEFAULT_WORKER_RATE
        self._current = dict(self.base)
        self._per_worker: dict[str, dict[str, float | int]] = {}
        self._epoch = uuid.uuid4().hex
        self._revision = 0
        self._hot_since: dict[str, float] = {}
        self._last_change = float("-inf")
        self._scheduler = None
        self._poll_task: asyncio.Task | None = None

    async def start(self, scheduler) -> None:  # type: ignore[override]
        self._scheduler = scheduler
        scheduler.log_event(STREAM_THROTTLE_TOPIC, self._state_payload())
        self._poll_task = asyncio.create_task(self._poll_events())

    def add_worker(self, scheduler, worker: str) -> None:  # type: ignore[override]
        # Replaying current state lets workers added after the initial throttle
        # change converge without adding a separate worker-side subscription.
        scheduler.log_event(STREAM_THROTTLE_TOPIC, self._state_payload())

    async def close(self) -> None:  # type: ignore[override]
        task = self._poll_task
        self._poll_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def update_rates(
        self,
        aggregate_rate: float,
        worker_rates: Mapping[str, float],
        *,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        """Update throttle state from rates and return a broadcast payload."""
        current_time = time.monotonic() if now is None else float(now)
        try:
            aggregate = max(float(aggregate_rate), 0.0)
        except (TypeError, ValueError, OverflowError):
            aggregate = 0.0
        desired_global = _params_for_rate(aggregate, self.aggregate_target, self.base)

        observed: dict[str, float] = {}
        for worker, rate in worker_rates.items():
            try:
                observed[str(worker)] = max(float(rate), 0.0)
            except (TypeError, ValueError, OverflowError):
                continue
        for worker in tuple(self._hot_since):
            if observed.get(worker, 0.0) <= self.worker_target:
                self._hot_since.pop(worker, None)
        for worker, rate in observed.items():
            if rate > self.worker_target:
                self._hot_since.setdefault(worker, current_time)

        desired_workers: dict[str, dict[str, float | int]] = {}
        for worker, rate in observed.items():
            hot_since = self._hot_since.get(worker)
            if hot_since is not None and current_time - hot_since > 5.0:
                params = _params_for_rate(rate, self.worker_target, self.base)
                if params != self.base:
                    desired_workers[worker] = params

        tightened = False
        relaxed = False

        next_global, did_tighten, did_relax = self._apply_hysteresis(
            self._current, desired_global, current_time
        )
        tightened |= did_tighten
        relaxed |= did_relax

        all_workers = set(self._per_worker) | set(desired_workers)
        next_workers: dict[str, dict[str, float | int]] = {}
        for worker in all_workers:
            old = self._per_worker.get(worker, self.base)
            desired = desired_workers.get(worker, self.base)
            selected, did_tighten, did_relax = self._apply_hysteresis(
                old, desired, current_time
            )
            tightened |= did_tighten
            relaxed |= did_relax
            if selected != self.base:
                next_workers[worker] = selected

        changed = next_global != self._current or next_workers != self._per_worker
        if not changed:
            return None
        self._current = next_global
        self._per_worker = next_workers
        self._last_change = current_time
        self._revision += 1
        return self._state_payload()

    def _state_payload(self) -> dict[str, Any]:
        return {
            "epoch": self._epoch,
            "revision": self._revision,
            "max_payload": self._current["max_payload"],
            "min_interval": self._current["min_interval"],
            "per_worker": {
                worker: dict(params) for worker, params in self._per_worker.items()
            },
        }

    def _apply_hysteresis(
        self,
        current: dict[str, float | int],
        desired: dict[str, float | int],
        now: float,
    ) -> tuple[dict[str, float | int], bool, bool]:
        if current == desired:
            return dict(current), False, False
        tighter = (
            int(desired["max_payload"]) < int(current["max_payload"])
            or float(desired["min_interval"]) > float(current["min_interval"])
        )
        looser = (
            int(desired["max_payload"]) > int(current["max_payload"])
            or float(desired["min_interval"]) < float(current["min_interval"])
        )
        if tighter and now - self._last_change >= 3.0:
            return dict(desired), True, False
        if looser and now - self._last_change >= 10.0:
            return dict(desired), False, True
        return dict(current), False, False

    async def _poll_events(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            scheduler = self._scheduler
            if scheduler is None:
                continue
            try:
                events = scheduler.get_events()
                now = time.time()
                aggregate_count = 0
                worker_counts: dict[str, int] = {}
                for topic, entries in events.items():
                    if not isinstance(topic, str) or not topic.startswith("seamless-stream-"):
                        continue
                    if topic == "seamless-stream-throttle":
                        continue
                    for timestamp, message in entries:
                        if now - 5.0 <= timestamp <= now and isinstance(message, dict):
                            aggregate_count += 1
                            worker = message.get("worker") or message.get("_worker")
                            if worker is not None:
                                key = str(worker)
                                worker_counts[key] = worker_counts.get(key, 0) + 1
                payload = self.update_rates(
                    aggregate_count / 5.0,
                    {worker: count / 5.0 for worker, count in worker_counts.items()},
                    now=time.monotonic(),
                )
                if payload is not None:
                    scheduler.log_event(STREAM_THROTTLE_TOPIC, payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                # A transient scheduler or worker RPC error should not stop future
                # rate samples from applying throttles.
                import logging

                logging.getLogger(__name__).debug(
                    "Stream throttle polling failed", exc_info=True
                )
