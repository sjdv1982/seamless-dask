"""Best-effort throttle controls for Seamless stream events."""

from __future__ import annotations

import os
import time
from typing import Any

try:
    from distributed.diagnostics.plugin import SchedulerPlugin
except Exception:  # pragma: no cover - distributed is optional at import time
    SchedulerPlugin = object  # type: ignore[misc,assignment]


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return default


DEFAULT_MAX_PAYLOAD = max(
    1, min(10240, _env_int("SEAMLESS_STREAM_MAX_PAYLOAD_BYTES", 8192))
)
DEFAULT_MIN_INTERVAL = max(
    0.05, _env_float("SEAMLESS_STREAM_MIN_INTERVAL_SECONDS", 2.0)
)
DEFAULT_AGGREGATE_TARGET = max(
    1.0, _env_float("SEAMLESS_STREAM_AGGREGATE_THROTTLE_RATE", 50.0)
)


def default_throttle() -> dict[str, float | int]:
    return {
        "max_payload": DEFAULT_MAX_PAYLOAD,
        "min_interval": DEFAULT_MIN_INTERVAL,
    }


def apply_worker_throttle(payload: dict[str, Any] | None = None) -> None:
    try:
        from seamless_transformer import worker as seamless_worker

        if isinstance(payload, dict):
            worker_payload = payload.get("worker") or payload
        else:
            worker_payload = default_throttle()
        seamless_worker.update_stream_throttle(worker_payload)
    except Exception:
        pass


class SeamlessStreamThrottlePlugin(SchedulerPlugin):
    """Scheduler-side throttle estimator for stream event topics.

    This intentionally errs on the quiet side: it observes the scheduler event
    ledger and publishes a tighter throttle only when recent stream traffic is
    above the configured aggregate target.
    """

    def __init__(self, *, target_rate: float = DEFAULT_AGGREGATE_TARGET) -> None:
        self.target_rate = float(target_rate)
        self._last_emit = 0.0
        self._current = default_throttle()

    def start(self, scheduler) -> None:  # type: ignore[override]
        self.scheduler = scheduler
        try:
            scheduler.loop.add_callback(self._tick)
        except Exception:
            pass

    async def _tick(self) -> None:
        scheduler = getattr(self, "scheduler", None)
        if scheduler is None:
            return
        try:
            while True:
                self._publish_if_needed(scheduler)
                await asyncio_sleep(1.0)
        except Exception:
            return

    def _publish_if_needed(self, scheduler) -> None:
        now = time.time()
        events = getattr(scheduler, "events", {}) or {}
        count = 0
        window = 5.0
        cutoff = now - window
        for topic, topic_events in list(events.items()):
            if not isinstance(topic, str) or not topic.startswith("seamless-stream-"):
                continue
            for event in topic_events:
                try:
                    ts = float(event[0])
                except Exception:
                    continue
                if ts >= cutoff:
                    count += 1
        rate = count / window
        if rate <= self.target_rate:
            desired = default_throttle()
        else:
            factor = max(1.0, rate / self.target_rate)
            desired = {
                "max_payload": max(1024, int(DEFAULT_MAX_PAYLOAD / factor)),
                "min_interval": min(30.0, DEFAULT_MIN_INTERVAL * factor),
            }
        if desired == self._current:
            return
        if now - self._last_emit < 3.0:
            return
        self._current = desired
        self._last_emit = now
        try:
            scheduler.log_event("seamless-stream-throttle", desired)
        except Exception:
            pass


async def asyncio_sleep(delay: float) -> None:
    import asyncio

    await asyncio.sleep(delay)


__all__ = [
    "SeamlessStreamThrottlePlugin",
    "apply_worker_throttle",
    "default_throttle",
]

