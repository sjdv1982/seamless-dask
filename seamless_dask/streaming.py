"""Streaming event rendering and lifecycle mixin for Seamless Dask clients."""

from __future__ import annotations

import asyncio
from contextlib import nullcontext
import logging
import os
import sys
import threading
import time
from typing import Any

from .stream_throttle import (
    STREAM_THROTTLE_TOPIC,
    SeamlessStreamThrottlePlugin,
    update_worker_state,
)
from .types import TransformationFutures

_LOGGER = logging.getLogger(__name__)


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    return default


_TQDM_AUTO = object()

class TqdmStreamRenderer:
    """Render streamed tqdm lifecycle chunks in the local client process."""

    def __init__(
        self,
        base_key: str,
        *,
        output=None,
        tqdm_module=_TQDM_AUTO,
    ) -> None:
        self.base_key = str(base_key)
        self.output = output if output is not None else sys.stderr
        self.bars: dict[str, Any] = {}
        self._fallback: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._closed = False
        if tqdm_module is _TQDM_AUTO:
            try:
                import tqdm as tqdm_module
            except ImportError:
                tqdm_module = None
        self._tqdm_factory = getattr(tqdm_module, "tqdm", None)
        if not callable(self._tqdm_factory) and callable(tqdm_module):
            self._tqdm_factory = tqdm_module

    def handle(self, message: dict[str, Any]) -> None:
        if not isinstance(message, dict):
            return
        kind = message.get("kind")
        bar_id = message.get("bar_id")
        if kind not in {"tqdm_open", "tqdm_update", "tqdm_close"}:
            return
        if not isinstance(bar_id, str) or not bar_id:
            return
        with self._lock:
            if self._closed:
                return
            if kind == "tqdm_open":
                self._open(message, bar_id)
            elif kind == "tqdm_update":
                self._update(message, bar_id)
            else:
                self._close(message, bar_id)

    def write_text(self, output, text: str) -> None:
        """Write text without leaving active tqdm bars stranded above it."""
        with self._lock:
            external_write_mode = getattr(
                self._tqdm_factory, "external_write_mode", None
            )
            try:
                mode = (
                    external_write_mode(file=None)
                    if callable(external_write_mode)
                    else nullcontext()
                )
            except Exception:
                mode = nullcontext()
            try:
                with mode:
                    output.write(text)
                    output.flush()
            except Exception:
                _LOGGER.debug(
                    "Could not write text alongside streamed tqdm bars",
                    exc_info=True,
                )

    def _open(self, message: dict[str, Any], bar_id: str) -> None:
        self._close({}, bar_id)
        desc = message.get("desc")
        total = message.get("total")
        unit = message.get("unit", "it")
        if self._tqdm_factory is None:
            self._fallback[bar_id] = {
                "desc": desc,
                "n": message.get("n", 0),
                "total": total,
            }
            self._write_fallback(bar_id, final=False)
            return

        kwargs = {
            "desc": desc,
            "total": total,
            "unit": unit,
            "unit_scale": message.get("unit_scale", False),
            "file": self.output,
        }
        if message.get("bar_format") is not None:
            kwargs["bar_format"] = message["bar_format"]
        try:
            bar = self._tqdm_factory(**kwargs)
        except Exception:
            _LOGGER.debug("Could not create a local tqdm bar", exc_info=True)
            self._fallback[bar_id] = {
                "desc": desc,
                "n": message.get("n", 0),
                "total": total,
            }
            self._write_fallback(bar_id, final=False)
            return
        self.bars[bar_id] = bar
        if message.get("n") not in (None, 0):
            self._update(message, bar_id)

    def _update(self, message: dict[str, Any], bar_id: str) -> None:
        if bar_id in self._fallback:
            state = self._fallback[bar_id]
            if "n" in message:
                state["n"] = message["n"]
            if "total" in message:
                state["total"] = message["total"]
            self._write_fallback(bar_id, final=False)
            return
        bar = self.bars.get(bar_id)
        if bar is None:
            return
        try:
            if "total" in message:
                bar.total = message["total"]
            if "n" in message:
                bar.n = message["n"]
            postfix = message.get("postfix")
            if postfix is not None:
                set_postfix = getattr(bar, "set_postfix_str", None)
                if callable(set_postfix):
                    set_postfix(str(postfix), refresh=False)
                else:
                    bar.postfix = str(postfix)
            bar.refresh()
        except Exception:
            _LOGGER.debug("Could not refresh a local tqdm bar", exc_info=True)

    def _close(self, message: dict[str, Any], bar_id: str) -> None:
        if bar_id in self._fallback:
            state = self._fallback[bar_id]
            if "n" in message:
                state["n"] = message["n"]
            if "total" in message:
                state["total"] = message["total"]
            self._write_fallback(bar_id, final=True)
            self._fallback.pop(bar_id, None)
            return
        bar = self.bars.pop(bar_id, None)
        if bar is None:
            return
        try:
            if "total" in message:
                bar.total = message["total"]
            if "n" in message:
                bar.n = message["n"]
            bar.refresh()
        except Exception:
            _LOGGER.debug("Could not render the final local tqdm state", exc_info=True)
        try:
            bar.close()
        except Exception:
            _LOGGER.debug("Could not close a local tqdm bar", exc_info=True)

    def _write_fallback(self, bar_id: str, *, final: bool) -> None:
        state = self._fallback.get(bar_id)
        if state is None:
            return
        total = state.get("total")
        total_text = "?" if total is None else str(total)
        desc = state.get("desc")
        label = f" {desc}" if desc else ""
        ending = "\n" if final else ""
        try:
            self.output.write(
                f"\r[{bar_id}]{label} {state.get('n', 0)}/{total_text}{ending}"
            )
            self.output.flush()
        except Exception:
            _LOGGER.debug("Could not write a tqdm fallback update", exc_info=True)

    def close_all(self) -> None:
        with self._lock:
            if self._closed:
                return
            for bar_id in tuple(self.bars):
                self._close({}, bar_id)
            for bar_id in tuple(self._fallback):
                self._close({}, bar_id)
            self._closed = True


class StreamingMixin:
    def _register_stream_throttle_plugin(self) -> None:
        if not getattr(self._client, "_seamless_stream_throttle_handler", None):
            dask_client = self._client
            asynchronous = bool(getattr(dask_client, "asynchronous", False))
            sync_apply_lock = threading.Lock()
            async_apply_lock = asyncio.Lock()
            last_applied_timestamp = {"value": float("-inf")}

            def _parse_event(event):
                try:
                    timestamp, payload = event
                    timestamp = float(timestamp)
                except (TypeError, ValueError, OverflowError):
                    return None
                if not isinstance(payload, dict):
                    return None
                return timestamp, payload

            def _apply_on_workers(payload) -> None:
                # None means all current workers. Avoid scheduler_info(), whose
                # default only returns a limited worker subset.
                dask_client.run(
                    update_worker_state,
                    payload,
                    workers=None,
                )

            async def _apply_on_workers_async(payload) -> None:
                result = dask_client.run(
                    update_worker_state,
                    payload,
                    workers=None,
                )
                if getattr(result, "__await__", None) is not None:
                    await result

            async def _handle_throttle_event(event) -> None:
                parsed = _parse_event(event)
                if parsed is None:
                    return
                timestamp, payload = parsed
                if asynchronous:
                    async with async_apply_lock:
                        if timestamp <= last_applied_timestamp["value"]:
                            return
                        for attempt in range(3):
                            try:
                                await _apply_on_workers_async(payload)
                                last_applied_timestamp["value"] = timestamp
                                return
                            except Exception:
                                if attempt == 2:
                                    _LOGGER.debug(
                                        "Could not apply a stream throttle event",
                                        exc_info=True,
                                    )
                                    return
                                await asyncio.sleep(0.25 * (2**attempt))
                    return

                def _apply_if_current() -> None:
                    with sync_apply_lock:
                        if timestamp <= last_applied_timestamp["value"]:
                            return
                        for attempt in range(3):
                            try:
                                _apply_on_workers(payload)
                                last_applied_timestamp["value"] = timestamp
                                return
                            except Exception:
                                if attempt == 2:
                                    _LOGGER.debug(
                                        "Could not apply a stream throttle event",
                                        exc_info=True,
                                    )
                                    return
                                time.sleep(0.25 * (2**attempt))

                await asyncio.to_thread(_apply_if_current)

            self._stream_throttle_handler = _handle_throttle_event
            try:
                self._client.subscribe_topic(
                    STREAM_THROTTLE_TOPIC, self._stream_throttle_handler
                )
                setattr(
                    self._client,
                    "_seamless_stream_throttle_handler",
                    self._stream_throttle_handler,
                )
            except Exception:
                _LOGGER.debug(
                    "Could not subscribe to stream throttle events", exc_info=True
                )
                self._stream_throttle_handler = None

            # A scheduler plugin may already be running when this Client connects,
            # so its start event will not be replayed by registration. Reapply the
            # latest retained public scheduler event after subscribing. Timestamp
            # ordering prevents this catch-up from replacing a newer live update.
            if self._stream_throttle_handler is not None:
                async def _catch_up_async() -> None:
                    try:
                        history = dask_client.get_events(STREAM_THROTTLE_TOPIC)
                        if getattr(history, "__await__", None) is not None:
                            history = await history
                        if isinstance(history, dict):
                            history = history.get(STREAM_THROTTLE_TOPIC, ())
                        if history:
                            await _handle_throttle_event(history[-1])
                    except Exception:
                        _LOGGER.debug(
                            "Could not read current stream throttle state",
                            exc_info=True,
                        )

                if asynchronous:
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                    if loop is not None:
                        loop.create_task(_catch_up_async())
                    else:
                        dask_loop = getattr(dask_client, "loop", None)
                        if dask_loop is not None:
                            try:
                                dask_loop.add_callback(
                                    lambda: asyncio.create_task(_catch_up_async())
                                )
                            except Exception:
                                _LOGGER.debug(
                                    "Could not schedule stream throttle catch-up",
                                    exc_info=True,
                                )
                else:
                    try:
                        history = self._client.get_events(STREAM_THROTTLE_TOPIC)
                        if isinstance(history, dict):
                            history = history.get(STREAM_THROTTLE_TOPIC, ())
                        if history:
                            parsed = _parse_event(history[-1])
                            if parsed is not None:
                                timestamp, payload = parsed
                                def _apply_catch_up() -> None:
                                    with sync_apply_lock:
                                        if timestamp <= last_applied_timestamp["value"]:
                                            return
                                        for attempt in range(3):
                                            try:
                                                _apply_on_workers(payload)
                                                last_applied_timestamp["value"] = timestamp
                                                return
                                            except Exception:
                                                if attempt == 2:
                                                    raise
                                                time.sleep(0.25 * (2**attempt))
                                _apply_catch_up()
                    except Exception:
                        _LOGGER.debug(
                            "Could not read current stream throttle state",
                            exc_info=True,
                        )
        try:
            self._client.register_plugin(
                SeamlessStreamThrottlePlugin(),
                name="seamless-stream-throttle",
            )
        except Exception:
            _LOGGER.debug("Could not register the stream throttle plugin", exc_info=True)

    def _subscribe_stream_topic(self, topic: str, base_key: str) -> None:
        with self._stream_topic_lock:
            current = self._stream_topics.get(topic)
            if current is not None:
                current["refs"] += 1
                return

            renderer = TqdmStreamRenderer(base_key)

            def _handler(event) -> None:
                self._render_stream_event(base_key, event, renderer=renderer)

            self._client.subscribe_topic(topic, _handler)
            self._stream_topics[topic] = {
                "handler": _handler,
                "renderer": renderer,
                "bars": renderer.bars,
                "refs": 1,
            }

    def _render_stream_event(
        self,
        base_key: str,
        event,
        *,
        renderer: TqdmStreamRenderer | None = None,
    ) -> None:
        try:
            _timestamp, message = event
        except (TypeError, ValueError):
            return
        if not isinstance(message, dict):
            return

        if renderer is None:
            topic = f"seamless-stream-{base_key}"
            with self._stream_topic_lock:
                current = self._stream_topics.get(topic)
                if current is not None:
                    renderer = current.get("renderer")

        kind = message.get("kind", "stream")
        if kind in {"tqdm_open", "tqdm_update", "tqdm_close"}:
            if renderer is not None:
                renderer.handle(message)
            return
        if kind != "stream":
            return

        stream_name = message.get("stream")
        if stream_name not in ("stdout", "stderr"):
            return
        text = message.get("text")
        if not isinstance(text, str) or not text:
            return

        prefix = f"[{base_key[:12]}] "
        if _parse_bool_env("SEAMLESS_STREAM_COLOR", default=False):
            color = 31 + (sum(base_key.encode("utf-8")) % 6)
            prefix = f"\033[{color}m{prefix}\033[0m"
        try:
            truncated = max(int(message.get("truncated_head_bytes", 0)), 0)
        except (TypeError, ValueError, OverflowError):
            truncated = 0
        marker = f"[... {truncated} bytes truncated from head ...]\n" if truncated else ""
        output = sys.stderr if stream_name == "stderr" else sys.stdout
        rendered_text = prefix + marker + text
        if renderer is not None:
            renderer.write_text(output, rendered_text)
        else:
            try:
                output.write(rendered_text)
                output.flush()
            except Exception:
                _LOGGER.debug(
                    "Could not write a streamed chunk to local output", exc_info=True
                )

    def _release_stream_topic(
        self, topic: str, token: dict[str, bool] | None = None
    ) -> None:
        with self._stream_topic_lock:
            if token is not None:
                if token.get("released"):
                    return
                token["released"] = True
            current = self._stream_topics.get(topic)
            if current is None:
                return
            current["refs"] -= 1
            if current["refs"] > 0:
                return
            self._stream_topics.pop(topic, None)
            renderer = current.get("renderer")
            if renderer is not None:
                renderer.close_all()
            try:
                self._client.unsubscribe_topic(topic)
            except Exception:
                _LOGGER.debug(
                    "Could not unsubscribe from stream topic %s", topic, exc_info=True
                )

    def _release_stream_for_futures(self, futures: TransformationFutures) -> None:
        with self._stream_topic_lock:
            if futures.stream_topic_released or not futures.stream_topic:
                return
            futures.stream_topic_released = True
            topic = futures.stream_topic
            token = futures._stream_release_token
        self._release_stream_topic(topic, token)

__all__ = ["TqdmStreamRenderer", "StreamingMixin"]
