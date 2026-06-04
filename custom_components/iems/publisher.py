"""MQTT publisher for iEMS telemetry + heartbeat.

The publish callable is injected so tests can mock without pulling paho-mqtt.
Production wiring injects `IotCorePublisher.publish` (see iot_core.py).

Signature of `publish_fn`:
    async def publish_fn(*, topic: str, payload: dict, qos: int) -> bool
        Returns True on success. Exceptions are caught here and treated
        as publish failures (payload is enqueued).
"""
from __future__ import annotations

import logging
from collections import deque
from typing import Awaitable, Callable

from .const import (
    BACKOFF_INITIAL_SECONDS,
    BACKOFF_MAX_SECONDS,
    HEARTBEAT_TOPIC_TEMPLATE,
    MAX_QUEUE_DEPTH,
    SETUP_TOPIC_TEMPLATE,
    TELEMETRY_TOPIC_TEMPLATE,
)

log = logging.getLogger("iems.publisher")

PublishFn = Callable[..., Awaitable[bool]]


# v0.2.5 (2026-05-26) — DATA-LOSS FIX: catch awscrt errors in _safe_publish.
#
# Pre-v0.2.5, `_safe_publish` only caught (OSError, TimeoutError, ValueError).
# When iot_core.publish() exhausted its v0.2.3 retry loop on a persistent
# AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION, the AwsCrtError propagated
# UNCAUGHT out of _safe_publish.  The `publish_telemetry`'s
# `self._queue.append(payload)` line never executed.  Every failed flush
# dropped its full row set (5h+ of production data lost on 2026-05-26).
#
# Fix: import awscrt.exceptions.AwsCrtError lazily and add it to the except
# tuple.  awscrt is required at runtime (declared in manifest.json) but
# unit tests run without it — so we import lazily and silently degrade
# (the type-name check below still catches doubles raised in tests).
try:  # pragma: no cover — import path varies by env
    from awscrt.exceptions import AwsCrtError as _AwsCrtError
except Exception:  # noqa: BLE001 — awscrt not installed in test env
    _AwsCrtError = None  # type: ignore[assignment]


def _is_awscrt_error(exc: BaseException) -> bool:
    """True if exc is awscrt.exceptions.AwsCrtError OR a test double of it.

    Production: matches by isinstance against the real class.
    Tests: matches by class-name string so test doubles need not depend on
    awscrt.  This mirrors the classifier predicate in iot_core.py.
    """
    if _AwsCrtError is not None and isinstance(exc, _AwsCrtError):
        return True
    return type(exc).__name__ == "AwsCrtError"


def backoff_sequence(max_attempts: int = 10):
    """Yield exponential backoff delays capped at BACKOFF_MAX_SECONDS.

    Sequence: 1, 2, 4, 8, 16, 32, 60, 60, ...
    """
    delay = BACKOFF_INITIAL_SECONDS
    for _ in range(max_attempts):
        yield min(delay, BACKOFF_MAX_SECONDS)
        delay *= 2


class TelemetryPublisher:
    """Thin orchestration layer around an injected async `publish_fn`.

    Responsibilities:
      - Format topics (telemetry QoS 1, heartbeat QoS 0).
      - Track batches_sent (for heartbeat metrics).
      - On publish failure, enqueue up to MAX_QUEUE_DEPTH payloads in RAM.
      - drain_queue() drains the queue, preserving FIFO order on partial failure.
    """

    def __init__(self, *, user_id: str, publish_fn: PublishFn) -> None:
        self._user_id = user_id
        self._publish_fn = publish_fn
        # deque(maxlen=N) gives us oldest-drop semantics for free.
        self._queue: deque[dict] = deque(maxlen=MAX_QUEUE_DEPTH)
        self._batches_sent = 0

    # --------------------------- Introspection -------------------------------

    @property
    def queue_depth(self) -> int:
        return len(self._queue)

    @property
    def batches_sent(self) -> int:
        return self._batches_sent

    # ----------------------------- Publish -----------------------------------

    async def _safe_publish(self, *, topic: str, payload: dict, qos: int) -> bool:
        """Call publish_fn, convert any expected exception to a publish failure.

        Caught:
          - OSError / TimeoutError / ValueError — local socket / timeout /
            programming errors.
          - awscrt.exceptions.AwsCrtError — surfaces when iot_core's v0.2.3
            retry loop has exhausted its 5 attempts on a persistent broker
            failure (e.g. AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION before
            v0.2.5's persistent-session fix lands a clean reconnect).
            Catching here means publish_telemetry() can enqueue the batch
            into self._queue instead of dropping it on the floor — this is
            the data-loss fix that closes the 2026-05-26 production gap.
        """
        try:
            return bool(await self._publish_fn(topic=topic, payload=payload, qos=qos))
        except (OSError, TimeoutError, ValueError) as exc:
            log.warning("publish error on %s: %s: %s",
                        topic, type(exc).__name__, exc)
            return False
        except Exception as exc:  # noqa: BLE001 — narrowed by _is_awscrt_error below
            if _is_awscrt_error(exc):
                log.warning(
                    "publish awscrt error on %s (will enqueue): %s: %s",
                    topic, type(exc).__name__, exc,
                )
                return False
            # Anything else is unexpected — surface so coordinator/tests see it.
            raise

    async def publish_telemetry(self, payload: dict) -> bool:
        """Attempt to publish a telemetry batch. Enqueue on failure."""
        topic = TELEMETRY_TOPIC_TEMPLATE.format(user_id=self._user_id)
        ok = await self._safe_publish(topic=topic, payload=payload, qos=1)
        if ok:
            self._batches_sent += 1
            return True
        # maxlen enforces oldest-drop when full.
        self._queue.append(payload)
        return False

    async def publish_heartbeat(self, payload: dict) -> bool:
        """Heartbeat — QoS 0, fire-and-forget, never enqueued."""
        topic = HEARTBEAT_TOPIC_TEMPLATE.format(user_id=self._user_id)
        return await self._safe_publish(topic=topic, payload=payload, qos=0)

    async def publish_setup_snapshot(self, payload: dict) -> bool:
        """Setup snapshot (#4, ADR 0005) — QoS 1 on the dedicated setup topic.

        The ONE payload that flows pre-confirmation. Published once on first
        install and once per `take_setup_snapshot` command — NOT a recurring
        stream. Returns True on success, False on a (caught) publish failure.

        Deliberately NOT enqueued on failure: the telemetry retry queue
        (`self._queue`) drains onto the TELEMETRY topic, so a queued snapshot
        would leak the wrong payload onto the wrong topic. A failed snapshot is
        re-driven by the caller (first-install retry on next setup, or a fresh
        take_setup_snapshot command). It is also NOT counted in `batches_sent`
        — that counter tracks telemetry batches only.
        """
        topic = SETUP_TOPIC_TEMPLATE.format(user_id=self._user_id)
        return await self._safe_publish(topic=topic, payload=payload, qos=1)

    # ------------------------------ Drain ------------------------------------

    async def drain_queue(self) -> int:
        """Attempt to publish every queued payload. Returns count drained.

        Preserves FIFO order on partial failure: a payload that fails
        during drain is put back at the FRONT of the queue so it's
        retried before newer payloads.
        """
        if not self._queue:
            return 0

        drained = 0
        pending = list(self._queue)
        self._queue.clear()
        topic = TELEMETRY_TOPIC_TEMPLATE.format(user_id=self._user_id)

        for i, payload in enumerate(pending):
            ok = await self._safe_publish(topic=topic, payload=payload, qos=1)
            if ok:
                drained += 1
                self._batches_sent += 1
            else:
                # Re-queue this one + every remaining pending item in order.
                remaining = pending[i:]
                # Insert from the right end so FIFO order is preserved.
                for p in reversed(remaining):
                    self._queue.appendleft(p)
                break
        return drained
