"""Ingestion pipeline gap watcher — Sprint 5 Track A Day 4.

Per-registered-entity sample-rate watcher. Emits MQTT `iems_event` with:
  {
    "event_type": "ingestion_pipeline_gap",
    "entity_id": str,
    "expected_samples": int,
    "actual_samples": int,
    "gap_pct": float,        # percentage of samples missing (0-100)
    "window_start": str,     # ISO-8601 UTC with Z
    "window_end": str,       # ISO-8601 UTC with Z
  }
when the sample rate drops below GAP_THRESHOLD_PCT (80%) over a trailing
GAP_WINDOW_S (30-minute) window.

Design notes:
  - Window math: HACS publishes a telemetry batch every BATCH_WINDOW_SECONDS (30s).
    In a 30-minute window, expected_samples = 1800 / 30 = 60 per entity.
  - Actual sample count comes from the coordinator's per-entity observation count
    (incremented on each captured state_changed event).
  - Idempotency: once a gap is fired for entity E in window W, no re-fire until
    either: (a) the next window starts, or (b) the entity recovers to ≥ threshold.
  - Recovery: when an entity that was in gap-state recovers (actual/expected >= threshold),
    it becomes eligible for re-firing on the next gap.
  - MQTT topic: same topic template as telemetry but with qos=0 (diagnostic event).
    In practice, the ingestion Lambda on the cloud side consumes this and writes a
    DIAGNOSE DECISION# row (Sprint 5 Track A Day 4, Priya's side).

Wire into __init__.py after coordinator.start():
  watcher = IngestionGapWatcher(
      hass=hass,
      publish_fn=publisher.publish_telemetry,
      user_id=creds.identity_id,
      entity_ids=list(entity_index.keys()),
  )
  await watcher.start()
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable

log = logging.getLogger("iems.gap_watcher")

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

# Trailing window for sample-rate check (seconds)
GAP_WINDOW_S: float = 1800.0  # 30 minutes

# Expected cadence per entity (matches BATCH_WINDOW_SECONDS in const.py)
BATCH_CADENCE_S: float = 30.0  # 1 sample per 30s per entity

# Fire gap event when actual_samples < expected * GAP_THRESHOLD_PCT
GAP_THRESHOLD_PCT: float = 80.0  # fire if < 80% of expected

# Check interval — how often the watcher evaluates the window
CHECK_INTERVAL_S: float = 60.0  # check every minute

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

PublishFn = Callable[..., Awaitable[bool]]


def _utc_now_z() -> str:
    """Return current UTC time as ISO-8601 with trailing Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _expected_samples(window_s: float, cadence_s: float) -> int:
    """Compute expected sample count for a trailing window at given cadence."""
    return max(1, int(window_s / cadence_s))


# --------------------------------------------------------------------------
# IngestionGapWatcher
# --------------------------------------------------------------------------


class IngestionGapWatcher:
    """Monitors per-entity sample rate and emits gap events on MQTT.

    Usage:
      watcher = IngestionGapWatcher(...)
      watcher.record_sample(entity_id)   # called by coordinator on each state_changed
      await watcher.start()              # starts background check loop
      await watcher.stop()               # cancels loop on unload
    """

    def __init__(
        self,
        *,
        publish_fn: PublishFn,
        user_id: str,
        entity_ids: list[str],
        gap_window_s: float = GAP_WINDOW_S,
        batch_cadence_s: float = BATCH_CADENCE_S,
        gap_threshold_pct: float = GAP_THRESHOLD_PCT,
        check_interval_s: float = CHECK_INTERVAL_S,
        hass: Any = None,
    ) -> None:
        # `hass` is optional so existing tests continue to work, but production
        # __init__.py wiring SHOULD pass hass=hass so start() can schedule the
        # background check loop via hass.async_create_task. Without it we fall
        # back to asyncio.create_task — which the loop only weak-refs (Python
        # asyncio docs §asyncio.create_task) and which is the same bug class
        # that surfaced in edge_poc_outage v0.1.13.
        self._hass = hass
        self._publish_fn = publish_fn
        self._user_id = user_id
        self._entity_ids: frozenset[str] = frozenset(entity_ids)
        self._gap_window_s = gap_window_s
        self._batch_cadence_s = batch_cadence_s
        self._gap_threshold_pct = gap_threshold_pct
        self._check_interval_s = check_interval_s

        # Per-entity ring buffer of sample timestamps (epoch floats).
        # Only timestamps within the trailing window are kept.
        self._sample_times: dict[str, list[float]] = defaultdict(list)

        # Per-entity: last window-start for which a gap event was fired.
        # Key: entity_id → window_start epoch float
        # Used to prevent re-firing within the same window.
        self._gap_fired_at: dict[str, float] = {}

        self._check_task: asyncio.Task | None = None
        self._running = False

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def record_sample(self, entity_id: str) -> None:
        """Record a sample event for entity_id at the current time.

        Called by the coordinator on each state_changed event captured for
        a registered entity. Thread-safe (asyncio single-thread model).
        """
        if entity_id not in self._entity_ids:
            return
        now = time.monotonic()
        self._sample_times[entity_id].append(now)
        # Prune old entries while we have the list open (amortised O(1))
        self._prune_old_samples(entity_id, now)

    def get_sample_count(self, entity_id: str) -> int:
        """Return the number of samples recorded for entity_id in the current window."""
        now = time.monotonic()
        self._prune_old_samples(entity_id, now)
        return len(self._sample_times[entity_id])

    async def start(self) -> None:
        """Start the background check loop.

        v0.1.14: Schedule via hass.async_create_task when hass is available,
        falling back to asyncio.create_task for tests. Plain loop.create_task
        only stores a weak reference — the long-running check loop can be
        silently garbage-collected (Python asyncio docs §asyncio.create_task,
        HA developer docs §Working with Async). Same bug class fixed in
        edge_poc_outage._schedule_amber.
        """
        self._running = True
        create_task = getattr(self._hass, "async_create_task", None) if self._hass else None
        if callable(create_task):
            self._check_task = create_task(self._check_loop())
        else:
            self._check_task = asyncio.create_task(self._check_loop())
        log.info(
            "iems: gap_watcher started: %d entities, window=%.0fs, threshold=%.0f%%",
            len(self._entity_ids), self._gap_window_s, self._gap_threshold_pct,
        )

    async def stop(self) -> None:
        """Stop the background check loop."""
        self._running = False
        if self._check_task and not self._check_task.done():
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
            self._check_task = None
        log.debug("iems: gap_watcher stopped")

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _prune_old_samples(self, entity_id: str, now: float) -> None:
        """Remove sample timestamps older than gap_window_s from the ring buffer."""
        cutoff = now - self._gap_window_s
        samples = self._sample_times[entity_id]
        # Find the first index not older than cutoff (binary search for efficiency
        # on long lists, but list.pop(0) is O(n) — use deque in production if
        # entity count × window_s / cadence is large, e.g. 500 × 3600 = 180k).
        while samples and samples[0] < cutoff:
            samples.pop(0)

    def _window_start_epoch(self, now: float) -> float:
        """Return the epoch of the window's start (now - gap_window_s)."""
        return now - self._gap_window_s

    def _build_gap_event(
        self,
        entity_id: str,
        expected: int,
        actual: int,
        window_start_wall: str,
        window_end_wall: str,
    ) -> dict[str, Any]:
        """Build the MQTT event payload for a gap detection."""
        gap_pct = round((1.0 - actual / expected) * 100, 1) if expected > 0 else 100.0
        return {
            "event_type": "ingestion_pipeline_gap",
            "entity_id": entity_id,
            "expected_samples": expected,
            "actual_samples": actual,
            "gap_pct": gap_pct,
            "window_start": window_start_wall,
            "window_end": window_end_wall,
        }

    async def _evaluate_entity(self, entity_id: str, now: float) -> bool:
        """Evaluate one entity. Returns True if a gap event was fired.

        Idempotency: if a gap was already fired for the current window
        (window_start within same gap_window_s bucket), does not re-fire.
        Re-fires when entity recovers (actual ≥ threshold) and then drops again.
        """
        self._prune_old_samples(entity_id, now)
        actual = len(self._sample_times[entity_id])
        expected = _expected_samples(self._gap_window_s, self._batch_cadence_s)
        threshold_samples = expected * (self._gap_threshold_pct / 100.0)

        if actual >= threshold_samples:
            # Entity is healthy — clear gap-fired flag (allows re-fire after recovery)
            self._gap_fired_at.pop(entity_id, None)
            return False

        # Below threshold — check idempotency
        window_start_epoch = self._window_start_epoch(now)
        last_fired = self._gap_fired_at.get(entity_id)
        if last_fired is not None and abs(window_start_epoch - last_fired) < self._check_interval_s:
            # Already fired in this window — skip
            return False

        # Fire the gap event
        window_start_str = datetime.now(timezone.utc).replace(
            microsecond=0
        ).isoformat().replace("+00:00", "Z")
        # Approximate window start by subtracting gap_window_s from now
        from datetime import timedelta
        wall_start = (datetime.now(timezone.utc) - timedelta(seconds=self._gap_window_s))
        window_start_wall = wall_start.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        window_end_wall = window_start_str

        event = self._build_gap_event(entity_id, expected, actual, window_start_wall, window_end_wall)

        log.warning(
            "iems: gap_watcher: gap detected entity=%s expected=%d actual=%d gap_pct=%.1f%%",
            entity_id, expected, actual, event["gap_pct"],
        )

        try:
            await self._publish_fn(event)
            self._gap_fired_at[entity_id] = window_start_epoch
            return True
        except (OSError, TimeoutError, ValueError) as exc:
            log.error("iems: gap_watcher: publish failed: %s: %s", type(exc).__name__, exc)
            return False

    async def _check_loop(self) -> None:
        """Background loop: evaluate all entities every check_interval_s."""
        while self._running:
            try:
                await asyncio.sleep(self._check_interval_s)
            except asyncio.CancelledError:
                return

            now = time.monotonic()
            fired_count = 0
            for entity_id in self._entity_ids:
                try:
                    fired = await self._evaluate_entity(entity_id, now)
                    if fired:
                        fired_count += 1
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log.error(
                        "iems: gap_watcher: error evaluating %s: %s: %s",
                        entity_id, type(exc).__name__, exc,
                    )

            if fired_count > 0:
                log.info("iems: gap_watcher: fired %d gap events", fired_count)
