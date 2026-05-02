"""Coordinator — bridges HA state events, classifier, and publisher.

Responsibilities:
  - Hold the entity_index (registry snapshot built at async_setup_entry).
  - Receive state_changed events (via HA's async_track_state_change_event),
    classify, enrich with brand/area/unit, append to `pending`.
  - Flush `pending` every BATCH_WINDOW_SECONDS by building a telemetry
    payload and handing it to the publisher.
  - Emit a heartbeat every HEARTBEAT_INTERVAL_SECONDS.
  - For MTronic switch/plug entities: derive dispatch state (shed/import) and
    immediately forward via dispatch_publisher (if wired).

Pure of HA APIs for the capture/flush/heartbeat paths so unit tests
only need a MagicMock hass. Real HA wiring lives in __init__.py.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .classifier import classify
from .const import BATCH_WINDOW_SECONDS, HEARTBEAT_INTERVAL_SECONDS
from .dedup import canonicalize_entity_id
from .mtronic_dispatch import DispatchCapture
from .telemetry import EmptyBatchError, build_batch, build_heartbeat

# Categories whose state must be a numeric float for TS# writes to succeed.
# HA always returns new_state.state as a str; we coerce here at the HACS boundary
# so the ingestion Lambda sees the correct type and writes TS# minute-bucket rows.
_NUMERIC_CATEGORIES: frozenset[str] = frozenset({
    "inverter.pv",
    "inverter.battery",
    "inverter.grid",
    "inverter.load",
    "battery.soc",
    "sensor.power",
    "sensor.energy",
    "meter.energy",
})

# The synthetic site-level PV aggregate entity published per-batch so that
# TS#sensor.site_pv_power gets live minute-bucket rows (not just backfill).
_SITE_PV_ENTITY_ID = "sensor.site_pv_power"

log = logging.getLogger("iems.coordinator")


def _coerce_state(raw: str, category: str) -> float | str:
    """Coerce HA state string to float for numeric categories.

    HA's state machine stores all state values as strings.  For numeric
    energy/power categories the ingestion Lambda's _upsert_ts_bucket guard
    requires isinstance(state, (int, float)).  We coerce here at the HACS
    boundary so the contract is satisfied before the payload leaves the device.

    Returns the original string unchanged for non-numeric categories or when
    parsing fails (e.g. 'unavailable', 'unknown').
    """
    if category not in _NUMERIC_CATEGORIES:
        return raw
    try:
        return float(raw)
    except (ValueError, TypeError):
        return raw


def _reduce_pv_entities_for_sum(pv_entities: list[dict]) -> list[dict]:
    """Deduplicate pv_entities before summing for the site-PV aggregate (FIX B).

    RCA 2026-04-24 (docs/sprints/sprint_03/hacs_0.1.9_classifier_delta.md):
    the unreduced sum of inverter.pv entries in a 30s batch can reach 85 kW on
    a 15.4 kWp system because:

    1. Each state_changed event is appended to pending independently — a single
       entity firing 4 times in a window contributes 4x its value to the sum.
    2. Per-MPPT-string entities (*_pv1_power, *_pv2_power) are summed alongside
       their parent aggregate (*_pv_power) — triple-counting a multi-MPPT
       inverter's generation.

    This function applies two reductions:

    PASS 1 — collapse repeated firings of the same entity_id to a single
             survivor (the entry with the highest ts in the window).

    PASS 2 — for each canonical entity_id (computed via
             dedup.canonicalize_entity_id which maps `*_pv1_*` → `*_pv_*`),
             if the aggregate `*_pv_power` entry is present, drop the numbered
             per-string variants.  If only per-string variants exist (no
             aggregate), keep them all so the fallback sum is correct.
    """
    if not pv_entities:
        return []

    # PASS 1: keep only the latest entry per entity_id (highest ts wins;
    # stable fallback for missing ts).
    latest_per_eid: dict[str, dict] = {}
    for e in pv_entities:
        eid = e.get("entity_id")
        if not eid:
            continue
        prev = latest_per_eid.get(eid)
        if prev is None:
            latest_per_eid[eid] = e
            continue
        prev_ts = prev.get("ts") or ""
        cur_ts = e.get("ts") or ""
        if cur_ts > prev_ts:
            latest_per_eid[eid] = e

    # PASS 2: for each canonical group, if the aggregate entity_id
    # (== canonical) is present, drop the per-string variants.
    by_canonical: dict[str, list[dict]] = {}
    for eid, e in latest_per_eid.items():
        canonical = canonicalize_entity_id(eid)
        by_canonical.setdefault(canonical, []).append(e)

    reduced: list[dict] = []
    for canonical, group in by_canonical.items():
        # Aggregate present? Prefer it and drop the per-string siblings.
        aggregate = next(
            (e for e in group if e.get("entity_id") == canonical),
            None,
        )
        if aggregate is not None:
            reduced.append(aggregate)
        else:
            # No aggregate — fall back to summing the per-string entries.
            reduced.extend(group)
    return reduced


def _build_site_pv_entity(pv_entities: list[dict]) -> dict | None:
    """Synthesize a site-level PV aggregate entity from per-inverter PV entities.

    Sums numeric inverter.pv state values in the current batch and returns
    a synthetic entity for sensor.site_pv_power.  This gives the ingestion
    Lambda a live numeric value to write into TS#sensor.site_pv_power so the
    Power History chart has live data (not just backfill/system_totals rows).

    Per FIX B (RCA 2026-04-24) the input list is first reduced by
    `_reduce_pv_entities_for_sum` to kill the repeated-firing and aggregate+
    sub-channel double-counts.

    Returns None if no numeric inverter.pv values exist in the batch.
    """
    reduced = _reduce_pv_entities_for_sum(pv_entities)

    total: float = 0.0
    latest_ts: str | None = None
    unit: str | None = None
    has_numeric = False

    for e in reduced:
        state = e.get("state")
        if isinstance(state, (int, float)) and not isinstance(state, bool):
            total += float(state)
            has_numeric = True
        # Keep the most recent timestamp as the aggregate timestamp
        ts = e.get("ts")
        if ts is not None and (latest_ts is None or ts > latest_ts):
            latest_ts = ts
        if unit is None and e.get("unit"):
            unit = e["unit"]

    if not has_numeric or latest_ts is None:
        return None

    entity: dict[str, Any] = {
        "entity_id": _SITE_PV_ENTITY_ID,
        "category": "inverter.pv",
        "ts": latest_ts,
        "state": total,
    }
    if unit:
        entity["unit"] = unit
    return entity


class IemsCoordinator:
    def __init__(
        self,
        *,
        hass,
        user_id: str,
        entity_index: dict[str, dict[str, Any]],
        publisher,
        dispatch_publisher=None,
        direct_entity_ids: frozenset[str] | None = None,
    ) -> None:
        self._hass = hass
        self._user_id = user_id
        self._entity_index = entity_index
        self._publisher = publisher
        self._dispatch_publisher = dispatch_publisher
        self._dispatch_capture = DispatchCapture(direct_entity_ids=direct_entity_ids)
        self.pending: list[dict] = []
        self._unsub_state = None
        self._batch_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._started_at = time.monotonic()

    # ---------------------- State capture ---------------------------------

    def capture_state_change(self, new_state) -> None:
        """Sync handler — called from HA's event bus callback. No I/O.

        For MTronic switch entities, also schedules a dispatch event publish
        (fire-and-forget coroutine on the asyncio loop).
        """
        if new_state is None:
            return
        entity_id = new_state.entity_id
        meta = self._entity_index.get(entity_id)
        if not meta:
            return  # not in our registry snapshot → drop

        ts = self._extract_ts(new_state)
        attrs = dict(getattr(new_state, "attributes", {}) or {})

        # MTronic dispatch capture — runs before classifier so suppressed
        # telemetry entities (domain blacklist) can still emit dispatch events.
        if self._dispatch_publisher is not None:
            dispatch_event = self._dispatch_capture.process_state_change(
                entity_id=entity_id,
                platform=meta.get("platform"),
                domain=meta.get("domain") or entity_id.split(".", 1)[0],
                new_state=new_state.state,
                attrs=attrs,
                ts=ts,
                area=meta.get("area"),
            )
            if dispatch_event is not None and dispatch_event.suppressed_by is None:
                # Schedule async publish without blocking the event loop callback.
                # In production HA, hass.async_create_task is the canonical way
                # to schedule a coroutine from a sync callback on the HA loop.
                # We fall back to asyncio.get_running_loop().create_task() for
                # test environments where hass is a MagicMock.
                coro = self._dispatch_publisher.publish_dispatch(dispatch_event)
                create_task = getattr(self._hass, "async_create_task", None)
                if callable(create_task):
                    create_task(coro)
                else:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(coro)
                    except RuntimeError:
                        # No event loop running (unit test without asyncio context).
                        # Store coroutine so tests can await it directly.
                        coro.close()  # avoid "coroutine never awaited" warning

        classified = classify({
            "entity_id": entity_id,
            "domain": meta.get("domain") or entity_id.split(".", 1)[0],
            "platform": meta.get("platform"),
            "device_class": meta.get("device_class"),
            "unit": meta.get("unit"),
            "name": meta.get("name"),
        })
        if not classified.get("surface"):
            return

        captured: dict[str, Any] = {
            "entity_id": entity_id,
            "category": classified["category"],
            "ts": ts,
            "state": _coerce_state(new_state.state, classified["category"]),
        }
        if meta.get("brand"):
            captured["brand"] = meta["brand"]
        if meta.get("area"):
            captured["area"] = meta["area"]
        if meta.get("unit"):
            captured["unit"] = meta["unit"]
        if attrs:
            captured["attributes"] = attrs

        self.pending.append(captured)

    @staticmethod
    def _extract_ts(new_state) -> str:
        """Normalize HA state.last_changed to ISO-8601 UTC with Z suffix."""
        try:
            iso = new_state.last_changed.isoformat()
        except (AttributeError, TypeError):
            # Fallback — won't validate strictly but keeps pipeline flowing
            from datetime import datetime, timezone
            iso = datetime.now(timezone.utc).isoformat()
        if iso.endswith("+00:00"):
            iso = iso[:-6] + "Z"
        return iso

    # ---------------------- Flush + publish -------------------------------

    async def flush(self) -> None:
        """Drain `pending` into a batch and hand to publisher.

        The publisher owns retry (via its bounded queue). We always clear
        `pending` after handing off, so we never double-ship a batch.

        Site-PV aggregate: if any inverter.pv entities are present we inject a
        synthetic sensor.site_pv_power entity so ingestion writes a live TS#
        minute-bucket row for the Power History chart.  Per-inverter PV rows
        continue to land in LATEST# (unchanged), but only the site-aggregate
        has a TS# row — matching the backfill pattern and Sarah's guidance.
        """
        if not self.pending:
            return
        batch = self.pending[:]
        self.pending.clear()

        # Synthesize site-level PV aggregate from per-inverter PV entities.
        pv_entities = [e for e in batch if e.get("category") == "inverter.pv"]
        if pv_entities:
            site_pv = _build_site_pv_entity(pv_entities)
            if site_pv is not None:
                # Only inject if no entity already covers site_pv_power (guard
                # against double-injection if an inverter integration happens to
                # publish a site-level aggregate entity directly).
                existing_ids = {e["entity_id"] for e in batch}
                if _SITE_PV_ENTITY_ID not in existing_ids:
                    batch.append(site_pv)
                    log.debug(
                        "flush: injected %s state=%.1f W (%d inverter.pv sources)",
                        _SITE_PV_ENTITY_ID,
                        site_pv["state"],
                        len(pv_entities),
                    )

        try:
            ha_version = getattr(self._hass.config, "version", "unknown")
            # v0.5.0: emit country/timezone when HA has them configured.
            country = getattr(self._hass.config, "country", None) or None
            timezone = getattr(self._hass.config, "time_zone", None)
            timezone = str(timezone) if timezone else None
            payload = build_batch(
                user_id=self._user_id,
                entities=batch,
                ha_version=ha_version,
                country=country,
                timezone=timezone,
            )
        except EmptyBatchError:
            return
        except ValueError as exc:
            log.warning("flush: build_batch rejected payload: %s", exc)
            return
        await self._publisher.publish_telemetry(payload)

    async def heartbeat_once(self) -> None:
        ha_version = getattr(self._hass.config, "version", "unknown")
        hb = build_heartbeat(
            user_id=self._user_id,
            ha_version=ha_version,
            uptime_s=int(time.monotonic() - self._started_at),
            batches_sent=getattr(self._publisher, "batches_sent", 0),
            queue_depth=getattr(self._publisher, "queue_depth", 0),
        )
        await self._publisher.publish_heartbeat(hb)

    # ---------------------- Background timers -----------------------------

    async def _batch_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(BATCH_WINDOW_SECONDS)
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - safety net
                log.error("batch flush crashed: %s: %s", type(exc).__name__, exc)

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                await self.heartbeat_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover
                log.error("heartbeat crashed: %s: %s", type(exc).__name__, exc)

    async def start(self) -> None:
        # v0.1.14: Schedule long-running loops via hass.async_create_task when
        # available. Plain asyncio.create_task only stores a WEAK reference in
        # the event loop — the task can be silently garbage-collected mid-flight
        # (Python asyncio docs §asyncio.create_task). Fall back to
        # asyncio.create_task in test envs where hass is a MagicMock that doesn't
        # implement async_create_task with the right signature.
        # See edge_poc_outage._schedule_amber for the full rationale + the
        # 2026-05-02 production incident that surfaced this bug class.
        create_task = getattr(self._hass, "async_create_task", None)
        if callable(create_task):
            self._batch_task = create_task(self._batch_loop())
            self._heartbeat_task = create_task(self._heartbeat_loop())
        else:
            self._batch_task = asyncio.create_task(self._batch_loop())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        for t in (self._batch_task, self._heartbeat_task):
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        if self._unsub_state:
            self._unsub_state()
            self._unsub_state = None
