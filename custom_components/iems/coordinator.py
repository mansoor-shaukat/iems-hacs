"""Coordinator — bridges HA state events, classifier, per-minute aggregator, and publisher.

Sprint 6 (2026-05-24): per-minute aggregation in HACS.

Why this changed
----------------
Until v0.1.15, every HA state_changed event was forwarded as its own telemetry
row.  The cloud ingestion Lambda then folded each event into a per-minute TS#
bucket via up to 3 DDB `update_item` calls per event.  At peak that drove ~50K
DDB writes per 5-min flush window.

CEO sign-off this session: HACS aggregates per minute and per entity locally,
then ships pre-built minute-bucket rows.  The cloud just stores them.

Per-minute aggregation contract
-------------------------------
For every (entity_id, minute_floor) pair we keep an Accumulator:
    {sum, count, min, max, category, ts, brand, area, unit, attributes}

On each state_changed event:
  1. Classify and enrich (brand, area, unit, attributes).
  2. Coerce numeric-category states to float at the HACS boundary.
  3. Compute the minute-floor of the event ts ("2026-05-24T14:22:00Z").
  4. If the entity already has a finalised minute later than this event's
     minute, drop the event (late arrival — HA state ordering is monotonic
     in practice, this is defensive).
  5. Update the accumulator: sum += state, count += 1, min/max updated.
  6. Non-numeric events (category not in _NUMERIC_CATEGORIES) bypass the
     numeric path and keep a latest-wins passthrough — switches, climate,
     light state-changes are semantic events, not measurements.

On the 5-min flush boundary (BATCH_WINDOW_SECONDS=300):
  1. Finalise every accumulator whose minute < current minute.  Each
     finalised accumulator yields one telemetry row:
         state   = sum/count  (mean)  — numeric categories
         min, max, samples = count   — numeric categories
         state   = latest passthrough — non-numeric categories
         samples = count               — non-numeric categories
         ts      = minute_floor + "Z"
  2. Build the batch, cap at 5 rows per (entity_id) (most-recent wins).
  3. Hand to the publisher, which owns retry via its bounded queue.
  4. Reset accumulators for finalised minutes; current-minute accumulator
     keeps accumulating into the next window.

Heartbeat tick: every HEARTBEAT_INTERVAL_SECONDS (300s, matches flush cadence
post-Sprint 6).  Calls publisher.drain_queue() so backlogged batches actually
leave the device.

Pure of HA APIs in the capture/flush/heartbeat paths so unit tests only need a
MagicMock hass.  Real HA wiring lives in __init__.py.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from .classifier import classify
from .const import (
    BATCH_WINDOW_SECONDS,
    CATEGORY_INVERTER_FAULT,
    DEFAULT_SHIPPING_MODE,
    ENERGY_DELTA_THRESHOLD_KWH,
    HEARTBEAT_INTERVAL_SECONDS,
    HUMIDITY_DELTA_THRESHOLD_PCT,
    INITIAL_FLUSH_DELAY_SECONDS,
    INITIAL_HEARTBEAT_DELAY_SECONDS,
    MAX_ENTITIES_PER_BATCH_PUBLISH,
    SOC_DELTA_THRESHOLD_PCT,
    TELEMETRY_SUPPRESSED_MODES,
    TEMPERATURE_DELTA_THRESHOLD_C,
    VALID_SHIPPING_MODES,
)
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
    # 2026-05-31 — sensor.temperature + sensor.humidity were missing here.
    # Climate entities pass through HA as strings ("94.1"); without coercion
    # they hit ingestion as strings and the numeric-only TS# guard skips
    # them silently. Phase-3 staging smoke caught this on its first run
    # (4 climate entities = 0 TS# rows in DDB while LATEST# was string-typed).
    # Both are legitimate numeric measurements — charting needs the TS# rows.
    "sensor.temperature",
    "sensor.humidity",
})

# Cap the number of finalised minute-rows we ship per entity per flush.
# At a 5-min flush window we expect at most 5 finalised minutes per entity,
# but defensive in case a publisher backlog forces a delayed flush.
_MAX_ROWS_PER_ENTITY: int = 5

# ----------------------------------------------------------------------------
# v0.3.0 send-policy — CEO-locked 2026-05-31.
# Per docs/architecture/send_policy.md, every classified category maps to one
# of four buckets that decide whether a per-minute accumulator row gets
# emitted to the wire.  Threshold values live in const.py (named, testable,
# tunable).
#
#   "always"       — emit every minute that had ≥1 state_changed (the v0.2.x
#                    default behaviour).  Fast-moving signals where the
#                    minute-by-minute curve IS the product.
#   "threshold"    — emit only when |finalised_mean - last_emitted_state| ≥
#                    `threshold`.  Slow-moving numerics whose chart can
#                    hold-last between meaningful steps.
#   "latest_only"  — no per-minute accumulator at all.  Emit ONE row only
#                    when the value actually changes; cloud-side this lands
#                    LATEST# and TS# naturally skips because the state is
#                    non-numeric (switch/light/climate.mode/text).
#
# State == "unavailable" / "unknown" is the universal short-circuit (see the
# capture path): emit ONE LATEST# transition, then silent until alive.
#
# Categories present in `classifier.VALID_CATEGORIES` but missing here default
# to "always" — safer than silently dropping.  Drift guard in
# `tests/hacs/test_coordinator.py::test_send_policy_covers_every_valid_category`
# enforces full coverage.
# ----------------------------------------------------------------------------
_SEND_POLICY: dict[str, dict[str, Any]] = {
    # ---- Always — every minute, the curve IS the product -------------------
    "inverter.pv":      {"bucket": "always",      "threshold": None},
    "inverter.grid":    {"bucket": "always",      "threshold": None},
    "inverter.load":    {"bucket": "always",      "threshold": None},
    "inverter.battery": {"bucket": "always",      "threshold": None},
    "sensor.power":     {"bucket": "always",      "threshold": None},
    # ---- Threshold — hold-last between meaningful steps --------------------
    "battery.soc":         {"bucket": "threshold", "threshold": SOC_DELTA_THRESHOLD_PCT},
    "sensor.temperature":  {"bucket": "threshold", "threshold": TEMPERATURE_DELTA_THRESHOLD_C},
    "sensor.humidity":     {"bucket": "threshold", "threshold": HUMIDITY_DELTA_THRESHOLD_PCT},
    "sensor.energy":       {"bucket": "threshold", "threshold": ENERGY_DELTA_THRESHOLD_KWH},
    # ---- Always (v0.3.1 fix) — instantaneous electrical measurements -------
    # `meter.energy` is a classifier CATCH-ALL for non-power/non-energy
    # electrical device classes: voltage, current, frequency, power_factor,
    # apparent_power, reactive_power (see classifier.METER_DEVICE_CLASSES).
    # The category name is historical and misleading — cumulative kWh meters
    # route to `sensor.energy` (device_class=energy at classifier step 4),
    # NOT here.  The v0.3.0 spec listed `meter.energy` in the threshold
    # bucket on the assumption that it carried cumulative-energy semantics;
    # in production it actually carries grid voltage, frequency, and current
    # — fast-moving signals whose minute-by-minute curve IS the product.
    #
    # 2026-06-01 v0.3.1 incident: staging `sensor.inverter_a_staging_grid_l1_voltage`
    # (device_class=voltage) classifies as `meter.energy`, was threshold-gated
    # at 1.0 kWh, mains voltage drift of ~0.5 V per minute never crossed the
    # gate → ONLY the first-boot row ever shipped for grid_l1_voltage post-deploy.
    # CEO's canonical grid-outage detector reads grid_l1_voltage; losing TS#
    # tracking on it breaks downstream outage detection.  Move to always.
    "meter.energy":        {"bucket": "always",    "threshold": None},
    # ---- Latest-only — transition-driven, no TS# ever ----------------------
    "switch.controllable": {"bucket": "latest_only", "threshold": None},
    "light":               {"bucket": "latest_only", "threshold": None},
    "climate":             {"bucket": "latest_only", "threshold": None},
    "other":               {"bucket": "latest_only", "threshold": None},
    # v0.3.2 (2026-07-02, send_policy.md amendment) — inverter fault/alarm
    # enum sensors (Autopilot DIAGNOSE opener, telemetry.schema.json v0.8.0).
    # Enum text states ('OK' / 'Problem' / named fault) change rarely — a
    # fault IS a state transition, so latest_only is the exact fit and the
    # state_changed-silence concern does not apply.  The raw register words
    # MUST ride attributes.value on the transition row (capture attrs pass
    # through _emit_transition_row unfiltered); the cloud decodes them to
    # F-codes (LD-F1: HACS stays dumb).  NOT in _NUMERIC_CATEGORIES — the
    # state stays a string, no accumulator, no TS# rows.
    CATEGORY_INVERTER_FAULT: {"bucket": "latest_only", "threshold": None},
}

# Default for any category not explicitly listed (defensive — see comment above).
_DEFAULT_POLICY: dict[str, Any] = {"bucket": "always", "threshold": None}

# State strings HA uses for "not reading" — both short-circuit the send policy.
_UNAVAILABLE_STATES: frozenset[str] = frozenset({"unavailable", "unknown"})


def _policy_for(category: str) -> dict[str, Any]:
    """Return the send-policy entry for a classifier category.

    Unknown categories default to "always" so the failure mode is over-shipping
    rather than silently dropping a signal class.
    """
    return _SEND_POLICY.get(category, _DEFAULT_POLICY)

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


def _minute_floor(ts: str) -> str:
    """Floor an ISO-8601 UTC timestamp to its minute.

    Input  : "2026-05-24T14:22:37Z"  or  "2026-05-24T14:22:37.182Z"
    Output : "2026-05-24T14:22:00Z"

    Pure string slice (no datetime parsing) to avoid timezone surprises. The
    HACS boundary normalises ts to '%Y-%m-%dT%H:%M:%SZ' (or with a
    fractional-seconds variant) — both shapes have the minute at positions
    14:16 and the seconds component immediately after.

    Falls back to the input unchanged if the string is too short to be ISO.
    """
    # Minimum length: "2026-05-24T14:22:00Z" == 20 chars; we just need positions
    # up to index 16 inclusive ("...T14:22") to be present.
    if len(ts) < 17 or ts[10] != "T":
        return ts
    # Slice to "YYYY-MM-DDTHH:MM" then append ":00Z".
    return ts[:16] + ":00Z"


@dataclass
class _MinuteAccumulator:
    """Per-(entity_id, minute) aggregation state.

    Numeric categories: sum / count / min / max.  finalise() emits
        state = sum/count, min, max, samples = count.

    Non-numeric categories: latest-wins state_passthrough.  finalise() emits
        state = state_passthrough, samples = count, no min/max.
    """
    entity_id: str
    category: str
    minute_iso: str
    sum: float = 0.0
    count: int = 0
    min: float | None = None
    max: float | None = None
    # Latest-seen passthrough state for non-numeric categories.  None when the
    # category is numeric (we use sum/count to compute the mean instead).
    state_passthrough: Any = None
    is_numeric: bool = False
    # Enrichment fields — copied from the FIRST event for the minute; HA
    # registry doesn't change inside a minute in practice.
    brand: str | None = None
    area: str | None = None
    unit: str | None = None
    # Attributes from the latest event in the minute (HA-state-shape).
    attributes: dict[str, Any] | None = None
    # v0.3.0 send-policy: seeded at accumulator creation from the coordinator's
    # per-entity `_last_emitted_state` map.  Used by threshold-bucket gating
    # in finalise() to compare `|mean - last_emitted_state|` against the
    # category threshold.  `None` means "no row has ever been emitted for
    # this entity" — first-boot rule: emit unconditionally.
    last_emitted_state: float | None = None

    def update_numeric(self, value: float) -> None:
        """Fold a numeric measurement into the running aggregate."""
        self.is_numeric = True
        self.sum += value
        self.count += 1
        if self.min is None or value < self.min:
            self.min = value
        if self.max is None or value > self.max:
            self.max = value

    def update_passthrough(self, value: Any) -> None:
        """Latest-wins update for non-numeric categories."""
        self.state_passthrough = value
        self.count += 1

    def finalise(self) -> dict[str, Any]:
        """Build the telemetry row for this minute."""
        row: dict[str, Any] = {
            "entity_id": self.entity_id,
            "category": self.category,
            "ts": self.minute_iso,
        }
        if self.is_numeric and self.count > 0 and self.min is not None and self.max is not None:
            row["state"] = self.sum / self.count
            row["min"] = self.min
            row["max"] = self.max
        else:
            row["state"] = self.state_passthrough
        row["samples"] = self.count
        if self.brand:
            row["brand"] = self.brand
        if self.area:
            row["area"] = self.area
        if self.unit:
            row["unit"] = self.unit
        if self.attributes:
            row["attributes"] = self.attributes
        return row


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

        # Per-(entity_id, minute_iso) accumulator.
        self._accumulators: dict[tuple[str, str], _MinuteAccumulator] = {}
        # Per-entity: the most-recent minute we have already FINALISED (shipped
        # or queued for shipping).  Used to drop late arrivals for already-sealed
        # minutes so they don't clobber a fresher row.
        self._last_finalised_minute: dict[str, str] = {}

        # v0.3.0 send-policy state — cross-minute persistence per entity.
        #
        # `_last_emitted_state` is the value of the most recent row this
        # coordinator shipped to the cloud for the entity.  Persists across
        # minutes; survives the per-minute accumulator lifecycle.  Used by:
        #   - Threshold-bucket finalise() to compare against the candidate
        #     mean (drop row if |mean - last| < threshold).
        #   - Latest-only capture path to suppress same-value state_changed
        #     events (HA fires events with no value delta — switch reasserted
        #     OFF, light flickering brightness=0).
        # `None` for an entity means "no row has ever been shipped" (first-boot
        # rule: emit unconditionally on first non-unavailable event).
        self._last_emitted_state: dict[str, Any] = {}

        # Sprint 7 (v0.3.2 send-policy) — per-entity last-emitted raw fault
        # words (`attributes.value`) for inverter.fault entities.  The enum
        # render collapses 56 of 64 fault bits to the same 'Problem' fallback
        # string, so two DIFFERENT faults back-to-back (F56 → F41 with no
        # intervening OK) look like a same-value re-fire to the plain
        # `_last_emitted_state` dedup.  The register words are the actual
        # diagnostic payload the cloud decodes — a word change IS a real
        # transition and must emit.  Only consulted/updated for
        # CATEGORY_INVERTER_FAULT entities.
        self._last_emitted_fault_value: dict[str, Any] = {}

        # Pending rows queued by the latest-only capture path (switches, lights,
        # climate.mode, text/other).  Drained alongside finalised accumulator
        # rows in `_drain_finalised_rows`.  One row per real state transition;
        # no minute aggregation, no TS# rows cloud-side (state is non-numeric).
        self._pending_latest_only_rows: list[dict[str, Any]] = []

        # Back-compat alias for any external reader that used to peek at
        # `coordinator.pending`.  Always an empty list now — kept so an
        # accidental ref doesn't AttributeError.  Tests should use the
        # accumulators / finalisation surface instead.
        self.pending: list[dict] = []

        # ---- Shipping-mode FSM (#9, ADR 0005) ---------------------------
        # The publish gate. `setup`/`paused` suppress the 30s telemetry batch
        # path; `active` ships it, filtered to `_whitelist` when one is set.
        # Cloud is authoritative — it commands transitions over the MQTT
        # command down-topic; HACS reconciles to cloud truth on reconnect via
        # the /hacs/status pull.  Capture + accumulation + edge-PoC keep
        # running in every mode; only the PUBLISH is gated, so a confirmed user
        # ships the freshest sealed minutes with no warm-up gap.
        self.shipping_mode: str = DEFAULT_SHIPPING_MODE
        # Entity whitelist for active-mode publishing.  `None` means "no
        # whitelist commanded" → ship everything (legacy pre-gate behaviour,
        # matches api.yaml's treatment of seeded-confirmed site_models).  A
        # non-None frozenset filters every batch so non-whitelisted entities
        # never leave HA (the ADR 0005 privacy posture).
        self._whitelist: frozenset[str] | None = None
        # Monotonic version of the whitelist the cloud last sent.  Used to
        # ignore out-of-order command/status updates (a lower version is stale).
        self._whitelist_version: int = -1

        self._unsub_state = None
        self._batch_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._started_at = time.monotonic()
        # Counter for build_batch ValueError swallows — surfaced in heartbeat
        # as `flush_rejects` from v0.2.2.  Existence here is the guardrail
        # that the silent-swallow regression (2026-05-26 P0) can't recur
        # without screaming.
        self._flush_rejects: int = 0

        # ---- v0.2.2 diagnostic counters (heartbeat surfacing only) ------
        # Telemetry is dead in prod despite 0.2.1 hotfix.  These counters
        # let the cloud side see WHICH stage of the pipeline is failing
        # without HA shell access.  Pure observability — no functional
        # effect on capture / flush / publish.
        #
        # `_batch_loop_iterations`: incremented every time _batch_loop()
        #   wakes from asyncio.sleep.  If this stays at 0 over uptime,
        #   the background loop isn't running at all (task GC'd, never
        #   scheduled, etc.).
        self._batch_loop_iterations: int = 0
        # `_last_flush_iso`: wall-clock ISO of the most recent flush() call.
        #   None until the first flush fires.  If None at uptime > 5min,
        #   _batch_loop scheduled flush() but it never executed.
        self._last_flush_iso: str | None = None
        # `_last_flush_row_count`: how many finalised rows the LAST flush
        #   handed to the publisher.  0 means "flush fired but accumulator
        #   was empty" — distinguishes capture failure from flush failure.
        self._last_flush_row_count: int = 0
        # `_last_publish_error`: exception type+message of the most recent
        #   publish_telemetry failure (truncated to 200 chars).  None means
        #   "no publish errors since start".
        self._last_publish_error: str | None = None

        # ---- v0.4.6 data-recovery ack (Sprint 7) ------------------------
        # The most-recent `recover_window` outcome, surfaced on the heartbeat
        # so the cloud learns whether HA's recorder actually had the gap rows.
        # None until the first recover attempt completes.  Shape per
        # docs/sprints/sprint_07/data_recovery_real_ha_check_spec.md:
        #   {window_id, start_ts, end_ts, result, rows_found, rows_published,
        #    completed_at}.  Additive + nullable — heartbeat schema unchanged.
        self._last_recovery: dict[str, Any] | None = None

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

        category: str = classified["category"]
        raw_state = new_state.state
        state = _coerce_state(raw_state, category)
        minute_iso = _minute_floor(ts)

        # Defensive late-arrival guard: if we have already finalised this
        # entity's minute (or a LATER one), drop the event.  HA state ordering
        # is monotonic in practice; this catches the rare clock-skew / replay
        # case.
        #
        # v0.4.5: comparison is `<=`, not `<`.  The cold-start force-seal path
        # (flush(seal_current_minute=True)) finalises the CURRENT partial
        # minute M and records `_last_finalised_minute[entity]=M`.  With a
        # strict `<` a fresh state_changed still inside minute M would equal
        # last_final, slip past the guard, re-open a minute-M accumulator, and
        # the next (300s) flush would ship a SECOND minute-M row for the same
        # entity.  `<=` makes a force-sealed minute un-reopenable, so a sealed
        # minute can never be double-shipped.  (This also tightens the ordinary
        # late-arrival case: an event whose minute exactly equals an already
        # finalised minute is genuinely stale and must be dropped.)
        last_final = self._last_finalised_minute.get(entity_id)
        if last_final is not None and minute_iso <= last_final:
            log.debug(
                "drop late state_changed for %s: event_min=%s <= last_finalised=%s",
                entity_id, minute_iso, last_final,
            )
            return

        policy = _policy_for(category)
        bucket = policy["bucket"]

        # ---- v0.3.0 unavailable/unknown short-circuit (universal) ----------
        # HA exposes "unavailable" (integration unreachable) and "unknown"
        # (value couldn't be parsed).  Per send_policy.md §"State = unavailable
        # semantics": emit ONE LATEST# transition row, then silent until alive.
        #
        # Implementation: emit exactly one row carrying state="unavailable"
        # (or "unknown"), clear any open accumulator for this entity, and
        # set _last_emitted_state to the unavailable string so a subsequent
        # alive event is detected as a real transition.  De-dupe on the
        # already-recorded unavailable state so a flapping integration
        # doesn't spam transition rows.
        if isinstance(raw_state, str) and raw_state in _UNAVAILABLE_STATES:
            prev = self._last_emitted_state.get(entity_id)
            if prev == raw_state:
                # Already emitted this unavailable transition — silent.
                return
            self._emit_transition_row(
                entity_id=entity_id,
                category=category,
                state=raw_state,
                ts=ts,
                attrs=attrs,
                meta=meta,
            )
            # Clear any open accumulator — we'll resume on the next alive
            # event.  Don't touch _last_finalised_minute (still meaningful
            # for late-arrival guarding when the entity comes back).
            self._drop_accumulators_for(entity_id)
            return

        # ---- v0.3.0 latest-only bucket — emit only on real value change ---
        # No per-minute accumulator at all.  Suppress same-value re-fires
        # (HA happily emits state_changed for unchanged values when an
        # attribute alone moves, or when a switch reasserts its current
        # state).  Cloud-side: LATEST# updates, TS# naturally skipped via
        # the ingestion Lambda's isinstance(state, (int, float)) guard.
        if bucket == "latest_only":
            prev = self._last_emitted_state.get(entity_id)
            if prev == state:
                if category != CATEGORY_INVERTER_FAULT:
                    return
                # inverter.fault: the diagnostic payload is the raw register
                # word list in attributes["value"], and the enum render
                # collapses most fault bits to the same 'Problem' fallback.
                # 'Problem' → 'Problem' with CHANGED words (F56 → F41, no
                # intervening OK) is a REAL fault transition — emit it.
                # Unchanged words → genuine same-value re-fire → suppress.
                if (
                    self._last_emitted_fault_value.get(entity_id)
                    == (attrs or {}).get("value")
                ):
                    return
            self._emit_transition_row(
                entity_id=entity_id,
                category=category,
                state=state,
                ts=ts,
                attrs=attrs,
                meta=meta,
            )
            if category == CATEGORY_INVERTER_FAULT:
                self._last_emitted_fault_value[entity_id] = (
                    (attrs or {}).get("value")
                )
            return

        key = (entity_id, minute_iso)
        acc = self._accumulators.get(key)
        if acc is None:
            acc = _MinuteAccumulator(
                entity_id=entity_id,
                category=category,
                minute_iso=minute_iso,
                brand=meta.get("brand"),
                area=meta.get("area"),
                unit=meta.get("unit"),
                # v0.3.0: seed the threshold-gate baseline from the
                # coordinator's cross-minute map.  `None` means "first
                # row ever for this entity" — first-boot rule emits.
                last_emitted_state=self._last_emitted_state.get(entity_id),
            )
            self._accumulators[key] = acc

        # Numeric vs passthrough fork.  bool is a subclass of int — exclude it
        # explicitly so switch.on/off doesn't get summed.
        if (
            category in _NUMERIC_CATEGORIES
            and isinstance(state, (int, float))
            and not isinstance(state, bool)
        ):
            acc.update_numeric(float(state))
        else:
            acc.update_passthrough(state)

        # Always refresh the latest attributes (HA semantics: the most recent
        # attribute snapshot is the one the cloud should see).
        if attrs:
            acc.attributes = attrs

    # ----- v0.3.0 helpers: latest-only / unavailable transition emission ----

    def _emit_transition_row(
        self,
        *,
        entity_id: str,
        category: str,
        state: Any,
        ts: str,
        attrs: dict | None,
        meta: dict,
    ) -> None:
        """Queue a single transition row for the latest-only / unavailable path.

        These rows do NOT pass through `_MinuteAccumulator` — they ARE the
        emitted value.  Stored in `_pending_latest_only_rows`, drained by
        the next `_drain_finalised_rows()` call alongside finalised
        accumulator rows.

        Updates `_last_emitted_state` to suppress same-value re-fires on
        subsequent capture events.
        """
        row: dict[str, Any] = {
            "entity_id": entity_id,
            "category": category,
            "ts": _minute_floor(ts),
            "state": state,
            # Single transition event ↔ samples=1.  Keeps the cloud-side row
            # shape stable across always / threshold / latest-only buckets.
            "samples": 1,
        }
        brand = meta.get("brand")
        area = meta.get("area")
        unit = meta.get("unit")
        if brand:
            row["brand"] = brand
        if area:
            row["area"] = area
        if unit:
            row["unit"] = unit
        if attrs:
            row["attributes"] = attrs
        self._pending_latest_only_rows.append(row)
        self._last_emitted_state[entity_id] = state

    def _drop_accumulators_for(self, entity_id: str) -> None:
        """Drop every open per-minute accumulator belonging to `entity_id`.

        Used when an entity transitions to unavailable: any partial
        in-flight minute aggregation is meaningless and would otherwise
        bleed an alive-period mean into the unavailable interval.
        """
        keys_to_drop = [k for k in self._accumulators if k[0] == entity_id]
        for k in keys_to_drop:
            self._accumulators.pop(k, None)

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

    # ---------------------- Minute-boundary finalisation ------------------

    def _current_minute_iso(self) -> str:
        """Return the wall-clock minute-floor as 'YYYY-MM-DDTHH:MM:00Z'."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%dT%H:%M:00Z")

    def _next_minute_iso(self) -> str:
        """Return the minute-floor ONE minute ahead of now (force-seal pivot).

        v0.4.5 cold-start force-seal: passing this to
        `_drain_finalised_rows(current_minute=...)` makes the drain treat the
        current (still-open) minute as already sealed — every accumulator with
        `minute_iso < next_minute` (i.e. including the current minute) finalises
        and ships.  Used only by `flush(seal_current_minute=True)` on the very
        first batch-loop iteration so the first telemetry row lands in seconds
        instead of waiting for a natural minute boundary.
        """
        from datetime import datetime, timedelta, timezone
        nxt = datetime.now(timezone.utc) + timedelta(minutes=1)
        return nxt.strftime("%Y-%m-%dT%H:%M:00Z")

    def _drain_finalised_rows(self, *, current_minute: str | None = None) -> list[dict]:
        """Finalise accumulators whose minute < current_minute and return their rows.

        Keeps the current-minute accumulator(s) alive so they keep collecting
        into the next flush window.  Per-entity, retains at most
        _MAX_ROWS_PER_ENTITY most-recent minute-rows.

        v0.3.0 send-policy gating:
          - Always-bucket accumulators emit unconditionally (v0.2.x behaviour).
          - Threshold-bucket accumulators emit only when the finalised mean
            diverges from `_last_emitted_state` by ≥ category threshold.
            First-boot rule: if no prior emission for the entity, emit
            unconditionally.  When a row IS emitted, update
            `_last_emitted_state` so future minutes gate against the
            most recent shipped value.  Non-emitted minutes are discarded —
            their accumulator state is lost (spec §"What threshold crossing
            means precisely" item 5).
          - Latest-only rows are NOT here — they pre-emit via
            `_emit_transition_row` and live in `_pending_latest_only_rows`.
        """
        current = current_minute or self._current_minute_iso()

        # Collect every sealed accumulator first, grouped by entity_id, sorted
        # chronologically — the send-policy threshold gate uses an EVOLVING
        # baseline that updates as rows are emitted, so we cannot iterate in
        # dict-order.  When a row IS emitted, the next minute's gate compares
        # against THAT row's value, not the original seeded baseline.
        sealed_by_entity: dict[str, list[tuple[str, _MinuteAccumulator]]] = {}
        keys_to_drop: list[tuple[str, str]] = []
        for key, acc in self._accumulators.items():
            entity_id, minute_iso = key
            if minute_iso >= current:
                continue  # still open — keep accumulating
            keys_to_drop.append(key)
            sealed_by_entity.setdefault(entity_id, []).append((minute_iso, acc))
            # Track high-water mark so late arrivals get dropped, even when
            # the send-policy gate ends up suppressing this minute's row.
            prev = self._last_finalised_minute.get(entity_id)
            if prev is None or minute_iso > prev:
                self._last_finalised_minute[entity_id] = minute_iso

        for key in keys_to_drop:
            self._accumulators.pop(key, None)

        # v0.3.0: emit per-entity in chronological order, threading the
        # evolving baseline through the gate.
        rows_by_entity: dict[str, list[dict]] = {}
        for entity_id, minute_accs in sealed_by_entity.items():
            minute_accs.sort(key=lambda pair: pair[0])
            for minute_iso, acc in minute_accs:
                row = acc.finalise()
                policy = _policy_for(acc.category)
                bucket = policy["bucket"]
                if bucket == "threshold" and acc.is_numeric:
                    threshold = policy["threshold"]
                    candidate_state = row.get("state")
                    last_emitted = self._last_emitted_state.get(entity_id)
                    # First-boot rule: emit unconditionally on the first row.
                    # Otherwise gate on |mean - last_emitted| ≥ threshold.
                    if (
                        last_emitted is not None
                        and isinstance(candidate_state, (int, float))
                        and isinstance(last_emitted, (int, float))
                        and threshold is not None
                        and abs(candidate_state - last_emitted) < threshold
                    ):
                        # Suppress this minute's row.  `_last_emitted_state`
                        # stays at whatever value last got published (spec
                        # §"What threshold crossing means precisely" item 4:
                        # keeps the gate from drifting closed during slow
                        # creep — small per-minute deltas accumulate against
                        # the unchanging baseline until they cross).
                        log.debug(
                            "send-policy: drop %s minute %s (mean=%.3f vs "
                            "last=%.3f below threshold %.3f)",
                            entity_id, minute_iso, candidate_state,
                            last_emitted, threshold,
                        )
                        continue
                    # Emit: update baseline to the value we just shipped.
                    if isinstance(candidate_state, (int, float)):
                        self._last_emitted_state[entity_id] = float(candidate_state)
                elif bucket == "always" and acc.is_numeric:
                    # Always-bucket numeric: every minute ships.  Keep
                    # `_last_emitted_state` in sync so a subsequent
                    # classifier change (always → threshold) gates
                    # correctly on the next tune cycle.
                    candidate_state = row.get("state")
                    if isinstance(candidate_state, (int, float)):
                        self._last_emitted_state[entity_id] = float(candidate_state)

                rows_by_entity.setdefault(entity_id, []).append(row)

        # Cap each entity to the _MAX_ROWS_PER_ENTITY most recent minute-rows.
        out: list[dict] = []
        for entity_id, rows in rows_by_entity.items():
            rows.sort(key=lambda r: r["ts"])
            if len(rows) > _MAX_ROWS_PER_ENTITY:
                dropped = len(rows) - _MAX_ROWS_PER_ENTITY
                log.warning(
                    "flush: capping %s to %d rows (dropped %d oldest)",
                    entity_id, _MAX_ROWS_PER_ENTITY, dropped,
                )
                rows = rows[-_MAX_ROWS_PER_ENTITY:]
            out.extend(rows)

        # v0.3.0: append pending latest-only transition rows (switches,
        # lights, climate.mode, unavailable transitions).  These already
        # carry their own `ts` (minute-floored at capture) and were filtered
        # at capture-time for same-value re-fires.
        #
        # v0.3.2 fix (staging 2026-06-01): an unavailable/unknown transition
        # row must NOT clobber a same-batch numeric recovery row at the cloud.
        # The cloud ingestion Lambda upserts LATEST# unconditionally in batch
        # array order (last row for an entity wins) — see
        # infra/lambdas/ingestion/handler.py `_write_latest`.  A mid-minute
        # `<value> → unavailable → <value>` flap (observed verbatim on
        # sensor.living_room_climate_staging_temperature) emits BOTH a
        # finalised numeric value row AND an unavailable transition row that
        # floor to the SAME minute `ts`.  With the transition row appended
        # last, the stale outage blip won LATEST# and froze the entity at
        # "unavailable" until a later minute happened to ship — a ~2.5h gap in
        # production because the coordinator was torn down before that
        # happened.  Per send_policy.md §"State == unavailable semantics":
        # `unavailable → <value>` MUST surface the recovered value as LATEST#.
        #
        # Defence: drop an unavailable/unknown transition row when this batch
        # also carries a numeric value row for the same entity at an equal or
        # later minute.  The recovered value is the truth; the intra-window
        # outage blip is stale by emit time.  A genuine outage with no
        # in-batch recovery keeps its transition row (no competing numeric
        # row), and non-numeric switch/light transitions are unaffected (their
        # categories never produce numeric accumulator rows).
        if self._pending_latest_only_rows:
            latest_numeric_ts: dict[str, str] = {}
            for entity_id, rows in rows_by_entity.items():
                for r in rows:
                    if isinstance(r.get("state"), (int, float)) and not isinstance(
                        r.get("state"), bool
                    ):
                        ts = r["ts"]
                        prev = latest_numeric_ts.get(entity_id)
                        if prev is None or ts > prev:
                            latest_numeric_ts[entity_id] = ts
            for row in self._pending_latest_only_rows:
                if row.get("state") in _UNAVAILABLE_STATES:
                    recovery_ts = latest_numeric_ts.get(row["entity_id"])
                    if recovery_ts is not None and recovery_ts >= row["ts"]:
                        log.debug(
                            "send-policy: drop stale unavailable transition for "
                            "%s @ %s — superseded by recovery value row @ %s",
                            row["entity_id"], row["ts"], recovery_ts,
                        )
                        continue
                out.append(row)
            self._pending_latest_only_rows = []

        return out

    # ---------------------- v0.2.2 diagnostic snapshot --------------------

    def _accumulator_stats(self) -> tuple[int, int, int]:
        """Snapshot accumulator state for the heartbeat payload.

        Returns
        -------
        (entity_count, total_samples, finalised_minutes_pending)
            entity_count: distinct entity_ids currently held in accumulators.
            total_samples: sum of `count` across all live accumulators.
            finalised_minutes_pending: count of accumulator entries whose
                minute_iso has already rolled past — i.e. they are eligible
                to be drained by the next flush() call.  If this stays > 0
                across heartbeats while _last_flush_iso never updates, the
                batch_loop is dead.

        Pure read of internal state — no mutation.
        """
        live_entities: set[str] = set()
        total_samples = 0
        current = self._current_minute_iso()
        pending = 0
        for (entity_id, minute_iso), acc in self._accumulators.items():
            live_entities.add(entity_id)
            total_samples += acc.count
            if minute_iso < current:
                pending += 1
        return (len(live_entities), total_samples, pending)

    # ---------------------- Shipping-mode FSM (#9) ------------------------

    @property
    def whitelist(self) -> frozenset[str] | None:
        """The active-mode entity whitelist, or None when none is commanded."""
        return self._whitelist

    @property
    def whitelist_version(self) -> int:
        """Monotonic version of the last whitelist the cloud sent (-1 = never)."""
        return self._whitelist_version

    def set_shipping_mode(
        self,
        mode: str,
        *,
        whitelist: list[str] | None = None,
        whitelist_version: int | None = None,
    ) -> None:
        """Transition the shipping mode and optionally reconcile the whitelist.

        Called from the command handler on a `set_shipping_mode` MQTT message.

        Parameters
        ----------
        mode
            One of VALID_SHIPPING_MODES.  An unknown mode raises ValueError and
            leaves the current mode UNTOUCHED — a malformed cloud command must
            never silently flip us into an undefined gate state.
        whitelist
            Optional entity-id list.  When present, reconciles the whitelist
            (subject to the version check).  When ABSENT, the existing whitelist
            is preserved — the cloud may re-send a mode without re-sending an
            unchanged whitelist.
        whitelist_version
            Monotonic version stamp for the whitelist.  A version <= the current
            one is treated as stale and ignored (out-of-order delivery guard).

        Raises
        ------
        ValueError
            On an unknown mode.
        """
        if mode not in VALID_SHIPPING_MODES:
            raise ValueError(
                f"unknown shipping_mode {mode!r}; "
                f"expected one of {sorted(VALID_SHIPPING_MODES)}"
            )
        prev = self.shipping_mode
        self.shipping_mode = mode
        if whitelist is not None:
            self.set_whitelist(whitelist, whitelist_version=whitelist_version)
        if prev != mode:
            log.info("shipping_mode transition: %s -> %s", prev, mode)

    def set_whitelist(
        self, whitelist: list[str], *, whitelist_version: int | None = None
    ) -> None:
        """Reconcile the active-mode whitelist.

        An update whose `whitelist_version` is <= the currently-held version is
        ignored as stale (commands and status pulls can race / arrive out of
        order).  A None version is always applied (callers that don't track a
        version — e.g. tests — opt out of the staleness guard).
        """
        if (
            whitelist_version is not None
            and whitelist_version <= self._whitelist_version
        ):
            log.debug(
                "ignoring stale whitelist v%s (current v%s)",
                whitelist_version, self._whitelist_version,
            )
            return
        self._whitelist = frozenset(whitelist)
        if whitelist_version is not None:
            self._whitelist_version = whitelist_version
        log.info(
            "whitelist reconciled: %d entities (v%s)",
            len(self._whitelist), whitelist_version,
        )

    def reconcile_from_status(self, status: dict[str, Any]) -> None:
        """Reconcile local FSM state to the cloud's truth from a /hacs/status pull.

        The reconnect safety net (ADR 0005 / issue #9): the cloud may have
        published a command while HACS was offline AND the broker's persistent
        session expired (>1h offline), dropping the queued command.  On
        reconnect HACS pulls /hacs/status once and adopts the cloud-declared
        mode + whitelist unconditionally — the cloud is authoritative.

        Malformed / missing fields are ignored so a bad status response can't
        corrupt local state (fail safe to whatever we already had).
        """
        mode = status.get("shipping_mode")
        if mode in VALID_SHIPPING_MODES:
            if mode != self.shipping_mode:
                log.info(
                    "reconcile: cloud shipping_mode=%s overrides local=%s",
                    mode, self.shipping_mode,
                )
            self.shipping_mode = mode
        elif mode is not None:
            log.warning("reconcile: ignoring invalid shipping_mode %r", mode)
        whitelist = status.get("whitelist")
        if isinstance(whitelist, list):
            self.set_whitelist(
                whitelist, whitelist_version=status.get("whitelist_version")
            )

    def set_last_recovery(self, ack: dict[str, Any]) -> None:
        """Stash the most-recent recover_window ack for the next heartbeat.

        Called by RecoveryManager after a recover attempt completes (v0.4.6,
        Sprint 7).  Pure state write — the value is read by `heartbeat_once`
        and shipped on the heartbeat's `last_recovery` field so the cloud
        learns whether HA's recorder had the gap rows.
        """
        self._last_recovery = ack

    def _telemetry_publishing_enabled(self) -> bool:
        """True iff the current shipping mode permits 30s telemetry batches."""
        return self.shipping_mode not in TELEMETRY_SUPPRESSED_MODES

    def _filter_to_whitelist(self, rows: list[dict]) -> list[dict]:
        """Drop rows for entities not on the whitelist.

        No whitelist commanded (None) → pass everything through (legacy
        pre-gate behaviour).  Otherwise only whitelisted entity rows survive;
        non-whitelisted entities never leave HA (ADR 0005 privacy posture).
        """
        if self._whitelist is None:
            return rows
        return [r for r in rows if r.get("entity_id") in self._whitelist]

    # ---------------------- Flush + publish -------------------------------

    async def flush(self, *, seal_current_minute: bool = False) -> None:
        """Finalise sealed-minute accumulators, build batch(es), ship them.

        v0.4.5 cold-start fast-path: when `seal_current_minute=True` (the very
        first batch-loop iteration after start), the CURRENT still-open minute
        is force-sealed too — we drain against a pivot one minute AHEAD of now,
        so the partial current-minute accumulators finalise and ship instead of
        waiting for a natural minute boundary.  That one cold-start row
        legitimately carries fewer `samples` (only the events seen in the first
        ~12s) — an accepted trade-off for ~instant first data.  Because the
        force-seal sets `_last_finalised_minute[entity]=<current minute>` and
        the late-arrival guard in `_record_state` uses `<=`, that minute can
        never be re-shipped by the next (steady-state) flush — no double-ship.
        All non-first flushes pass `seal_current_minute=False` and behave
        exactly as before.

        v0.2.1 (2026-05-26 hotfix): chunked publish. A 5-min flush window
        across many active entities can easily produce > 700 minute-rows,
        which exceeds the AWS IoT Core MQTT v3.1.1 128 KiB message limit
        (~180 bytes/row → ~700 rows per safe message). We split the row set
        into sequential chunks of at most MAX_ENTITIES_PER_BATCH_PUBLISH,
        each its own batch with a fresh batch_id, and publish in order.

        The publisher owns retry (via its bounded queue), so we always discard
        the finalised rows after handing off — we never double-ship.

        Build-side rejections (e.g. classifier drift producing an invalid
        category) are logged at ERROR + tracked in self._flush_rejects so
        the next silent-swallow regression can't repeat the 2026-05-26 P0.

        v0.2.2 (2026-05-26): stamps `_last_flush_iso`, `_last_flush_row_count`,
        and `_last_publish_error` for heartbeat surfacing.  These are pure
        observability writes — they never alter the data path.  Publisher
        exceptions are recorded and re-raised so the _batch_loop safety net
        still handles them exactly as before.
        """
        # Stamp every flush call — whether or not it actually ships rows.
        # This lets us distinguish "batch loop is wedged" (last_flush_iso stays
        # null) from "batch loop runs but accumulator is empty" (last_flush_iso
        # advances every 5 min while last_flush_row_count stays 0).
        from datetime import datetime, timezone
        self._last_flush_iso = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        # v0.4.5: on the cold-start fast-path, pivot one minute ahead so the
        # current partial minute is treated as sealed and ships now.
        drain_pivot = self._next_minute_iso() if seal_current_minute else None
        rows = self._drain_finalised_rows(current_minute=drain_pivot)

        # ---- Shipping-mode gate (#9, ADR 0005) --------------------------
        # setup / paused suppress the 30s telemetry batch path entirely:
        # NO telemetry batches leave HA before the user confirms.  We still
        # DRAIN the accumulators above (sealing minutes, advancing the
        # late-arrival high-water marks, keeping memory bounded) — we just
        # don't build or publish a batch.  Capture + edge-PoC keep running,
        # so the user is not unprotected during onboarding.  Heartbeat is on
        # a separate path and is unaffected.
        if not self._telemetry_publishing_enabled():
            if rows:
                log.debug(
                    "shipping_mode=%s: suppressing %d telemetry rows "
                    "(no batch published)",
                    self.shipping_mode, len(rows),
                )
            self._last_flush_row_count = 0
            return

        # active mode: filter to the whitelist so non-whitelisted entities
        # never leave HA (ADR 0005 privacy posture).  None whitelist → all.
        rows = self._filter_to_whitelist(rows)
        self._last_flush_row_count = len(rows)
        if not rows:
            return

        ha_version = getattr(self._hass.config, "version", "unknown")
        # v0.5.0: emit country/timezone when HA has them configured.
        country = getattr(self._hass.config, "country", None) or None
        timezone = getattr(self._hass.config, "time_zone", None)
        timezone = str(timezone) if timezone else None

        # Split into MQTT-message-sized chunks. Each chunk is its own batch
        # with a fresh batch_id (uuid4 inside build_batch), so the ingestion
        # Lambda treats them as independent for idempotency.
        chunk_size = MAX_ENTITIES_PER_BATCH_PUBLISH
        total = len(rows)
        for offset in range(0, total, chunk_size):
            chunk = rows[offset:offset + chunk_size]
            try:
                payload = build_batch(
                    user_id=self._user_id,
                    entities=chunk,
                    ha_version=ha_version,
                    country=country,
                    timezone=timezone,
                )
            except EmptyBatchError:
                # Defensive: range() above ensures chunk is non-empty, but
                # keep the guard in case the slice math changes.
                continue
            except ValueError as exc:
                # LOUD on purpose. The 2026-05-26 P0 was a silent log.warning
                # that no one noticed until the dashboard froze for 2h.
                self._flush_rejects += 1
                log.error(
                    "flush: build_batch rejected payload "
                    "(chunk_rows=%d, total_rows=%d, rejects_total=%d): %s",
                    len(chunk), total, self._flush_rejects, exc,
                )
                continue
            # v0.2.2: capture publish errors for heartbeat surfacing.  We
            # RE-RAISE so the existing _batch_loop safety net handles them
            # exactly as before — pure observability, no behaviour change.
            #
            # v0.2.7 (2026-05-28): CLEAR `_last_publish_error` on success.
            # Pre-v0.2.7 the field was sticky — set on failure, NEVER cleared
            # on a subsequent successful publish.  A single transient publish
            # error (e.g. one /hacs-auth `ClientConnectorDNSError` observed
            # live on 2026-05-28) would pollute the heartbeat field for the
            # rest of HA uptime even though every subsequent flush succeeded
            # and DDB stayed fresh.  Portal freshness consumers reading this
            # field would render "unhealthy" indefinitely — the freshness
            # signal lied because the field reflected HISTORICAL failure,
            # not CURRENT state.  Clear on success so the heartbeat reflects
            # the latest publish outcome.
            try:
                await self._publisher.publish_telemetry(payload)
            except Exception as exc:
                self._last_publish_error = (
                    f"{type(exc).__name__}: {exc}"
                )[:200]
                raise
            else:
                # Publish succeeded — the most recent failure (if any) is
                # no longer the CURRENT state.  Clear so the heartbeat
                # consumer sees an honest "no failures since last flush".
                self._last_publish_error = None

    async def heartbeat_once(self) -> None:
        ha_version = getattr(self._hass.config, "version", "unknown")
        entity_count, total_samples, pending = self._accumulator_stats()
        # v0.2.6 — payload-size observability.  The publisher delegates to
        # IotCorePublisher (via the injected publish_fn), so reach through
        # to the underlying iot_core instance for these counters.  Guarded
        # with getattr so tests using a plain MagicMock publish_fn don't
        # break: missing counter defaults to None (heartbeat omits field).
        iot_core = getattr(self._publisher, "_publish_fn", None)
        # publish_fn is a bound method on IotCorePublisher in production;
        # __self__ gets us back to the instance.
        iot_core_instance = getattr(iot_core, "__self__", None) if iot_core else None
        last_publish_payload_bytes = (
            getattr(iot_core_instance, "last_publish_payload_bytes", None)
            if iot_core_instance is not None else None
        )
        payload_too_large_count = (
            getattr(iot_core_instance, "payload_too_large_count", None)
            if iot_core_instance is not None else None
        )
        client_error_disconnects = (
            getattr(iot_core_instance, "client_error_disconnects", None)
            if iot_core_instance is not None else None
        )
        last_disconnect_reason = (
            getattr(iot_core_instance, "last_disconnect_reason", None)
            if iot_core_instance is not None else None
        )
        hb = build_heartbeat(
            user_id=self._user_id,
            ha_version=ha_version,
            uptime_s=int(time.monotonic() - self._started_at),
            batches_sent=getattr(self._publisher, "batches_sent", 0),
            queue_depth=getattr(self._publisher, "queue_depth", 0),
            # v0.2.2 diagnostic counters — surface internal pipeline state
            # so the cloud side can pinpoint where telemetry is dying.
            flush_rejects=self._flush_rejects,
            accumulator_entity_count=entity_count,
            accumulator_total_samples=total_samples,
            finalised_minutes_pending=pending,
            batch_loop_iterations=self._batch_loop_iterations,
            last_flush_iso=self._last_flush_iso,
            last_flush_row_count=self._last_flush_row_count,
            last_publish_error=self._last_publish_error,
            # v0.2.6 — payload-size observability.
            last_publish_payload_bytes=last_publish_payload_bytes,
            payload_too_large_count=payload_too_large_count,
            client_error_disconnects=client_error_disconnects,
            last_disconnect_reason=last_disconnect_reason,
            # v0.4.6 — data-recovery ack.  None until the first recover_window
            # completes; once set, every heartbeat re-reports the latest
            # outcome so a missed heartbeat doesn't lose the ack.
            last_recovery=self._last_recovery,
        )
        await self._publisher.publish_heartbeat(hb)
        # Drain any backlogged batches the publisher accumulated while the
        # cloud was unreachable. Isolated from the heartbeat path: a drain
        # failure must not kill the device's liveness signal.
        drain = getattr(self._publisher, "drain_queue", None)
        if drain is not None:
            try:
                await drain()
            except (OSError, TimeoutError, ValueError) as exc:
                log.warning(
                    "drain_queue failed during heartbeat: %s: %s",
                    type(exc).__name__, exc,
                )

    # ---------------------- Background timers -----------------------------

    async def _batch_loop(self) -> None:
        # v0.4.5 cold-start fix: the FIRST iteration sleeps a short delay and
        # force-seals the current partial minute so first telemetry lands in
        # ~12s; every subsequent iteration reverts to the unchanged 300s
        # steady-state window.  `first` flips to False only after the first
        # sleep returns, so the iteration-counter / flush semantics below are
        # otherwise identical to pre-v0.4.5.
        first = True
        while True:
            try:
                await asyncio.sleep(
                    INITIAL_FLUSH_DELAY_SECONDS if first else BATCH_WINDOW_SECONDS
                )
                # v0.2.2: tick the iteration counter AFTER the sleep returns
                # so a value of 0 in the heartbeat unambiguously means
                # "the loop never woke up" (task GC'd, never scheduled, etc.).
                self._batch_loop_iterations += 1
                await self.flush(seal_current_minute=first)
                first = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - safety net
                # Even on a crashed first flush, revert to steady-state so a
                # one-off cold-start failure can't pin the loop at the short
                # delay forever.
                first = False
                log.error("batch flush crashed: %s: %s", type(exc).__name__, exc)

    async def _heartbeat_loop(self) -> None:
        # v0.4.5 cold-start fix: first heartbeat fires after a short delay (~5s)
        # so the liveness/version signal lands before the first data flush;
        # subsequent ticks revert to the unchanged 300s interval.
        first = True
        while True:
            try:
                await asyncio.sleep(
                    INITIAL_HEARTBEAT_DELAY_SECONDS if first else HEARTBEAT_INTERVAL_SECONDS
                )
                await self.heartbeat_once()
                first = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover
                first = False
                log.error("heartbeat crashed: %s: %s", type(exc).__name__, exc)

    async def start(self, entry: Any = None) -> None:
        # v0.4.1 (2026-06-04) — BOOTSTRAP FIX.  These are never-ending loops;
        # they MUST NOT be awaited during config-entry setup or HA's bootstrap
        # "wait for platforms" stage blocks forever and the supervisor watchdog
        # restart-loops the install (the 0.4.0 prod incident — HA logged
        # "Setup timed out for bootstrap waiting on" _batch_loop/_heartbeat_loop).
        #
        # `ConfigEntry.async_create_background_task` (HA 2022.8+) is the correct
        # API: it keeps a STRONG ref (so the v0.1.14 weak-ref GC bug can't
        # recur) AND registers the task as a BACKGROUND task that HA does NOT
        # await at bootstrap.  `hass.async_create_task`/`entry.async_create_task`
        # (the old path) register FOREGROUND tasks that ARE awaited — wrong for
        # an infinite loop.
        #
        # Fallback ladder for older HA cores / test envs:
        #   entry.async_create_background_task  -> hass.async_create_background_task
        #   -> asyncio.create_task (test MagicMock hass / no entry).
        bg_entry = getattr(entry, "async_create_background_task", None)
        bg_hass = getattr(self._hass, "async_create_background_task", None)
        if callable(bg_entry):
            self._batch_task = bg_entry(
                self._hass, self._batch_loop(), name="iems_batch_loop"
            )
            self._heartbeat_task = bg_entry(
                self._hass, self._heartbeat_loop(), name="iems_heartbeat_loop"
            )
        elif callable(bg_hass):
            self._batch_task = bg_hass(self._batch_loop(), name="iems_batch_loop")
            self._heartbeat_task = bg_hass(
                self._heartbeat_loop(), name="iems_heartbeat_loop"
            )
        else:
            # Test env (MagicMock hass) or HA core too old for background tasks.
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
