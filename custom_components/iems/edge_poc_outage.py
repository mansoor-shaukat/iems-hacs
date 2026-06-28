"""Edge PoC — continuous grid-health lamp indicator.

Sprint 5 Track B. Single-site PoC (Mansoor's home). Zero cloud round-trip.

CEO directive 2026-05-02 (v0.1.15):
  Lamp is a continuous grid-state indicator, NOT a fire-once outage signal.
  Grid up   → cool white  (color_temp_kelvin = 5500, brightness 200).
  Grid down → blue        (xy_color [0.1532, 0.0475], brightness 200).

State is derived each evaluation. There is no captured/restored prior state —
v0.1.14 had a capture/restore choreography that made the integration impossible
to verify without a real outage (the safety guard cancelled any pending apply
whenever it saw grid-up). v0.1.15 makes the lamp's job trivial to verify: just
look at it.

Trigger: ALL four grid-voltage sensors simultaneously NULL or < 50V (AND rule).
This mirrors the production-tested canonical detector in
solarman_bridge/manager.py:_check_grid_status.
DO NOT use binary_sensor.inverter[12]_grid — those flap on Solarman polling
glitches (3 confirmed false positives across 14 days, substations held 210-216V).
See: docs/architecture/canonical_signals.md §Grid availability.

Lifecycle:
  startup           → evaluate grid → paint matching colour IMMEDIATELY (no debounce)
  voltage transition → evaluate grid → if state flipped, schedule apply with debounce
  state unchanged   → no apply (idempotent)

Detection helper: _grid_is_down(states)
  Returns True  iff EVERY listed inverter's voltage is missing/None or < 50V.
  Returns False if ANY voltage is ≥ 50V.
  Returns False (inconclusive, fail-safe "grid up") when < 4 inverters have
             reported within the last 60s — the canonical AND-of-all rule
             requires ALL FOUR fresh; if any one is stale we cannot confirm.
             CEO directive 2026-05-01 (tightened from < 3).

Phasing: CEO-scoped single-site PoC. NOT a commercial CONTROL ship.

See: docs/integrations/edge_poc_outage_color.md for full spec.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("iems.edge_poc")

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

LAMP_ENTITY_ID = "light.living_lamp"

# Four canonical voltage sensors — live entity_ids verified 2026-04-29.
# Ground Master HA entity uses the _2 suffix (duplicate entry disambiguation).
# Source: docs/architecture/canonical_signals.md §Grid availability.
VOLTAGE_ENTITY_IDS: tuple[str, ...] = (
    "sensor.inverter1_grid_l1_voltage",
    "sensor.inverter2_grid_l1_voltage",
    "sensor.ground_master_grid_l1_voltage_2",
    "sensor.ground_slave_grid_l1_voltage",
)

# Voltage below this threshold is treated as "no grid" (0.0 is written when
# HA reports 'unavailable'; <50 guards against residual-bus readings near 0).
GRID_DOWN_VOLTAGE_THRESHOLD: float = 50.0

# Minimum number of inverters that must have reported within STALE_WINDOW_S
# seconds for the check to be considered conclusive. The canonical AND-of-all
# rule requires ALL FOUR fresh; CEO directive 2026-05-01 tightened from 3 to 4.
MIN_REPORTING_INVERTERS: int = 4
STALE_WINDOW_S: float = 60.0

# Outage CIE 1931 xy — Philips Hue Gamut C blue corner (deep, high-saturation).
# CEO directive 2026-05-01: BLUE replaces the prior amber (0.564, 0.426).
# Source: Philips Hue published Gamut C — deep blue corner ≈ (0.1532, 0.0475).
# Brightness raised 180 → 200 because deep-blue is perceptually dimmer than
# amber at the same brightness; 200 keeps the visual signal strength comparable
# without being painful at night.
OUTAGE_XY: tuple[float, float] = (0.1532, 0.0475)
OUTAGE_BRIGHTNESS: int = 200

# Grid-up colour — clean cool white via Kelvin (CEO directive 2026-05-02).
# 5500K reads as a clean cool white on Hue lamps without going harsh; the
# lamp's `supported_color_modes` include both `color_temp` and `xy`, so
# Kelvin is the cleaner channel (no chromaticity drift).
# Brightness matches OUTAGE_BRIGHTNESS so signal strength is comparable
# across the two states.
GRID_UP_COLOR_TEMP_KELVIN: int = 5500
GRID_UP_BRIGHTNESS: int = 200

# Debounce windows (seconds) — 60s/60s per CEO directive 2026-04-29.
# Analysis: all 3 confirmed false positives lasted 37–38s; real outages start
# at ≥6 min. 60s sustained-NULL across all 4 inverters = unambiguous outage.
# v0.1.15: debounce is per-transition, not per-event-type. Initial paint on
# startup does NOT debounce — the lamp must reflect actual current state on
# load, not 60s later.
GRID_OFF_DEBOUNCE_S: float = 60.0
GRID_ON_DEBOUNCE_S: float = 60.0

# SQLite log path (under HA config dir; resolved at runtime via hass.config.path)
SQLITE_FILENAME = "iems_poc_decisions.sqlite"

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS poc_decisions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL,
    event       TEXT    NOT NULL,
    latency_ms  INTEGER,
    prior_state TEXT,
    success     INTEGER NOT NULL
);
"""

# Internal state-flag values — used both as the desired-colour token passed
# into the scheduler and as the bookkeeping for "what colour does the lamp
# currently believe it's set to". `None` means "unknown / not yet painted".
_STATE_GRID_UP = "cool_white"
_STATE_GRID_DOWN = "blue"

# ── Lamp control retired → native HA automation owns the lamp (CEO 2026-06-28) ──
# CEO moved the living-room lamp's grid-state control OFF this HACS edge-PoC and
# ONTO a native Home Assistant automation (CTO-authored). The lamp now has exactly
# ONE owner. When this flag is True the edge-PoC NEVER writes to light.living_lamp —
# not cool-white on grid-up, not blue on grid-down. The grid-detection helper
# (_grid_is_down) and the SQLite decision log remain present but the apply path is
# a no-op, so HACS and the native automation can never fight over the lamp.
#
# Primary disable is at the wiring layer (__init__.py no longer starts the handler
# or registers the services). This module-level guard is belt-and-braces: even a
# direct async_start()/service call cannot paint the lamp while it is True.
#
# REVERSIBLE: flip this to False AND restore the 3 wiring lines in __init__.py
# (EdgePocOutageHandler(...) / register_services(...) / await edge_poc.async_start())
# to bring the edge-PoC lamp indicator back. No other code was deleted.
LAMP_CONTROL_RETIRED: bool = True


# --------------------------------------------------------------------------
# Grid-down detection (canonical AND-of-all rule)
# --------------------------------------------------------------------------


def _grid_is_down(states: list[Any]) -> bool:
    """Return True iff all reporting inverters have NULL/< 50V grid voltage.

    Mirrors solarman_bridge/manager.py:_check_grid_status semantics.
    DO NOT re-derive — that function is production-tested since 2026-04-09.

    Args:
        states: list of HA State objects for the 4 voltage entities (or
                mock objects in tests with .state and .last_updated attrs).

    Returns:
        True  — all inverters report NULL or < GRID_DOWN_VOLTAGE_THRESHOLD.
        False — any inverter reports a voltage ≥ threshold (grid is up).
        False — fewer than MIN_REPORTING_INVERTERS inverters have a
                non-'unavailable'/non-None state within STALE_WINDOW_S
                (inconclusive, fail-safe to "grid up").
    """
    now = time.time()
    reporting = 0
    all_down = True

    for state_obj in states:
        if state_obj is None:
            # Entity not found in HA state machine — skip, doesn't count.
            continue

        raw = state_obj.state
        # HA marks unavailable/unknown entities with those literal strings.
        if raw in ("unavailable", "unknown", None):
            # This inverter is not reporting — not safe to conclude "down"
            # from it, but also don't count it as "reporting".
            continue

        # Check staleness via last_updated attribute (present on real HA State
        # objects; tests may inject a numeric epoch or skip).
        last_updated = getattr(state_obj, "last_updated", None)
        if last_updated is not None:
            try:
                if hasattr(last_updated, "timestamp"):
                    lu_epoch = last_updated.timestamp()
                else:
                    lu_epoch = float(last_updated)
                if now - lu_epoch > STALE_WINDOW_S:
                    continue  # stale — ignore for this check
            except (TypeError, ValueError):
                pass  # can't determine staleness; proceed

        reporting += 1
        try:
            voltage = float(raw)
        except (ValueError, TypeError):
            # Non-numeric string state — treat as unavailable.
            continue

        if voltage >= GRID_DOWN_VOLTAGE_THRESHOLD:
            all_down = False

    if reporting < MIN_REPORTING_INVERTERS:
        # Inconclusive — not enough inverters seen recently.
        log.debug(
            "edge_poc: _grid_is_down inconclusive: only %d of %d inverters reporting",
            reporting, len(states),
        )
        return False

    return all_down


# --------------------------------------------------------------------------
# SQLite logging
# --------------------------------------------------------------------------


def _utc_now_z() -> str:
    """Return current UTC time as ISO-8601 with trailing Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_schema(db_path: str) -> None:
    """Create table if it does not already exist. Idempotent."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(_SCHEMA_SQL)
        conn.commit()


def log_event(
    db_path: str,
    event_type: str,
    latency_ms: int | None,
    prior_state: dict | None,
    success: bool,
) -> None:
    """Write one row to the PoC decisions SQLite log.

    Never raises — a logging failure must not interrupt the automation path.
    Uses parameterized placeholders (security rule: no f-strings in SQL).

    `prior_state` is retained as a column for backwards compatibility with
    the existing schema and pre-v0.1.15 rows, but v0.1.15 always writes NULL
    here (lamp colour is now derived, not captured).
    """
    try:
        _ensure_schema(db_path)
        prior_json = json.dumps(prior_state) if prior_state is not None else None
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO poc_decisions (ts, event, latency_ms, prior_state, success)"
                " VALUES (?, ?, ?, ?, ?)",
                (_utc_now_z(), event_type, latency_ms, prior_json, int(success)),
            )
            conn.commit()
        log.debug("edge_poc: logged event=%s latency_ms=%s success=%s", event_type, latency_ms, success)
    except sqlite3.Error as exc:
        log.error("edge_poc: SQLite log failed: %s: %s", type(exc).__name__, exc)


# --------------------------------------------------------------------------
# Lamp apply helpers
# --------------------------------------------------------------------------


async def apply_outage_color(hass, entity_id: str) -> None:
    """Turn lamp BLUE — the grid-down indicator (xy_color [0.1532,0.0475], brightness 200)."""
    log.info("edge_poc: applying outage colour (blue) to %s", entity_id)
    await hass.services.async_call(
        "light",
        "turn_on",
        {
            "entity_id": entity_id,
            "xy_color": list(OUTAGE_XY),
            "brightness": OUTAGE_BRIGHTNESS,
            "transition": 0,
        },
        blocking=True,
    )


async def apply_grid_up_color(hass, entity_id: str) -> None:
    """Turn lamp COOL WHITE — the grid-up indicator (color_temp_kelvin 5500, brightness 200).

    Uses Kelvin rather than xy_color so the lamp picks the cleanest white path
    its hardware supports. We deliberately do NOT also send xy/hs in the same
    payload: some Hue firmwares prioritise the chromaticity field over Kelvin
    and the lamp would fall slightly off-white.
    """
    log.info("edge_poc: applying grid-up colour (cool white %dK) to %s",
             GRID_UP_COLOR_TEMP_KELVIN, entity_id)
    await hass.services.async_call(
        "light",
        "turn_on",
        {
            "entity_id": entity_id,
            "color_temp_kelvin": GRID_UP_COLOR_TEMP_KELVIN,
            "brightness": GRID_UP_BRIGHTNESS,
            "transition": 0,
        },
        blocking=True,
    )


# --------------------------------------------------------------------------
# Handler
# --------------------------------------------------------------------------


class EdgePocOutageHandler:
    """Manages the lamp colour ↔ grid-state relationship.

    Wired into IemsCoordinator.start() via async_setup_entry.
    Subscribes to voltage state changes on all 4 VOLTAGE_ENTITY_IDS.
    On each state change, calls _grid_is_down() (AND-of-all check).

    Lifecycle:
      async_start      → paint current grid state IMMEDIATELY (no debounce)
      voltage transition (grid_down → grid_up)   → schedule cool-white (60s debounce)
      voltage transition (grid_up   → grid_down) → schedule blue       (60s debounce)
      voltage event with state unchanged          → no-op (idempotent)

    Public service-call entry-points (kept for contract compatibility with
    existing user automations):
      on_grid_off()       — re-evaluates actual state; reflects truth, not fiction
      on_grid_recovered() — same; lamp is a state indicator, not a remote control
    """

    def __init__(self, hass, db_path: str) -> None:
        self._hass = hass
        self._db_path = db_path
        # Single pending task slot — there's only ever one in-flight transition
        # because the two debounces both push toward the same lamp.
        self._pending_task: asyncio.Task | None = None
        # The colour token we last asked the lamp to display, OR the colour
        # the pending debounce is heading toward. Used to suppress redundant
        # applies and to detect "state flipped back" cancellation.
        self._target_state: str | None = None
        self._unsub: list = []

    # ------------------------------------------------------------------ #
    # Public entry-points                                                  #
    # ------------------------------------------------------------------ #

    def on_grid_off(self) -> None:
        """Manual `iems.edge_poc_outage_grid_off` service call.

        v0.1.15: kept for contract compatibility; delegates to the actual-state
        evaluator. The lamp tells the truth — it does not lie because a user
        manually fired a service call. CEO directive 2026-05-02.
        """
        log.info("edge_poc: manual grid_off service call — re-evaluating actual state")
        self.handle_voltage_state_change()

    def on_grid_recovered(self) -> None:
        """Manual `iems.edge_poc_outage_grid_recovered` service call.

        Same as on_grid_off: re-evaluates real state. Lamp reflects reality.
        """
        log.info("edge_poc: manual grid_recovered service call — re-evaluating actual state")
        self.handle_voltage_state_change()

    def handle_voltage_state_change(self) -> None:
        """Called on any VOLTAGE_ENTITY_IDS state change event.

        Reads current state of all 4 voltage entities, runs _grid_is_down(),
        and either:
          - schedules a debounced colour apply (if state flipped), or
          - cancels a pending debounce (if state flipped back to current), or
          - no-ops (if state matches what we already painted).
        """
        target = self._evaluate_target_state()

        if target == self._target_state:
            # Already painted (or pending) the right colour — nothing to do.
            return

        self._schedule_apply(target)

    # ------------------------------------------------------------------ #
    # Internal — state evaluation + scheduling                             #
    # ------------------------------------------------------------------ #

    def _evaluate_target_state(self) -> str:
        """Read current voltages and decide the target lamp colour.

        Returns _STATE_GRID_UP or _STATE_GRID_DOWN. The fail-safe-to-up
        semantics inside _grid_is_down() mean inconclusive readings produce
        _STATE_GRID_UP — we never paint blue on an unconfirmed outage.
        """
        voltage_states = [
            self._hass.states.get(eid) for eid in VOLTAGE_ENTITY_IDS
        ]
        return _STATE_GRID_DOWN if _grid_is_down(voltage_states) else _STATE_GRID_UP

    def _schedule_apply(self, target: str) -> None:
        """Cancel any pending apply and schedule a new one for `target`.

        Uses hass.async_create_task — NOT loop.create_task. v0.1.14 fix:
        Python's asyncio loop holds only weak refs to tasks created via
        loop.create_task, which can be GC'd mid-flight. HA's
        `hass.async_create_task` registers the task on `hass._tasks` (strong
        ref), ties it to integration setup/teardown, and uses eager_start.
        """
        self._cancel_pending()
        self._target_state = target
        self._pending_task = self._hass.async_create_task(
            self._debounced_apply(target)
        )

    def _cancel_pending(self) -> None:
        if self._pending_task and not self._pending_task.done():
            self._pending_task.cancel()
        self._pending_task = None

    async def _debounced_apply(self, target: str) -> None:
        """Wait the debounce, re-check grid state, apply if still matching.

        Re-checking after the sleep guards against a race: if the grid
        flipped back during the debounce, _evaluate_target_state() will
        return the old colour and we skip the apply. This is belt-and-
        braces on top of _schedule_apply's cancel-on-flip logic.
        """
        debounce_s = (
            GRID_OFF_DEBOUNCE_S if target == _STATE_GRID_DOWN
            else GRID_ON_DEBOUNCE_S
        )
        try:
            await asyncio.sleep(debounce_s)
        except asyncio.CancelledError:
            log.debug("edge_poc: debounce cancelled (target=%s)", target)
            return

        # Re-evaluate after the sleep — if grid flipped back, abandon.
        actual = self._evaluate_target_state()
        if actual != target:
            log.debug(
                "edge_poc: state flipped during debounce (target=%s, actual=%s) — skipping apply",
                target, actual,
            )
            self._target_state = actual
            return

        await self._apply_now(target)

    async def _apply_now(self, target: str) -> None:
        """Paint the lamp the target colour and log the event.

        Used by _debounced_apply (post-debounce) and by async_start
        (immediate startup paint, no debounce).

        RETIRED (CEO 2026-06-28): when LAMP_CONTROL_RETIRED is set, this is a
        hard no-op — the edge-PoC never issues a light.turn_on. The native HA
        automation is the sole owner of light.living_lamp. Belt-and-braces with
        the wiring-layer disable in __init__.py.
        """
        if LAMP_CONTROL_RETIRED:
            log.debug(
                "edge_poc: lamp control retired — suppressing %s apply "
                "(native HA automation owns light.living_lamp)", target,
            )
            self._target_state = target
            return

        t0 = time.monotonic()
        success = False
        event_type = (
            "grid_off_fire" if target == _STATE_GRID_DOWN
            else "grid_recovered_apply"
        )
        try:
            if target == _STATE_GRID_DOWN:
                await apply_outage_color(self._hass, LAMP_ENTITY_ID)
            else:
                await apply_grid_up_color(self._hass, LAMP_ENTITY_ID)
            self._target_state = target
            success = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.error(
                "edge_poc: apply failed (target=%s): %s: %s",
                target, type(exc).__name__, exc,
            )
        finally:
            latency_ms = int((time.monotonic() - t0) * 1000)
            log_event(self._db_path, event_type, latency_ms, None, success)

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def async_start(self) -> None:
        """Subscribe to voltage state changes + paint current state immediately.

        v0.1.15: on integration load, the lamp is painted to the matching
        grid-state colour right away — no 60s debounce on first apply. The
        debounce only matters for transitions, not initial state. Without
        this, a user installing the integration would see no lamp change
        for a full minute even though grid state is known instantly from
        the live voltage entities.

        v0.1.13 P0 fix preserved: subscribe to async_track_state_change_event
        for all 4 canonical voltage entities so the handler fires automatically
        on voltage transitions (no YAML automation required).
        """
        # Auto-subscribe to voltage state-changes — v0.1.13 fix.
        # Wrap the import + subscribe so test environments without HA installed
        # still allow async_start to run.
        try:
            from homeassistant.helpers.event import (  # type: ignore[import]
                async_track_state_change_event,
            )
            from homeassistant.core import callback  # type: ignore[import]

            @callback
            def _on_voltage_change(event):  # noqa: ARG001 — HA passes the event
                self.handle_voltage_state_change()

            unsub = async_track_state_change_event(
                self._hass, list(VOLTAGE_ENTITY_IDS), _on_voltage_change
            )
            self._unsub.append(unsub)
            log.info(
                "edge_poc: auto-subscribed to %d voltage entities for state-change events",
                len(VOLTAGE_ENTITY_IDS),
            )
        except ImportError as exc:
            log.warning(
                "edge_poc: HA helpers unavailable (%s) — running in service-only"
                " mode; voltage auto-subscribe disabled",
                exc,
            )
        except Exception as exc:  # noqa: BLE001
            log.error(
                "edge_poc: unexpected error wiring state-change subscription:"
                " %s: %s — falling back to service-only mode",
                type(exc).__name__, exc,
            )

        # Initial paint — no debounce. Whatever the grid is doing right now,
        # the lamp must reflect it within seconds of integration load.
        target = self._evaluate_target_state()
        log.info("edge_poc: startup state evaluation → target=%s", target)

        # Special case: if the voltage check was inconclusive, _evaluate_target_state
        # returns _STATE_GRID_UP (fail-safe-up). We don't want to lie by painting
        # cool-white when we genuinely don't know the grid state. Distinguish by
        # checking whether ANY voltage entity is reporting a fresh numeric value.
        if not self._has_any_fresh_voltage_reading():
            log.warning(
                "edge_poc: startup: no fresh voltage readings yet — deferring"
                " initial paint until first voltage state-change event"
            )
            return

        await self._apply_now(target)

    def _has_any_fresh_voltage_reading(self) -> bool:
        """True iff at least one voltage entity has a fresh numeric reading.

        Used at startup to distinguish "grid is genuinely up" from "we just
        booted and entities haven't populated yet". In the latter case we
        defer the initial paint to the first state-change event — the lamp
        keeps whatever state the user left it in rather than getting falsely
        painted cool-white.
        """
        now = time.time()
        for eid in VOLTAGE_ENTITY_IDS:
            state_obj = self._hass.states.get(eid)
            if state_obj is None:
                continue
            raw = state_obj.state
            if raw in ("unavailable", "unknown", None):
                continue
            last_updated = getattr(state_obj, "last_updated", None)
            if last_updated is not None:
                try:
                    if hasattr(last_updated, "timestamp"):
                        lu_epoch = last_updated.timestamp()
                    else:
                        lu_epoch = float(last_updated)
                    if now - lu_epoch > STALE_WINDOW_S:
                        continue
                except (TypeError, ValueError):
                    pass
            try:
                float(raw)
            except (ValueError, TypeError):
                continue
            return True
        return False

    def stop(self) -> None:
        """Cancel pending tasks on integration unload."""
        self._cancel_pending()
        for unsub in self._unsub:
            try:
                unsub()
            except Exception as exc:  # noqa: BLE001
                log.warning("edge_poc: unsub failed: %s: %s", type(exc).__name__, exc)
        self._unsub.clear()


# --------------------------------------------------------------------------
# Service registration helpers (called by __init__.py)
# --------------------------------------------------------------------------


def register_services(hass, handler: EdgePocOutageHandler) -> None:
    """Register the three iems.edge_poc_outage_* services.

    All three delegate to handle_voltage_state_change so that the lamp
    always reflects ACTUAL grid state, not user fiction. The grid_off /
    grid_recovered names are kept for back-compat with existing user
    automations from v0.1.14 and earlier.
    """

    async def _handle_voltage_change(call) -> None:  # noqa: ARG001
        handler.handle_voltage_state_change()

    async def _handle_grid_off(call) -> None:  # noqa: ARG001
        handler.on_grid_off()

    async def _handle_grid_recovered(call) -> None:  # noqa: ARG001
        handler.on_grid_recovered()

    hass.services.async_register(
        "iems", "edge_poc_outage_voltage_change", _handle_voltage_change
    )
    hass.services.async_register(
        "iems", "edge_poc_outage_grid_off", _handle_grid_off
    )
    hass.services.async_register(
        "iems", "edge_poc_outage_grid_recovered", _handle_grid_recovered
    )
    log.debug(
        "edge_poc: services registered: iems.edge_poc_outage_{voltage_change,grid_off,grid_recovered}"
    )


def resolve_db_path(hass) -> str:
    """Resolve absolute path for the PoC SQLite log file.

    Uses hass.config.path() if available (real HA); falls back to a
    path relative to the HA config dir environment variable or /tmp for
    test environments.
    """
    config_path_fn = getattr(hass.config, "path", None)
    if callable(config_path_fn):
        return config_path_fn(SQLITE_FILENAME)
    import os
    config_dir = os.environ.get("HASS_CONFIG", "/tmp")
    return str(Path(config_dir) / SQLITE_FILENAME)
