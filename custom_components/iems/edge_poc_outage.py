"""Edge PoC — outage-signal → light.living_lamp blue (outage signal).

Sprint 5 Track B. Single-site PoC (Mansoor's home). Zero cloud round-trip.

Trigger: ALL four grid-voltage sensors simultaneously NULL or < 50V (AND rule).
This mirrors the production-tested canonical detector in
solarman_bridge/manager.py:_check_grid_status.
DO NOT use binary_sensor.inverter[12]_grid — those flap on Solarman polling
glitches (3 confirmed false positives across 14 days, substs held 210-216V).
See: docs/architecture/canonical_signals.md §Grid availability.

When grid loss is confirmed (4-of-4 NULL/<50V, ≥60s sustained):
  1. Capture current lamp state to HA Storage (survives HA restart).
  2. Apply blue (xy_color [0.1532, 0.0475], brightness 200) within ≤2s.
  3. When ALL 4 voltages ≥ 50V for ≥60s, restore prior lamp state.
  4. Log fire/restore events + latency to local SQLite.

Color directive (CEO 2026-05-01): outage lamp is BLUE — not amber, not crimson.
xy_color [0.1532, 0.0475] is the Philips Hue published Gamut C blue corner
(highest-saturation deep blue physically realisable on Hue color lamps).
Bilal verifies against the lamp's reported gamut on first deploy.

Detection helper: _grid_is_down(states)
  Returns True  iff EVERY listed inverter's voltage is missing/None or < 50V.
  Returns False if ANY voltage is ≥ 50V.
  Returns False (inconclusive, fail-safe "grid up") when < 4 inverters have
             reported within the last 60s — the canonical AND-of-all rule
             requires ALL FOUR fresh; if any one is stale we cannot confirm.
             CEO directive 2026-05-01 (tightened from < 3).

Phasing: CEO-scoped single-site PoC. NOT a commercial CONTROL ship.
Sprint 7 CONTROL gate does not apply. No rollout until:
  (a) 14-day burn-in verdict positive
  (b) VISUAL_INDICATOR sub-class taxonomised by Ilya v0.2.0
  (c) User-data implications reviewed (onboarding_privacy_gdpr.md)

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

# Debounce windows (seconds) — 60s/60s per CEO directive 2026-04-29.
# Analysis: all 3 confirmed false positives lasted 37–38s; real outages start
# at ≥6 min. 60s sustained-NULL across all 4 inverters = unambiguous outage.
GRID_OFF_DEBOUNCE_S: float = 60.0
GRID_ON_DEBOUNCE_S: float = 60.0

# HA Storage key for captured lamp state
STORAGE_KEY = "iems_poc_lamp_state"
STORAGE_VERSION = 1

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
                # HA returns a datetime; convert to epoch for comparison.
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
# Lamp state capture / apply / restore
# --------------------------------------------------------------------------


async def capture_lamp_state(hass, entity_id: str) -> dict[str, Any]:
    """Read current lamp state from HA state machine.

    Returns a dict with all color fields (whichever are populated by HA).
    Persists the dict to HA Storage so it survives HA restarts.

    Returns a minimal dict with state="off" if lamp is off or unavailable.
    """
    state_obj = hass.states.get(entity_id)
    if state_obj is None:
        log.warning("edge_poc: capture_lamp_state: entity %s not found", entity_id)
        return {"state": "off", "captured_at": _utc_now_z()}

    attrs = dict(state_obj.attributes) if state_obj.attributes else {}
    captured: dict[str, Any] = {
        "state": state_obj.state,
        "brightness": attrs.get("brightness"),
        "color_mode": attrs.get("color_mode"),
        "xy_color": attrs.get("xy_color"),
        "hs_color": attrs.get("hs_color"),
        "rgb_color": attrs.get("rgb_color"),
        "color_temp": attrs.get("color_temp"),
        "captured_at": _utc_now_z(),
    }

    # Persist to HA Storage (survives restarts)
    try:
        from homeassistant.helpers.storage import Store  # type: ignore[import]
        store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        await store.async_save(captured)
        log.debug("edge_poc: lamp state captured and persisted: state=%s", captured["state"])
    except (ImportError, Exception) as exc:  # noqa: BLE001 — storage failure is non-fatal
        log.warning("edge_poc: failed to persist lamp state to storage: %s: %s", type(exc).__name__, exc)

    return captured


async def apply_outage_color(hass, entity_id: str, prior_state: dict[str, Any]) -> None:
    """Turn lamp to the outage color (blue: xy_color [0.1532, 0.0475], brightness 200).

    CEO directive 2026-05-01: outage signal is BLUE (was amber Day 1-4).

    prior_state is expected to already be persisted by capture_lamp_state.
    This call does NOT re-persist — it only issues the service call.
    """
    log.info(
        "edge_poc: applying outage color (blue) to %s (prior state=%s brightness=%s)",
        entity_id, prior_state.get("state"), prior_state.get("brightness"),
    )
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


async def restore_state(hass, entity_id: str, prior_state: dict[str, Any]) -> None:
    """Restore lamp to its captured prior state.

    If prior state was "off", turns the lamp off.
    If prior state was "on", restores color + brightness using whichever
    color mode was captured (xy preferred, then hs, then rgb, then color_temp).
    """
    if prior_state.get("state") == "off":
        log.info("edge_poc: restoring %s to off (prior state was off)", entity_id)
        await hass.services.async_call(
            "light",
            "turn_off",
            {"entity_id": entity_id},
            blocking=True,
        )
        return

    service_data: dict[str, Any] = {"entity_id": entity_id, "transition": 0}
    if prior_state.get("brightness") is not None:
        service_data["brightness"] = prior_state["brightness"]

    # Apply whichever color representation was captured
    if prior_state.get("xy_color") is not None:
        service_data["xy_color"] = list(prior_state["xy_color"])
    elif prior_state.get("hs_color") is not None:
        service_data["hs_color"] = list(prior_state["hs_color"])
    elif prior_state.get("rgb_color") is not None:
        service_data["rgb_color"] = list(prior_state["rgb_color"])
    elif prior_state.get("color_temp") is not None:
        service_data["color_temp"] = prior_state["color_temp"]

    log.info(
        "edge_poc: restoring %s color_mode=%s brightness=%s",
        entity_id, prior_state.get("color_mode"), prior_state.get("brightness"),
    )
    await hass.services.async_call(
        "light",
        "turn_on",
        service_data,
        blocking=True,
    )


async def _load_persisted_state(hass) -> dict[str, Any] | None:
    """Load previously persisted lamp state from HA Storage.

    Returns None if storage key absent or JSON-malformed (Scenario C).
    """
    try:
        from homeassistant.helpers.storage import Store  # type: ignore[import]
        store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        data = await store.async_load()
        if data and isinstance(data, dict):
            return data
    except (ImportError, Exception) as exc:  # noqa: BLE001
        log.warning("edge_poc: failed to load persisted state: %s: %s", type(exc).__name__, exc)
    return None


async def _clear_persisted_state(hass) -> None:
    """Remove persisted lamp state after successful restore."""
    try:
        from homeassistant.helpers.storage import Store  # type: ignore[import]
        store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        await store.async_remove()
    except (ImportError, Exception) as exc:  # noqa: BLE001
        log.warning("edge_poc: failed to clear persisted state: %s: %s", type(exc).__name__, exc)


# --------------------------------------------------------------------------
# Handler
# --------------------------------------------------------------------------


class EdgePocOutageHandler:
    """Manages the outage → blue → restore lifecycle for the edge PoC.

    Wired into IemsCoordinator.start() via async_setup_entry.
    Subscribes to voltage state changes on all 4 VOLTAGE_ENTITY_IDS.
    On each state change, calls _grid_is_down() (AND-of-all check).

    Lifecycle:
      all 4 voltages NULL/<50V  →  [60s debounce]  →  capture + outage color (blue)
      all 4 voltages ≥50V       →  [60s debounce]  →  restore

    The external API (on_grid_off / on_grid_recovered) is preserved for
    service-call-based automation YAML wiring and unit tests.
    """

    def __init__(self, hass, db_path: str) -> None:
        self._hass = hass
        self._db_path = db_path
        self._amber_task: asyncio.Task | None = None
        self._restore_task: asyncio.Task | None = None
        self._unsub: list = []
        self._outage_active: bool = False
        self._prior_state: dict[str, Any] | None = None

    # ------------------------------------------------------------------ #
    # Public HA event entry-points (called from service handlers +        #
    # voltage state-change subscription)                                  #
    # ------------------------------------------------------------------ #

    def on_grid_off(self) -> None:
        """Called when AND-of-all voltage check concludes grid is down.

        Cancels any pending restore and schedules the outage-color action
        after GRID_OFF_DEBOUNCE_S seconds.
        """
        log.info("edge_poc: grid off detected — scheduling outage color (debounce=%.1fs)", GRID_OFF_DEBOUNCE_S)
        self._cancel_restore()
        self._schedule_amber()

    def on_grid_recovered(self) -> None:
        """Called when AND-of-all voltage check concludes grid is up.

        Cancels any pending outage-color action (if grid recovered before
        debounce elapsed) and schedules restore after GRID_ON_DEBOUNCE_S seconds.
        """
        log.info("edge_poc: grid recovered — scheduling restore (debounce=%.1fs)", GRID_ON_DEBOUNCE_S)
        self._cancel_amber()
        if self._outage_active:
            self._schedule_restore()

    def handle_voltage_state_change(self) -> None:
        """Called on any VOLTAGE_ENTITY_IDS state change event.

        Reads current state of all 4 voltage entities, runs _grid_is_down(),
        and delegates to on_grid_off() or on_grid_recovered() as appropriate.
        Only transitions are acted on (avoids re-scheduling on repeated same state).
        """
        voltage_states = [
            self._hass.states.get(eid) for eid in VOLTAGE_ENTITY_IDS
        ]
        grid_down = _grid_is_down(voltage_states)

        if grid_down and not self._outage_active and not self._amber_task:
            self.on_grid_off()
        elif not grid_down and self._outage_active and not self._restore_task:
            self.on_grid_recovered()
        elif not grid_down and not self._outage_active and self._amber_task:
            # Grid came back before outage-color debounce fired — cancel it.
            self._cancel_amber()

    # ------------------------------------------------------------------ #
    # Internal scheduling                                                  #
    # ------------------------------------------------------------------ #

    def _schedule_amber(self) -> None:
        # v0.1.14: Schedule via hass.async_create_task, NEVER loop.create_task.
        #
        # Python's asyncio docs warn that the event loop only keeps WEAK
        # references to tasks created via asyncio.create_task / loop.create_task.
        # A task that isn't strongly referenced "may get garbage collected at
        # any time, even before it's done."
        # https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
        #
        # Home Assistant's official guidance (Working with Async, developers.
        # home-assistant.io) is to use `hass.async_create_task` from integration
        # code: it registers the task on `hass._tasks` (strong ref), ties the
        # task lifecycle to integration setup/teardown, and uses eager_start so
        # the coroutine begins executing immediately rather than next loop tick.
        #
        # Why we cared: v0.1.13 used loop.create_task here. CTO end-to-end test
        # 2026-05-02 — service call → on_grid_off → _schedule_amber → 65 s
        # later the lamp had not changed. Direct REST `light.turn_on` with
        # the same xy/brightness fired the lamp instantly, proving the lamp
        # path was healthy and the task was being silently dropped en route.
        self._cancel_amber()
        self._amber_task = self._hass.async_create_task(self._debounced_amber())

    def _schedule_restore(self) -> None:
        # See _schedule_amber for full rationale. Same fix, same reason.
        self._cancel_restore()
        self._restore_task = self._hass.async_create_task(
            self._debounced_restore()
        )

    def _cancel_amber(self) -> None:
        if self._amber_task and not self._amber_task.done():
            self._amber_task.cancel()
            self._amber_task = None

    def _cancel_restore(self) -> None:
        if self._restore_task and not self._restore_task.done():
            self._restore_task.cancel()
            self._restore_task = None

    async def _debounced_amber(self) -> None:
        try:
            await asyncio.sleep(GRID_OFF_DEBOUNCE_S)
        except asyncio.CancelledError:
            log.debug("edge_poc: outage-color debounce cancelled (grid recovered before threshold)")
            return

        t0 = time.monotonic()
        prior: dict[str, Any] | None = None
        success = False
        try:
            prior = await capture_lamp_state(self._hass, LAMP_ENTITY_ID)
            self._prior_state = prior
            await apply_outage_color(self._hass, LAMP_ENTITY_ID, prior)
            self._outage_active = True
            success = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.error("edge_poc: outage-color apply failed: %s: %s", type(exc).__name__, exc)
        finally:
            latency_ms = int((time.monotonic() - t0) * 1000)
            log_event(self._db_path, "grid_off_fire", latency_ms, prior, success)

    async def _debounced_restore(self) -> None:
        try:
            await asyncio.sleep(GRID_ON_DEBOUNCE_S)
        except asyncio.CancelledError:
            log.debug("edge_poc: restore debounce cancelled (grid dropped again)")
            return

        self._outage_active = False
        t0 = time.monotonic()
        prior: dict[str, Any] | None = None
        success = False
        event_type = "grid_recovered_restore"
        try:
            # Use in-memory prior if available; fall back to HA Storage
            prior = self._prior_state
            if prior is None:
                prior = await _load_persisted_state(self._hass)

            if prior is None:
                # Scenario C: state lost — fail-safe off
                log.warning(
                    "edge_poc: prior state not found — turning lamp off (fail-safe)"
                )
                await self._hass.services.async_call(
                    "light", "turn_off",
                    {"entity_id": LAMP_ENTITY_ID}, blocking=True,
                )
                event_type = "restore_fail_no_prior_state"
                success = False
            else:
                await restore_state(self._hass, LAMP_ENTITY_ID, prior)
                await _clear_persisted_state(self._hass)
                self._prior_state = None
                success = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.error("edge_poc: restore failed: %s: %s", type(exc).__name__, exc)
            event_type = "restore_fail_lamp_error"
        finally:
            latency_ms = int((time.monotonic() - t0) * 1000)
            log_event(self._db_path, event_type, latency_ms, prior, success)

    # ------------------------------------------------------------------ #
    # Startup — re-arm if grid is already down (HA restart mid-outage)    #
    # ------------------------------------------------------------------ #

    async def async_start(self) -> None:
        """Subscribe to voltage state changes + check grid state on startup.

        v0.1.13 P0 fix (CEO directive 2026-05-02): subscribe to
        async_track_state_change_event for all 4 canonical voltage entities so
        the handler fires automatically on voltage transitions. v0.1.12 was
        deaf — it registered services but never wired a listener, so the only
        way to fire the lamp during an outage was a manual service call or a
        user-wired YAML automation. Neither is acceptable for one-tap install.

        Also handles Scenario A (HA restart mid-outage): all voltages already
        NULL/low, lamp already showing outage color, storage has prior state.
        We mark outage_active so restore fires when grid returns — no re-fire
        since lamp is already in the outage color.
        """
        # Auto-subscribe to voltage state-changes — this is the core fix.
        # Wrap the import + subscribe so that test environments without HA
        # installed still allow the rest of async_start to run (services and
        # the one-time grid check still work; only the auto-fire path is lost).
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
            # Test environment without HA installed — keep the handler usable
            # via registered services. Production HA will always have these.
            log.warning(
                "edge_poc: HA helpers unavailable (%s) — running in service-only"
                " mode; voltage auto-subscribe disabled",
                exc,
            )
        except Exception as exc:  # noqa: BLE001 — never let async_start kill setup
            log.error(
                "edge_poc: unexpected error wiring state-change subscription:"
                " %s: %s — falling back to service-only mode",
                type(exc).__name__,
                exc,
            )

        voltage_states = [
            self._hass.states.get(eid) for eid in VOLTAGE_ENTITY_IDS
        ]
        if _grid_is_down(voltage_states):
            log.info(
                "edge_poc: startup: all voltages NULL/<50V — marking outage active"
                " (HA restart mid-outage scenario)",
            )
            self._outage_active = True
            # Load persisted prior state if available
            self._prior_state = await _load_persisted_state(self._hass)

    def stop(self) -> None:
        """Cancel pending tasks on integration unload."""
        self._cancel_amber()
        self._cancel_restore()
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
    """Register iems.edge_poc_outage_voltage_change service.

    This service is called by the automation YAML on any voltage entity
    state change; the handler runs the AND-of-all check internally.

    The grid_off / grid_recovered services are kept for direct invocation
    from automation YAML (HA numeric_state below/above triggers) and tests.
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
    # Test / fallback
    import os
    config_dir = os.environ.get("HASS_CONFIG", "/tmp")
    return str(Path(config_dir) / SQLITE_FILENAME)
