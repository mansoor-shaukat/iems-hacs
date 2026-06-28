"""Cloud-command handler (#9, ADR 0005) — the command down-topic dispatch seam.

The cloud pushes shipping-mode + snapshot commands to `iems/{user_id}/command`
(contracts/mqtt_topics.md §command).  This module is the seam between the awscrt
subscribe callback and the coordinator/snapshot FSM:

    {"action": "set_shipping_mode", "mode": "...", "whitelist?": [...],
     "whitelist_version?": int}
        -> coordinator.set_shipping_mode(mode, whitelist=..., whitelist_version=...)

    {"action": "take_setup_snapshot"}
        -> snapshot_manager.handle_take_setup_snapshot_command()   (any mode)

    {"action": "recover_window", "window_id": "...", "start_ts": "...Z",
     "end_ts": "...Z"}
        -> recovery_manager.recover_window(window_id=..., start_ts=...,
                                           end_ts=...)              (any mode)

    {"action": "rename_device", "device_id": "<ha_device_id>",
     "name_by_user": "<new name>"}
        -> device_registry.async_update_device(device_id,
                                               name_by_user=name)  (any mode)

    {"action": "enable_automation", "id": "<ha_automation_id>",
     "enabled": true|false}
        -> hass.services.call("automation", "turn_on"|"turn_off",
                              {"entity_id": <resolved_entity_id>},
                              blocking=True)                        (any mode)

`rename_device` (contracts/mqtt_topics.md v0.4.0) is the FIRST iEMS write INTO
HA.  HACS runs inside HA, so it applies the label change in-process via the
device-registry helper — no external WS round-trip.  It is label-only and
reversible: it changes the user-visible device name (`name_by_user`) and NEVER
touches entity_ids.  Requires `hass` to be wired into the handler; when it
isn't, the command logs + drops as un-dispatchable (callback never crashes).

`enable_automation` (contracts/mqtt_topics.md v0.4.1) is the SECOND iEMS write
INTO HA.  It toggles a named automation on or off using HA's automation service
calls (automation.turn_on / automation.turn_off) applied IN-PROCESS — no
external WS round-trip.  The command carries the automation stable `id` (NOT
the entity_id slug); HACS resolves id→entity_id by scanning the automation
EntityComponent in hass.data['automation'] (the same source _extract_automations
in snapshot.py uses — unique_id on each automation entity equals its stable id).
If no automation matches the id, the command is logged and dropped as
automation_not_found; the subscribe callback never crashes.

Design — pure decode + thin dispatch
------------------------------------
`decode_command` is a pure bytes/str → dict parser (raises InvalidCommandError
on anything that isn't a JSON object).  `CommandHandler.handle_command` is the
typed dispatch (raises InvalidCommandError on a malformed/unknown command).
`CommandHandler.on_message` is the awscrt-callback-facing wrapper: it decodes +
dispatches and NEVER raises — a malformed command logged-and-dropped must not
kill the subscribe callback (an exception out of an awscrt callback is silently
swallowed by the threadpool anyway, so we log it ourselves for visibility).
"""
from __future__ import annotations

import json
import logging
from typing import Any

from .const import (
    COMMAND_ACTION_ENABLE_AUTOMATION,
    COMMAND_ACTION_RECOVER_WINDOW,
    COMMAND_ACTION_RENAME_DEVICE,
    COMMAND_ACTION_SET_SHIPPING_MODE,
    COMMAND_ACTION_TAKE_SETUP_SNAPSHOT,
)

log = logging.getLogger("iems.command_handler")


class InvalidCommandError(ValueError):
    """Raised when a command payload is malformed, unknown, or un-dispatchable."""


def decode_command(raw: bytes | str | dict) -> dict[str, Any]:
    """Decode a raw MQTT command payload to a command dict.

    Accepts bytes (the awscrt payload), a str, or an already-decoded dict.
    Raises InvalidCommandError when the payload is not valid JSON or is valid
    JSON but not a JSON object (e.g. an array or a bare scalar).
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidCommandError(f"command payload is not UTF-8: {exc}") from exc
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise InvalidCommandError(f"command payload is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise InvalidCommandError(
            f"command payload must be a JSON object, got {type(obj).__name__}"
        )
    return obj


class CommandHandler:
    """Dispatches decoded cloud commands to the coordinator / snapshot manager.

    Dependencies are injected so tests run with plain MagicMock/AsyncMock
    stand-ins:
      - `coordinator` — exposes `set_shipping_mode(mode, *, whitelist,
        whitelist_version)` (sync).
      - `snapshot_manager` — exposes `async handle_take_setup_snapshot_command()`.
      - `recovery_manager` — optional; exposes
        `async recover_window(*, window_id, start_ts, end_ts)`.  None when the
        recovery feature isn't wired (a `recover_window` command then logs +
        drops as un-dispatchable, never crashing the callback).
      - `hass` — optional HomeAssistant instance.  Needed by `rename_device`
        (device-registry write) and `enable_automation` (automation service
        call).  None when the handler isn't wired with HA (those commands then
        log + drop as un-dispatchable, never crashing the callback).
    """

    def __init__(
        self,
        *,
        coordinator,
        snapshot_manager,
        recovery_manager=None,
        hass=None,
    ) -> None:
        self._coordinator = coordinator
        self._snapshot_manager = snapshot_manager
        self._recovery_manager = recovery_manager
        self._hass = hass

    async def handle_command(self, command: dict[str, Any]) -> None:
        """Dispatch a decoded command dict. Raises InvalidCommandError on error."""
        action = command.get("action")
        if action == COMMAND_ACTION_SET_SHIPPING_MODE:
            await self._handle_set_shipping_mode(command)
        elif action == COMMAND_ACTION_TAKE_SETUP_SNAPSHOT:
            await self._handle_take_setup_snapshot(command)
        elif action == COMMAND_ACTION_RECOVER_WINDOW:
            await self._handle_recover_window(command)
        elif action == COMMAND_ACTION_RENAME_DEVICE:
            await self._handle_rename_device(command)
        elif action == COMMAND_ACTION_ENABLE_AUTOMATION:
            await self._handle_enable_automation(command)
        elif action is None:
            raise InvalidCommandError("command missing 'action'")
        else:
            raise InvalidCommandError(f"unknown command action {action!r}")

    async def _handle_set_shipping_mode(self, command: dict[str, Any]) -> None:
        mode = command.get("mode")
        if not mode:
            raise InvalidCommandError("set_shipping_mode missing 'mode'")
        whitelist = command.get("whitelist")
        whitelist_version = command.get("whitelist_version")
        try:
            # coordinator.set_shipping_mode is sync (mutates in-memory state).
            self._coordinator.set_shipping_mode(
                mode, whitelist=whitelist, whitelist_version=whitelist_version
            )
        except ValueError as exc:
            # Unknown mode from the coordinator's own validation — re-wrap so
            # the callback path treats every bad command uniformly.
            raise InvalidCommandError(str(exc)) from exc

    async def _handle_take_setup_snapshot(self, command: dict[str, Any]) -> None:
        # take_setup_snapshot fires regardless of the current shipping mode
        # (AC #3): the snapshot is the one payload that flows pre-confirmation,
        # and a user-triggered re-scan must work even after going active.
        await self._snapshot_manager.handle_take_setup_snapshot_command()

    async def _handle_recover_window(self, command: dict[str, Any]) -> None:
        """Dispatch a `recover_window` command to the recovery manager.

        Validates the three required string fields BEFORE touching the
        recorder.  A missing/blank field raises InvalidCommandError so the
        on_message wrapper logs + drops it — the recover action never crashes
        the callback.

        The recover itself runs OFF the steady-state telemetry path: it queries
        HA's recorder on the recorder executor thread (never the event loop) and
        replays through the publisher.  We AWAIT the manager here — the manager's
        own internal work is non-blocking (executor-offloaded), and the manager
        is contractually no-raise (it captures every failure as an `error` ack),
        so awaiting it cannot break the callback invariant.
        """
        if self._recovery_manager is None:
            raise InvalidCommandError(
                "recover_window received but no recovery_manager is wired"
            )
        window_id = command.get("window_id")
        start_ts = command.get("start_ts")
        end_ts = command.get("end_ts")
        for field, value in (
            ("window_id", window_id),
            ("start_ts", start_ts),
            ("end_ts", end_ts),
        ):
            if not isinstance(value, str) or not value.strip():
                raise InvalidCommandError(
                    f"recover_window missing/invalid {field!r}"
                )
        await self._recovery_manager.recover_window(
            window_id=window_id, start_ts=start_ts, end_ts=end_ts
        )
        # v0.4.7: fire an IMMEDIATE heartbeat so the `last_recovery` ack reaches
        # the cloud in seconds, not up to HEARTBEAT_INTERVAL_SECONDS (5 min)
        # later on the next scheduled tick. The recover result the user is
        # watching for rides the heartbeat; the 5-min cadence made the portal's
        # "Checking…" feel stuck. Best-effort — a heartbeat failure here must
        # never break the command callback invariant (logged + swallowed).
        hb = getattr(self._coordinator, "heartbeat_once", None)
        if callable(hb):
            try:
                await hb()
            except Exception as exc:  # noqa: BLE001 — never break the callback
                log.warning(
                    "recover_window: immediate heartbeat failed: %s: %s",
                    type(exc).__name__, exc,
                )

    async def _handle_rename_device(self, command: dict[str, Any]) -> None:
        """Apply a `rename_device` command IN-PROCESS via HA's device registry.

        This is the FIRST iEMS write INTO HA (contracts/mqtt_topics.md v0.4.0).
        HACS runs inside HA, so there is no external WS round-trip — we call the
        device-registry helper directly.  The write is label-only and
        reversible: it sets the user-facing device name (`name_by_user`) and
        NEVER touches entity_ids.

        Validation is strict BEFORE the write: `device_id` and `name_by_user`
        must both be non-empty strings, else InvalidCommandError → the
        on_message wrapper logs + drops it (the callback never crashes).

        An UNKNOWN device_id (the registry raises / has no such device) is also
        logged + dropped, never propagated — a stale cloud-side device list must
        not be able to kill the subscribe callback.
        """
        device_id = command.get("device_id")
        name = command.get("name_by_user")
        for field, value in (("device_id", device_id), ("name_by_user", name)):
            if not isinstance(value, str) or not value.strip():
                raise InvalidCommandError(
                    f"rename_device missing/invalid {field!r}"
                )
        if self._hass is None:
            raise InvalidCommandError(
                "rename_device received but no hass is wired"
            )
        # Local import — homeassistant is only importable inside a running HA.
        from homeassistant.helpers import device_registry as dr

        registry = dr.async_get(self._hass)
        try:
            # async_update_device is SYNCHRONOUS (mutates the in-memory registry
            # + schedules a debounced save).  name_by_user is the ONLY field we
            # touch — entity_ids are never passed, so they are untouched.
            registry.async_update_device(device_id, name_by_user=name)
        except Exception as exc:  # noqa: BLE001 — see below
            # An unknown device_id makes HA raise: depending on the HA version
            # that is KeyError or HomeAssistantError (NOT a stable, importable
            # ValueError subclass we can name without importing HA).  We catch
            # broadly and re-wrap as InvalidCommandError so handle_command
            # ALWAYS surfaces a registry-side failure uniformly (logged + dropped
            # by on_message), and the subscribe callback can never crash on a
            # stale cloud-side device list.
            raise InvalidCommandError(
                f"rename_device could not update {device_id!r}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        log.info(
            "rename_device: set name_by_user for device %s", device_id
        )

    async def _handle_enable_automation(self, command: dict[str, Any]) -> None:
        """Toggle an automation on/off IN-PROCESS via HA's automation service.

        This is the SECOND iEMS write INTO HA (contracts/mqtt_topics.md v0.4.1).
        HACS runs inside HA so the service call executes in-process — no
        external WS round-trip required.  The write is state-only and
        reversible: it only changes whether the automation runs; it NEVER
        touches entity_ids or automation config.

        id → entity_id resolution
        -------------------------
        The command carries the automation's STABLE ID (e.g. "1749454742695"
        or "iems_grid_outage_lamp") which HA stores as the automation entity's
        `unique_id`.  We resolve it by scanning the automation EntityComponent
        at hass.data['automation'].entities — the SAME in-process source that
        snapshot.py:_extract_automations uses.  We MUST NOT assume the
        entity_id is "automation.<id>" — that mapping is not guaranteed by HA.

        If the id matches no automation (stale cloud-side list, deleted
        automation, or no automations configured), we raise InvalidCommandError
        with "automation_not_found".  The on_message wrapper logs + drops it —
        a stale cloud entry MUST NOT kill the subscribe callback.

        Validation is strict BEFORE the service call:
          - 'id' must be a non-empty string.
          - 'enabled' must be a Python bool (True or False) — JSON strings
            like "true", integers like 1/0, and None are ALL rejected.
          - hass must be wired.
        """
        automation_id = command.get("id")
        enabled = command.get("enabled")

        if not isinstance(automation_id, str) or not automation_id.strip():
            raise InvalidCommandError(
                "enable_automation missing/invalid 'id'"
            )
        # enabled MUST be a literal bool — reject ints, strings, None.
        # isinstance(True, int) is True in Python, so we must check bool first.
        if not isinstance(enabled, bool):
            raise InvalidCommandError(
                f"enable_automation 'enabled' must be a bool, got {type(enabled).__name__!r}"
            )
        if self._hass is None:
            raise InvalidCommandError(
                "enable_automation received but no hass is wired"
            )

        # Resolve automation id → entity_id by scanning the EntityComponent.
        # unique_id on each HA automation entity equals its stable automation id
        # (as confirmed in snapshot.py:_extract_automations, line ~910-914).
        entity_id = self._resolve_automation_entity_id(automation_id)
        if entity_id is None:
            raise InvalidCommandError(
                f"automation_not_found: no automation with id {automation_id!r}"
            )

        service = "turn_on" if enabled else "turn_off"
        try:
            await self._hass.services.call(
                "automation",
                service,
                {"entity_id": entity_id},
                blocking=True,
            )
        except Exception as exc:  # noqa: BLE001 — HA service failures vary widely
            # Service errors (unknown entity, HA not ready, etc.) are wrapped so
            # the callback path handles them uniformly.  The subscribe callback
            # can never crash on a service-layer exception.
            raise InvalidCommandError(
                f"enable_automation: service call failed for {entity_id!r}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        log.info(
            "enable_automation: automation %r (%s) → %s",
            automation_id, entity_id, service,
        )

    def _resolve_automation_entity_id(self, automation_id: str) -> str | None:
        """Return the entity_id for the automation whose unique_id == automation_id.

        Scans hass.data['automation'].entities IN-PROCESS (same source as
        snapshot.py:_extract_automations — no WS round-trip).  Returns None
        when no match is found (missing component, no automations, or stale id).

        This method is factored out for testability — it reads only from self._hass
        and returns a plain str or None, so tests can mock hass.data independently.
        """
        component = (
            self._hass.data.get("automation")
            if hasattr(self._hass, "data")
            else None
        )
        entities = getattr(component, "entities", None)
        if not entities:
            return None
        for ent in entities:
            if getattr(ent, "unique_id", None) == automation_id:
                return getattr(ent, "entity_id", None)
        return None

    async def on_message(self, raw: bytes | str | dict) -> bool:
        """awscrt-callback-facing entry point: decode + dispatch, never raises.

        Returns True if the command was handled, False if it was decoded-or-
        dispatched into an error (logged, not raised).  A bad command must not
        propagate out of the subscribe callback.
        """
        try:
            command = decode_command(raw)
            await self.handle_command(command)
            return True
        except InvalidCommandError as exc:
            log.warning("dropping invalid cloud command: %s", exc)
            return False
        except Exception as exc:  # noqa: BLE001 — last-ditch: callback must survive
            log.error(
                "unexpected error handling cloud command: %s: %s",
                type(exc).__name__, exc,
            )
            return False
