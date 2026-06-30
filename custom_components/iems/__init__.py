"""iEMS HACS integration — cloud-push telemetry agent.

async_setup_entry responsibilities:
  1. Pull the API key from the config entry (stored by config_flow).
  2. Build the auth provider (IemsCloudAuthProvider in production).
  3. Fetch temporary IoT credentials via the auth provider.
  4. Build the MQTT adapter using those credentials + the server-
     provided iot_endpoint (NOT hardcoded).
  5. Build entity_index from HA's registries.
  6. Construct IemsCoordinator, subscribe to state_changed, start
     batch + heartbeat timers.
  7. (RETIRED 2026-06-28) The Sprint 5 Track B EdgePocOutageHandler lamp
     control is no longer wired here — the living-room lamp is now owned by a
     native HA automation, so HACS must never write to light.living_lamp. The
     handler module remains in the tree (guarded by LAMP_CONTROL_RETIRED) for
     reversibility. See the retirement note in async_setup_entry.
  8. Stash adapter/coordinator/publisher in hass.data for unload.

The auth provider is the ONLY entry point to cloud endpoint routing —
there are no `IOT_ENDPOINT` / `IOT_PORT` / `DEV_USER_ID` hardcodes
anywhere else in the codebase.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from .auth import (
    AuthExchangeError,
    IemsAuthProvider,
    IemsCloudAuthProvider,
    InvalidApiKey,
)
from .const import (
    CONF_API_KEY,
    CONF_IOT_ENDPOINT,
    CONF_REGION,
    CONF_USER_ID,
    DOMAIN,
    SETUP_CLOUD_OP_TIMEOUT_S,
    VERSION,
)

log = logging.getLogger("iems")

try:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant, callback
    from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
    from homeassistant.helpers import (
        area_registry as ar,
        device_registry as dr,
        entity_registry as er,
    )
    from homeassistant.helpers.event import (
        async_call_later,
        async_track_state_change_event,
    )
    from homeassistant.helpers.entity_registry import (
        EVENT_ENTITY_REGISTRY_UPDATED,
    )
    from homeassistant.components.automation import EVENT_AUTOMATION_RELOADED
    _HA_AVAILABLE = True
except ImportError:  # pragma: no cover - dev env
    _HA_AVAILABLE = False

from .automation_sync import AutomationChangeSync
from .command_handler import CommandHandler
from .config_flow import _is_legacy_unique_id
from .const import COMMAND_TOPIC_TEMPLATE
from .coordinator import IemsCoordinator
# Edge-PoC lamp control RETIRED (CEO 2026-06-28) — native HA automation owns
# light.living_lamp; HACS no longer starts the handler. Import left commented
# (not deleted) so the wiring is reversible in one place. To restore: uncomment
# this import, restore the three wiring lines in async_setup_entry, and flip
# edge_poc_outage.LAMP_CONTROL_RETIRED → False.
# from .edge_poc_outage import EdgePocOutageHandler, register_services, resolve_db_path
from .publisher import TelemetryPublisher
from .recovery import RecoveryManager
from .snapshot import SetupSnapshotManager, collect_setup_snapshot
from .status_client import HacsStatusClient


def _consumer_device_ids(er_reg, entities) -> frozenset:
    """Return the set of device_ids that have a colocated consumer binary sensor.

    A device is considered a consumer device (Ring doorbell, Aqara motion/smoke
    sensor, etc.) when it has at least one binary_sensor entity whose entity_id
    or device_class contains a consumer-safety keyword (motion, smoke, door,
    occupancy, window, tamper, vibration, moisture).  We never suppress energy
    storage (Deye, Pylontech) this way because those devices don't have
    colocated consumer safety sensors.

    N4 (2026-04-25): prevents Ring/Aqara device-battery entities from
    appearing as battery.soc in the cloud telemetry.
    """
    _CONSUMER_KEYWORDS = frozenset(
        {"motion", "smoke", "door", "occupancy", "window", "tamper", "vibration", "moisture"}
    )

    consumer_ids: set[str] = set()
    for ent in entities:
        if ent.domain != "binary_sensor" or not ent.device_id:
            continue
        dc = (ent.device_class or ent.original_device_class or "").lower()
        eid_lower = ent.entity_id.lower()
        if dc in _CONSUMER_KEYWORDS or any(kw in eid_lower for kw in _CONSUMER_KEYWORDS):
            consumer_ids.add(ent.device_id)
    return frozenset(consumer_ids)


def _build_entity_index(hass) -> dict[str, dict[str, Any]]:
    """Snapshot HA registries into a flat dict consulted on every event."""
    er_reg = er.async_get(hass)
    dr_reg = dr.async_get(hass)
    ar_reg = ar.async_get(hass)

    # N4: pre-compute the set of device_ids that carry consumer safety sensors
    # so we can mark their battery entities as consumer_device=True.
    consumer_dev_ids = _consumer_device_ids(er_reg, er_reg.entities.values())

    index: dict[str, dict[str, Any]] = {}
    for ent in er_reg.entities.values():
        device = dr_reg.async_get(ent.device_id) if ent.device_id else None
        area_id = ent.area_id or (device.area_id if device else None)
        area = ar_reg.async_get_area(area_id) if area_id else None
        brand = device.manufacturer if device else None

        # v0.5.7 — friendly_name from state attributes (Bug 1 fix).
        # For MQTT-discovery entities (e.g. MTronic, Solarman), ent.name and
        # ent.original_name are empty/None — the entity registry never receives
        # the human-friendly label; that label lives ONLY in the state
        # attributes under "friendly_name". The old fallback to ent.entity_id
        # shipped entity_registry entries with the cryptic id as the name
        # ("light.reserve" instead of "Lobby lamp"). Priority:
        #   1. state attributes "friendly_name" (the source HA itself uses)
        #   2. entity registry name (ent.name or ent.original_name)
        #   3. None — NEVER fall back to entity_id. A null name is correct for
        #      a truly-unnamed entity; the AI just won't match it by name.
        state_obj = hass.states.get(ent.entity_id)
        state_friendly_name = None
        if state_obj is not None:
            try:
                state_friendly_name = state_obj.attributes.get("friendly_name") or None
            except Exception:  # noqa: BLE001 — never kill the index on a bad state read
                pass
        friendly_name = state_friendly_name or ent.name or ent.original_name or None

        index[ent.entity_id] = {
            "platform": ent.platform,
            "domain": ent.domain,
            "device_class": ent.device_class or ent.original_device_class,
            "unit": ent.unit_of_measurement,
            "name": friendly_name,
            "area": area.name if area else None,
            "brand": brand,
            "consumer_device": bool(ent.device_id and ent.device_id in consumer_dev_ids),
        }
    return index


if _HA_AVAILABLE:

    async def async_setup_entry(hass: "HomeAssistant", entry: "ConfigEntry") -> bool:
        """Bootstrap the integration from a validated config entry.

        On auth failure we raise ConfigEntryAuthFailed so HA surfaces a
        "repair" button to the user that takes them back through config
        flow. On transient network errors we raise ConfigEntryNotReady
        so HA retries with backoff.
        """
        api_key = entry.data.get(CONF_API_KEY)
        if not api_key:
            # Should be impossible — config_flow enforces it.
            raise ConfigEntryAuthFailed("iEMS: no API key stored in config entry")

        log.info("iEMS %s: starting setup", VERSION)

        # Build the auth provider. Format-validates the key one more time
        # as a defense-in-depth; the real validity check happens at
        # credential exchange.
        try:
            auth: IemsAuthProvider = IemsCloudAuthProvider(api_key=api_key)
        except InvalidApiKey as exc:
            raise ConfigEntryAuthFailed(f"iEMS: API key format invalid: {exc}") from exc

        # First credential exchange. This is the integration's ONE
        # allowed outbound call during setup that can touch the cloud —
        # failure here blocks startup.
        #
        # v0.4.1 (2026-06-04) — BOOTSTRAP FIX: bound the whole exchange with an
        # explicit wait_for. The auth provider has internal HTTP + boto3
        # timeouts, but this belt-and-braces ceiling guarantees setup can't
        # wedge HA bootstrap on a partially-hung cloud call. A timeout surfaces
        # as ConfigEntryNotReady → HA retries with backoff and the install boots.
        try:
            creds = await asyncio.wait_for(
                auth.get_credentials(), timeout=SETUP_CLOUD_OP_TIMEOUT_S
            )
        except AuthExchangeError as exc:
            # Likely revoked or wrong key — user needs to re-enter.
            raise ConfigEntryAuthFailed(f"iEMS auth exchange failed: {exc}") from exc
        except (OSError, TimeoutError, asyncio.TimeoutError) as exc:
            # Network hiccup / cloud slow — HA will retry with backoff.
            raise ConfigEntryNotReady(f"iEMS cloud unreachable: {exc}") from exc

        log.info(
            "iems: auth OK; identity_id=%s... user_sub=%s... iot=%s region=%s",
            creds.identity_id[:16], creds.user_id[:8], creds.iot_endpoint, creds.region,
        )

        # v0.4.4 (2026-06-05) — REAUTH MIGRATION: proactively heal a legacy
        # config-entry unique_id while the stored key is still valid. Entries
        # created <=0.4.2 store a sha256(api_key)[:32] hash (or a derived UUID
        # on <=0.4.1); v0.4.3+ stores the resolved Cognito identity_id. We've
        # just proved the key is valid (the exchange above succeeded), so reuse
        # that SAME `creds.identity_id` — NO second network call — to migrate
        # the unique_id in place. This means a valid-key install never has to
        # hit the reauth flow's legacy branch at all; it self-heals on first
        # 0.4.4 boot. Non-fatal by construction: if the key were revoked, the
        # exchange above would already have raised ConfigEntryAuthFailed and we
        # never reach here — and the reauth flow's legacy branch is the safety
        # net for the revoked-key case (config_flow.async_step_reauth_confirm).
        if _is_legacy_unique_id(entry.unique_id):
            log.info(
                "iems: migrating legacy config-entry unique_id to account "
                "identity (v0.4.4 reauth-scheme heal)"
            )
            hass.config_entries.async_update_entry(
                entry, unique_id=creds.identity_id
            )

        # Build the MQTT adapter once we have real credentials.
        # NOTE: IotCorePublisher class currently lives in the monorepo
        # and assumes cert-based auth. A follow-up commit in THIS repo
        # will replace it with a SigV4-signed MQTT-over-WSS client using
        # the temp creds from `creds`. That class is the last thing we
        # wire after Priya's spec lands.
        from .iot_core import IotCorePublisher
        adapter = IotCorePublisher(auth_provider=auth)
        # v0.4.1 — bound the initial connect so a hung MQTT handshake can't
        # wedge bootstrap. adapter.connect() already times the awscrt connect
        # future at MQTT_CONNECT_TIMEOUT_SECONDS; this is the outer ceiling.
        try:
            await asyncio.wait_for(
                adapter.connect(), timeout=SETUP_CLOUD_OP_TIMEOUT_S
            )
        except (OSError, TimeoutError, asyncio.TimeoutError) as exc:
            raise ConfigEntryNotReady(
                f"iEMS IoT Core connect timed out: {exc}"
            ) from exc

        # Registry snapshot
        entity_index = _build_entity_index(hass)
        log.info("iems: indexed %d entities", len(entity_index))

        publisher = TelemetryPublisher(
            user_id=creds.identity_id,
            publish_fn=adapter.publish,
        )
        coordinator = IemsCoordinator(
            hass=hass,
            user_id=creds.identity_id,
            entity_index=entity_index,
            publisher=publisher,
        )

        @callback
        def _state_changed(event) -> None:
            coordinator.capture_state_change(event.data.get("new_state"))

        unsub = async_track_state_change_event(
            hass,
            list(entity_index.keys()),
            _state_changed,
        )
        coordinator._unsub_state = unsub

        # v0.4.1 — pass `entry` so the batch + heartbeat loops are scheduled as
        # BACKGROUND tasks (entry.async_create_background_task), which HA does
        # NOT await at bootstrap. The 0.4.0 prod incident logged "Setup timed
        # out for bootstrap waiting on" these loops because they were foreground
        # tasks (hass.async_create_task).
        await coordinator.start(entry)

        # Onboarding v2 (#4, ADR 0005) — setup snapshot.
        # The snapshot is the ONE payload that flows pre-confirmation. We
        # publish it once on first install (and once per take_setup_snapshot
        # command) on the dedicated `iems/{user_id}/setup` topic. It is NOT a
        # telemetry batch — no 30s stream side effect.
        #
        # NOTE: shipping-mode gating of the 30s telemetry path (setup/paused →
        # no telemetry batches) is a separate slice the CTO sequences after
        # this one; this dispatch only wires the snapshot publish path.
        def _collect(source_kind: str):
            # Returns a coroutine; SetupSnapshotManager awaits awaitable
            # collectors (see _collect_snapshot).
            #
            # v0.5.9 — REBUILD the entity index from HA's CURRENT registry on
            # every snapshot. The boot-time `entity_index` (built at setup, line
            # ~247) misses entities that register AFTER this integration starts —
            # e.g. MTronic switches (switch.sp_*) that load progressively. A
            # rescan (take_setup_snapshot) that replayed the boot index would
            # silently drop them, so "Scan for new devices" never picked up a
            # late-loading or newly-added device, and the AI builder couldn't
            # resolve them by name (e.g. "study lamp" → switch.sp_146; first-run
            # incident 2026-06-30). Re-reading the registry is a cheap in-memory
            # walk. The snapshot's entity_classifications[] + entity_registry[]
            # are both derived from this index, so both go current.
            fresh_index = _build_entity_index(hass)
            return collect_setup_snapshot(
                hass,
                user_id=creds.identity_id,
                source_kind=source_kind,
                entity_index=fresh_index,
            )

        snapshot_manager = SetupSnapshotManager(
            publisher=publisher,
            collect=_collect,
        )

        # v0.5.11 — auto-sync the setup snapshot when HA automations change
        # OUT-OF-BAND (user edits/deletes an automation directly in HA, not via
        # the iEMS card). The card reads the cached setup_snapshot.automations;
        # before this, a user-side change wasn't reflected until a rescan or
        # restart. We listen on HA's two automation-change signals and fire a
        # DEBOUNCED re-snapshot (coalesces a burst into one publish). The
        # debounce timer + task scheduler are injected so the core is HA-free
        # and unit-tested. `suppress()` is handed to the command handler so our
        # OWN write/delete reload doesn't double-fire (we already re-snapshot in
        # _resnapshot_after_apply).
        from functools import partial as _partial

        automation_sync = AutomationChangeSync(
            trigger=snapshot_manager.handle_take_setup_snapshot_command,
            schedule_later=_partial(async_call_later, hass),
        )

        # Onboarding v2 (#9, ADR 0005) — shipping-mode command channel.
        # Subscribe to the cloud→HACS command down-topic (QoS 1, persistent
        # session) so the cloud can flip shipping_mode + reconcile the
        # whitelist + trigger a rescan snapshot.  The coordinator's flush()
        # gates the 30s telemetry path on shipping_mode; first install starts
        # in `setup` so NO telemetry flows until the cloud commands `active`
        # after the user confirms the site_model in the wizard.
        # Data-recovery (#?, Sprint 7) — in-process recorder replay for a gap
        # window.  The cloud sends `recover_window` on the SAME command
        # down-topic; HACS queries HA's local recorder, replays the found rows
        # through the publisher, and acks the truth on the heartbeat
        # (coordinator.set_last_recovery → build_heartbeat last_recovery).  Runs
        # off the steady-state telemetry path (recorder executor thread); no new
        # MQTT topic, no IAM change.
        recovery_manager = RecoveryManager(
            hass=hass,
            user_id=creds.identity_id,
            entity_index=entity_index,
            publisher=publisher,
            set_last_recovery=coordinator.set_last_recovery,
        )
        command_handler = CommandHandler(
            coordinator=coordinator,
            snapshot_manager=snapshot_manager,
            recovery_manager=recovery_manager,
            # Devices Rename (contracts/mqtt_topics.md v0.4.0) — the
            # rename_device action applies a label-only device-registry write
            # IN-PROCESS, so the handler needs the HA instance.
            hass=hass,
            # v0.5.11 — open the auto-sync suppression window around our own
            # automation write/delete so the reload WE fire doesn't trigger a
            # second (redundant) snapshot via the out-of-band listener.
            on_self_apply=automation_sync.suppress,
        )

        # Register the two HA automation-change listeners. Each returns an
        # unsub callable; stashed for cleanup on unload. These are pure event-
        # bus subscriptions (no network, no telemetry side effect).
        @callback
        def _on_registry_event(event) -> None:
            automation_sync.handle_registry_event(
                event.data.get("action"), event.data.get("entity_id")
            )

        @callback
        def _on_automation_reloaded(event) -> None:
            automation_sync.handle_reload_event()

        _unsub_reg = hass.bus.async_listen(
            EVENT_ENTITY_REGISTRY_UPDATED, _on_registry_event
        )
        _unsub_reload = hass.bus.async_listen(
            EVENT_AUTOMATION_RELOADED, _on_automation_reloaded
        )
        status_client = HacsStatusClient(api_key=api_key)
        command_topic = COMMAND_TOPIC_TEMPLATE.format(user_id=creds.identity_id)

        async def _reconcile_on_resume() -> None:
            """Reconnect safety net (#9): resubscribe + pull /hacs/status.

            Fired on every broker resume.  Re-issues the command-topic
            subscription (the persistent session may have expired while
            offline) and pulls /hacs/status once to reconcile the local
            shipping_mode to the cloud's truth — covers the case where the
            cloud commanded a transition while HACS was offline and the
            queued MQTT command was dropped by session expiry.
            """
            try:
                await adapter.resubscribe_all()
            except (OSError, TimeoutError) as exc:
                log.warning(
                    "iems: resubscribe-on-resume failed: %s: %s",
                    type(exc).__name__, exc,
                )
            status = await status_client.fetch_status()
            if status is not None:
                coordinator.reconcile_from_status(status)

        adapter.set_on_resume(_reconcile_on_resume)

        # v0.4.1 (2026-06-04) — BOOTSTRAP FIX: the post-connect onboarding
        # network work (first-install snapshot publish, command-topic subscribe,
        # startup /hacs/status reconcile) is the cluster that wedged 0.4.0
        # bootstrap on prod. None of it gates whether the integration can run —
        # telemetry defaults safe (`setup` = no telemetry until the cloud
        # commands `active`), the resume hook re-issues the subscribe on the
        # next reconnect, and a missed reconcile recovers on the next status
        # pull. So it MUST NOT block async_setup_entry. We defer all three to a
        # single background task that HA does NOT await at bootstrap.
        #
        # Onboarding behavior is fully preserved — the snapshot still publishes,
        # the command topic is still subscribed, and status still reconciles —
        # they just happen a beat after setup returns rather than blocking it.
        async def _deferred_onboarding_wiring() -> None:
            # First-install setup snapshot. Failure is non-fatal — the manager
            # logs + leaves the one-off guard unset so a later setup retry
            # re-fires. Bounded so a hung publish can't pin this task forever.
            try:
                await asyncio.wait_for(
                    snapshot_manager.publish_on_first_install(),
                    timeout=SETUP_CLOUD_OP_TIMEOUT_S,
                )
            except (OSError, TimeoutError, asyncio.TimeoutError, ValueError) as exc:
                log.warning(
                    "iems: first-install setup snapshot failed (non-fatal): %s: %s",
                    type(exc).__name__, exc,
                )

            # Initial command-topic subscribe. Non-fatal on failure — the resume
            # hook re-issues it on the next reconnect, and telemetry gating
            # defaults safe (`setup` = no telemetry) so a missed command can't
            # leak data.
            try:
                await asyncio.wait_for(
                    adapter.subscribe(
                        topic=command_topic,
                        qos=1,
                        message_handler=command_handler.on_message,
                    ),
                    timeout=SETUP_CLOUD_OP_TIMEOUT_S,
                )
            except (OSError, TimeoutError, asyncio.TimeoutError) as exc:
                log.warning(
                    "iems: command-topic subscribe failed (will retry on "
                    "resume): %s: %s",
                    type(exc).__name__, exc,
                )

            # Initial reconcile — pull /hacs/status once so a HACS restart
            # (config-entry reload) adopts the cloud's current mode rather than
            # resetting to the `setup` default. Non-fatal; degrades to the
            # default mode if the endpoint is unavailable. status_client already
            # times out internally and returns None on any failure.
            try:
                startup_status = await status_client.fetch_status()
                if startup_status is not None:
                    coordinator.reconcile_from_status(startup_status)
            except (OSError, TimeoutError, asyncio.TimeoutError) as exc:
                log.warning(
                    "iems: startup /hacs/status pull failed (non-fatal): %s: %s",
                    type(exc).__name__, exc,
                )

        # Schedule as a BACKGROUND task (HA does not await it at bootstrap).
        # Fallback ladder mirrors coordinator.start for older cores / safety.
        _bg = getattr(entry, "async_create_background_task", None)
        if callable(_bg):
            _bg(hass, _deferred_onboarding_wiring(), name="iems_onboarding_wiring")
        else:  # pragma: no cover — HA core too old for background tasks
            hass.async_create_task(_deferred_onboarding_wiring())

        # Sprint 5 Track B — Edge PoC lamp control: RETIRED (CEO 2026-06-28).
        #
        # The living-room lamp's grid-state control moved OFF this HACS edge-PoC
        # and ONTO a native Home Assistant automation (CTO-authored) so the lamp
        # has exactly ONE owner. HACS must NEVER write to light.living_lamp —
        # neither cool-white on grid-up nor blue on grid-down — otherwise the two
        # owners fight (the edge-PoC repaints cool-white on grid-up while the
        # native automation wants the lamp left alone except during an outage).
        #
        # We therefore no longer start the handler or register its services. The
        # EdgePocOutageHandler module (edge_poc_outage.py) is left intact and is
        # additionally guarded by its LAMP_CONTROL_RETIRED kill-switch, so this
        # is fully reversible: restore the three lines below AND flip
        # LAMP_CONTROL_RETIRED → False to bring the lamp indicator back.
        #
        #   db_path = resolve_db_path(hass)
        #   edge_poc = EdgePocOutageHandler(hass=hass, db_path=db_path)
        #   register_services(hass, edge_poc)
        #   await edge_poc.async_start()
        #
        # Everything else HACS does (telemetry, heartbeat, setup snapshot,
        # dispatch, recovery) is unaffected.
        # See: docs/integrations/edge_poc_outage_color.md
        log.info(
            "iems: edge-poc lamp control retired (CEO 2026-06-28) — native HA "
            "automation owns light.living_lamp; HACS does not touch it"
        )

        hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
            "coordinator": coordinator,
            "adapter": adapter,
            "publisher": publisher,
            "auth": auth,
            # Onboarding v2 (#4) — kept so the take_setup_snapshot command
            # handler can re-publish a rescan snapshot.
            "snapshot_manager": snapshot_manager,
            # Onboarding v2 (#9) — shipping-mode command channel.
            "command_handler": command_handler,
            "status_client": status_client,
            # Data-recovery (Sprint 7) — recover_window recorder replay.
            "recovery_manager": recovery_manager,
            # v0.5.11 — out-of-band automation-change auto-sync + its two event
            # listeners' unsub callables, cleaned up on unload.
            "automation_sync": automation_sync,
            "automation_sync_unsubs": [_unsub_reg, _unsub_reload],
        }
        return True

    async def async_unload_entry(hass: "HomeAssistant", entry: "ConfigEntry") -> bool:
        record = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
        if not record:
            return True
        coord = record.get("coordinator")
        adapter = record.get("adapter")
        auth = record.get("auth")
        edge_poc = record.get("edge_poc")
        # v0.5.11 — tear down the automation-change listeners + cancel any
        # pending debounce timer so a config-entry reload doesn't leak them.
        for unsub in record.get("automation_sync_unsubs", []) or []:
            try:
                unsub()
            except Exception as exc:  # noqa: BLE001 — unload must not raise
                log.warning(
                    "iems: automation_sync unsub failed: %s: %s",
                    type(exc).__name__, exc,
                )
        sync = record.get("automation_sync")
        if sync is not None:
            sync.cancel()
        if edge_poc:
            edge_poc.stop()
        if coord:
            await coord.stop()
        if adapter:
            await adapter.disconnect()
        if auth:
            await auth.close()
        return True


__all__ = ["DOMAIN", "VERSION"]
