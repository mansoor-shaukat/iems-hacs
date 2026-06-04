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
  7. Wire EdgePocOutageHandler (Sprint 5 Track B) — grid-off → outage color
     (blue per CEO directive 2026-05-01), grid-on → restore. Local-only,
     zero cloud round-trip.
  8. Stash adapter/coordinator/publisher/edge_poc_handler in hass.data
     for unload.

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
    from homeassistant.helpers.event import async_track_state_change_event
    _HA_AVAILABLE = True
except ImportError:  # pragma: no cover - dev env
    _HA_AVAILABLE = False

from .command_handler import CommandHandler
from .const import COMMAND_TOPIC_TEMPLATE
from .coordinator import IemsCoordinator
from .edge_poc_outage import EdgePocOutageHandler, register_services, resolve_db_path
from .publisher import TelemetryPublisher
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

        index[ent.entity_id] = {
            "platform": ent.platform,
            "domain": ent.domain,
            "device_class": ent.device_class or ent.original_device_class,
            "unit": ent.unit_of_measurement,
            "name": ent.name or ent.original_name or ent.entity_id,
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
        try:
            creds = await auth.get_credentials()
        except AuthExchangeError as exc:
            # Likely revoked or wrong key — user needs to re-enter.
            raise ConfigEntryAuthFailed(f"iEMS auth exchange failed: {exc}") from exc
        except (OSError, TimeoutError) as exc:
            # Network hiccup — HA will retry.
            raise ConfigEntryNotReady(f"iEMS cloud unreachable: {exc}") from exc

        log.info(
            "iems: auth OK; identity_id=%s... user_sub=%s... iot=%s region=%s",
            creds.identity_id[:16], creds.user_id[:8], creds.iot_endpoint, creds.region,
        )

        # Build the MQTT adapter once we have real credentials.
        # NOTE: IotCorePublisher class currently lives in the monorepo
        # and assumes cert-based auth. A follow-up commit in THIS repo
        # will replace it with a SigV4-signed MQTT-over-WSS client using
        # the temp creds from `creds`. That class is the last thing we
        # wire after Priya's spec lands.
        from .iot_core import IotCorePublisher
        adapter = IotCorePublisher(auth_provider=auth)
        await adapter.connect()

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

        await coordinator.start()

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
            return collect_setup_snapshot(
                hass, user_id=creds.identity_id, source_kind=source_kind,
            )

        snapshot_manager = SetupSnapshotManager(
            publisher=publisher,
            collect=_collect,
        )
        # Fire the first-install snapshot. Failure is non-fatal — the manager
        # logs + leaves the one-off guard unset so a later setup retry re-fires.
        try:
            await snapshot_manager.publish_on_first_install()
        except (OSError, TimeoutError, ValueError) as exc:
            log.warning(
                "iems: first-install setup snapshot failed (non-fatal): %s: %s",
                type(exc).__name__, exc,
            )

        # Onboarding v2 (#9, ADR 0005) — shipping-mode command channel.
        # Subscribe to the cloud→HACS command down-topic (QoS 1, persistent
        # session) so the cloud can flip shipping_mode + reconcile the
        # whitelist + trigger a rescan snapshot.  The coordinator's flush()
        # gates the 30s telemetry path on shipping_mode; first install starts
        # in `setup` so NO telemetry flows until the cloud commands `active`
        # after the user confirms the site_model in the wizard.
        command_handler = CommandHandler(
            coordinator=coordinator,
            snapshot_manager=snapshot_manager,
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

        # Initial subscribe.  Non-fatal on failure — the resume hook re-issues
        # it on the next reconnect, and telemetry gating defaults safe (`setup`
        # = no telemetry) so a missed command can't leak data.
        try:
            await adapter.subscribe(
                topic=command_topic,
                qos=1,
                message_handler=command_handler.on_message,
            )
        except (OSError, TimeoutError) as exc:
            log.warning(
                "iems: command-topic subscribe failed (will retry on resume): "
                "%s: %s",
                type(exc).__name__, exc,
            )

        # Initial reconcile — pull /hacs/status once at startup so a HACS
        # restart (config-entry reload) adopts the cloud's current mode rather
        # than resetting to the `setup` default.  Non-fatal; degrades to the
        # default mode if the endpoint is unavailable.
        try:
            startup_status = await status_client.fetch_status()
            if startup_status is not None:
                coordinator.reconcile_from_status(startup_status)
        except (OSError, TimeoutError) as exc:
            log.warning(
                "iems: startup /hacs/status pull failed (non-fatal): %s: %s",
                type(exc).__name__, exc,
            )

        # Sprint 5 Track B — Edge PoC: outage signal → light.living_lamp blue.
        # CEO directive 2026-05-01: blue (was amber Day 1-4).
        # Local-only (no cloud round-trip). Single-site PoC (Mansoor's home).
        # See: docs/integrations/edge_poc_outage_color.md
        db_path = resolve_db_path(hass)
        edge_poc = EdgePocOutageHandler(hass=hass, db_path=db_path)
        register_services(hass, edge_poc)
        await edge_poc.async_start()
        log.info("iems: edge-poc outage handler started; db=%s", db_path)

        hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
            "coordinator": coordinator,
            "adapter": adapter,
            "publisher": publisher,
            "auth": auth,
            "edge_poc": edge_poc,
            # Onboarding v2 (#4) — kept so the take_setup_snapshot command
            # handler can re-publish a rescan snapshot.
            "snapshot_manager": snapshot_manager,
            # Onboarding v2 (#9) — shipping-mode command channel.
            "command_handler": command_handler,
            "status_client": status_client,
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
