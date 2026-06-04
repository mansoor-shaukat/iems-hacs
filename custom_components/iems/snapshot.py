"""Setup-snapshot collector (GitHub #4, ADR 0005).

The setup snapshot is the ONE payload that flows pre-confirmation. It is a
distinct payload class from the 30s telemetry batch — published on a dedicated
up-topic `iems/{user_id}/setup` (NOT the telemetry topic), once on first
install and once per `take_setup_snapshot` command. The cloud receiver Lambda
writes it onto `PROFILE#SITE_MODEL` with `status='draft'`; Stage-2 (#6) and
Stage-3 (#8) classifiers consume it from there.

Contract: `contracts/setup_snapshot.schema.json` (v0.13.0, CTO-owned, read-only).

Design — pure core + thin impure shell
--------------------------------------
`build_setup_snapshot` is PURE, deterministic, side-effect-free: every input is
passed in (config dict, energy_prefs, device list, ts string), the output is the
fixed JSON shape. No HA APIs, no clock, no uuid, no I/O. This matches the
`classifier.classify` test pattern and makes the mutate-flips-output acceptance
test trivial.

`collect_setup_snapshot(hass, ...)` is the thin impure shell that extracts the
three inputs from a live HA instance (`hass.config`, the HA `energy/get_prefs`
WS result, the device registry) and delegates to the pure builder. The WS call
and registry walk live there so the pure path stays testable with plain dicts.
"""
from __future__ import annotations

import inspect
import logging
from typing import Any, Callable

log = logging.getLogger("iems.snapshot")

# Pinned to the contract `const`. The drift guard in
# tests/hacs/test_snapshot.py::test_snapshot_schema_version_matches_contract
# fails loudly if this diverges from contracts/setup_snapshot.schema.json.
SCHEMA_VERSION = "0.13.0"

# Per the contract: site_config.additionalProperties = false. We lift EXACTLY
# these keys from hass.config (currency_from_locale is derived from HA currency)
# so a future HA field can't silently leak into the pre-confirmation payload.
_SITE_CONFIG_KEYS: tuple[str, ...] = (
    "lat",
    "lon",
    "country",
    "time_zone",
    "ha_version",
    "currency_from_locale",
)

# Per the contract: device item additionalProperties = false. `device_id` is
# optional; manufacturer/model/integration_domain are required (nullable).
_DEVICE_KEYS: tuple[str, ...] = (
    "device_id",
    "manufacturer",
    "model",
    "sw_version",
    "integration_domain",
)

_VALID_SOURCE_KINDS = frozenset({"first_install", "rescan"})


def _project(src: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    """Copy only the contract-allowed keys from `src`.

    Drops unknown keys (the contract is additionalProperties:false). Missing
    keys are omitted rather than coerced — the caller (collect_setup_snapshot)
    is responsible for supplying every REQUIRED key; this helper only enforces
    the whitelist so an over-eager hass.config read can't widen the payload.
    """
    return {k: src[k] for k in keys if k in src}


def build_setup_snapshot(
    *,
    user_id: str,
    config: dict[str, Any],
    energy_prefs: dict[str, Any] | None,
    devices: list[dict[str, Any]],
    source_kind: str,
    ts: str,
) -> dict[str, Any]:
    """Build a `setup_snapshot.schema.json`-conforming payload. PURE.

    Parameters
    ----------
    user_id
        Full Cognito Identity-Pool identity_id (region:UUID), matching the
        MQTT topic segment.
    config
        Site config dict — must carry the required `site_config` keys
        (lat, lon, country, time_zone, ha_version, currency_from_locale).
        Unknown keys are dropped (contract is additionalProperties:false).
    energy_prefs
        Verbatim result of HA's `energy/get_prefs` WS call, or None when the
        user has no Energy Dashboard configured.
    devices
        One dict per HA device. Each must carry manufacturer / model /
        integration_domain (nullable); device_id + sw_version optional.
        Unknown keys are dropped per the contract.
    source_kind
        "first_install" | "rescan" — provenance. Distinguishes the
        first-install capture from a user-triggered re-scan.
    ts
        ISO-8601 UTC capture time; MUST end in 'Z' (contract pattern Z$).

    Returns
    -------
    dict
        The snapshot payload. Determinstic for fixed inputs — no clock, no
        uuid, no I/O. Does not mutate any input.

    Raises
    ------
    ValueError
        On an unknown source_kind or a ts that does not end in 'Z'.
    """
    if source_kind not in _VALID_SOURCE_KINDS:
        raise ValueError(
            f"source_kind must be one of {sorted(_VALID_SOURCE_KINDS)}, "
            f"got {source_kind!r}"
        )
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise ValueError(f"ts must be an ISO-8601 UTC string ending in 'Z', got {ts!r}")

    site_config = _project(config, _SITE_CONFIG_KEYS)
    device_registry_snapshot = [_project(d, _DEVICE_KEYS) for d in devices]

    return {
        "schema_version": SCHEMA_VERSION,
        "user_id": user_id,
        "ts": ts,
        "source": {"kind": source_kind},
        "site_config": site_config,
        # Verbatim — opaque to the contract (additionalProperties:true). Stage-2
        # parses flow_from/flow_to/solar/battery selections as ground truth.
        "ha_energy_prefs": energy_prefs,
        "device_registry_snapshot": device_registry_snapshot,
    }


class SetupSnapshotManager:
    """Orchestrates setup-snapshot publishing per ADR 0005.

    The snapshot is the ONLY payload that flows pre-confirmation. This manager
    enforces the publish discipline:

      - `publish_on_first_install()` — fires EXACTLY ONCE per session. A repeat
        call (e.g. a config-entry reload) is a no-op so the cloud receiver
        isn't spammed with duplicate first-install snapshots.
      - `handle_take_setup_snapshot_command()` — fires on EACH cloud
        `take_setup_snapshot` command (a user-triggered "Scan for new
        devices"). Always emits a fresh `rescan` snapshot.

    Neither path publishes a telemetry batch — that's the whole point of ADR
    0005's `setup` shipping mode. Telemetry only starts once the user confirms
    and the cloud commands shipping_mode='active'.

    Dependencies are injected:
      - `publisher` — exposes `async publish_setup_snapshot(payload) -> bool`.
      - `collect(source_kind) -> dict | Awaitable[dict]` — returns a
        contract-conforming snapshot for the given provenance. In production
        this is the async `collect_setup_snapshot(hass, ...)` shell (it reads
        the device registry + awaits the energy-prefs WS call); in tests it's a
        plain sync callable returning a dict. Both are supported.
    """

    def __init__(
        self,
        *,
        publisher,
        collect: Callable[[str], dict[str, Any]],
    ) -> None:
        self._publisher = publisher
        self._collect = collect
        # Guards the one-off first-install publish against config-entry reloads.
        self._first_install_published = False

    async def _collect_snapshot(self, source_kind: str) -> dict[str, Any]:
        """Call the injected collector, awaiting it if it's async."""
        result = self._collect(source_kind)
        if inspect.isawaitable(result):
            return await result
        return result

    async def publish_on_first_install(self) -> bool:
        """Publish the first-install snapshot once. No-op on repeat calls.

        Returns True if a snapshot was published this call, False if it was
        skipped (already published this session).
        """
        if self._first_install_published:
            log.debug("setup snapshot: first-install already published, skipping")
            return False
        snapshot = await self._collect_snapshot("first_install")
        ok = await self._publisher.publish_setup_snapshot(snapshot)
        # Mark published even on a transient failure=False? No — only on a
        # successful hand-off, so a failed first publish can be retried on the
        # next setup attempt. The publisher returns True on success.
        if ok:
            self._first_install_published = True
            log.info("setup snapshot: first-install published")
        else:
            log.warning("setup snapshot: first-install publish failed, will retry")
        return ok

    async def handle_take_setup_snapshot_command(self) -> bool:
        """Publish a rescan snapshot in response to a take_setup_snapshot command.

        Fires every time — a re-scan is explicitly user-triggered, so duplicate
        suppression is the cloud receiver's idempotency job (replay of an
        identical snapshot is a no-op there), not the device's.
        """
        snapshot = await self._collect_snapshot("rescan")
        ok = await self._publisher.publish_setup_snapshot(snapshot)
        if ok:
            log.info("setup snapshot: rescan published (take_setup_snapshot)")
        else:
            log.warning("setup snapshot: rescan publish failed")
        return ok


def _currency_from_locale(hass) -> str | None:
    """Best-effort ISO-4217 currency from HA config.

    HA exposes `hass.config.currency` (e.g. 'PKR', 'USD'). The contract calls
    this `currency_from_locale` because it derives from the user's HA locale.
    Returns None when HA hasn't set one.
    """
    currency = getattr(hass.config, "currency", None)
    return currency or None


def _extract_site_config(hass) -> dict[str, Any]:
    """Lift the site_config dict from a live hass instance. Impure (reads hass)."""
    cfg = hass.config
    time_zone = getattr(cfg, "time_zone", None)
    return {
        "lat": getattr(cfg, "latitude", None),
        "lon": getattr(cfg, "longitude", None),
        "country": getattr(cfg, "country", None) or None,
        "time_zone": str(time_zone) if time_zone else None,
        "ha_version": str(getattr(cfg, "version", "unknown")),
        "currency_from_locale": _currency_from_locale(hass),
    }


def _integration_domain_for(hass, device_entry) -> str | None:
    """Resolve the HA integration domain for a device-registry entry.

    A DeviceEntry doesn't carry the domain directly — it carries a set of
    config-entry IDs. We resolve the FIRST config entry's `domain`
    (deterministic: config_entries is iterated in sorted id order). Returns
    None when the device has no config entry (helper/manual devices) or the
    entry can't be resolved.
    """
    config_entry_ids = getattr(device_entry, "config_entries", None) or ()
    for ce_id in sorted(config_entry_ids):
        entry = hass.config_entries.async_get_entry(ce_id)
        if entry is not None:
            return getattr(entry, "domain", None)
    return None


def _extract_devices(hass) -> list[dict[str, Any]]:
    """Map HA device-registry entries → contract device dicts. Impure shell.

    Reads HA's device registry and resolves each device's integration domain
    via its config entry. Sorted by device id for deterministic ordering.
    """
    from homeassistant.helpers import device_registry as dr  # local — HA only

    dr_reg = dr.async_get(hass)
    out: list[dict[str, Any]] = []
    for entry in sorted(dr_reg.devices.values(), key=lambda d: d.id):
        out.append(
            {
                "device_id": entry.id,
                "manufacturer": entry.manufacturer,
                "model": entry.model,
                "sw_version": entry.sw_version,
                "integration_domain": _integration_domain_for(hass, entry),
            }
        )
    return out


async def _fetch_energy_prefs(hass) -> dict[str, Any] | None:
    """Fetch HA Energy Dashboard prefs (energy/get_prefs). Impure; HA only.

    Returns the full prefs dict, or None when the Energy Dashboard isn't
    configured / the energy component isn't loaded. Never raises into the
    caller — a missing Energy Dashboard is a legitimate state (the cloud
    classifier falls back to category + keyword matching).
    """
    try:
        from homeassistant.components.energy.data import (  # type: ignore
            async_get_manager,
        )
    except ImportError:
        return None
    try:
        manager = await async_get_manager(hass)
    except Exception as exc:  # noqa: BLE001 — energy component optional/unloaded
        log.warning(
            "setup snapshot: energy/get_prefs unavailable: %s: %s",
            type(exc).__name__, exc,
        )
        return None
    return getattr(manager, "data", None)


async def collect_setup_snapshot(
    hass,
    *,
    user_id: str,
    source_kind: str,
) -> dict[str, Any]:
    """Impure shell: gather inputs from a live HA instance + build the snapshot.

    Reads `hass.config`, the HA Energy Dashboard prefs, and the device
    registry, then delegates to the PURE `build_setup_snapshot`. The clock read
    for `ts` happens HERE (the impure boundary) so the pure builder stays
    deterministic.
    """
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    config = _extract_site_config(hass)
    energy_prefs = await _fetch_energy_prefs(hass)
    devices = _extract_devices(hass)
    return build_setup_snapshot(
        user_id=user_id,
        config=config,
        energy_prefs=energy_prefs,
        devices=devices,
        source_kind=source_kind,
        ts=ts,
    )
