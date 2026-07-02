"""Entity classifier for the iEMS HACS integration.

Ported from `ha_bridge/classifier.py` in the main repo, with output
categories translated into the `contracts/telemetry.schema.json` enum.

Evaluation order (per hacs_spec.md §3a, approved by Sarah 2026-04-19):

  1. Suppression — dedup / platform blacklist / domain blacklist.
  2. Battery SOC detection — BEFORE any inverter.* hint.
  2b. Inverter fault/alarm enum sensors → inverter.fault (v0.8.0, Sprint 7
      Autopilot DIAGNOSE opener).  Requires device_class=enum AND a
      device_fault/device_alarm name/entity_id hint — conservative on
      purpose so no other enum sensor is swept in.
  3. Inverter.* hints — require entity_id keyword AND a power/energy signal.
  4. Generic sensor.power / sensor.energy by device_class.
  5. meter.energy for remaining electrical device_classes.
  6. Environment (sensor.temperature / sensor.humidity).
  7. Controllable (switch / light / climate).
  8. Surviving sensor/binary_sensor → other. Otherwise suppressed.
"""
from typing import Optional

# Schema enum — MUST stay in sync with
# contracts/telemetry.schema.json $defs.Entity.properties.category.enum
# (enforced by test_valid_categories_matches_schema).
VALID_CATEGORIES: set[str] = {
    "inverter.pv",
    "inverter.battery",
    "inverter.grid",
    "inverter.load",
    "battery.soc",
    "meter.energy",
    "switch.controllable",
    "sensor.power",
    "sensor.energy",
    "sensor.temperature",
    "sensor.humidity",
    "light",
    "climate",
    "inverter.fault",
    "other",
}

# Platforms that are pure HA internals → hard suppress.
# Previously included solarman/hue/sun/met under the assumption that iEMS had
# direct bridges for those. Cloud architecture has NO direct bridges — HA is
# the only source. v0.1.3 removed them for this reason; v0.1.5 bulk-sync
# reintroduced them; v0.1.9 removes them again and locks the fix here AND in
# public_repo_staging/.
PLATFORM_BLACKLIST: set[str] = {
    # HA plumbing / internals
    "hassio", "backup", "hacs", "homeassistant", "automation", "person",
    "mobile_app", "shopping_list", "google_translate",
    "ibeacon", "cast", "apple_tv", "upnp",
}

# Domains that carry HA-internal config / UX state, not physical device state.
DOMAIN_BLACKLIST: set[str] = {
    "number", "select", "button", "update", "time", "datetime", "event",
    "text", "scene", "automation", "tts", "siren", "remote", "media_player",
    "todo", "person", "device_tracker",
}

# Device-class + unit hints — power/energy signal required for inverter.* routing.
POWER_DEVICE_CLASSES: set[str] = {"power", "energy"}
POWER_UNITS: set[str] = {"w", "kw", "mw", "wh", "kwh", "mwh"}

# Electrical-measurement classes that aren't pure power/energy.
METER_DEVICE_CLASSES: set[str] = {
    "voltage", "current", "frequency",
    "power_factor", "apparent_power", "reactive_power",
}

# Environment device classes → category.
ENV_DEVICE_CLASSES: dict[str, str] = {
    "temperature": "sensor.temperature",
    "humidity": "sensor.humidity",
}

# Controllable domains → category.
CONTROLLABLE_DOMAIN_MAP: dict[str, str] = {
    "switch": "switch.controllable",
    "light": "light",
    "climate": "climate",
}

# Name-keyword → inverter category. `battery` here means "inverter battery
# power channel"; SOC is resolved first.
# N5 (2026-04-25): Enphase Envoy keywords added. Enphase reports generation
# via `production`/`generation` (PV side) and household draw via `consumption`
# (load side). `envoy` is the Enphase hub entity name prefix — maps to PV-side
# as fallback, but `consumption` is checked first so a combined entity_id like
# `sensor.envoy_xxx_current_power_consumption` correctly routes to inverter.load.
# Evaluation order matters: more-specific keywords (load-side: consumption, load)
# are checked before the hub-name fallback (envoy).
INVERTER_KEYWORDS: dict[str, str] = {
    "pv": "inverter.pv",
    "solar": "inverter.pv",
    "production": "inverter.pv",
    "generation": "inverter.pv",
    "grid": "inverter.grid",
    "consumption": "inverter.load",
    "load": "inverter.load",
    "envoy": "inverter.pv",
    "battery": "inverter.battery",
}

SOC_NAME_HINTS: tuple[str, ...] = ("soc", "state_of_charge")

# Sprint 7 (2026-07-02) — inverter fault/alarm enum sensors (Autopilot
# DIAGNOSE opener, telemetry.schema.json v0.8.0).  ha-solarman exposes the
# Deye fault/alarm registers as enum sensors named
# `sensor.{prefix}_device_fault[_2]` / `sensor.{prefix}_device_alarm[_2]`
# (device_class=enum, `options` list, raw register words in the `value`
# attribute).  Detection requires BOTH device_class == "enum" AND one of
# these hints in the entity_id or name — conservative on purpose so other
# enum sensors (washer cycle states, HVAC presets, ...) never match.
FAULT_NAME_HINTS: tuple[str, ...] = ("device_fault", "device_alarm")


def _lower(s: Optional[str]) -> str:
    return (s or "").lower().strip()


def _has_power_signal(dc: str, unit: str) -> bool:
    """True iff device_class or unit indicates electrical power/energy."""
    return dc in POWER_DEVICE_CLASSES or unit in POWER_UNITS


def classify(entity: dict) -> dict:
    """Stamp `surface` + `category` on the entity dict and return it.

    Input keys (all optional unless noted):
      entity_id    : HA entity_id, used as fallback name and for keyword match
      domain       : HA domain (e.g. 'sensor', 'switch')
      platform     : HA platform that created the entity
      device_class : HA device_class (lowercase)
      unit         : unit_of_measurement
      name         : human-friendly name (falls back to entity_id)
      suppressed_by: if set by upstream dedup matcher, skip all logic

    Sets:
      surface  : bool  — True means ship to cloud
      category : str | None — one of VALID_CATEGORIES when surface=True
    """
    platform = _lower(entity.get("platform"))
    domain = _lower(entity.get("domain"))
    suppressed = entity.get("suppressed_by")
    dc = _lower(entity.get("device_class"))
    unit = _lower(entity.get("unit"))
    name = _lower(entity.get("name") or entity.get("entity_id"))

    # 1. Suppression tiers
    if suppressed or platform in PLATFORM_BLACKLIST or domain in DOMAIN_BLACKLIST:
        entity["surface"] = False
        entity["category"] = None
        return entity

    has_power = _has_power_signal(dc, unit)
    name_has_soc = any(k in name for k in SOC_NAME_HINTS)

    # 2. Battery SOC — evaluated BEFORE inverter.battery name-hint.
    #    N4 (2026-04-25): consumer-device filter. If the entity's device has a
    #    colocated motion/smoke/door binary sensor the battery belongs to a
    #    consumer device (Ring doorbell, Aqara sensor, smoke detector) — NOT
    #    energy storage. Suppress it from battery.soc and route to other.
    #    The `consumer_device` flag is set by _build_entity_index in __init__.py.
    if dc == "battery" or name_has_soc:
        if entity.get("consumer_device"):
            entity["surface"] = True
            entity["category"] = "other"
            return entity
        entity["surface"] = True
        entity["category"] = "battery.soc"
        return entity

    # 2b. Inverter fault/alarm enum sensors → inverter.fault (v0.8.0).
    #     Requires device_class=enum AND a device_fault/device_alarm hint in
    #     the entity_id or name.  The name may come from the HA registry with
    #     spaces ("Ground Master Device Fault") — normalise spaces to
    #     underscores before matching.  Placed BEFORE the inverter.* power
    #     hints and every catch-all: enum sensors carry no power signal, so
    #     without this step they'd fall through to `other` and the cloud
    #     DIAGNOSE rule would never see them.
    if dc == "enum":
        entity_id = _lower(entity.get("entity_id"))
        name_normalised = name.replace(" ", "_")
        if any(
            hint in entity_id or hint in name_normalised
            for hint in FAULT_NAME_HINTS
        ):
            entity["surface"] = True
            entity["category"] = "inverter.fault"
            return entity

    # 3. Inverter.* — require BOTH entity_id/name keyword AND power signal.
    if has_power:
        for keyword, category in INVERTER_KEYWORDS.items():
            if keyword in name:
                entity["surface"] = True
                entity["category"] = category
                return entity

    # 4. Generic power / energy by device_class.
    if dc == "power":
        entity["surface"] = True
        entity["category"] = "sensor.power"
        return entity
    if dc == "energy":
        entity["surface"] = True
        entity["category"] = "sensor.energy"
        return entity

    # 5. meter.energy — remaining electrical-measurement classes.
    if dc in METER_DEVICE_CLASSES:
        entity["surface"] = True
        entity["category"] = "meter.energy"
        return entity

    # 6. Environment.
    if dc in ENV_DEVICE_CLASSES:
        entity["surface"] = True
        entity["category"] = ENV_DEVICE_CLASSES[dc]
        return entity

    # 7. Controllable domains.
    if domain in CONTROLLABLE_DOMAIN_MAP:
        entity["surface"] = True
        entity["category"] = CONTROLLABLE_DOMAIN_MAP[domain]
        return entity

    # 8. Surviving sensor / binary_sensor → other.
    if domain in ("sensor", "binary_sensor"):
        entity["surface"] = True
        entity["category"] = "other"
        return entity

    # Default: suppress.
    entity["surface"] = False
    entity["category"] = None
    return entity


def classify_all(entities: list[dict]) -> None:
    """Run classify() on every entity in the list, in-place."""
    for e in entities:
        classify(e)
