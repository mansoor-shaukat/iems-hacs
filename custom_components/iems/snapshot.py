"""Setup-snapshot collector (GitHub #4, ADR 0005).

The setup snapshot is the ONE payload that flows pre-confirmation. It is a
distinct payload class from the 30s telemetry batch — published on a dedicated
up-topic `iems/{user_id}/setup` (NOT the telemetry topic), once on first
install and once per `take_setup_snapshot` command. The cloud receiver Lambda
writes it onto `PROFILE#SITE_MODEL` with `status='draft'`; Stage-2 (#6) and
Stage-3 (#8) classifiers consume it from there.

Contract: `contracts/setup_snapshot.schema.json` (rev v0.15.0, CTO-owned,
read-only). NOTE: the contract's `schema_version` *const* is pinned at "0.13.0"
even though the document revision is v0.15.0 — the v0.13.1 change (the optional
top-level `entity_classifications[]` field), v0.14.0 (the optional
`automations[]` field), and v0.15.0 (the optional `entity_registry[]` field)
are all ADDITIVE, so a pre-collector 0.13.0 snapshot still validates. The wire
`schema_version` therefore stays "0.13.0"; the drift guard enforces it.

Design — pure core + thin impure shell
--------------------------------------
`build_setup_snapshot` is PURE, deterministic, side-effect-free: every input is
passed in (config dict, energy_prefs, device list, entity_index, ts string), the
output is the fixed JSON shape. No HA APIs, no clock, no uuid, no I/O. This
matches the `classifier.classify` test pattern and makes the mutate-flips-output
acceptance test trivial.

`collect_setup_snapshot(hass, ...)` is the thin impure shell that extracts the
inputs from a live HA instance (`hass.config`, the HA `energy/get_prefs` WS
result, the device registry) and delegates to the pure builder. The WS call and
registry walk live there so the pure path stays testable with plain dicts. The
already-built `entity_index` (the same per-entity registry snapshot the
coordinator classifies for telemetry) is passed IN by the caller — the shell
does not rebuild it.

Why entity_classifications matters (CEO fresh-user-walk, 2026-06-10)
-------------------------------------------------------------------
A real fresh-user onboarding produced an EMPTY site model (no pv/grid/load/
battery entities) even though the home has 4 Deye inverters publishing
telemetry. Root cause: the snapshot carried only `ha_energy_prefs` (EMPTY when
the user hasn't configured HA's Energy Dashboard — the common case) plus a
device-level `device_registry_snapshot` (no entity IDs). The cloud Stage-2
classifier's energy-prefs tier was empty, its entity-keyword tier had no
entities, and it fell to device-registry shape-only inference → correct shape,
ZERO entities → onboarding had nothing to show. The cloud classifier ALREADY
reads a top-level `entity_classifications[]` (handler.py `classify()` Tier-2);
HACS just never sent it. This module now emits it, reusing the SAME
`classifier.classify` HACS runs for the telemetry whitelist.
"""
from __future__ import annotations

import inspect
import logging
from typing import Any, Callable

from .classifier import classify

log = logging.getLogger("iems.snapshot")

# Pinned to the contract `const` ("0.13.0", NOT the v0.13.1 doc revision — see
# the module docstring). The drift guard in
# tests/hacs/test_snapshot.py::test_snapshot_schema_version_matches_contract
# fails loudly if this diverges from contracts/setup_snapshot.schema.json.
SCHEMA_VERSION = "0.13.0"

# Categories that are ENERGY-relevant for the cloud Stage-2 site-model
# classifier. These are the categories its `_CATEGORY_TO_BUCKET` buckets
# directly (inverter.{pv,grid,battery,load}, battery.soc, meter.energy) PLUS the
# generic power/energy sensors whose entity_id keywords (`solar`, `grid`,
# `load`, `consumption`, ...) feed its `_KEYWORD_TO_BUCKET` fallback. We emit
# ONLY these to keep the pre-confirmation payload lean: controllables
# (switch/light/climate), environment (temperature/humidity) and `other` carry
# no site-model signal and would only inflate the snapshot toward the 128 KiB
# IoT limit. Mirrors classifier.VALID_CATEGORIES minus the non-energy members.
_ENERGY_CATEGORIES: frozenset[str] = frozenset(
    {
        "inverter.pv",
        "inverter.grid",
        "inverter.load",
        "inverter.battery",
        "battery.soc",
        "meter.energy",
        "sensor.power",
        "sensor.energy",
    }
)

# Hard ceiling on entity_classifications[] entries so a pathological install
# (thousands of power sensors) can't push the setup payload past the 128 KiB
# IoT Core limit (MQTT_MESSAGE_SIZE_HARD_LIMIT_BYTES). The setup snapshot is
# published via iot_core.publish, which RAISES PayloadTooLargeError on any
# payload > 131072 bytes — i.e. an oversized snapshot doesn't truncate, it FAILS
# WHOLESALE and onboarding gets nothing. So this cap must keep the COMPLETE
# snapshot (site_config + device_registry_snapshot + entity_classifications)
# under the limit with the hacs.md ≥35% headroom margin (≤ 80% × 128 KiB ≈ 104
# KiB target).
#
# Sizing (MEASURED worst case: 350 maximally-long entity names + a 181-device
# registry of maximally-long manufacturer/model strings — far heavier than any
# real home):
#   - 350 classifications × ~168 B               ≈ 58 KiB
#   - device_registry_snapshot, 181 heavy devices ≈ 36 KiB (≥ CEO's home)
#   - site_config + envelope                      ≈  1 KiB
#   - WORST-CASE TOTAL                            ≈ 95.4 KiB  (under the ~104 KiB
#                                                   80%-headroom target, ~36 KiB
#                                                   below the 128 KiB hard limit)
# Realistic homes are far smaller: CEO's home surfaces ~146 entities TOTAL, of
# which a minority are energy-category → a ~16 KiB snapshot. 350 is a defensive
# ceiling well above any real energy-entity count while still guaranteeing the
# publish can't trip PayloadTooLargeError. Dropped entities are logged loudly —
# never silently truncated.
_MAX_ENTITY_CLASSIFICATIONS: int = 350

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
    "name",
    "manufacturer",
    "model",
    "sw_version",
    "integration_domain",
)

_VALID_SOURCE_KINDS = frozenset({"first_install", "rescan"})

# ---------------------------------------------------------------------------
# entity_registry[] — Smart Home AI-builder (#24), contract v0.15.0
# ---------------------------------------------------------------------------
#
# The AI builder needs a device-name → entity_id index so it can resolve
# "lobby lamp" → light.reserve without guessing. LATEST# telemetry rows carry
# NO friendly_name and entity_classifications[] is energy-only (no lights), so
# without this the AI has zero name signal and every name-based draft fails.
#
# We emit ALL CONTROLLABLE DOMAIN entities (the domains the AI can actually
# act on) PLUS any entity that carries a non-empty friendly_name the user
# might say — regardless of domain. friendly_name comes from the entity state
# attributes (HA always populates it) or falls back to the entity registry
# `name` field; area is resolved to the human area NAME (same resolver the
# automation collector uses); domain = entity_id.split(".")[0].
#
# The set is intentionally BROADER than entity_classifications[] — that set is
# energy-only (inverter power sensors); this set is controllable+named
# (lights, switches, fans, locks, climate, media_player ...) with NO energy
# sensors (they're already in entity_classifications so adding them here would
# only inflate the snapshot for zero AI benefit). The two sets are
# complementary and are used by different cloud consumers.
#
# SIZING: a controllable entity item is small (~100–150 B): entity_id (~40 B)
# + friendly_name (~20 B) + area (~15 B) + domain (~10 B) + JSON overhead.
# Realistic homes: 50–200 controllable entities = ~10–30 KiB. Cap 500 gives
# 500 × 150 B = 75 KiB worst-case, which combined with the other snapshot
# sections (device_registry_snapshot ~36 KiB, entity_classifications ~15 KiB,
# site_config ~1 KiB) lands well under the 80% × 128 KiB ≈ 104 KiB target.
# Matches _MAX_AUTOMATIONS to signal the same "bounded above real-home ceiling"
# intent. Dropped entities logged loudly — never silently.

# HA domains whose entities the AI can control. Mirrors the Smart Home domain
# scope from the product spec. This list is intentionally fixed at the
# HACS level (controllable at the HA service-call layer); discovery-only
# domains (sensor / binary_sensor / weather) are excluded — the AI can READ
# those via the telemetry channel, not WRITE them.
_CONTROLLABLE_DOMAINS: frozenset[str] = frozenset(
    {
        "light",
        "switch",
        "fan",
        "cover",
        "climate",
        "scene",
        "script",
        "input_boolean",
        "input_number",
        "input_select",
        "media_player",
        "lock",
        "vacuum",
        "humidifier",
        "water_heater",
        "button",
        "number",
        "select",
    }
)

# Hard ceiling on entity_registry[] entries to stay under the 128 KiB IoT
# limit. Matches _MAX_AUTOMATIONS intent; see SIZING comment above. Dropped
# entries are logged loudly — never silently truncated.
_MAX_ENTITY_REGISTRY: int = 500


def _build_entity_registry(
    entity_index: dict[str, dict[str, Any]] | None,
    area_registry: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Build the entity_registry[] for the AI-builder. PURE.

    Iterates entity_index and emits entries for:
      - Any entity whose domain is in _CONTROLLABLE_DOMAINS, OR
      - Any entity carrying a non-empty friendly_name (the words the user
        would say to the AI, e.g. "lobby lamp") — regardless of domain.

    For each emitted entry:
      entity_id   — the HA entity_id (key in entity_index)
      friendly_name — from meta["name"] (the per-entity display name the
                      coordinator's _build_entity_index pulled from the HA
                      entity registry or state attributes); nullable.
      area        — human area NAME resolved from meta["area"]. meta["area"]
                    is ALREADY the resolved area NAME (not area_id) in the
                    coordinator's entity_index structure — the coordinator calls
                    area_registry.areas[area_id].name when it builds the index.
                    area_registry is accepted here for forward-compatibility but
                    is NOT used when meta["area"] is already a string (the
                    current production path). See __init__._build_entity_index.
      domain      — entity_id.split(".")[0]

    Sorting: prefer entries WITH a friendly_name first (the AI needs them
    most), then sort by entity_id for determinism within each group. Capped
    at _MAX_ENTITY_REGISTRY with a loud log on truncation.

    `entity_index` None (no index supplied) → empty list (back-compat).
    """
    if not entity_index:
        return []

    entries: list[dict[str, Any]] = []
    for entity_id, meta in entity_index.items():
        domain = entity_id.split(".")[0] if "." in entity_id else entity_id
        friendly_name = meta.get("name") or None
        is_controllable = domain in _CONTROLLABLE_DOMAINS
        has_friendly_name = bool(friendly_name)

        if not is_controllable and not has_friendly_name:
            # Pure read-only entity with no user-facing name — not useful for
            # AI name resolution, omit.
            continue

        # area: meta["area"] is the HUMAN area name in the coordinator's
        # entity_index (the coordinator resolves area_id → name at index-build
        # time via __init__._build_entity_index). Coerce empty string → None.
        area = meta.get("area") or None

        entries.append(
            {
                "entity_id": entity_id,
                "friendly_name": friendly_name,
                "area": area,
                "domain": domain,
            }
        )

    # Sort: friendly_name entries first (cloud AI benefits most from names),
    # then by entity_id for determinism.
    entries.sort(key=lambda e: (e["friendly_name"] is None, e["entity_id"]))

    if len(entries) > _MAX_ENTITY_REGISTRY:
        dropped = len(entries) - _MAX_ENTITY_REGISTRY
        log.warning(
            "setup snapshot: %d entity_registry entries found, capping at %d "
            "(dropping %d) to stay under the 128 KiB IoT payload limit",
            len(entries),
            _MAX_ENTITY_REGISTRY,
            dropped,
        )
        entries = entries[:_MAX_ENTITY_REGISTRY]

    return entries


def _project(src: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    """Copy only the contract-allowed keys from `src`.

    Drops unknown keys (the contract is additionalProperties:false). Missing
    keys are omitted rather than coerced — the caller (collect_setup_snapshot)
    is responsible for supplying every REQUIRED key; this helper only enforces
    the whitelist so an over-eager hass.config read can't widen the payload.
    """
    return {k: src[k] for k in keys if k in src}


def _build_entity_classifications(
    entity_index: dict[str, dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Classify each entity in `entity_index` and emit the energy-relevant set.

    PURE (no I/O, no HA). Reuses `classifier.classify` — the SAME classifier
    HACS runs per-entity for the telemetry whitelist — so the categories the
    cloud sees in the snapshot are identical to the categories it sees on the
    wire. We do NOT re-implement classification.

    Output item shape (matches the cloud consumer
    `infra/lambdas/site_model_classifier/handler.py` `classify()` Tier-2 and the
    contract's `entity_classifications.items`):

        {"entity_id": str, "category": str, "friendly_name": str | None}

    Only entities whose classified category is in `_ENERGY_CATEGORIES` are
    emitted — `other`, controllables and environment sensors carry no
    site-model signal and are dropped to keep the payload lean. Results are
    sorted by entity_id for deterministic output. If more than
    `_MAX_ENTITY_CLASSIFICATIONS` energy entities classify, the list is capped
    (after sort) and the drop is logged loudly — never silently truncated.

    `entity_index` is the coordinator's per-entity registry snapshot keyed by
    entity_id, each value `{platform, domain, device_class, unit, name, area,
    brand, consumer_device}` (see __init__._build_entity_index). `None` (no
    index supplied) yields an empty list — a back-compat path for callers that
    predate the entity_index wiring.
    """
    if not entity_index:
        return []

    classified: list[dict[str, Any]] = []
    for entity_id, meta in entity_index.items():
        # classify() mutates+returns its input dict — pass a shallow copy with
        # entity_id injected so we never mutate the coordinator's live index.
        candidate = dict(meta)
        candidate["entity_id"] = entity_id
        result = classify(candidate)
        if not result.get("surface"):
            continue
        category = result.get("category")
        if category not in _ENERGY_CATEGORIES:
            continue
        classified.append(
            {
                "entity_id": entity_id,
                "category": category,
                # friendly name from the registry-index `name`; classify()
                # falls back to entity_id internally for its own matching, but
                # for the snapshot we surface the human-friendly name (nullable
                # per the contract) so the wizard can label ambiguous entities.
                "friendly_name": meta.get("name"),
            }
        )

    classified.sort(key=lambda item: item["entity_id"])

    if len(classified) > _MAX_ENTITY_CLASSIFICATIONS:
        dropped = len(classified) - _MAX_ENTITY_CLASSIFICATIONS
        log.warning(
            "setup snapshot: %d energy entities classified, capping "
            "entity_classifications at %d (dropping %d) to stay under the "
            "128 KiB IoT payload limit",
            len(classified),
            _MAX_ENTITY_CLASSIFICATIONS,
            dropped,
        )
        classified = classified[:_MAX_ENTITY_CLASSIFICATIONS]

    return classified


# ---------------------------------------------------------------------------
# automations[] — Smart Home (#21/#22), contract v0.14.0
# ---------------------------------------------------------------------------
#
# HACS reads the user's HA automations and emits a COMPACT structured summary
# (NEVER raw YAML) the cloud uses to derive the archetype (schedule vs event),
# compose the plain-English headline, and group by room. The collector is split
# the same way as the rest of this module: PURE shaping functions
# (summarize_trigger / summarize_action / build_automations) that take plain
# dicts, plus an impure shell (_extract_automations) that reads the live HA
# automation config + state + registries.

# The iEMS author marker. An automation HACS authors (#24, not yet built)
# carries `author: "iems"` in the dict the extractor produces — derived from a
# sentinel the write path stamps into the automation. The marker CONVENTION:
# the cloud/HACS write path (#24) prefixes the automation `id` with
# `IEMS_AUTHOR_ID_PREFIX` ("iems_") AND/OR sets a top-level `iems_authored: true`
# in the automation config's `variables` block. The extractor below reads either
# signal. TODAY no iEMS-authored automations exist, so in practice every
# automation resolves to "user". `build_automations` only validates the already-
# resolved `author` value against the contract enum (defence in depth); the
# marker DETECTION lives in `_resolve_author` on the impure side.
IEMS_AUTHOR_ID_PREFIX = "iems_"

_VALID_AUTHORS = frozenset({"user", "iems"})

# Compact trigger fields the contract allows (additionalProperties:false).
# `platform` is the only required one. Everything else is optional/nullable.
_TRIGGER_KEYS: tuple[str, ...] = (
    "platform",
    "entity_id",
    "at",
    "above",
    "below",
    "to",
    "event",
)

# Compact action fields the contract allows. None required (an action with only
# a data_summary is legal), but in practice `service` + `entity_id` carry the
# headline.
_ACTION_KEYS: tuple[str, ...] = (
    "service",
    "entity_id",
    "target_area_id",
    "data_summary",
)

# Hard ceiling on automations[] entries so a pathological install can't push the
# setup payload past the 128 KiB IoT limit. A summarized automation is small
# (~0.3-0.6 KiB), so 500 is far above any real home while keeping the snapshot
# bounded. Dropped automations are logged loudly, never silently truncated.
_MAX_AUTOMATIONS: int = 500


def summarize_trigger(trig: dict[str, Any]) -> dict[str, Any]:
    """Compact one HA trigger config -> the contract's trigger summary. PURE.

    Reads BOTH the legacy `platform:` key and the newer (HA 2024.10+) `trigger:`
    key — newer installs write `trigger:` and an extractor that only read
    `platform` would emit a blank platform for every modern automation.

    Carries ONLY the contract-allowed compact fields (`_TRIGGER_KEYS`); all raw-
    YAML keys (`for`, `id`, `value_template`, ...) are dropped. The cloud needs
    enough to (a) distinguish schedule (time/time_pattern/sun) from event-driven
    (state/numeric_state/event/template) and (b) render the canonical condition
    (the 4 grid-voltage entities + the below-50 threshold) — so we keep
    entity_id / at / above / below / to / event.
    """
    # `platform` is canonical; `trigger` is the modern alias. Prefer whichever
    # is present (platform wins if both, deterministic).
    platform = trig.get("platform") or trig.get("trigger")
    out: dict[str, Any] = {"platform": platform}
    for key in _TRIGGER_KEYS:
        if key == "platform":
            continue
        if key in trig and trig[key] is not None:
            out[key] = trig[key]
    return out


def _data_summary(service: str | None, data: dict[str, Any] | None) -> str | None:
    """Render a SHORT human-ish note of the key service data. PURE.

    NOT the full data block — just enough for the cloud to say "turns blue" or
    "to 22 C". Recognises the common light-colour shapes (the grid-lamp worked
    example needs xy_color carried) and a few scalar comforts; otherwise falls
    back to a compact "k=v" join of the first couple of keys. Returns None when
    there's no data worth summarising.
    """
    if not data or not isinstance(data, dict):
        return None
    parts: list[str] = []
    # Colour names / temperature first — the most descriptive.
    if "color_name" in data:
        parts.append(str(data["color_name"]))
    if "xy_color" in data:
        xy = data["xy_color"]
        if isinstance(xy, (list, tuple)) and len(xy) == 2:
            parts.append(f"xy {xy[0]},{xy[1]}")
    if "rgb_color" in data:
        rgb = data["rgb_color"]
        if isinstance(rgb, (list, tuple)):
            parts.append("rgb " + ",".join(str(c) for c in rgb))
    if "color_temp_kelvin" in data:
        parts.append(f"{data['color_temp_kelvin']}K")
    if "temperature" in data:
        parts.append(f"{data['temperature']}°")
    if "brightness" in data:
        parts.append(f"bri {data['brightness']}")
    if parts:
        return " / ".join(parts)
    # Generic fallback: first two scalar keys, compactly. Skips nested blocks.
    scalar_items = [
        (k, v)
        for k, v in data.items()
        if isinstance(v, (str, int, float, bool))
    ]
    if not scalar_items:
        return None
    return " / ".join(f"{k}={v}" for k, v in scalar_items[:2])


def summarize_action(act: dict[str, Any]) -> dict[str, Any]:
    """Compact one HA action config -> the contract's action summary. PURE.

    Reads BOTH the legacy `service:` key and the newer (HA 2024.10+) `action:`
    key. Resolves the action target from either the flat `entity_id` /
    `area_id` shape OR the nested `target:` block (the modern shape). Composes a
    short `data_summary` from the service `data` — NOT the raw block.

    Drops every raw-YAML key outside the compact `_ACTION_KEYS` set. The cloud
    composes the headline action phrase ("the lamp turns blue") + the condition
    strip from these fields.
    """
    service = act.get("service") or act.get("action")
    target = act.get("target") if isinstance(act.get("target"), dict) else {}

    # entity_id can sit at the top level OR under target (modern). Top-level
    # wins when both present (deterministic); else fall back to target.
    entity_id = act.get("entity_id")
    if entity_id is None:
        entity_id = target.get("entity_id")

    # area_id only ever lives under target in a service call.
    target_area_id = target.get("area_id")
    if target_area_id is None:
        target_area_id = act.get("area_id")

    out: dict[str, Any] = {}
    if service is not None:
        out["service"] = service
    if entity_id is not None:
        out["entity_id"] = entity_id
    if target_area_id is not None:
        out["target_area_id"] = target_area_id
    data_summary = _data_summary(service, act.get("data"))
    if data_summary is not None:
        out["data_summary"] = data_summary
    return out


def _resolve_author_value(author: Any) -> str:
    """Validate an already-resolved author value against the contract enum.

    Defence in depth on the PURE side: the impure extractor (`_resolve_author`)
    does the marker detection and hands us "user" or "iems"; here we guarantee
    the wire value is one of the two enum members so a junk value (a bug in a
    future extractor, a hand-edited snapshot) can never leak past the contract.
    Anything not exactly "iems" collapses to "user" (fail-safe: never falsely
    claim iEMS authored a user's automation).
    """
    if author == "iems":
        return "iems"
    return "user"


def _build_one_automation(auto: dict[str, Any]) -> dict[str, Any]:
    """Shape ONE already-extracted automation dict -> the contract item. PURE.

    `auto` carries: id, entity_id?, alias?, enabled, last_triggered?, mode?,
    area_id?, author?, triggers[] (raw HA trigger configs), actions[] (raw HA
    action configs). We whitelist + summarize so no raw YAML leaks.

    `last_triggered` is OMITTED (not null) when the automation never fired — the
    contract types it as the isoUtc $ref (pattern `Z$`) which forbids null, so a
    never-fired automation must drop the key entirely. The portal degrades the
    "last ran" stamp to hidden when the key is absent.
    """
    item: dict[str, Any] = {
        "id": str(auto["id"]),
        "enabled": bool(auto.get("enabled", False)),
        "entity_id": auto.get("entity_id"),
        "alias": auto.get("alias"),
        "mode": auto.get("mode"),
        "area_id": auto.get("area_id"),
        "author": _resolve_author_value(auto.get("author")),
        "triggers": [summarize_trigger(t) for t in (auto.get("triggers") or [])],
        "actions": [summarize_action(a) for a in (auto.get("actions") or [])],
    }
    # last_triggered: include ONLY when present (never-fired -> key absent).
    last_triggered = auto.get("last_triggered")
    if last_triggered:
        item["last_triggered"] = last_triggered
    return item


def build_automations(automations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Shape a list of extracted automation dicts -> the contract array. PURE.

    Deterministic (sorted by id), non-mutating, no I/O, no HA. Each input dict
    is the merge of an automation's raw config + its live-state bits + the
    HACS-resolved action-target area_id (see `_extract_automations`). Output
    items conform to `setup_snapshot.schema.json#/properties/automations/items`.

    Capped at `_MAX_AUTOMATIONS` (after sort) with a loud log — never silently
    truncated — so a pathological install can't trip the 128 KiB IoT limit.
    """
    if not automations:
        return []
    built = [_build_one_automation(a) for a in automations]
    built.sort(key=lambda item: item["id"])
    if len(built) > _MAX_AUTOMATIONS:
        dropped = len(built) - _MAX_AUTOMATIONS
        log.warning(
            "setup snapshot: %d automations found, capping automations[] at %d "
            "(dropping %d) to stay under the 128 KiB IoT payload limit",
            len(built),
            _MAX_AUTOMATIONS,
            dropped,
        )
        built = built[:_MAX_AUTOMATIONS]
    return built


def build_setup_snapshot(
    *,
    user_id: str,
    config: dict[str, Any],
    energy_prefs: dict[str, Any] | None,
    devices: list[dict[str, Any]],
    source_kind: str,
    ts: str,
    entity_index: dict[str, dict[str, Any]] | None = None,
    automations: list[dict[str, Any]] | None = None,
    entity_registry_index: dict[str, dict[str, Any]] | None = None,
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
    entity_index
        The coordinator's per-entity registry snapshot keyed by entity_id (the
        SAME structure classified for the telemetry whitelist). Each value is
        `{platform, domain, device_class, unit, name, ...}`. Used to build the
        top-level `entity_classifications[]` (energy-relevant entities only) via
        `classifier.classify`. `None` (omitted) emits an empty list — a
        back-compat path. This is the field whose absence produced an EMPTY
        onboarding for the CEO's real home (no Energy Dashboard configured) on
        2026-06-10; see module docstring.
    automations
        Optional list of extracted HA automation dicts (raw config merged with
        live-state bits + HACS-resolved area_id; see `_extract_automations`).
        Threaded through `build_automations` into the optional, additive
        top-level `automations[]` (contract v0.14.0, Smart Home #21/#22). `None`
        (omitted) leaves the key ABSENT — a back-compat / pre-Smart-Home path.
        An empty list emits an empty `automations[]` (a home with zero
        automations — distinct from "never collected").
    entity_registry_index
        Optional entity_index to build the AI-builder `entity_registry[]`
        (contract v0.15.0, Smart Home #24). When supplied, _build_entity_registry
        emits controllable-domain and named entities so the cloud can resolve
        "lobby lamp" → light.reserve without guessing. The same entity_index
        dict used for entity_classifications is acceptable here — the builder
        reads the same meta fields (name, area, domain from entity_id). `None`
        (omitted) leaves the key ABSENT — a back-compat path for installs that
        predate Smart Home #24.

    Returns
    -------
    dict
        The snapshot payload. Deterministic for fixed inputs — no clock, no
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
    entity_classifications = _build_entity_classifications(entity_index)

    snapshot: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "user_id": user_id,
        "ts": ts,
        "source": {"kind": source_kind},
        "site_config": site_config,
        # Verbatim — opaque to the contract (additionalProperties:true). Stage-2
        # parses flow_from/flow_to/solar/battery selections as ground truth.
        "ha_energy_prefs": energy_prefs,
        "device_registry_snapshot": device_registry_snapshot,
        # Top-level per-entity classifier output (contract v0.13.1). REQUIRED
        # input for the cloud Stage-2 category+keyword fallback when
        # ha_energy_prefs is empty — without it a real fresh user gets an EMPTY
        # site model. Energy-relevant categories only; see
        # _build_entity_classifications.
        "entity_classifications": entity_classifications,
    }

    # automations[] is OPTIONAL + ADDITIVE (contract v0.14.0). Emit the key ONLY
    # when the caller supplies an automations list — a pre-Smart-Home install
    # (or a caller that didn't read the automation registry) omits it entirely,
    # which is exactly how the contract degrades (cloud has no automation cache,
    # Smart Home tab shows the empty state). `None` -> key absent; `[]` -> the
    # key is present and empty (a home with zero automations, a real state the
    # cloud distinguishes from "never collected").
    if automations is not None:
        snapshot["automations"] = build_automations(automations)

    # entity_registry[] is OPTIONAL + ADDITIVE (contract v0.15.0, Smart Home
    # #24). Emit ONLY when the caller supplies a registry index. Pre-Smart-Home
    # installs (or callers that didn't read the entity registry) omit it; the
    # cloud AI builder degrades to zero name signal in that case.
    if entity_registry_index is not None:
        snapshot["entity_registry"] = _build_entity_registry(entity_registry_index)

    return snapshot


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
                # User-facing device name: the user's rename wins, else HA's default
                # name. This is what the SmartHome "what else we found" scene labels
                # devices with (mtronic devices carry no model, so without this the
                # list rendered empty). Nullable per the contract.
                "name": entry.name_by_user or entry.name,
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


def _last_triggered_iso_z(state_obj) -> str | None:
    """Render an automation's `last_triggered` attribute as ISO-8601 UTC Z.

    HA stores `last_triggered` as a tz-aware `datetime` (or None if never
    fired). Returns the Z-suffixed string the contract requires, or None when
    the automation has never triggered. Mirrors the `_dt_to_iso_z` pattern in
    recovery.py — strftime to a trailing Z, never `+00:00`. Tolerates a string
    value (older HA / recorder rows) by normalising +00:00 → Z.
    """
    from datetime import datetime, timezone

    raw = None
    if state_obj is not None:
        raw = state_obj.attributes.get("last_triggered")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(raw, str) and raw.strip():
        s = raw.strip()
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            log.warning(
                "setup snapshot: unparseable automation last_triggered %r; "
                "omitting", raw,
            )
            return None
    return None


def _first_action_entity_id(actions: list[dict[str, Any]]) -> str | None:
    """Return the entity_id of the PRIMARY action target. Impure-input, pure.

    The PRIMARY action is the FIRST action that targets a concrete entity (per
    the design grouping rule: group by the entity the automation CHANGES, not
    the trigger source). Reads the modern `target:` block and the flat
    `entity_id` shape; a list entity_id resolves to its first member. Returns
    None when no action names an entity (e.g. a notify-only automation) — the
    caller then resolves area to None ⇒ cloud buckets under "Whole Home".
    """
    for act in actions or []:
        target = act.get("target") if isinstance(act.get("target"), dict) else {}
        entity_id = act.get("entity_id")
        if entity_id is None:
            entity_id = target.get("entity_id")
        if isinstance(entity_id, list):
            entity_id = entity_id[0] if entity_id else None
        if isinstance(entity_id, str) and entity_id:
            return entity_id
    return None


def _resolve_action_area(hass, actions: list[dict[str, Any]]) -> str | None:
    """Resolve the area_id of an automation's PRIMARY ACTION TARGET. Impure.

    Per the design grouping rule the tab groups by the room the automation
    AFFECTS — i.e. the area of the entity the action CHANGES, never the trigger
    source. Resolution order for the primary action's entity:
      1. an explicit `target.area_id` on the action wins (the user named an area
         directly — no entity to resolve);
      2. else the action's primary target entity → entity_registry → its own
         area_id, falling back to its device's area_id (same precedence as
         `__init__._build_entity_index`).
    Returns None when unresolvable (no entity / no area / registries
    unavailable) ⇒ cloud buckets the automation under "Whole Home".
    """
    # 1. explicit area on any action target wins.
    for act in actions or []:
        target = act.get("target") if isinstance(act.get("target"), dict) else {}
        area_id = target.get("area_id") or act.get("area_id")
        if isinstance(area_id, list):
            area_id = area_id[0] if area_id else None
        if isinstance(area_id, str) and area_id:
            return area_id

    # 2. resolve via the primary action target entity.
    entity_id = _first_action_entity_id(actions)
    if not entity_id:
        return None
    try:
        from homeassistant.helpers import (  # local — HA only
            device_registry as dr,
            entity_registry as er,
        )
    except ImportError:
        return None
    er_reg = er.async_get(hass)
    ent = er_reg.async_get(entity_id)
    if ent is None:
        return None
    if ent.area_id:
        return ent.area_id
    if ent.device_id:
        dr_reg = dr.async_get(hass)
        device = dr_reg.async_get(ent.device_id)
        if device is not None and device.area_id:
            return device.area_id
    return None


def _resolve_author(raw_config: dict[str, Any], automation_id: str) -> str:
    """Detect the iEMS author marker on an automation config. Impure-input, pure.

    The marker CONVENTION (documented in this module's header) — an automation
    HACS authors (#24, not yet built) is tagged either by:
      - an `id` prefixed with `IEMS_AUTHOR_ID_PREFIX` ("iems_"), or
      - a top-level `variables.iems_authored: true` in the config.
    Returns "iems" when EITHER signal is present, else "user". TODAY no iEMS-
    authored automations exist (the writer is slice #24), so this returns "user"
    for every current automation — but the reader is ready for when #24 lands.
    """
    if isinstance(automation_id, str) and automation_id.startswith(
        IEMS_AUTHOR_ID_PREFIX
    ):
        return "iems"
    variables = raw_config.get("variables")
    if isinstance(variables, dict) and variables.get("iems_authored") is True:
        return "iems"
    return "user"


def _as_list(value: Any) -> list[Any]:
    """Normalise a HA config block that may be a single dict or a list. PURE.

    HA automation `trigger`/`action` may be a single mapping OR a list of
    mappings; `condition` similarly. Returns a list either way; a None/missing
    block returns []. Non-dict members are dropped (defensive against malformed
    configs).
    """
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    return []


def _extract_automations(hass) -> list[dict[str, Any]]:
    """Read the user's HA automations into extractor dicts. Impure shell.

    Sources, all read IN-PROCESS (HACS runs inside HA — no WS round-trip):
      - the `automation` integration's EntityComponent (`hass.data['automation']`)
        for each automation's `raw_config` (id / alias / mode / trigger / action)
        and its `entity_id`;
      - `hass.states.get(entity_id)` for the live `state` (on/off → enabled) and
        the `last_triggered` attribute;
      - the entity + device + area registries to resolve the PRIMARY ACTION
        TARGET's area_id (the grouping axis).

    Returns a list of dicts in the shape `build_automations` consumes. NEVER
    raises into the caller — a missing automation component (no automations
    configured) or a malformed single config is logged and skipped, so the
    snapshot still publishes. The PURE `build_automations` does the final
    whitelist + summary + contract shaping.
    """
    component = hass.data.get("automation") if hasattr(hass, "data") else None
    entities = getattr(component, "entities", None)
    if not entities:
        return []

    out: list[dict[str, Any]] = []
    for ent in entities:
        try:
            raw_config = getattr(ent, "raw_config", None) or {}
            entity_id = getattr(ent, "entity_id", None)
            # unique_id is the automation's stable `id`; fall back to the
            # raw_config id, then the entity_id (last resort — still unique).
            automation_id = (
                getattr(ent, "unique_id", None)
                or raw_config.get("id")
                or entity_id
            )
            if automation_id is None:
                log.warning(
                    "setup snapshot: automation with no id (entity_id=%r); "
                    "skipping", entity_id,
                )
                continue

            state_obj = (
                hass.states.get(entity_id) if entity_id is not None else None
            )
            enabled = state_obj is not None and state_obj.state == "on"

            triggers = _as_list(
                raw_config.get("trigger", raw_config.get("triggers"))
            )
            actions = _as_list(
                raw_config.get("action", raw_config.get("actions"))
            )

            out.append(
                {
                    "id": automation_id,
                    "entity_id": entity_id,
                    "alias": raw_config.get("alias"),
                    "enabled": enabled,
                    "last_triggered": _last_triggered_iso_z(state_obj),
                    "mode": raw_config.get("mode"),
                    "area_id": _resolve_action_area(hass, actions),
                    "author": _resolve_author(raw_config, automation_id),
                    "triggers": triggers,
                    "actions": actions,
                }
            )
        except Exception as exc:  # noqa: BLE001 — one bad config must not
            # sink the whole snapshot; log + skip the offending automation.
            log.warning(
                "setup snapshot: skipping unreadable automation %r: %s: %s",
                getattr(ent, "entity_id", "<unknown>"),
                type(exc).__name__, exc,
            )
    return out


async def collect_setup_snapshot(
    hass,
    *,
    user_id: str,
    source_kind: str,
    entity_index: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Impure shell: gather inputs from a live HA instance + build the snapshot.

    Reads `hass.config`, the HA Energy Dashboard prefs, and the device
    registry, then delegates to the PURE `build_setup_snapshot`. The clock read
    for `ts` happens HERE (the impure boundary) so the pure builder stays
    deterministic.

    `entity_index` is the already-built per-entity registry snapshot the caller
    (`__init__.async_setup_entry`) constructs once via `_build_entity_index` and
    passes to the coordinator. We thread it through so the snapshot's
    `entity_classifications[]` is classified from the SAME index the telemetry
    whitelist uses — no second registry walk, no drift. `None` (not supplied)
    emits an empty `entity_classifications[]`.

    The HA automation registry is read here too (`_extract_automations`) and
    threaded into the optional, additive `automations[]` (Smart Home #21/#22).
    The extractor never raises — a home with no automations yields [], and the
    snapshot still publishes.

    The same entity_index is also passed as `entity_registry_index` to build
    the AI-builder `entity_registry[]` (Smart Home #24, contract v0.15.0):
    controllable-domain and named entities so the cloud can resolve user
    device names without guessing. Uses the SAME dict already built — no
    second registry walk.
    """
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    config = _extract_site_config(hass)
    energy_prefs = await _fetch_energy_prefs(hass)
    devices = _extract_devices(hass)
    automations = _extract_automations(hass)
    return build_setup_snapshot(
        user_id=user_id,
        config=config,
        energy_prefs=energy_prefs,
        devices=devices,
        source_kind=source_kind,
        ts=ts,
        entity_index=entity_index,
        automations=automations,
        # entity_registry_index uses the SAME entity_index (the coordinator's
        # per-entity registry snapshot) — the AI builder needs the same set
        # of entities the telemetry whitelist knows about: friendly names,
        # area names, and controllable-domain filtering all live in that dict.
        entity_registry_index=entity_index,
    )
