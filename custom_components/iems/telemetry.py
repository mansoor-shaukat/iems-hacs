"""Build telemetry + heartbeat payloads for iEMS cloud.

Schema: contracts/telemetry.schema.json (owned by CTO, read-only here).
Heartbeat shape: hacs_spec.md §3f.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from .classifier import VALID_CATEGORIES
from .const import MAX_ENTITIES_PER_BATCH, SCHEMA_VERSION, VERSION

# Attribute keys we strip on ingestion — HA UI noise, not semantic state.
ATTRIBUTE_STRIP_KEYS = frozenset({
    "friendly_name",
    "icon",
    "entity_picture",
    "supported_features",
    "assumed_state",
    "hidden",
    "editable",
    "restored",
})


class EmptyBatchError(ValueError):
    """Raised when build_batch is called with zero entities.

    Schema requires `entities` minItems: 1, so an empty batch must never
    be shipped.
    """


def _now_iso() -> str:
    """ISO-8601 UTC with trailing 'Z' per schema `format: date-time`."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# v0.2.4 (2026-05-26) — value-side JSON-safety pass on HA attribute dicts.
# v0.2.3 fixed the publisher-layer awscrt cancellation, exposing a deeper bug:
# `json.dumps(payload)` in iot_core.py raised `TypeError: Object of type set is
# not JSON serializable` because some HA integrations expose attribute values
# as `set` (Hue `effect_list`, Z-Wave `option_groups`, climate `hvac_modes`,
# etc.).  `_clean_attributes` only filtered KEYS; values flowed through raw.
# We now coerce every value to a JSON-safe shape, drop anything we can't
# round-trip (datetime, custom HA classes), and preserve falsy scalars
# (0/False/"") which are semantically meaningful in telemetry.
_JSON_SCALARS = (str, int, float, bool, type(None))


def _coerce_value(v: Any) -> Any:
    """Coerce an HA attribute value to a JSON-serializable shape.

    Returns None for values we cannot safely round-trip (datetime, custom
    classes, etc.).  Callers must distinguish "coerced to None" from the
    legitimate scalar `None` — see `_clean_attributes` below.
    """
    if isinstance(v, bool) or isinstance(v, _JSON_SCALARS):
        # bool first because bool is a subclass of int — order matters only
        # for documentation; both branches are JSON-safe.
        return v
    if isinstance(v, (set, frozenset)):
        # Sort by str() for determinism in snapshot/diff tests.  HA sets are
        # typically small (mode lists, effect lists), so the cost is trivial.
        return sorted(v, key=str)
    if isinstance(v, tuple):
        return [_coerce_value(x) for x in v]
    if isinstance(v, list):
        return [_coerce_value(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _coerce_value(val) for k, val in v.items()}
    # datetime, custom HA classes, etc. — drop.  Telemetry doesn't need them.
    return None


def _clean_attributes(attrs: dict[str, Any] | None) -> dict[str, Any] | None:
    if not attrs:
        return None
    cleaned: dict[str, Any] = {}
    for k, v in attrs.items():
        if k in ATTRIBUTE_STRIP_KEYS:
            continue
        coerced = _coerce_value(v)
        # Keep the field if (a) the value coerced to something non-None, or
        # (b) the original value was an explicit JSON scalar — which covers
        # legitimate None / False / 0 / "".  This drops only values that
        # _coerce_value rejected (datetime, custom classes, etc.).
        if coerced is not None or isinstance(v, _JSON_SCALARS):
            cleaned[k] = coerced
    return cleaned or None


def _validate_entity(e: dict) -> None:
    if e.get("category") not in VALID_CATEGORIES:
        raise ValueError(
            f"Entity {e.get('entity_id')!r} has invalid category "
            f"{e.get('category')!r}"
        )


def build_batch(
    *,
    user_id: str,
    entities: Iterable[dict],
    ha_version: str,
    instance_id: str | None = None,
    country: str | None = None,
    timezone: str | None = None,
) -> dict:
    """Build a `telemetry.schema.json`-conforming payload.

    Parameters
    ----------
    user_id
        Cognito sub (36-char UUID); goes into the payload and topic.
    entities
        Classified entity dicts. Required keys: entity_id, category, ts,
        state. Optional: brand, area, unit, attributes.
    ha_version
        Home Assistant core version string.
    instance_id
        HA instance UUID (optional; multi-install debugging).
    country
        ISO 3166-1 alpha-2 country code from HA core.config.country.
        Optional (v0.5.0); omitted from payload when None or empty.
    timezone
        HA core.config.time_zone string (e.g. 'Asia/Karachi').
        Optional (v0.5.0); omitted from payload when None or empty.

    Raises
    ------
    EmptyBatchError
        When `entities` is empty (schema requires minItems: 1).
    ValueError
        When len(entities) > MAX_ENTITIES_PER_BATCH or a category is not
        in VALID_CATEGORIES.
    """
    entity_list = list(entities)
    if not entity_list:
        raise EmptyBatchError("Cannot build a telemetry batch from zero entities")
    if len(entity_list) > MAX_ENTITIES_PER_BATCH:
        raise ValueError(
            f"Batch size {len(entity_list)} exceeds max {MAX_ENTITIES_PER_BATCH}"
        )

    out_entities: list[dict] = []
    for e in entity_list:
        _validate_entity(e)
        item: dict[str, Any] = {
            "entity_id": e["entity_id"],
            "category": e["category"],
            "ts": e["ts"],
            "state": e["state"],
        }
        if e.get("brand"):
            item["brand"] = e["brand"]
        if e.get("area"):
            item["area"] = e["area"]
        if e.get("unit"):
            item["unit"] = e["unit"]
        # Sprint 6 (v0.2.0): per-minute aggregation adds min/max/samples.
        # These pass through verbatim; the (proposed v0.6.0) contract defines
        # min/max as numbers and samples as integer >= 1.  Older v0.5.0
        # consumers ignore them — contract change is additive only.
        if "min" in e and e["min"] is not None:
            item["min"] = e["min"]
        if "max" in e and e["max"] is not None:
            item["max"] = e["max"]
        if "samples" in e and e["samples"] is not None:
            item["samples"] = e["samples"]
        cleaned = _clean_attributes(e.get("attributes"))
        if cleaned:
            item["attributes"] = cleaned
        out_entities.append(item)

    source: dict[str, Any] = {
        "integration_version": VERSION,
        "ha_version": ha_version,
    }
    if instance_id:
        source["instance_id"] = instance_id
    # v0.5.0 optional fields — only emit when HA has them configured.
    if country:
        source["country"] = country
    if timezone:
        source["timezone"] = timezone

    return {
        "schema_version": SCHEMA_VERSION,
        "user_id": user_id,
        "batch_id": str(uuid.uuid4()),
        "ts": _now_iso(),
        "source": source,
        "entities": out_entities,
    }


def build_heartbeat(
    *,
    user_id: str,
    ha_version: str,
    uptime_s: int,
    batches_sent: int,
    queue_depth: int,
    flush_rejects: int | None = None,
    accumulator_entity_count: int | None = None,
    accumulator_total_samples: int | None = None,
    finalised_minutes_pending: int | None = None,
    batch_loop_iterations: int | None = None,
    last_flush_iso: str | None = None,
    last_flush_row_count: int | None = None,
    last_publish_error: str | None = None,
    last_publish_payload_bytes: int | None = None,
    payload_too_large_count: int | None = None,
    client_error_disconnects: int | None = None,
    last_disconnect_reason: str | None = None,
    last_recovery: dict | None = None,
    last_self_update: dict | None = None,
) -> dict:
    """Heartbeat payload — shape per hacs_spec.md §3f.

    Published to `iems/{user_id}/heartbeat` at QoS 0 every 5 min (Sprint 6).
    Consumed by Priya's CloudWatch metric filter for liveness monitoring.

    v0.2.2 (2026-05-26): diagnostic counters added.  The 0.2.1 hotfix did NOT
    restore telemetry — heartbeat fires but PROFILE.last_seen_at stays frozen.
    These fields surface internal coordinator state on the heartbeat row so the
    cloud side can pinpoint where the pipeline is dying without HA access.

    All diagnostic fields are scalars (int / string / null).  They are emitted
    only when explicitly provided so older test paths that don't pass them keep
    working without an empty diagnostics block.
    """
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "user_id": user_id,
        "ts": _now_iso(),
        "integration_version": VERSION,
        "ha_version": ha_version,
        "uptime_s": int(uptime_s),
        "batches_sent": int(batches_sent),
        "queue_depth": int(queue_depth),
    }
    # Diagnostic counters — emitted when supplied (None means "not measured
    # this tick", null in JSON).  Int counters are coerced to int defensively.
    if flush_rejects is not None:
        payload["flush_rejects"] = int(flush_rejects)
    if accumulator_entity_count is not None:
        payload["accumulator_entity_count"] = int(accumulator_entity_count)
    if accumulator_total_samples is not None:
        payload["accumulator_total_samples"] = int(accumulator_total_samples)
    if finalised_minutes_pending is not None:
        payload["finalised_minutes_pending"] = int(finalised_minutes_pending)
    if batch_loop_iterations is not None:
        payload["batch_loop_iterations"] = int(batch_loop_iterations)
    # ISO timestamp + last publish error are nullable strings — explicit None
    # means "never happened yet" (e.g. flush() has not fired since start-up).
    payload["last_flush_iso"] = last_flush_iso
    if last_flush_row_count is not None:
        payload["last_flush_row_count"] = int(last_flush_row_count)
    payload["last_publish_error"] = last_publish_error
    # v0.2.6 (2026-05-27) — payload-size observability.  Surfaces
    # IotCorePublisher's pre-publish size guard so we can see
    # PAYLOAD_LIMIT_EXCEEDED rejections in DDB without broker logs.
    # `last_publish_payload_bytes` is the most-recent attempt size (any
    # topic).  `payload_too_large_count` is the cumulative rejection
    # counter since uptime.  Both emit only when iot_core actually
    # provides them (tests with MagicMock publish_fn omit).
    if last_publish_payload_bytes is not None:
        payload["last_publish_payload_bytes"] = int(last_publish_payload_bytes)
    if payload_too_large_count is not None:
        payload["payload_too_large_count"] = int(payload_too_large_count)
    # v0.2.6 — broker-rejection observability.  CLIENT_ERROR-class
    # disconnects (PAYLOAD_LIMIT_EXCEEDED, PROTOCOL_ERROR, etc.) bump
    # `client_error_disconnects`; `last_disconnect_reason` carries the
    # awscrt error string (truncated 200 chars) for diagnosis.
    if client_error_disconnects is not None:
        payload["client_error_disconnects"] = int(client_error_disconnects)
    if last_disconnect_reason is not None:
        payload["last_disconnect_reason"] = str(last_disconnect_reason)
    # v0.4.6 (2026-06-06) — data-recovery ack (Sprint 7).  Additive + nullable:
    # the cloud's heartbeat-consumer persists this onto the gap's
    # recovery_status / rows_found.  Shape per
    # docs/sprints/sprint_07/data_recovery_real_ha_check_spec.md §"Up ack":
    #   {window_id, start_ts, end_ts, result, rows_found, rows_published,
    #    completed_at}.  Emitted only when a recover attempt has run (None means
    # "no recover_window since start" — field omitted, heartbeat schema
    # otherwise UNCHANGED so older consumers are unaffected).
    if last_recovery is not None:
        payload["last_recovery"] = last_recovery
    # v0.5.13 (2026-07-02) — fleet self-update ack (Sprint 7 PoC).  Additive +
    # nullable, same carriage pattern as last_recovery: shape
    #   {result, from, to, command_id, completed_at, reason?}
    # where result ∈ {noop, self_update_started, error}.  Emitted only when a
    # self_update command has run since start-up (None → field omitted, so the
    # heartbeat schema is otherwise UNCHANGED and older consumers unaffected).
    # Post-restart ground truth of a completed update is integration_version
    # (HEARTBEAT.version) above, not this ack.
    if last_self_update is not None:
        payload["last_self_update"] = last_self_update
    return payload
