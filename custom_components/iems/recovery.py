"""Data-recovery — in-process HA-recorder replay for a gap window.

Sprint 7 (2026-06-06) — "real HA-coverage check" feature
(docs/sprints/sprint_07/data_recovery_real_ha_check_spec.md).

Why
---
The cloud `/data-recovery` page can only *guess* whether a telemetry gap is
recoverable — it has no way to ask the user's Home Assistant whether the local
recorder still has the rows.  The recover action IS the check: the cloud sends a
`recover_window` command on the existing `iems/{user_id}/command` down-topic;
HACS queries HA's LOCAL recorder in-process for the window, replays whatever rows
it finds via the normal telemetry publish path (so the gap backfills itself), and
reports the truth HA returned on the heartbeat `last_recovery` ack.

Architecture — RUNS OFF THE STEADY-STATE PATH
---------------------------------------------
This module never touches the coordinator's per-minute accumulators, the 5-min
flush cadence, or the v0.4.5 cold-start fast-flush.  A recover attempt:

  1. Resolves the surfacing entity whitelist from the coordinator's entity_index
     (same classifier the live publish path uses, so wire-format whitelist parity
     holds — zero drift, exactly what HACS would publish).
  2. Queries HA's recorder via `recorder.get_instance(hass).async_add_executor_job`
     so the (blocking) SQLite read happens on the recorder's executor thread, NOT
     the HA event loop (respects the v0.4.2 blocking-call lesson).
  3. Classifies + captures the found rows with the SAME `classify` +
     `_classify_and_capture` shape the backfill scripts use, then builds batches
     via the production `build_batch` helper (same SCHEMA_VERSION, same
     VALID_CATEGORIES, same attribute strip, same 200-row chunk cap, same 128 KiB
     pre-publish guard inherited from the publisher's iot_core).
  4. Publishes each batch through the injected publisher's `publish_telemetry`
     — the exact path coordinator.flush() uses, so idempotency (fresh batch_id
     per chunk) and the persistent-session retry stack all apply unchanged.
  5. Records the outcome (`recovered` / `no_data` / `partial` / `error`) into
     `last_recovery`, which `coordinator.heartbeat_once()` reads onto the next
     heartbeat so the cloud learns the truth.

In-process recorder API (the make-or-break — FEASIBLE)
------------------------------------------------------
HA exposes a stable public history reader designed to run inside the recorder
executor:

    from homeassistant.components.recorder import get_instance
    from homeassistant.components.recorder.history import (
        get_significant_states,
    )

    instance = get_instance(hass)
    bound = functools.partial(
        get_significant_states, hass, start_dt, end_dt, entity_ids,
        include_start_time_state=True, significant_changes_only=False,
        minimal_response=False, no_attributes=True,
    )
    states = await instance.async_add_executor_job(bound)

`get_significant_states` returns `dict[entity_id, list[State|dict]]`.  We read
each State's `.state` (raw string) + `.last_changed` (timezone-aware datetime).
NO HA REST token is needed — we are running inside HA's own process.  Recovered
rows replay with no attributes (`no_attributes=True`) — same as the backfill
scripts, which set `attributes={}`; live attributes come from HA's in-memory
state, not the recorder.

Imports are LAZY (inside the query function) so the unit-test environment —
which has no `homeassistant` package installed (see __init__.py's guarded
import) — can mock the recorder entirely.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from .classifier import classify
from .const import MAX_ENTITIES_PER_BATCH_PUBLISH
from .telemetry import EmptyBatchError, build_batch

log = logging.getLogger("iems.recovery")

# Categories whose state must be a numeric float for cloud TS# writes to land.
# MUST mirror coordinator._NUMERIC_CATEGORIES + the backfill scripts so the
# wire-format of a recovered row is byte-identical to a live-published row.
_NUMERIC_CATEGORIES: frozenset[str] = frozenset({
    "inverter.pv",
    "inverter.battery",
    "inverter.grid",
    "inverter.load",
    "battery.soc",
    "sensor.power",
    "sensor.energy",
    "meter.energy",
    "sensor.temperature",
    "sensor.humidity",
})

# HA sentinels that are not real measurements — suppressed on capture (mirrors
# the backfill scripts' _classify_and_capture and the live unavailable path).
_SUPPRESSED_STATES: frozenset[str] = frozenset({"unavailable", "unknown", ""})

# Recovery result vocabulary — matches the heartbeat `last_recovery.result`
# enum in the spec (data_recovery_real_ha_check_spec.md §"Up ack").
RESULT_RECOVERED = "recovered"
RESULT_NO_DATA = "no_data"
RESULT_PARTIAL = "partial"
RESULT_ERROR = "error"


def _parse_iso_utc(s: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp. Accepts a trailing Z or +00:00."""
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dt_to_iso_z(dt: datetime) -> str:
    """Render a datetime as ISO-8601 UTC with a trailing Z (schema format)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_state(raw: str, category: str) -> float | str:
    """Coerce a raw recorder state string to float for numeric categories.

    Mirror of `coordinator._coerce_state` so a recovered row's `state` has the
    same type a live-published row would — the ingestion Lambda's TS# guard
    requires `isinstance(state, (int, float))`.
    """
    if category not in _NUMERIC_CATEGORIES:
        return raw
    try:
        return float(raw)
    except (ValueError, TypeError):
        return raw


def _classify_and_capture(
    *,
    entity_id: str,
    raw_state: str,
    ts_iso: str,
    meta: dict[str, Any],
) -> dict[str, Any] | None:
    """Classify a recorder row → captured-entity dict, or None if suppressed.

    Same shape + suppression rules as scripts/backfill/replay_ha_recorder.py
    `_classify_and_capture`, so a recovery batch is byte-identical to a backfill
    batch for the same source rows.
    """
    if raw_state is None or (isinstance(raw_state, str) and raw_state in _SUPPRESSED_STATES):
        return None
    classified = classify({
        "entity_id": entity_id,
        "domain": meta.get("domain") or entity_id.split(".", 1)[0],
        "platform": meta.get("platform"),
        "device_class": meta.get("device_class"),
        "unit": meta.get("unit"),
        "name": meta.get("name"),
    })
    if not classified.get("surface"):
        return None
    category: str = classified["category"]
    captured: dict[str, Any] = {
        "entity_id": entity_id,
        "category": category,
        "ts": ts_iso,
        "state": _coerce_state(str(raw_state), category),
    }
    if meta.get("brand"):
        captured["brand"] = meta["brand"]
    if meta.get("area"):
        captured["area"] = meta["area"]
    if meta.get("unit"):
        captured["unit"] = meta["unit"]
    return captured


async def _query_recorder(
    hass: Any,
    *,
    start_dt: datetime,
    end_dt: datetime,
    entity_ids: list[str],
) -> dict[str, list[Any]]:
    """Query HA's LOCAL recorder in-process for the window [start_dt, end_dt).

    Uses `recorder.get_instance(hass).async_add_executor_job(...)` so the
    blocking SQLite read runs on the recorder's executor thread, NEVER the HA
    event loop.  Returns `dict[entity_id, list[State]]` (HA's history shape).

    Imports are LAZY so the unit-test environment (no `homeassistant` package)
    can patch this function wholesale.  Any failure to import/query raises —
    the caller turns it into a `result="error"` ack rather than crashing the
    awscrt callback.
    """
    import functools

    from homeassistant.components.recorder import get_instance
    from homeassistant.components.recorder.history import get_significant_states

    instance = get_instance(hass)
    # async_add_executor_job takes positional args only, so bind the keyword
    # flags via functools.partial.  Using KEYWORDS (not positionals) for the
    # optional flags makes the call robust to the minor argument-order drift in
    # HA's get_significant_states signature across the 2023.x–2026.x range:
    #   get_significant_states(hass, start_time, end_time=None, entity_ids=None,
    #     filters=None, include_start_time_state=True,
    #     significant_changes_only=True, minimal_response=False,
    #     no_attributes=False, compressed_state_format=False)
    #
    #   include_start_time_state=True  — capture the value as the gap opened
    #   significant_changes_only=False — replay ALL recorded changes, not just
    #                                    HA's "significant" subset
    #   minimal_response=False         — we need the raw state strings
    bound = functools.partial(
        get_significant_states,
        hass,
        start_dt,
        end_dt,
        entity_ids,
        include_start_time_state=True,
        significant_changes_only=False,
        minimal_response=False,
        no_attributes=True,
    )
    return await instance.async_add_executor_job(bound)


def _row_last_changed_dt(state_obj: Any) -> datetime | None:
    """Extract a timezone-aware UTC `datetime` from a recorder State (or dict) row.

    Returns the row's `last_changed` as an aware UTC `datetime`, or None if it
    can't be parsed.  This is the basis for the GENUINE-in-window filter in
    `recover_window`: a row whose `last_changed` is BEFORE the window start is a
    carried-forward `include_start_time_state` boundary value (HA's last-known
    state as the gap opened), NOT real in-window history.  We compare the parsed
    datetime — never the ISO string — so the boundary test is exact.
    """
    last_changed = getattr(state_obj, "last_changed", None)
    if last_changed is None and isinstance(state_obj, dict):
        last_changed = state_obj.get("last_changed")
    if isinstance(last_changed, datetime):
        if last_changed.tzinfo is None:
            last_changed = last_changed.replace(tzinfo=timezone.utc)
        return last_changed.astimezone(timezone.utc)
    if isinstance(last_changed, (int, float)):
        return datetime.fromtimestamp(last_changed, tz=timezone.utc)
    if isinstance(last_changed, str):
        try:
            return _parse_iso_utc(last_changed)
        except (ValueError, TypeError):
            return None
    return None


def _row_ts_iso(state_obj: Any) -> str | None:
    """Extract an ISO-Z timestamp from a recorder State (or dict) row."""
    dt = _row_last_changed_dt(state_obj)
    return _dt_to_iso_z(dt) if dt is not None else None


def _row_state(state_obj: Any) -> Any:
    """Extract the raw state value from a recorder State (or dict) row."""
    if isinstance(state_obj, dict):
        return state_obj.get("state")
    return getattr(state_obj, "state", None)


def _chunk(seq: list[dict], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


class RecoveryManager:
    """Handles `recover_window` — in-process recorder replay for a gap window.

    Injected dependencies keep unit tests free of real HA / MQTT:
      - `hass`        — passed straight to the recorder query (mocked in tests).
      - `user_id`     — Cognito identity_id, stamped into each batch.
      - `entity_index`— the coordinator's registry snapshot; drives the
                        classifier whitelist + per-entity enrichment (brand/area).
      - `publisher`   — exposes `async publish_telemetry(payload) -> bool`
                        (the production TelemetryPublisher); recovery rides the
                        SAME path coordinator.flush() uses.
      - `set_last_recovery` — callback that stashes the ack dict where
                        `coordinator.heartbeat_once()` can read it.
      - `query_recorder` — overridable for tests (defaults to the in-process
                        recorder reader).
    """

    def __init__(
        self,
        *,
        hass: Any,
        user_id: str,
        entity_index: dict[str, dict[str, Any]],
        publisher: Any,
        set_last_recovery: Callable[[dict[str, Any]], None],
        query_recorder: Callable[..., Any] | None = None,
    ) -> None:
        self._hass = hass
        self._user_id = user_id
        self._entity_index = entity_index
        self._publisher = publisher
        self._set_last_recovery = set_last_recovery
        self._query_recorder = query_recorder or _query_recorder

    def _surfacing_entity_ids(self) -> list[str]:
        """The entity whitelist to pull history for — exactly what HACS surfaces.

        Pre-classify every registry entry once (HA's recorder readers want the
        entity-id list up front) and keep only the ones the live classifier
        would publish.  Zero drift from the steady-state whitelist.
        """
        out: list[str] = []
        for eid, meta in self._entity_index.items():
            classified = classify({
                "entity_id": eid,
                "domain": meta.get("domain") or eid.split(".", 1)[0],
                "platform": meta.get("platform"),
                "device_class": meta.get("device_class"),
                "unit": meta.get("unit"),
                "name": meta.get("name"),
            })
            if classified.get("surface"):
                out.append(eid)
        return out

    async def recover_window(
        self, *, window_id: str, start_ts: str, end_ts: str
    ) -> dict[str, Any]:
        """Replay the recorder rows for [start_ts, end_ts) and report the ack.

        Returns the `last_recovery` ack dict (also pushed via
        `set_last_recovery`).  NEVER raises — any failure is captured as an
        `error` ack so the awscrt callback survives (the command handler relies
        on this invariant).
        """
        try:
            start_dt = _parse_iso_utc(start_ts)
            end_dt = _parse_iso_utc(end_ts)
        except (ValueError, TypeError) as exc:
            return self._finish(
                window_id=window_id, start_ts=start_ts, end_ts=end_ts,
                result=RESULT_ERROR, rows_found=0, rows_published=0,
                detail=f"bad window timestamps: {type(exc).__name__}: {exc}",
            )
        if not (start_dt < end_dt):
            return self._finish(
                window_id=window_id, start_ts=start_ts, end_ts=end_ts,
                result=RESULT_ERROR, rows_found=0, rows_published=0,
                detail="start_ts must be strictly before end_ts",
            )

        entity_ids = self._surfacing_entity_ids()
        if not entity_ids:
            # No surfacing entities at all — nothing HA could have for us.
            return self._finish(
                window_id=window_id, start_ts=start_ts, end_ts=end_ts,
                result=RESULT_NO_DATA, rows_found=0, rows_published=0,
                detail="no surfacing entities in registry",
            )

        # --- Query the in-process recorder (executor thread) ----------------
        try:
            history = await self._query_recorder(
                self._hass,
                start_dt=start_dt,
                end_dt=end_dt,
                entity_ids=entity_ids,
            )
        except Exception as exc:  # noqa: BLE001 — recorder absent/erroring → error ack
            log.error(
                "recover_window %s: recorder query failed: %s: %s",
                window_id, type(exc).__name__, exc,
            )
            return self._finish(
                window_id=window_id, start_ts=start_ts, end_ts=end_ts,
                result=RESULT_ERROR, rows_found=0, rows_published=0,
                detail=f"recorder query failed: {type(exc).__name__}",
            )

        # --- Classify + capture the found rows ------------------------------
        # CRITICAL (recover_false_success_2026-06-07): the recorder query uses
        # include_start_time_state=True, so for a window where HA has NOTHING
        # inside [start_dt, end_dt) it STILL returns one carried-forward
        # start-of-window State per entity (its last_changed is BEFORE start_dt).
        # Those synthetic boundary states are NOT genuine in-window history — if
        # we counted them toward rows_found or published them, an unrecoverable
        # gap would falsely score thousands "found" and flip the card to
        # "recovered" off a single boundary row.  So: a row counts as GENUINE
        # only when start_dt <= last_changed < end_dt (datetime compare, not the
        # ISO string).  Boundary states (last_changed < start_dt) are excluded
        # from rows_found AND from the captured/published set.
        captured: list[dict] = []
        rows_found = 0  # GENUINE in-window rows only (excludes boundary states)
        boundary_skipped = 0  # carried-forward include_start_time_state rows
        for entity_id, state_list in (history or {}).items():
            meta = self._entity_index.get(entity_id) or {}
            for state_obj in state_list or []:
                raw_state = _row_state(state_obj)
                last_changed = _row_last_changed_dt(state_obj)
                if raw_state is None or last_changed is None:
                    continue
                # Exclude the synthetic include_start_time_state boundary value
                # (and anything outside the window) — only true interior history
                # proves the gap is recoverable.
                if not (start_dt <= last_changed < end_dt):
                    boundary_skipped += 1
                    continue
                ts_iso = _dt_to_iso_z(last_changed)
                rows_found += 1
                item = _classify_and_capture(
                    entity_id=entity_id,
                    raw_state=raw_state,
                    ts_iso=ts_iso,
                    meta=meta,
                )
                if item is not None:
                    captured.append(item)

        if rows_found == 0:
            # No GENUINE in-window rows — HA had nothing for the window itself
            # (boundary_skipped carried-forward states don't count).  This is the
            # honest "Data unrecoverable" truth the cloud maps to no_data_in_ha.
            log.info(
                "recover_window %s: NO_DATA — 0 genuine in-window rows "
                "(%d carried-forward boundary states excluded)",
                window_id, boundary_skipped,
            )
            return self._finish(
                window_id=window_id, start_ts=start_ts, end_ts=end_ts,
                result=RESULT_NO_DATA, rows_found=0, rows_published=0,
            )
        if not captured:
            # HA had genuine in-window rows but every one was a suppressed
            # sentinel (unavailable/unknown) / non-surfacing — still "no usable
            # data" from the product's point of view.
            return self._finish(
                window_id=window_id, start_ts=start_ts, end_ts=end_ts,
                result=RESULT_NO_DATA, rows_found=rows_found, rows_published=0,
            )

        # --- Build chunked batches + publish via the live telemetry path ----
        ha_version = getattr(getattr(self._hass, "config", None), "version", "unknown")
        country = getattr(getattr(self._hass, "config", None), "country", None) or None
        tz = getattr(getattr(self._hass, "config", None), "time_zone", None)
        tz = str(tz) if tz else None

        rows_published = 0
        publish_failures = 0
        for chunk in _chunk(captured, MAX_ENTITIES_PER_BATCH_PUBLISH):
            try:
                payload = build_batch(
                    user_id=self._user_id,
                    entities=chunk,
                    ha_version=ha_version,
                    country=country,
                    timezone=tz,
                )
            except EmptyBatchError:
                continue
            except ValueError as exc:
                # Classifier drift / oversized chunk — log loud, count as failure.
                publish_failures += len(chunk)
                log.error(
                    "recover_window %s: build_batch rejected chunk (%d rows): %s",
                    window_id, len(chunk), exc,
                )
                continue
            try:
                ok = await self._publisher.publish_telemetry(payload)
            except Exception as exc:  # noqa: BLE001 — publish failure → partial ack
                publish_failures += len(chunk)
                log.error(
                    "recover_window %s: publish failed for chunk (%d rows): %s: %s",
                    window_id, len(chunk), type(exc).__name__, exc,
                )
                continue
            if ok:
                rows_published += len(chunk)
            else:
                # Publisher caught + enqueued the batch (transient) — it WILL
                # drain on a later heartbeat, but we can't honestly claim those
                # rows landed yet, so they count as a partial.
                publish_failures += len(chunk)
                log.warning(
                    "recover_window %s: publish returned falsy for chunk "
                    "(%d rows) — enqueued, counted partial",
                    window_id, len(chunk),
                )

        # Result rule (recover_false_success_2026-06-07) — by this point
        # `captured`/`rows_published` contain ONLY genuine in-window rows; the
        # carried-forward boundary states were already excluded above, so a
        # static-entity boundary value can never force "recovered":
        #   - rows_published == 0 + failures  -> error    (genuine rows existed
        #                                                   but publish failed)
        #   - rows_published == 0, no failures -> no_data  (nothing usable to
        #                                                   publish)
        #   - some published, some failed/enqueued -> partial (coverage is
        #                                                   incomplete — don't
        #                                                   claim the gap filled)
        #   - all genuine rows published cleanly    -> recovered
        if rows_published == 0:
            result = RESULT_ERROR if publish_failures else RESULT_NO_DATA
        elif publish_failures:
            result = RESULT_PARTIAL
        else:
            result = RESULT_RECOVERED

        return self._finish(
            window_id=window_id, start_ts=start_ts, end_ts=end_ts,
            result=result, rows_found=rows_found, rows_published=rows_published,
        )

    def _finish(
        self,
        *,
        window_id: str,
        start_ts: str,
        end_ts: str,
        result: str,
        rows_found: int,
        rows_published: int,
        detail: str | None = None,
    ) -> dict[str, Any]:
        """Build the `last_recovery` ack, push it to the heartbeat slot, log."""
        ack: dict[str, Any] = {
            "window_id": window_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "result": result,
            "rows_found": int(rows_found),
            "rows_published": int(rows_published),
            "completed_at": _now_iso_z(),
        }
        if result == RESULT_RECOVERED:
            log.info(
                "recover_window %s: RECOVERED %d rows (found %d)",
                window_id, rows_published, rows_found,
            )
        elif result == RESULT_NO_DATA:
            log.info(
                "recover_window %s: NO_DATA (HA had %d raw rows, 0 usable/published)",
                window_id, rows_found,
            )
        else:
            log.warning(
                "recover_window %s: %s (found=%d published=%d)%s",
                window_id, result, rows_found, rows_published,
                f" — {detail}" if detail else "",
            )
        try:
            self._set_last_recovery(ack)
        except Exception as exc:  # noqa: BLE001 — never let the ack-store break the callback
            log.error(
                "recover_window %s: failed to store last_recovery ack: %s: %s",
                window_id, type(exc).__name__, exc,
            )
        return ack
