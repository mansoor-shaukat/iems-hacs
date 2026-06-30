"""Constants for the iEMS HACS integration."""

DOMAIN = "iems"
# Sprint 6 (2026-05-24): per-minute aggregation in HACS — material architecture
# change (was raw state_changed forwarding).  Bumping to 0.2.0 so support has a
# clean cut-line between "raw events" and "pre-aggregated minute rows".
# v0.3.0 (2026-05-31): per-category send-policy gating layered on top of
# per-minute aggregation.  Always / threshold / latest-only / skip-on-unavailable
# buckets per docs/architecture/send_policy.md.  No wire-shape change; gates
# WHICH rows enter the batch, not their structure.
# v0.3.1 (2026-06-01): move `meter.energy` (catch-all for voltage / current /
# frequency / PF / VA / VAR — fast-moving instantaneous electrical signals)
# from the threshold bucket to the Always bucket.  Cumulative kWh classifies
# as `sensor.energy` and stays threshold-gated.  Fixes silent suppression
# of `sensor.*_grid_l1_voltage` TS# rows that broke downstream grid-outage
# detection in staging.  See docs/architecture/send_policy.md update.
# v0.4.0 (2026-06-03): shipping-mode FSM (#9, ADR 0005).  The 30s telemetry
# publish path is now gated on `shipping_mode` (setup/paused suppress, active
# ships filtered to the whitelist).  Cloud commands transitions over the
# `iems/{user_id}/command` down-topic (QoS 1, persistent subscribe); HACS
# reconciles to cloud truth on reconnect via the /hacs/status pull.  No
# wire-shape change to telemetry; gates WHETHER a batch publishes + WHICH
# entities it carries.  Enforces "no telemetry until confirmed".
# v0.4.1 (2026-06-04): BOOTSTRAP FIX. 0.4.0 wedged HA bootstrap — the batch +
# heartbeat loops ran as FOREGROUND tasks (HA awaited them at the "wait for
# platforms" stage → "Setup timed out for bootstrap waiting on" → supervisor
# restart-loop), and the new setup-time cloud work (snapshot publish, command
# subscribe, /hacs/status reconcile) was awaited synchronously on a possibly
# slow/dead network. 0.4.1 moves the loops to entry.async_create_background_task
# (HA does NOT await background tasks), defers the onboarding network work to a
# background task, and puts a wait_for ceiling on credential exchange + IoT
# connect so a hung cloud call surfaces as ConfigEntryNotReady (retry) instead
# of wedging bootstrap. No wire-shape / FSM behavior change.
# v0.4.2 (2026-06-04): fresh-user-walk fixes — (1) reauth flow in config_flow
# (async_step_reauth/_confirm) so a rotated/revoked key re-prompts in place
# instead of forcing delete+re-add; (2) connection BUILDER moved to an executor
# (iot_core) so the awsiotsdk-METADATA filesystem reads stop tripping HA's
# loop-protection "Detected blocking call" warning; (3) config-flow copy fix
# "Account →" → "Settings →". No wire-shape / FSM / telemetry behavior change.
# v0.4.3 (2026-06-05): REAUTH SAME-ACCOUNT FIX. v0.4.2's reauth guard set the
# config-entry unique_id to a SHA-256 hash of the API KEY STRING, then asserted
# the re-entered key produced the same unique_id. A rotated/re-minted key for
# the SAME account is a different string → different hash → the same-account
# assertion ALWAYS failed, so reauth could never succeed (and the old key was
# already revoked). Fix: unique_id is now the RESOLVED ACCOUNT IDENTITY (Cognito
# identity_id) — the new helper `resolve_account_identity()` exchanges the key
# and returns the account it belongs to. A different KEY for the SAME account
# resolves to the same identity → reauth passes; a DIFFERENT-account key → still
# aborts (reauth_account_mismatch). Onboarding also resolves the identity now, so
# duplicate-install prevention is account-scoped (not key-string-scoped) and a
# revoked/garbage key is caught at the config-flow form (invalid_api_key /
# auth_failed / cannot_connect) instead of failing later at setup. No wire-shape
# / FSM / telemetry behavior change.
# v0.4.4 (2026-06-05): REAUTH MIGRATION FIX. v0.4.3 switched the config-entry
# unique_id from sha256(api_key)[:32] to the resolved Cognito identity_id, but
# never migrated EXISTING entries. Every pre-0.4.3 install still stored the old
# 32-hex hash (or a <=0.4.1 36-char UUID) — neither contains a colon, while an
# identity_id always does. Reauth's same-account guard compared identity(new key)
# vs the stored legacy hash → ALWAYS mismatched → "different account" abort on
# every existing install, even with a correct same-account key. Two-part fix:
# (1) setup-time heal in __init__ — when an entry loads with a legacy (no-colon)
# unique_id and the stored key still exchanges OK, migrate unique_id to the
# resolved identity_id (reusing the setup exchange, no extra network call), so a
# valid-key install never reaches reauth. (2) reauth legacy-branch in config_flow
# — when the stored unique_id is legacy (key already revoked, setup-heal never
# ran), accept any VALID new key and migrate unique_id to its identity; the
# one-way hash is not reversible to an identity so same-account can't be proven,
# and it is the user's own reauth (SECURITY trade-off, legacy entries only). The
# strict identity-format same-account abort is preserved unchanged. No wire-shape
# / FSM / telemetry / payload behavior change.
# v0.4.5 (2026-06-06): COLD-START LATENCY FIX. After config-entry setup the
# batch + heartbeat loops slept their FULL steady-state interval (300s) BEFORE
# the first iteration, so the first telemetry row and first heartbeat each
# landed ~5 minutes after start (measured on staging: restart 19:04 → first DDB
# row 19:09:47). Fix: the first iteration of each loop sleeps a short INITIAL_*
# delay (telemetry ~12s, heartbeat ~5s); every SUBSEQUENT iteration reverts to
# the unchanged 300s cadence. The first batch flush force-seals the current
# partial minute (flush(seal_current_minute=True)) so it actually ships rows
# instead of waiting for a natural minute boundary — that one cold-start row
# legitimately carries fewer `samples`. The late-arrival guard was tightened
# from strict `<` to `<=` so a force-sealed minute can NEVER be re-shipped by
# the next (300s) flush. Steady-state cadence (BATCH_WINDOW_SECONDS /
# HEARTBEAT_INTERVAL_SECONDS = 300s, cost/payload-tuned, CEO-locked) is
# UNCHANGED. No wire-shape / payload-composition / FSM change; chunk cap stays
# 200. SCHEMA_VERSION untouched.
# v0.4.6 (2026-06-06): DATA-RECOVERY "real HA check" (Sprint 7). New
# `recover_window` command on the EXISTING command down-topic: HACS queries HA's
# LOCAL recorder in-process (recorder.get_instance(hass).async_add_executor_job +
# history.get_significant_states — NO HA REST token, runs on the recorder
# executor thread so the event loop never blocks) for [start_ts, end_ts), replays
# the found rows through the production telemetry publish path (same classifier +
# build_batch + 200-row chunk cap + 128 KiB guard as steady-state, so the gap
# backfills with byte-identical wire format), and acks the truth HA returned on
# the heartbeat via a new nullable `last_recovery` field
# {window_id,start_ts,end_ts,result,rows_found,rows_published,completed_at}.
# result=no_data when the recorder returns zero rows (genuine HA-had-nothing) —
# this is how the cloud learns recoverability instead of guessing from duration.
# Recovery runs OFF the steady-state path: it never touches the per-minute
# accumulators, the 300s flush cadence, or the 0.4.5 cold-start fast-flush.
# Heartbeat schema_version unchanged (last_recovery is additive + nullable);
# telemetry wire-shape / chunk cap (200) / FSM all UNCHANGED.
# v0.4.8 (2026-06-07): RECOVER FALSE-SUCCESS FIX (Sprint 7,
# docs/followups/recover_false_success_2026-06-07.md). v0.4.6's recover_window
# queried the recorder with include_start_time_state=True across all ~146
# surfacing entities; when HA had NOTHING inside the window it STILL returned one
# carried-forward start-of-window State per entity (last_changed BEFORE start),
# and the capture loop counted every such boundary row toward rows_found +
# published it. Combined with result=recovered firing on rows_published>0, an
# unrecoverable gap (HA recorder genuinely empty for the window) reported
# "recovered, 4639 rows" off a single boundary row — same counter-is-not-evidence
# family as the 2026-05-27 telemetry incident (verified on CEO's prod home
# 066de039-2251, Jun-5 gap). Fix is in how rows are COUNTED/CAPTURED, NOT the
# query flags (include_start_time_state stays True — still useful for the
# boundary value): a recorder row counts as GENUINE in-window only when
# start_dt <= last_changed < end_dt (datetime compare, not the ISO string).
# Carried-forward boundary states (last_changed < start_dt) and end-boundary
# rows are excluded from BOTH rows_found and the published set. With no genuine
# in-window rows the result is now no_data (cloud maps no_data -> no_data_in_ha
# -> "Data unrecoverable"); a static-entity boundary state can never force
# "recovered". Recovery still runs OFF the steady-state path. No wire-shape /
# payload-composition / chunk-cap (200) / FSM / SCHEMA_VERSION change.
# v0.4.9 (2026-06-10): SETUP-SNAPSHOT entity_classifications (onboarding, issue
# #4 follow-up / #20). The setup snapshot now carries a top-level
# entity_classifications[] ({entity_id, category, friendly_name}) classified
# from the SAME entity_index + classifier.classify the telemetry whitelist uses
# (energy categories only — inverter.{pv,grid,load,battery}, battery.soc,
# meter.energy, sensor.{power,energy}; `other`/controllable/environment dropped
# to stay lean under the 128 KiB IoT limit, capped at 350 with a logged drop —
# worst-case full snapshot measured at ~95 KiB, ~36 KiB under the hard limit).
# Fixes a real fresh-user onboarding (CEO's home, 2026-06-10) that produced an
# EMPTY site model: the snapshot carried only ha_energy_prefs (EMPTY without a
# configured HA Energy Dashboard) + device-level device_registry_snapshot (no
# entity IDs), so the cloud Stage-2 classifier's energy-prefs + entity-keyword
# tiers were both empty → device-registry shape-only inference → correct shape,
# ZERO entities. The cloud classifier already reads entity_classifications[]
# (site_model_classifier handler.py Tier-2); HACS just never sent it. Setup
# snapshot is a distinct pre-confirmation payload — NO telemetry wire-shape /
# SCHEMA_VERSION (telemetry stays 0.6.0; snapshot stays contract const 0.13.0) /
# chunk-cap (200) / FSM change. Only the setup-topic payload composition grows.
# v0.5.1 (2026-06-14): COMMAND-SUBSCRIPTION RE-REGISTER ON REAUTH RECONNECT +
# recovery per-minute aggregation. (1) _build_and_connect now calls
# _reregister_subscriptions() so a freshly-BUILT awscrt connection (initial
# connect, the ~hourly credential-refresh reconnect via _reconnect_with_fresh_creds,
# or a publish-path reconnect) carries the iems/{id}/command callback. Root cause
# (broker-log confirmed): a credential-refresh rebuild made a brand-new awscrt
# Connection with an EMPTY native callback table and never re-subscribed; only
# awscrt's auto-resume (which REUSES its Connection object + fires
# on_connection_resumed) re-registered. With clean_session=False the broker kept
# the persistent-session subscription and kept DELIVERING commands, but awscrt had
# no local /command callback -> recover_window / take_setup_snapshot /
# set_shipping_mode were silently dropped while telemetry+heartbeat (publish-only)
# kept flowing. The re-register is a no-op on initial connect (subscriptions are
# recorded later) so it never double-subscribes; a resubscribe miss is logged,
# never raised, so it can't abort the connect. (2) recovery.py now folds replayed
# recorder rows into the coordinator's _MinuteAccumulator keyed by
# (entity_id, minute_floor) -- the SAME accumulator the live steady-state path uses
# -- so a recovered minute-row is BYTE-IDENTICAL in shape to a live one
# (numeric -> state=mean,min,max,samples; non-numeric -> latest passthrough). That
# routes ingestion to the single conditional put_item path (vs the legacy ~2.5x
# update_item path) and makes re-recovery self-idempotent. rows_found still counts
# genuine raw in-window rows; rows_published counts the aggregated minute-rows
# (published <= found by construction). PAYLOAD: recovery now emits FEWER rows of
# the already-measured live shape -- per-row byte size unchanged, row count only
# drops, so the 200-row chunk cap + 128 KiB guard headroom are unaffected. No
# telemetry wire-shape / SCHEMA_VERSION (stays 0.6.0) / chunk-cap (200) / FSM
# change. Patch release: bugfix + recovery write-efficiency, no contract change.
# v0.5.2: add rename_device command (first cloud->HA write; label-only name_by_user).
# v0.5.3: edge-PoC lamp control RETIRED (CEO 2026-06-28). The living-room lamp's
# grid-state control moved to a native HA automation so the lamp has ONE owner;
# HACS no longer starts the EdgePocOutageHandler (wiring disabled in __init__.py)
# and the handler is additionally guarded by LAMP_CONTROL_RETIRED. HACS never
# writes light.living_lamp. No telemetry/heartbeat/snapshot/dispatch/recovery
# change; no contract / SCHEMA_VERSION change. Reversible point release.
# v0.5.4 (2026-06-28): Smart Home automation toggle — enable_automation command
# (contracts/mqtt_topics.md v0.4.1, GitHub #23). Cloud sends enable_automation
# on the EXISTING command down-topic; HACS resolves id→entity_id IN-PROCESS
# via hass.data['automation'].entities (unique_id == stable id, same source as
# _extract_automations in snapshot.py) then calls automation.turn_on /
# automation.turn_off via hass.services.call(blocking=True). Unknown ids are
# logged and dropped (automation_not_found) — never crash the callback. No new
# MQTT topic, no IAM change. No wire-shape / SCHEMA_VERSION / FSM change.
# v0.5.5 (2026-06-28): Smart Home write + delete automation commands
# (contracts/mqtt_topics.md v0.4.2, GitHub #24 + #29). Cloud sends
# write_automation to create/update an automation config and delete_automation
# to remove one; both use the EXISTING command down-topic (no new MQTT topic,
# no IAM change). HACS applies them IN-PROCESS via HA's config automation
# StorageCollection (hass.data["automation_config"]) — the same storage HA's
# Lovelace automation editor uses; no external WS round-trip, no auth token
# required. After each write or delete the automation component is reloaded
# so the change takes effect immediately. write_automation is idempotent on
# draft_token: a duplicate draft_token is logged and dropped without a second
# write (per-instance set on CommandHandler). The cloud already stamps
# variables.iems_authored + an iems_ id prefix — HACS preserves both so the
# setup snapshot's _resolve_author marks these as "iems". An unknown
# automation_id in delete_automation is logged and no-op (never crash the
# callback). No wire-shape / SCHEMA_VERSION / FSM change.
# Payload write: {"action":"write_automation","automation_id":"<iems_id>",
#                 "draft_token":"<uuid>","automation":{...full HA config...}}
# Payload delete: {"action":"delete_automation","id":"<ha_automation_id>"}
# v0.5.6 (2026-06-30): HACS setup-snapshot entity_registry[] — Smart Home
# AI-builder fix (#24 first-run). The cloud AI draft prompt was failing on
# name-based targets ("lobby lamp") because LATEST# telemetry rows carry no
# friendly_name and entity_classifications[] is energy-only (no lights/
# switches). Fix: HACS now includes an entity_registry[] in every setup
# snapshot: {entity_id, friendly_name, area, domain} for CONTROLLABLE
# domains (light/switch/fan/cover/climate/scene/script/input_boolean/
# input_number/input_select/media_player/lock/vacuum/humidifier/water_heater/
# button/number/select) PLUS any entity carrying a non-empty friendly_name.
# Friendly name from the coordinator's existing entity_index meta["name"];
# area from meta["area"] (already resolved to human name by the coordinator).
# No new MQTT topic, no IAM change, no telemetry wire-shape change, no
# SCHEMA_VERSION change (field is optional/additive per the v0.15.0 contract).
# Capped at 500 entries with a loud log on truncation.
# v0.5.7 (2026-06-30): entity_registry[] TWO-BUG FIX.
# Bug 1 — friendly_name was the entity_id, not the real name. For
# MQTT-discovery entities (MTronic, Solarman, etc.) ent.name and
# ent.original_name are empty/None; the human label lives ONLY in HA state
# attributes under "friendly_name". _build_entity_index now reads
# hass.states.get(entity_id).attributes.get("friendly_name") FIRST, then
# falls back to ent.name/ent.original_name, then None (never entity_id).
# Bug 2 — number/select config knobs flooded the 500-entry cap, evicting
# real devices (switch.sp_146 "study lamp" was cut). The sort key in
# _build_entity_registry now uses a domain-tier: real device domains
# (light/switch/fan/cover/climate/lock/media_player/vacuum/humidifier/
# water_heater/scene/script) sort BEFORE config knobs (number/select/button/
# input_*), so the 500-entry cap always keeps real devices first.
# No new MQTT topic, no IAM change, no telemetry/SCHEMA_VERSION change.
VERSION = "0.5.8"

# Config entry keys — stored in the HA config entry, never logged
CONF_API_KEY = "api_key"
CONF_USER_ID = "user_id"
CONF_IOT_ENDPOINT = "iot_endpoint"
CONF_REGION = "region"

# API key — opaque prefixed token issued by the iEMS portal.
# Validation guards only against obvious malformed pastes; trusted
# validation is the server's response to the credential exchange.
#
# Format per Cloud-team spec (cognito_auth_flow.md §3.1):
#   iems_live_<26 chars of Crockford base32>
#   prefix (10) + body (26) = total length 36
# Body charset: [0-9a-z] (lowercase Crockford). 130 bits of entropy.
# Reserves iems_test_* for future test-mode keys (Sprint 3).
API_KEY_PREFIX = "iems_live_"
API_KEY_LENGTH = 36  # prefix(10) + body(26)
API_KEY_REGEX = r"^iems_live_[0-9a-z]{26}$"

# Timing
# Sprint 6 (2026-05-24): with per-minute aggregation in HACS the flush window
# moves from 30s → 300s (5 minutes).  Each flush carries up to 5 sealed
# minute-rows per entity.  Heartbeat cadence matches the flush so the publisher
# queue gets drained on the same tick.
BATCH_WINDOW_SECONDS = 300
HEARTBEAT_INTERVAL_SECONDS = 300

# v0.4.5 (2026-06-06) — cold-start latency fix.  The FIRST iteration of each
# background loop sleeps one of these short delays instead of the full
# steady-state interval, so first telemetry/heartbeat lands in seconds rather
# than ~5 minutes after coordinator.start().  Every SUBSEQUENT iteration reverts
# to BATCH_WINDOW_SECONDS / HEARTBEAT_INTERVAL_SECONDS — steady-state cadence is
# unchanged (cost/payload-tuned, CEO-locked).  Heartbeat fires first (5s) so the
# liveness/version signal lands before the first data flush; the batch delay
# (12s) gives newly-subscribed state_changed events a moment to populate the
# current-minute accumulators before the force-sealed first flush ships them.
INITIAL_HEARTBEAT_DELAY_SECONDS = 5
INITIAL_FLUSH_DELAY_SECONDS = 12

MAX_QUEUE_DEPTH = 10  # 10 batches @ 5min = 50 minutes of offline buffering
MQTT_CONNECT_TIMEOUT_SECONDS = 10
MQTT_PUBLISH_TIMEOUT_SECONDS = 10
BACKOFF_INITIAL_SECONDS = 1
BACKOFF_MAX_SECONDS = 60

# v0.2.3 (2026-05-26) — publish-layer retry on awscrt CANCELLED_FOR_CLEAN_SESSION.
# When awscrt auto-reconnects mid-flight (idle keep-alive miss, network blip),
# every in-flight publish future is cancelled with this error code.  The
# resumed connection is healthy, but the publish call already failed.  We
# retry up to MQTT_PUBLISH_RETRY_ATTEMPTS times with exponential backoff
# starting at MQTT_PUBLISH_RETRY_INITIAL_SECONDS so the next attempt lands
# on the resumed connection.  See docs/followups/
# freshness_signal_heartbeat_vs_telemetry_2026-05-26.md for the live
# DDB heartbeat row that surfaced this failure mode in production.
#
# v0.2.5 (2026-05-26) — bumped attempts 3 → 5.  Total retry window now
# ~15s (was ~7s).  v0.2.5 also flips clean_session=False so the broker
# queues across reconnects; the retry loop is now the INNER cushion for
# intra-flap failures, not the primary defence.  Two flaps inside ~7s
# was observed in production (114 finalised minutes pending after the
# v0.2.3 retry exhausted); 5 attempts × 1/2/4/4/4s = 15s cushion covers
# the wider flap window.
MQTT_PUBLISH_RETRY_ATTEMPTS = 5
MQTT_PUBLISH_RETRY_INITIAL_SECONDS = 1.0
MQTT_PUBLISH_RETRY_MAX_SECONDS = 4.0

# Credential refresh — refresh N seconds before STS expiry to avoid
# in-flight publish failures. Conservative 5-minute window per
# cognito_auth_flow.md §7 Q5.
CREDENTIAL_REFRESH_LEAD_SECONDS = 300

# iEMS cloud auth endpoint — the ONE hardcoded URL. Everything else
# (iot_endpoint, region, identity_pool_id) comes back in the
# /hacs-auth response, so we can shift regions without a client
# update. Dev stage for Sprint 2; custom-domain cutover is a
# separate Sprint 3 item.
IEMS_AUTH_URL = "https://mnrwhhjnuf.execute-api.eu-central-1.amazonaws.com/hacs-auth"
IEMS_AUTH_HTTP_TIMEOUT_SECONDS = 10

# #9 (ADR 0005) — HACS /hacs/status reconnect-reconcile endpoint.
# Pulled once on broker resume to recover a shipping-mode command the cloud
# may have published while HACS was offline + the persistent session expired.
# Sits on the same API-GW stage as /hacs-auth; authed with the same API key
# (the only auth mechanism HACS holds — see contract gap note in the #9 PR).
# CONTRACT GAP: the server-side endpoint + its exact auth contract are not yet
# locked. The client degrades to None on 404/error so wiring it now is safe.
IEMS_STATUS_URL = "https://mnrwhhjnuf.execute-api.eu-central-1.amazonaws.com/hacs-status"
IEMS_STATUS_HTTP_TIMEOUT_SECONDS = 10

# v0.4.1 (2026-06-04) — BOOTSTRAP-SAFETY ceiling on any cloud operation that
# runs inside async_setup_entry (credential exchange + IoT connect, and the
# deferred onboarding-wiring background task). A timeout here surfaces as
# ConfigEntryNotReady so HA retries with backoff instead of wedging bootstrap
# (the 0.4.0 supervisor restart-loop incident). Set slightly above the per-call
# HTTP/MQTT timeouts (10s) so one internal retry can complete before this outer
# ceiling trips.
SETUP_CLOUD_OP_TIMEOUT_S = 12.0

# Rate-limit backoff — per spec §7 Q4: 400/401 are permanent fails
# (no retry); 429 uses 30s→10min exponential; 5xx uses uncapped
# exponential starting at BACKOFF_INITIAL_SECONDS.
RATE_LIMIT_BACKOFF_INITIAL_SECONDS = 30
RATE_LIMIT_BACKOFF_MAX_SECONDS = 600

# Topic templates — per contracts/mqtt_topics.md in the monorepo.
# user_id comes from the auth provider, never hardcoded here.
TELEMETRY_TOPIC_TEMPLATE = "iems/{user_id}/telemetry"
HEARTBEAT_TOPIC_TEMPLATE = "iems/{user_id}/heartbeat"
# Onboarding v2 (#4, ADR 0005) — dedicated setup-snapshot up-topic. The ONE
# payload that flows pre-confirmation; distinct from telemetry. One-off,
# published on first install + on each take_setup_snapshot command.
SETUP_TOPIC_TEMPLATE = "iems/{user_id}/setup"
# Onboarding v2 (#4) — cloud→HACS shipping-mode + snapshot command down-topic.
COMMAND_TOPIC_TEMPLATE = "iems/{user_id}/command"

# ---------------------------------------------------------------------------
# Shipping-mode FSM (#9, ADR 0005) — the publish-gate state machine.
#
# Three explicit modes govern whether the 30s telemetry batch path publishes:
#   setup   — first install, before the user confirms anything. Setup snapshot
#             + heartbeat only. NO telemetry batches. Edge-PoC outage detection
#             is local-only and still runs (user not unprotected).
#   paused  — setup snapshot landed, cloud built a draft site_model, awaiting
#             the user's wizard confirmation. Same gating as setup.
#   active  — user confirmed the site_model. 30s telemetry batches publish,
#             filtered to the whitelisted entities only (non-whitelisted
#             entities never leave HA — the privacy posture in ADR 0005).
#
# Cloud is authoritative: it commands the transitions via MQTT on the command
# down-topic. HACS reconciles to the cloud's truth on reconnect via the
# /hacs/status pull. The default-on-fresh-install mode is `setup`.
# ---------------------------------------------------------------------------
SHIPPING_MODE_SETUP = "setup"
SHIPPING_MODE_PAUSED = "paused"
SHIPPING_MODE_ACTIVE = "active"

# Valid shipping modes — the command handler rejects anything outside this set.
VALID_SHIPPING_MODES = frozenset(
    {SHIPPING_MODE_SETUP, SHIPPING_MODE_PAUSED, SHIPPING_MODE_ACTIVE}
)

# Modes in which the 30s telemetry batch path is SUPPRESSED. `active` is the
# only mode that ships telemetry; everything else is snapshot + heartbeat only.
TELEMETRY_SUPPRESSED_MODES = frozenset({SHIPPING_MODE_SETUP, SHIPPING_MODE_PAUSED})

# Default shipping mode on a fresh install — first install starts in `setup`
# so no telemetry flows until the cloud commands `active` post-confirmation.
DEFAULT_SHIPPING_MODE = SHIPPING_MODE_SETUP

# Command actions on the down-topic (contracts/mqtt_topics.md §command).
COMMAND_ACTION_SET_SHIPPING_MODE = "set_shipping_mode"
COMMAND_ACTION_TAKE_SETUP_SNAPSHOT = "take_setup_snapshot"
# v0.4.6 (2026-06-06) — Data-recovery "real HA check" (Sprint 7).
# Cloud sends recover_window on the existing command down-topic; HACS queries
# HA's local recorder in-process for [start_ts, end_ts), replays the found rows
# via the normal telemetry publish path, and acks the truth on the heartbeat
# `last_recovery` field. NO new MQTT topic, NO IAM change.
# See docs/sprints/sprint_07/data_recovery_real_ha_check_spec.md.
COMMAND_ACTION_RECOVER_WINDOW = "recover_window"
# v0.5.2 (2026-06-27) — Devices Rename (contracts/mqtt_topics.md v0.4.0).
# Cloud sends rename_device on the EXISTING command down-topic; HACS applies it
# IN-PROCESS via HA's device-registry helper async_update_device(device_id,
# name_by_user=…) — the FIRST iEMS write INTO HA.  Label-only + reversible:
# changes the user-visible device name, NEVER entity_ids.  No new MQTT topic,
# no IAM change.  Payload {"action":"rename_device","device_id":"<ha_device_id>",
# "name_by_user":"<new name>"}.
COMMAND_ACTION_RENAME_DEVICE = "rename_device"
# v0.5.4 (2026-06-28) — Smart Home automation toggle (contracts/mqtt_topics.md
# v0.4.1, GitHub #23). Cloud sends enable_automation on the EXISTING command
# down-topic; HACS applies it IN-PROCESS via HA's automation service calls
# (automation.turn_on / automation.turn_off).  The command carries the
# automation stable `id` (NOT the entity_id slug); HACS resolves id→entity_id
# by scanning the automation EntityComponent in hass.data['automation'] (the
# same in-process source the setup snapshot uses — snapshot.py:_extract_automations).
# unique_id on each automation entity equals its stable id.  Reversible: only
# changes the enabled state, never touches entity_ids or automation config.
# No new MQTT topic, no IAM change.
# Payload: {"action":"enable_automation","id":"<ha_automation_id>","enabled":true|false}
COMMAND_ACTION_ENABLE_AUTOMATION = "enable_automation"
# v0.5.5 (2026-06-28) — Smart Home write + delete (contracts/mqtt_topics.md
# v0.4.2, GitHub #24 + #29). Both ride the existing command down-topic.
# write_automation creates or updates an automation IN-PROCESS via HA's config
# automation StorageCollection (hass.data["automation_config"]), idempotent on
# draft_token.  delete_automation removes an automation by id; unknown id =
# no-op (never crash the callback).
COMMAND_ACTION_WRITE_AUTOMATION = "write_automation"
COMMAND_ACTION_DELETE_AUTOMATION = "delete_automation"

# Schema — MUST match server-side ingestion validator version
SCHEMA_VERSION = "0.6.0"

# Schema-side cap on entities per batch. v0.6.0 bumped this 500 → 5000 in the
# contract to accommodate the larger per-batch row counts under the new 5-min
# aggregation cadence (HACS rewrite, 2026-05-24). The HACS-side const lagged
# until the 2026-05-26 production incident — see docs/followups/
# freshness_signal_heartbeat_vs_telemetry_2026-05-26.md.
MAX_ENTITIES_PER_BATCH = 5000

# Publish-side cap on entities per MQTT message.
#
# AWS IoT Core MQTT message size hard limit: 128 KiB (131072 bytes).
# Reference: https://docs.aws.amazon.com/general/latest/gr/iot-core.html
#            #message-broker-limits
#
# v0.2.6 (2026-05-27): LOWERED 700 → 200.  The original v0.2.1 sizing used
# ~180 bytes/row, but v0.2.4's `_clean_attributes` value coercion now
# PRESERVES set/tuple/dict attribute values that were previously dropped.
# Realistic measurement on a mixed inverter/MTronic/Hue/climate workload
# (scripts/size_realistic_batch.py) shows ~384 bytes/row average.  At 700
# rows the payload was ~262 KiB — broker silently rejected with
# Publish-In Failure reason=PAYLOAD_LIMIT_EXCEEDED, then disconnected
# with CLIENT_ERROR.  Tight reconnect-publish-reject loop was the
# root cause of the 2026-05-27 telemetry-dead incident (broker logs
# confirmed by Sarah).
#
# Chunk cap math at 200 rows:
#   200 * ~384 bytes = ~76.8 KiB worst-case observed
#   leaves ~50 KiB headroom for outlier-heavy entities (Z-Wave
#   `option_groups`, multi-zone climate, scene platforms)
#   well under the 100 KiB soft safety threshold and the
#   131072-byte hard broker limit
#
# Distinct from MAX_ENTITIES_PER_BATCH (schema cap, 5000) by design:
# the schema allows up to 5000 per batch logically, but a single MQTT
# message can't carry that.  v0.2.7 may revisit this with dynamic
# size-based chunking (see release notes); v0.2.6 ships the static
# lower cap as the immediate hotfix.
MAX_ENTITIES_PER_BATCH_PUBLISH = 200

# Hard limit per AWS IoT Core MQTT v3.1.1.  Used as a defensive guard in
# iot_core.publish() — any payload exceeding this is rejected pre-publish
# with PayloadTooLargeError (no broker round-trip, no reconnect storm).
# Set EXACTLY to the broker's documented limit so we never enqueue or
# retry a payload that we KNOW the broker will reject.
MQTT_MESSAGE_SIZE_HARD_LIMIT_BYTES = 131072

# ---------------------------------------------------------------------------
# v0.3.0 send-policy thresholds — CEO-locked 2026-05-31.
#
# Per docs/architecture/send_policy.md, slow-moving numeric signals (SoC,
# temperature, humidity, cumulative energy) only emit a per-minute TS# row
# when the *finalised* mean diverges from the entity's last-emitted value by
# at least the per-category threshold below.  Cuts ~40% of write volume off
# the always-emit-every-minute v0.2.0 baseline (cost model in the spec doc).
#
# Threshold values are FINAL pending real-world fleet data.  If a class of
# entity surfaces a false-negative (gate keeping users blind to a real
# signal), the fix is a one-line const tune here — not a wire-shape change.
#
# The constants live HERE (not inlined in coordinator.py) so tests can
# import them and so a future tune is one diff, not a hunt across files.
# ---------------------------------------------------------------------------

# Battery state-of-charge — emit on Δ ≥ 0.1 percentage points of SoC.
# Mansoor's 20 kWh battery moves ~3-4% per hour at typical discharge; 0.1%
# is well below the visible-on-chart threshold but above sensor noise.
SOC_DELTA_THRESHOLD_PCT = 0.1

# Indoor / outdoor temperature — emit on Δ ≥ 0.5°C.  HA climate entities
# self-report at this precision; smaller deltas are sensor wobble.
TEMPERATURE_DELTA_THRESHOLD_C = 0.5

# Humidity — emit on Δ ≥ 1%.  Slowest-moving environmental signal in the
# fleet; 1 percentage point is the chart-visible step.
HUMIDITY_DELTA_THRESHOLD_PCT = 1.0

# Cumulative energy meters (sensor.energy / meter.energy) — emit on
# Δ ≥ 1.0 kWh.  Cumulative counters monotonically increase; 1 kWh is the
# Sprint-7 reporting granularity for energy widgets.
ENERGY_DELTA_THRESHOLD_KWH = 1.0
