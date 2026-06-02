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
VERSION = "0.3.2"

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

# Rate-limit backoff — per spec §7 Q4: 400/401 are permanent fails
# (no retry); 429 uses 30s→10min exponential; 5xx uses uncapped
# exponential starting at BACKOFF_INITIAL_SECONDS.
RATE_LIMIT_BACKOFF_INITIAL_SECONDS = 30
RATE_LIMIT_BACKOFF_MAX_SECONDS = 600

# Topic templates — per contracts/mqtt_topics.md in the monorepo.
# user_id comes from the auth provider, never hardcoded here.
TELEMETRY_TOPIC_TEMPLATE = "iems/{user_id}/telemetry"
HEARTBEAT_TOPIC_TEMPLATE = "iems/{user_id}/heartbeat"

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
