"""IoT Core MQTT adapter — SigV4 WebSocket publish using temp STS creds.

Implements the Cognito DAI flow wired by auth.py:
  - Credentials come exclusively from auth_provider.get_credentials()
  - ClientId == Cognito Identity sub (enforced by Priya's IoT policy)
  - Topics per contracts/mqtt_topics.md:
      iems/{user_id}/telemetry  QoS 1
      iems/{user_id}/heartbeat  QoS 0
  - On ExpiredTokenException the connection is torn down and rebuilt
    with a fresh credential exchange.
  - awsiot MqttClientConnection + awscrt SigV4 WebSocket signing.

No cert files. No hardcoded endpoints. No hardcoded user IDs.
All routing data flows from auth_provider.get_credentials().
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from .auth import IemsAuthProvider
from .const import (
    MQTT_CONNECT_TIMEOUT_SECONDS,
    MQTT_MESSAGE_SIZE_HARD_LIMIT_BYTES,
    MQTT_PUBLISH_RETRY_ATTEMPTS,
    MQTT_PUBLISH_RETRY_INITIAL_SECONDS,
    MQTT_PUBLISH_RETRY_MAX_SECONDS,
    MQTT_PUBLISH_TIMEOUT_SECONDS,
)

log = logging.getLogger("iems.iot_core")


class PayloadTooLargeError(Exception):
    """Raised pre-publish when payload exceeds AWS IoT Core's 128 KiB MQTT limit.

    v0.2.6 (2026-05-27).  Sized payloads that we KNOW the broker will reject
    must not enter the publish path — pre-v0.2.6 they were sent, the broker
    rejected with Publish-In Failure reason=PAYLOAD_LIMIT_EXCEEDED, then
    disconnected with CLIENT_ERROR.  awscrt auto-reconnected and HACS
    retried the same oversized payload, producing a tight reconnect-publish-
    reject loop while heartbeats (small) sailed through.  That was the root
    cause of the 2026-05-27 telemetry-dead incident.

    This exception is raised pre-flight (before any broker round-trip) and
    is NOT in the retry whitelist nor the publisher-queue catch tuple — the
    coordinator drops the batch loudly.  Data loss on this one oversized
    payload is preferable to a reconnect storm that masks every subsequent
    publish.

    The right fix is upstream: cap chunk size in coordinator.flush() so the
    payload never gets near the limit.  This guard is the belt-and-braces
    that catches any miss in that sizing (e.g. an entity with an outlier
    attribute payload that pushes the chunk over the line).

    Reference: AWS IoT Core message broker limits
    https://docs.aws.amazon.com/general/latest/gr/iot-core.html#message-broker-limits
    """

# v0.2.3 (2026-05-26) — substring-match these awscrt error tokens to decide a
# publish attempt is retry-eligible.  We match on str(exc) because awscrt
# raises a single AwsCrtError exception class with a `.name` attribute that
# carries the symbolic code; matching the substring is cheap, version-tolerant,
# and survives the awscrt python binding's habit of mutating attribute names
# across minor releases.  Order matters only for the log line — all entries
# trigger the same retry path.
#
# Live evidence (2026-05-26 HEARTBEAT row):
#   AwsCrtError: AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION:
#     Old requests from the previous session are cancelled,
#     and offline request will not be accept.
#
# We also include the broader CANCELLED token and the offline-request token
# because the same root cause (publish issued in a reconnect window) can
# surface as either depending on which side of the resume callback we land on.
#
# v0.2.7 (2026-05-28) — added AWS_ERROR_MQTT_CONNECTION_DESTROYED.  The
# publisher-layer queue (publisher.py `_is_awscrt_error`) already absorbs
# this token, so it was never data-loss; but the inner publish-side retry
# loop recovers faster (one short sleep on the same coroutine) than the
# outer queue-and-drain-next-heartbeat cycle (up to 5 min round-trip).
# Observed in the live HA log when awscrt tears down the connection
# struct after a session-level fault; reconnect callback usually fires
# within ~1s and the retry lands on the new connection cleanly.
_RETRYABLE_AWSCRT_TOKENS: tuple[str, ...] = (
    "AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION",
    "AWS_ERROR_MQTT_CONNECTION_DISCONNECTING",
    "AWS_ERROR_MQTT_NOT_CONNECTED",
    "AWS_ERROR_MQTT_CONNECTION_DESTROYED",
)


def _is_retryable_awscrt_error(exc: BaseException) -> bool:
    """Return True if exc is a known transient awscrt publish failure.

    The publisher-layer retry catches these and re-issues the publish after
    a short sleep so the next attempt can land on a resumed connection.
    Caller is responsible for the sleep + attempt counting; this helper is
    a pure predicate so tests can assert classification independently from
    timing behaviour.
    """
    exc_name = type(exc).__name__
    if exc_name != "AwsCrtError":
        return False
    msg = str(exc)
    return any(token in msg for token in _RETRYABLE_AWSCRT_TOKENS)


class IotCorePublisher:
    """Adapter for AWS IoT Core MQTT publish with auth-provider-driven creds.

    Thread-safety: all public methods are coroutines that run in the HA
    asyncio event loop. awscrt callbacks are dispatched on a thread pool
    and wrapped with asyncio.wrap_future / loop.call_soon_threadsafe so
    we never block the event loop.
    """

    def __init__(self, *, auth_provider: IemsAuthProvider) -> None:
        self._auth = auth_provider
        self._connection: Any | None = None  # awsiot.MqttClientConnection
        self._connected = False
        self._connect_lock = asyncio.Lock()
        # #9 (ADR 0005) — optional async hook fired on every broker resume
        # (reconnect).  Set by __init__.py to run the reconnect safety net:
        # resubscribe to the command topic + pull /hacs/status to reconcile
        # the shipping mode to the cloud's truth.  Signature: `async () -> None`.
        # None = no hook (publish-only deployments).
        self._on_resume = None
        # Captured at connect() time; reused by awscrt threadpool callbacks
        # to post state changes back to HA's asyncio loop via call_soon_threadsafe.
        self._event_loop: asyncio.AbstractEventLoop | None = None
        # v0.2.6 (2026-05-27) — payload-size observability + rejection counter.
        # Surfaced through the heartbeat so DDB tells us "we tried to publish X
        # bytes; broker hard limit is 128 KiB" without needing broker logs or
        # HA log access.  Reset only on integration reload, NOT on reconnect.
        self._last_publish_payload_bytes: int = 0
        self._payload_too_large_count: int = 0
        # v0.2.6 — observability for broker-side disconnects that look like
        # publish-rejection signals (CLIENT_ERROR / PROTOCOL_ERROR family).
        # Pre-v0.2.6, these were logged as transient "connection interrupted"
        # and awscrt auto-reconnected, hiding the broker rejection.  This
        # counter surfaces them in the heartbeat row so we notice broker-side
        # rejections we don't currently catch with a specific guard.  See
        # `_on_interrupted` for the classification tokens.
        self._client_error_disconnects: int = 0
        self._last_disconnect_reason: str | None = None
        # #9 (ADR 0005) — command down-topic subscriptions.  topic -> {qos,
        # message_handler}.  Stored so they can be replayed on reconnect
        # (resubscribe_all): clean_session=False means the broker SHOULD
        # restore subscriptions, but if the session expired (>1h offline) the
        # subscription is gone, so we re-issue defensively on every resume.
        self._subscriptions: dict[str, dict[str, Any]] = {}
        # Strong references to fire-and-forget tasks scheduled from awscrt
        # threadpool callbacks (command dispatch + on-resume hook).  CPython's
        # loop.create_task keeps only a WEAK reference, so without retaining
        # the Task here the GC can collect + cancel it mid-flight before the
        # coroutine completes — the exact v0.1.14 P0 failure mode.  Each task
        # add_done_callback(discard)s itself when it finishes so the set never
        # grows unbounded.
        self._bg_tasks: set[asyncio.Task] = set()

    @property
    def last_publish_payload_bytes(self) -> int:
        """Size in bytes of the most recent publish attempt (any topic, any QoS).

        Set in `publish()` after JSON-encoding the payload, BEFORE the
        size-limit check.  A value above MQTT_MESSAGE_SIZE_HARD_LIMIT_BYTES
        means the last publish was rejected pre-flight with
        PayloadTooLargeError — cross-check against
        `payload_too_large_count`.
        """
        return self._last_publish_payload_bytes

    @property
    def payload_too_large_count(self) -> int:
        """Total count of publish attempts rejected by the size guard since uptime.

        Non-zero is a signal that coordinator-side chunking is undersized:
        either MAX_ENTITIES_PER_BATCH_PUBLISH is too high for the current
        attribute mix, or some entity has an outlier attribute payload
        pushing the chunk over the line.
        """
        return self._payload_too_large_count

    @property
    def client_error_disconnects(self) -> int:
        """Count of broker disconnects classified as rejection-shaped (CLIENT_ERROR).

        Bumped by `_on_interrupted` when the awscrt error string matches a
        known broker-side rejection signal (CLIENT_ERROR / PROTOCOL_ERROR
        / SERVER_INVALID_DATA).  Heartbeat surfaces this so silent broker
        rejections don't hide behind awscrt's "transient interruption +
        auto-reconnect" abstraction (which was the 2026-05-27 root-cause
        masking pattern — broker disconnected with CLIENT_ERROR every
        oversized publish, awscrt called it transient).
        """
        return self._client_error_disconnects

    @property
    def last_disconnect_reason(self) -> str | None:
        """Most recent disconnect error string (truncated to 200 chars).

        None until the first interruption.  Pairs with
        `client_error_disconnects` for diagnosis in DDB.
        """
        return self._last_disconnect_reason

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Establish the MQTT-over-WSS connection to IoT Core.

        Uses temporary STS credentials from auth_provider to sign the
        WebSocket URL (SigV4). ClientId == Cognito Identity sub so the
        IoT policy allows the connect.
        """
        async with self._connect_lock:
            if self._connected:
                return
            await self._build_and_connect()

    async def disconnect(self) -> None:
        """Close the MQTT connection if open."""
        self._connected = False
        conn = self._connection
        self._connection = None
        if conn is not None:
            try:
                disconnect_future = conn.disconnect()
                loop = asyncio.get_event_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(None, disconnect_future.result),
                    timeout=MQTT_CONNECT_TIMEOUT_SECONDS,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort teardown
                log.warning("iot_core: disconnect error (ignored): %s", exc)

    async def publish(self, *, topic: str, payload: dict, qos: int) -> bool:
        """Publish a JSON payload to the given topic.

        Returns True on broker ACK. On connection-drop (network blip,
        keep-alive miss, broker-side hangup) silently rebuilds the
        connection with the same credentials before the attempt. On
        ExpiredTokenException tears the connection down and
        re-authenticates.

        Args:
            topic: Full MQTT topic string (must start with iems/{user_id}/).
            payload: Dict that will be JSON-serialised.
            qos:    0 (fire-and-forget) or 1 (at-least-once with ACK).

        Raises:
            RuntimeError: if publish fails for a non-recoverable reason.
            asyncio.TimeoutError: if broker ACK times out after reconnect.
        """
        # If the awscrt on_connection_interrupted callback flipped us to
        # disconnected, transparently re-establish before publishing.
        # This replaces the old "raise RuntimeError" path which dropped
        # every batch between a drop and the next explicit connect() call.
        if not self._connected or self._connection is None:
            log.info("iot_core: connection dropped, reconnecting before publish")
            try:
                await self.connect()
            except Exception as exc:
                # Can't get back online — surface to caller, coordinator
                # will log + drop the batch and retry on the next tick.
                raise RuntimeError(
                    f"IotCorePublisher.publish: connect failed ({type(exc).__name__}: {exc})"
                ) from exc

        payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
        # v0.2.6: observability counter.  Set BEFORE the size-limit check so
        # `last_publish_payload_bytes` reflects the size we attempted even if
        # the guard rejects it.  Heartbeat surfaces this so DDB sees the
        # rejection without needing broker logs.
        self._last_publish_payload_bytes = len(payload_bytes)

        # v0.2.6: pre-publish size guard.  AWS IoT Core MQTT v3.1.1 caps
        # message size at 128 KiB (131072 bytes).  Sending a larger payload
        # produces Publish-In Failure reason=PAYLOAD_LIMIT_EXCEEDED followed
        # by a Disconnect with reason=CLIENT_ERROR — which awscrt treats as
        # transient and auto-reconnects, only to have HACS retry the same
        # oversized payload.  That tight loop was the 2026-05-27 telemetry-
        # dead incident.  Reject pre-flight with a SPECIFIC exception that is
        # NOT in the retry whitelist and NOT in the publisher-queue catch
        # tuple — coordinator drops the batch loudly.  Data loss on one
        # oversized payload is preferable to a reconnect storm.
        if len(payload_bytes) > MQTT_MESSAGE_SIZE_HARD_LIMIT_BYTES:
            self._payload_too_large_count += 1
            log.error(
                "iot_core: payload too large topic=%s size_bytes=%d "
                "hard_limit=%d rejections_total=%d — DROPPING "
                "(see MAX_ENTITIES_PER_BATCH_PUBLISH in const.py)",
                topic,
                len(payload_bytes),
                MQTT_MESSAGE_SIZE_HARD_LIMIT_BYTES,
                self._payload_too_large_count,
            )
            raise PayloadTooLargeError(
                f"Payload size {len(payload_bytes)} bytes exceeds "
                f"AWS IoT Core MQTT hard limit "
                f"{MQTT_MESSAGE_SIZE_HARD_LIMIT_BYTES} bytes on topic={topic}"
            )

        qos_enum = self._qos_enum(qos)

        # v0.2.3 retry loop — re-issues the publish on AWS_ERROR_MQTT_CANCELLED_
        # FOR_CLEAN_SESSION (and friends).  These errors surface when awscrt
        # auto-reconnects in the middle of a chunked-publish sequence: every
        # in-flight publish future is cancelled by the broker even though the
        # resumed connection is healthy.  A short sleep + retry on the same
        # connection object recovers without bouncing creds.  See
        # _is_retryable_awscrt_error for the classification rule.
        delay = MQTT_PUBLISH_RETRY_INITIAL_SECONDS
        last_exc: BaseException | None = None
        for attempt in range(1, MQTT_PUBLISH_RETRY_ATTEMPTS + 1):
            try:
                await self._publish_once(
                    topic=topic, payload_bytes=payload_bytes, qos_enum=qos_enum,
                )
                if attempt > 1:
                    log.info(
                        "iot_core: publish recovered on attempt %d/%d topic=%s",
                        attempt, MQTT_PUBLISH_RETRY_ATTEMPTS, topic,
                    )
                return True
            except Exception as exc:
                last_exc = exc
                exc_name = type(exc).__name__
                # ExpiredToken is its own recovery path — tear down + fresh
                # creds, then re-raise so the publisher layer enqueues.
                if "ExpiredToken" in exc_name or "ExpiredToken" in str(exc):
                    log.warning("iot_core: credentials expired — reconnecting")
                    await self._reconnect_with_fresh_creds()
                    log.error(
                        "iot_core: publish failed topic=%s exc=%s: %s",
                        topic, exc_name, exc,
                    )
                    raise
                # Retry path: known transient awscrt errors during reconnect.
                if _is_retryable_awscrt_error(exc) and attempt < MQTT_PUBLISH_RETRY_ATTEMPTS:
                    log.warning(
                        "iot_core: publish attempt %d/%d hit transient awscrt error, "
                        "retrying in %.1fs topic=%s exc=%s: %s",
                        attempt, MQTT_PUBLISH_RETRY_ATTEMPTS, delay,
                        topic, exc_name, exc,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, MQTT_PUBLISH_RETRY_MAX_SECONDS)
                    continue
                # Non-retryable or attempts exhausted — log and surface.
                log.error(
                    "iot_core: publish failed topic=%s attempt=%d exc=%s: %s",
                    topic, attempt, exc_name, exc,
                )
                raise

        # Unreachable in practice — the loop above either returns True or
        # raises.  Defensive belt-and-braces so static analysis is happy.
        if last_exc is not None:
            raise last_exc
        return False

    async def subscribe(self, *, topic: str, qos: int, message_handler) -> None:
        """Subscribe to an MQTT topic, routing messages to `message_handler`.

        #9 (ADR 0005): HACS subscribes to `iems/{user_id}/command` (QoS 1) so
        the cloud can push shipping-mode + snapshot commands.

        `message_handler` is an async callable `(payload: bytes) -> Any`.  It is
        invoked from the awscrt threadpool callback via
        `loop.call_soon_threadsafe` + `asyncio.create_task` so it runs on the
        HA event loop, never on the awscrt thread.  Exceptions inside the
        handler are swallowed by the handler itself (see CommandHandler.
        on_message) — but we also guard here so a handler that forgets can't
        kill the callback.

        The subscription is recorded in `self._subscriptions` so it can be
        replayed on reconnect via `resubscribe_all`.

        Args:
            topic: Full MQTT topic string (e.g. iems/{user_id}/command).
            qos:   0 or 1.
            message_handler: async (payload: bytes) -> Any.
        """
        if not self._connected or self._connection is None:
            log.info("iot_core: connection dropped, reconnecting before subscribe")
            await self.connect()

        # Record first so a reconnect that races the subscribe still replays it.
        self._subscriptions[topic] = {"qos": qos, "message_handler": message_handler}
        await self._issue_subscribe(topic=topic, qos=qos, message_handler=message_handler)
        log.info("iot_core: subscribed topic=%s qos=%d", topic, qos)

    async def _issue_subscribe(self, *, topic: str, qos: int, message_handler) -> None:
        """Issue a single awscrt subscribe + await the broker SUBACK."""
        qos_enum = self._qos_enum(qos)

        def _on_message(topic, payload, dup, qos, retain, **_kwargs) -> None:
            """awscrt message callback — fires on the awscrt threadpool thread.

            Schedules the async handler on the HA event loop.  Never blocks the
            awscrt thread, never raises out of it.
            """
            loop = self._event_loop
            if loop is None or not loop.is_running():
                log.warning(
                    "iot_core: dropping command on %s — no running event loop",
                    topic,
                )
                return

            def _schedule() -> None:
                try:
                    # Retain a strong ref so the GC can't collect the task
                    # mid-flight (v0.1.14 P0); discard it on completion.
                    task = loop.create_task(
                        self._run_message_handler(message_handler, payload)
                    )
                    self._bg_tasks.add(task)
                    task.add_done_callback(self._bg_tasks.discard)
                except RuntimeError as exc:  # pragma: no cover — loop teardown race
                    log.warning("iot_core: could not schedule command handler: %s", exc)

            loop.call_soon_threadsafe(_schedule)

        sub_future, _ = self._connection.subscribe(
            topic=topic,
            qos=qos_enum,
            callback=_on_message,
        )
        loop = asyncio.get_event_loop()
        await asyncio.wait_for(
            loop.run_in_executor(None, sub_future.result),
            timeout=MQTT_CONNECT_TIMEOUT_SECONDS,
        )

    @staticmethod
    async def _run_message_handler(message_handler, payload) -> None:
        """Await the injected handler, swallowing any error it lets escape."""
        try:
            await message_handler(payload)
        except Exception as exc:  # noqa: BLE001 — callback path must survive
            log.error(
                "iot_core: command handler raised %s: %s",
                type(exc).__name__, exc,
            )

    async def resubscribe_all(self) -> None:
        """Re-issue every recorded subscription against the current connection.

        Called after a reconnect.  clean_session=False means the broker should
        restore subscriptions on a resumed session, but if the persistent
        session expired (>1h offline) they're gone — so we replay defensively.
        Failures are logged, not raised: a resubscribe miss must not crash the
        reconnect path (the next reconnect retries).
        """
        if not self._subscriptions:
            return
        if not self._connected or self._connection is None:
            log.info("iot_core: reconnecting before resubscribe")
            await self.connect()
        for topic, sub in list(self._subscriptions.items()):
            try:
                await self._issue_subscribe(
                    topic=topic,
                    qos=sub["qos"],
                    message_handler=sub["message_handler"],
                )
                log.info("iot_core: resubscribed topic=%s", topic)
            except (OSError, TimeoutError, asyncio.TimeoutError) as exc:
                log.warning(
                    "iot_core: resubscribe failed topic=%s: %s: %s",
                    topic, type(exc).__name__, exc,
                )

    async def _publish_once(self, *, topic: str, payload_bytes: bytes, qos_enum) -> None:
        """Single attempt at a publish — issues the awscrt future and awaits ACK.

        Split out from publish() so the retry loop can re-issue cleanly without
        re-serialising the payload or re-resolving the QoS enum.  Always raises
        on failure; caller decides whether to retry, reconnect, or surface.
        """
        pub_future, _ = self._connection.publish(
            topic=topic,
            payload=payload_bytes,
            qos=qos_enum,
        )
        loop = asyncio.get_event_loop()
        await asyncio.wait_for(
            loop.run_in_executor(None, pub_future.result),
            timeout=MQTT_PUBLISH_TIMEOUT_SECONDS,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _build_and_connect(self) -> None:
        """Build MQTT connection from fresh credentials and connect."""
        try:
            from awsiot import mqtt_connection_builder
            from awscrt import auth as crt_auth, io as crt_io
        except ImportError as exc:
            raise RuntimeError(
                "awsiotsdk / awscrt not installed. "
                "Add 'awsiotsdk' to manifest.json requirements."
            ) from exc

        creds = await self._auth.get_credentials()

        credentials_provider = crt_auth.AwsCredentialsProvider.new_static(
            access_key_id=creds.access_key_id,
            secret_access_key=creds.secret_access_key,
            session_token=creds.session_token,
        )

        event_loop_group = crt_io.EventLoopGroup(1)
        host_resolver = crt_io.DefaultHostResolver(event_loop_group)
        client_bootstrap = crt_io.ClientBootstrap(event_loop_group, host_resolver)

        # ClientId MUST equal Cognito Identity ID (not User Pool sub) — IAM
        # policy condition is `iot:ClientId == cognito-identity.amazonaws.com:sub`
        # which resolves to the Identity Pool identity_id, not user_sub.
        client_id = creds.identity_id

        # Capture the HA asyncio loop so awscrt threadpool callbacks can
        # post state changes back via call_soon_threadsafe without racing.
        self._event_loop = asyncio.get_event_loop()

        def _on_interrupted(connection, error, **_kwargs) -> None:
            """Called on awscrt thread when TCP/MQTT link drops.

            v0.2.6: classify the interruption.  awscrt treats every
            disconnect as transient and auto-reconnects, which hides
            broker-side rejections (CLIENT_ERROR / PROTOCOL_ERROR /
            SERVER_INVALID_DATA) — the 2026-05-27 PAYLOAD_LIMIT_EXCEEDED
            disconnects were silent through this path.  We bump a counter
            and capture the last disconnect reason so the heartbeat
            surfaces broker rejections.  We do NOT alter the publish-
            success path here — that is the size guard's job (raised
            pre-flight in publish()), this is observability only.
            """
            error_str = str(error)
            log.warning("iot_core: connection interrupted: %s", error_str)
            # Classify broker-side rejection-shaped disconnects.  Token
            # list intentionally narrow: only the failure modes the broker
            # uses to terminate a session for malformed/oversized publishes.
            _CLIENT_ERROR_TOKENS = (
                "CLIENT_ERROR",
                "PROTOCOL_ERROR",
                "SERVER_INVALID_DATA",
                "PAYLOAD_LIMIT",
            )
            if any(tok in error_str for tok in _CLIENT_ERROR_TOKENS):
                self._client_error_disconnects += 1
            self._last_disconnect_reason = error_str[:200]
            loop = self._event_loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(self._mark_disconnected)

        def _on_resumed(connection, return_code, session_present, **_kwargs) -> None:
            """Called on awscrt thread when awscrt auto-reconnects.

            v0.2.5: connection now uses clean_session=False, so on resume the
            broker reports session_present=True and replays every QoS 1
            publish that was queued for this ClientId during the disconnect
            window.  Log that explicitly so the production HEARTBEAT log
            line tells us whether the persistent-session path actually
            engaged.  session_present=False after a resume means the broker
            either expired the session (>1h offline) or this is the first
            connect of a new ClientId — both worth seeing in the logs.
            """
            log.info(
                "iot_core: connection resumed rc=%s session_present=%s "
                "(queued QoS 1 publishes will be replayed by broker if True)",
                return_code, session_present,
            )
            loop = self._event_loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(self._mark_connected)
                # #9 — fire the reconnect safety net (resubscribe + /hacs/status
                # reconcile) on the HA loop.  Scheduled, never run on the awscrt
                # thread.  session_present=False means the broker dropped our
                # session (>1h offline) so subscriptions + any queued command
                # are gone — exactly when the reconcile pull matters most.
                if self._on_resume is not None:
                    loop.call_soon_threadsafe(self._schedule_on_resume)

        # v0.2.5 (2026-05-26) — clean_session=False enables AWS IoT Core
        # MQTT 3.1.1 persistent sessions.  The broker queues QoS 1 publishes
        # across the brief disconnect windows that awscrt's auto-reconnect
        # produces (keep-alive miss every 30s, network blip, broker hangup);
        # without this, every in-flight publish at reconnect time is
        # cancelled with AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION (which
        # is exactly the production failure mode v0.2.2's heartbeat
        # diagnostics captured on 2026-05-26).
        #
        # Persistent session TTL: AWS IoT Core default is 1 hour.  ClientId
        # is the Cognito Identity ID (stable across DAI credential refresh
        # — IAM policy condition at line 262-264 keys off identity_id, not
        # the temp creds' session_token).  Per AWS IoT MQTT docs:
        # "In MQTT 3, the default value of persistent sessions expiration
        # time is an hour, and this applies to all the sessions in the
        # account."  See:
        # https://docs.aws.amazon.com/iot/latest/developerguide/mqtt.html#mqtt-persistent-sessions
        #
        # The v0.2.3 retry loop (publish() above) stays — it is the inner
        # cushion that handles the publish-side cancellation if a flap
        # happens within a single attempt.  v0.2.5 + v0.2.3 layer cleanly:
        # broker queue absorbs cross-reconnect publishes, retry loop
        # absorbs intra-flap publishes.
        connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=creds.iot_endpoint,
            region=creds.region,
            credentials_provider=credentials_provider,
            client_bootstrap=client_bootstrap,
            client_id=client_id,
            clean_session=False,
            keep_alive_secs=30,
            on_connection_interrupted=_on_interrupted,
            on_connection_resumed=_on_resumed,
        )

        loop = asyncio.get_event_loop()
        connect_future = connection.connect()
        await asyncio.wait_for(
            loop.run_in_executor(None, connect_future.result),
            timeout=MQTT_CONNECT_TIMEOUT_SECONDS,
        )

        self._connection = connection
        self._connected = True
        log.info(
            "iot_core: connected endpoint=%s client_id=%s...",
            creds.iot_endpoint,
            client_id[:8],
        )

    async def _reconnect_with_fresh_creds(self) -> None:
        """Tear down stale connection and reconnect with a fresh exchange."""
        self._connected = False
        old_conn = self._connection
        self._connection = None
        if old_conn is not None:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, old_conn.disconnect().result)
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass
        # Force a fresh credential exchange on next connect
        await self._auth.close()
        async with self._connect_lock:
            await self._build_and_connect()

    def _mark_disconnected(self) -> None:
        """Flip internal state to disconnected. Called on HA loop only."""
        self._connected = False

    def _mark_connected(self) -> None:
        """Flip internal state to connected. Called on HA loop only."""
        self._connected = True

    def set_on_resume(self, hook) -> None:
        """Register the async reconnect-safety-net hook (#9). `async () -> None`."""
        self._on_resume = hook

    def _schedule_on_resume(self) -> None:
        """Schedule the on_resume hook on the HA loop. Called on HA loop only."""
        hook = self._on_resume
        if hook is None:
            return
        loop = self._event_loop
        if loop is None or not loop.is_running():
            return
        # Retain a strong ref so the GC can't collect the task mid-flight
        # (v0.1.14 P0); discard it on completion.
        task = loop.create_task(self._run_on_resume(hook))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    @staticmethod
    async def _run_on_resume(hook) -> None:
        """Await the resume hook, swallowing errors so a reconnect can't crash."""
        try:
            await hook()
        except Exception as exc:  # noqa: BLE001 — reconnect path must survive
            log.error(
                "iot_core: on_resume hook raised %s: %s",
                type(exc).__name__, exc,
            )

    @staticmethod
    def _qos_enum(qos: int):
        """Convert integer QoS to awscrt enum."""
        from awscrt import mqtt as crt_mqtt
        if qos == 0:
            return crt_mqtt.QoS.AT_MOST_ONCE
        if qos == 1:
            return crt_mqtt.QoS.AT_LEAST_ONCE
        raise ValueError(f"Unsupported QoS value: {qos}. Use 0 or 1.")

    def __repr__(self) -> str:
        # Never expose auth internals
        return f"IotCorePublisher(connected={self._connected})"
