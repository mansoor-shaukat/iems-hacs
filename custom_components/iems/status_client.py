"""HACS /hacs/status client (#9, ADR 0005) — the reconnect-reconcile safety net.

ADR 0005's command channel is MQTT push.  The persistent broker session
(clean_session=False) replays a command HACS missed while briefly offline — but
only within the ~1h session TTL.  If HACS is offline LONGER than that, the
session expires, the cloud's queued command is dropped, and HACS would resume
with a stale local shipping_mode.

The fix (issue #9 AC): on every reconnect, HACS pulls /hacs/status ONCE and
reconciles `coordinator.shipping_mode` (+ whitelist) to the cloud's truth.

This client is intentionally forgiving: ANY failure (endpoint not yet deployed,
network blip, malformed body) returns None, and the caller leaves the local mode
untouched.  A status-pull miss must never crash the reconnect path — the next
reconnect (or the next MQTT command) recovers.

CONTRACT GAP (flagged in the #9 PR): the server-side /hacs/status endpoint and
its exact auth contract are not yet locked.  This client mirrors the only auth
mechanism HACS holds today (the API key, same as /hacs-auth).  When the CTO
locks the endpoint the URL/auth here may need a one-line adjustment.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from .const import IEMS_STATUS_HTTP_TIMEOUT_SECONDS, IEMS_STATUS_URL

log = logging.getLogger("iems.status_client")


class HacsStatusClient:
    """Best-effort puller for the cloud's last-known HACS shipping state."""

    def __init__(
        self,
        *,
        api_key: str,
        status_url: str = IEMS_STATUS_URL,
        http_timeout_s: float = IEMS_STATUS_HTTP_TIMEOUT_SECONDS,
    ) -> None:
        # Name-mangled so the key never appears in default __dict__ dumps.
        self.__api_key = api_key
        self._status_url = status_url
        self._http_timeout = http_timeout_s

    async def fetch_status(self) -> dict[str, Any] | None:
        """Pull /hacs/status. Returns the status dict, or None on ANY failure.

        Expected body shape (subset consumed by coordinator.reconcile_from_status):
            {"shipping_mode": "setup|paused|active",
             "whitelist": [...], "whitelist_version": int}
        """
        import aiohttp

        try:
            timeout = aiohttp.ClientTimeout(total=self._http_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self._status_url,
                    json={"api_key": self.__api_key},
                ) as resp:
                    if resp.status != 200:
                        log.info(
                            "hacs-status pull non-200 (status=%s) — "
                            "leaving local shipping_mode untouched",
                            resp.status,
                        )
                        return None
                    try:
                        body = await resp.json()
                    except (aiohttp.ContentTypeError, ValueError) as exc:
                        log.warning("hacs-status non-JSON body: %s", exc)
                        return None
        except asyncio.TimeoutError:
            log.warning("hacs-status pull timed out after %ss", self._http_timeout)
            return None
        except aiohttp.ClientError as exc:
            log.warning("hacs-status network error: %s", type(exc).__name__)
            return None

        if not isinstance(body, dict):
            log.warning("hacs-status body is not a JSON object — ignoring")
            return None
        return body

    def __repr__(self) -> str:
        # Never leak the api_key.
        return f"HacsStatusClient(url={self._status_url!r})"
