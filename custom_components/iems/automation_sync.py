"""Auto-sync the setup snapshot when HA automations change out-of-band (#?, v0.5.11).

The portal Smart Home card reads the cached
`PROFILE#SITE_MODEL.setup_snapshot.automations`. Before v0.5.11 that snapshot
only re-published on:
  - first install (`SetupSnapshotManager.publish_on_first_install`),
  - a `take_setup_snapshot` cloud command, and
  - our own apply path (`CommandHandler._resnapshot_after_apply`, v0.5.10).

So when a user creates / edits / deletes an automation DIRECTLY in Home
Assistant (the HA UI automation editor, or a hand-edited `automations.yaml`
+ reload), iEMS never noticed — the card showed a stale or already-deleted
automation until the next rescan or HA restart.

This module closes that gap. It listens for HA's two automation-change signals
and fires a DEBOUNCED setup-snapshot re-publish so the card converges on HA's
truth:

  - `entity_registry_updated` filtered to `entity_id` starting `automation.` —
    covers the UI editor's CREATE and DELETE (verified against a real HA: a UI
    delete removes the automation entity from the registry and does NOT reload,
    so the registry event is the ONLY signal for a delete);
  - `automation_reloaded` — covers YAML edits + any `automation.reload`
    (UI create/update goes through the config view which reloads).

Design constraints baked in:
  - **Hard debounce.** A bulk reload, or a burst of registry events from one UI
    action, must coalesce into exactly ONE re-snapshot after a short quiet
    window — never a snapshot storm (each snapshot is a ~tens-of-KiB MQTT
    publish, bounded by the 128 KiB setup-payload limit).
  - **No feedback loop with our own writes.** Our `write_automation` /
    `delete_automation` already re-snapshot in `_resnapshot_after_apply`. Their
    `automation.reload` would ALSO fire `automation_reloaded` here → a double
    snapshot. The command handler calls `suppress(window)` around its own apply
    so the reload WE caused is ignored by this listener.
  - **HA-free core.** The timer + clock are injected, so this is unit-testable
    without a running HA. `__init__.py` wires the real HA event bus +
    `async_call_later` to it.
  - **Never crashes the event loop.** `handle_event` is a plain callback that
    only (re)arms a timer; the timer body awaits `trigger()` and swallows every
    exception (a snapshot failure must not take down the listener).
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

log = logging.getLogger("iems.automation_sync")

# Default quiet window before a coalesced re-snapshot fires. Long enough to
# absorb a UI action's burst (registry event + reload land within a few hundred
# ms) and a bulk `automation.reload` of many automations, short enough that the
# card converges quickly.
DEFAULT_DEBOUNCE_SECONDS: float = 5.0

# Default self-apply suppression window. Covers the gap between the command
# handler starting its write and its own reload firing `automation_reloaded`.
DEFAULT_SELF_APPLY_SUPPRESS_SECONDS: float = 8.0

_AUTOMATION_ENTITY_PREFIX = "automation."


class AutomationChangeSync:
    """Debounced re-snapshot trigger for out-of-band HA automation changes.

    Parameters
    ----------
    trigger:
        Async callable run (awaited) once per coalesced burst — in production
        `SetupSnapshotManager.handle_take_setup_snapshot_command`. Must be
        no-raise-friendly (we swallow its exceptions anyway).
    schedule_later:
        Injected timer factory `(delay_seconds, async_callback) -> cancel`.
        In production this is HA's `functools.partial(async_call_later, hass)`.
        The callback we pass is an async function (a coroutine function); HA's
        `async_call_later` wraps it in a HassJob and awaits it on the event loop
        when the delay elapses — so the re-snapshot runs ON the loop, never on a
        worker thread. HA passes a `now` arg; our callback accepts and ignores
        it.
    clock:
        Monotonic seconds source (default `time.monotonic`) for the self-apply
        suppression window. Injected so tests control time.
    debounce_seconds / self_apply_suppress_seconds:
        Tunables; defaults above.
    """

    def __init__(
        self,
        *,
        trigger: Callable[[], Awaitable[object]],
        schedule_later: Callable[[float, Callable[..., Awaitable[None]]], Callable[[], None]],
        clock: Optional[Callable[[], float]] = None,
        debounce_seconds: float = DEFAULT_DEBOUNCE_SECONDS,
        self_apply_suppress_seconds: float = DEFAULT_SELF_APPLY_SUPPRESS_SECONDS,
    ) -> None:
        import time

        self._trigger = trigger
        self._schedule_later = schedule_later
        self._clock = clock or time.monotonic
        self._debounce_seconds = debounce_seconds
        self._self_apply_suppress_seconds = self_apply_suppress_seconds

        # Pending debounce timer cancel handle (None when idle).
        self._cancel_timer: Optional[Callable[[], None]] = None
        # Monotonic deadline until which self-triggered reloads are ignored.
        # 0.0 == not suppressing.
        self._suppress_until: float = 0.0

    # -- event ingress --------------------------------------------------------

    def handle_registry_event(self, action: object, entity_id: object) -> None:
        """Handle an `entity_registry_updated` event (action + entity_id).

        Filters to automation entities only. action/entity_id come straight
        from `event.data`; we tolerate any types (a malformed event must not
        crash the bus callback). create/remove/update on an `automation.*`
        entity all warrant a re-snapshot (create/delete change the set; an
        update e.g. rename changes the displayed name).
        """
        if not isinstance(entity_id, str) or not entity_id.startswith(
            _AUTOMATION_ENTITY_PREFIX
        ):
            return
        self._on_change(f"registry:{action}:{entity_id}")

    def handle_reload_event(self) -> None:
        """Handle an `automation_reloaded` event (no useful data fields)."""
        self._on_change("automation_reloaded")

    # -- self-apply suppression ----------------------------------------------

    def suppress(self, window_seconds: Optional[float] = None) -> None:
        """Ignore automation-change signals for the next `window_seconds`.

        Called by the command handler around its OWN write/delete so the reload
        IT triggers (which the command handler already re-snapshots for) does
        not also fire this debounced listener — avoiding a double snapshot.
        Extends (never shortens) any window already in flight.
        """
        window = (
            window_seconds
            if window_seconds is not None
            else self._self_apply_suppress_seconds
        )
        deadline = self._clock() + window
        if deadline > self._suppress_until:
            self._suppress_until = deadline

    # -- internals ------------------------------------------------------------

    def _on_change(self, reason: str) -> None:
        """(Re)arm the debounce timer unless we're in a self-apply window."""
        if self._clock() < self._suppress_until:
            log.debug(
                "automation_sync: ignoring self-applied change (%s) — "
                "suppression window active", reason,
            )
            return
        log.debug("automation_sync: change detected (%s) — arming debounce", reason)
        # Reset any pending timer so a burst coalesces into ONE fire.
        if self._cancel_timer is not None:
            try:
                self._cancel_timer()
            except Exception:  # noqa: BLE001 — cancel must never raise out
                pass
            self._cancel_timer = None
        self._cancel_timer = self._schedule_later(
            self._debounce_seconds, self._on_debounce_elapsed
        )

    async def _on_debounce_elapsed(self, *_args: object) -> None:
        """Debounce window elapsed — run exactly one re-snapshot.

        This is an async callback awaited ON the event loop by HA's
        `async_call_later` (it wraps a coroutine function in a HassJob and
        awaits it on the loop). It clears the timer handle and awaits the
        trigger, swallowing every exception so a snapshot failure never escapes
        the timer callback or breaks the listener.
        """
        self._cancel_timer = None
        log.info(
            "automation_sync: re-publishing setup snapshot (HA automations changed)"
        )
        try:
            await self._trigger()
        except Exception as exc:  # noqa: BLE001 — never break the listener
            log.warning(
                "automation_sync: re-snapshot failed: %s: %s",
                type(exc).__name__, exc,
            )

    def cancel(self) -> None:
        """Cancel any pending debounce timer (called on unload). No-op-safe."""
        if self._cancel_timer is not None:
            try:
                self._cancel_timer()
            except Exception:  # noqa: BLE001
                pass
            self._cancel_timer = None
