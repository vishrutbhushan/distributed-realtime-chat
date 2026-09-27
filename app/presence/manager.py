"""Track online presence from live browser streams and recent CLI activity."""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

logger = logging.getLogger(__name__)

CLI_OFFLINE_THRESHOLD = 300
STREAM_RECONNECT_GRACE = 10
INITIAL_BROWSER_GRACE = 10
SWEEP_INTERVAL = 1


@dataclass
class _SessionPresence:
    user_id: str
    client_kind: str
    expires_at: int
    last_activity: float
    last_db_touch: float
    streams: set = field(default_factory=set)
    connect_deadline: Optional[float] = None
    disconnect_deadline: Optional[float] = None


class PresenceManager:
    """Own presence state for every authenticated session in this app process.

    Browser sessions are online while at least one event stream is connected,
    with a short grace after the last stream closes. Non-browser RPC clients
    retain the legacy five-minute recent-activity policy.
    """

    def __init__(
        self,
        db,
        on_presence_change: Optional[Callable[[list], None]] = None,
        *,
        monotonic=None,
        wall_clock=None,
        start_worker: bool = True,
    ):
        self.db = db
        self.on_presence_change = on_presence_change
        self._monotonic = monotonic or time.monotonic
        self._wall_clock = wall_clock or time.time
        self._lock = threading.RLock()
        self._sessions = {}
        self._user_sessions = {}
        self._stream_tokens = {}
        self._stop_event = threading.Event()
        self._sweeper = None
        if start_worker:
            self.reset_stale_presence()
            self._sweeper = threading.Thread(
                target=self._sweep_loop, daemon=True, name="presence-sweeper"
            )
            self._sweeper.start()

    @staticmethod
    def _normalize_kind(client_kind: str) -> str:
        return "browser" if (client_kind or "").lower() == "browser" else "cli"

    def register_session(
        self, user_id: str, token: str, client_kind: str = "cli", expires_at: int = 0
    ):
        """Start presence tracking for a newly created login session."""
        if not token or not user_id:
            return
        now = self._monotonic()
        kind = self._normalize_kind(client_kind)
        state = _SessionPresence(
            user_id=user_id,
            client_kind=kind,
            expires_at=int(expires_at or (self._wall_clock() + CLI_OFFLINE_THRESHOLD)),
            last_activity=now,
            last_db_touch=now,
            connect_deadline=now + INITIAL_BROWSER_GRACE if kind == "browser" else None,
        )
        with self._lock:
            old = self._sessions.get(token)
            if old:
                self._remove_session_locked(token, old)
            self._sessions[token] = state
            self._user_sessions.setdefault(user_id, set()).add(token)
            changed = self._write_status_locked(user_id, active=True, touch=True)
        self._notify([user_id] if changed else [])

    def note_rpc_activity(
        self,
        user_id: str,
        token: str,
        expires_at: int,
        client_kind: str = "cli",
    ):
        """Refresh CLI activity; browser reads never serve as presence heartbeats."""
        if not token or not user_id:
            return
        now = self._monotonic()
        kind = self._normalize_kind(client_kind)
        with self._lock:
            state = self._sessions.get(token)
            if state is None:
                # A browser must establish its event stream before its first
                # snapshot can count as presence, including after a restart.
                if kind == "browser":
                    return
                state = _SessionPresence(
                    user_id=user_id,
                    client_kind=kind,
                    expires_at=int(expires_at),
                    last_activity=now,
                    last_db_touch=now,
                    connect_deadline=now + INITIAL_BROWSER_GRACE if kind == "browser" else None,
                )
                self._sessions[token] = state
                self._user_sessions.setdefault(user_id, set()).add(token)
            elif state.user_id != user_id:
                logger.error("[PRESENCE] Session token changed user identity")
                return

            # Once a token has opened a browser stream, later snapshots or
            # delayed requests cannot keep its disconnected tab online.
            if state.client_kind == "browser":
                if not state.streams:
                    return
                changed = self._write_status_locked(
                    user_id,
                    active=True,
                    touch=(now - state.last_db_touch >= 10),
                )
                if now - state.last_db_touch >= 10:
                    state.last_db_touch = now
            else:
                state.last_activity = now
                state.expires_at = int(expires_at)
                changed = self._write_status_locked(
                    user_id,
                    active=True,
                    touch=(now - state.last_db_touch >= 10),
                )
                if now - state.last_db_touch >= 10:
                    state.last_db_touch = now
        self._notify([user_id] if changed else [])

    def connect_stream(self, user_id: str, token: str, stream_id: str, expires_at: int):
        """Register one event stream before its READY event is sent."""
        if not (user_id and token and stream_id):
            return
        now = self._monotonic()
        with self._lock:
            state = self._sessions.get(token)
            if state is None or state.user_id != user_id:
                state = _SessionPresence(
                    user_id=user_id,
                    client_kind="browser",
                    expires_at=int(expires_at),
                    last_activity=now,
                    last_db_touch=now,
                )
                self._sessions[token] = state
                self._user_sessions.setdefault(user_id, set()).add(token)
            else:
                state.client_kind = "browser"
                state.expires_at = int(expires_at)
                state.connect_deadline = None
                state.disconnect_deadline = None
            state.streams.add(stream_id)
            self._stream_tokens[stream_id] = token
            changed = self._write_status_locked(user_id, active=True, touch=True)
        self._notify([user_id] if changed else [])

    def disconnect_stream(self, stream_id: str):
        """Remove this stream only; other tabs sharing the token remain live."""
        now = self._monotonic()
        with self._lock:
            token = self._stream_tokens.pop(stream_id, None)
            state = self._sessions.get(token) if token else None
            if state is None or stream_id not in state.streams:
                return
            state.streams.remove(stream_id)
            if state.client_kind == "browser" and not state.streams:
                state.connect_deadline = None
                state.disconnect_deadline = now + STREAM_RECONNECT_GRACE

    def end_session(self, token: str):
        """Forget a logged-out or expired session and recompute account status."""
        if not token:
            return
        with self._lock:
            state = self._sessions.get(token)
            if state is None:
                return
            user_id = state.user_id
            self._remove_session_locked(token, state)
            changed = self._write_status_locked(
                user_id, active=self._user_is_active_locked(user_id), touch=False
            )
        self._notify([user_id] if changed else [])

    def request_status(self, token: str, status: str) -> bool:
        """Apply the legacy explicit-status RPC without overriding live tabs."""
        requested = (status or "").lower()
        if requested not in ("active", "inactive"):
            return False
        now = self._monotonic()
        with self._lock:
            state = self._sessions.get(token)
            if state is None:
                return False
            if requested == "active":
                if state.client_kind == "browser":
                    if not state.streams:
                        state.connect_deadline = now + INITIAL_BROWSER_GRACE
                        state.disconnect_deadline = None
                else:
                    state.last_activity = now
            elif state.client_kind == "browser":
                # A connected tab is the source of truth for browser presence.
                if state.streams:
                    return True
                state.connect_deadline = None
                state.disconnect_deadline = None
            else:
                state.last_activity = now - CLI_OFFLINE_THRESHOLD

            active = self._user_is_active_locked(state.user_id)
            changed = self._write_status_locked(state.user_id, active, touch=active)
            user_id = state.user_id
        self._notify([user_id] if changed else [])
        return True

    def _remove_session_locked(self, token: str, state: _SessionPresence):
        self._sessions.pop(token, None)
        for stream_id in tuple(state.streams):
            self._stream_tokens.pop(stream_id, None)
        state.streams.clear()
        tokens = self._user_sessions.get(state.user_id)
        if tokens:
            tokens.discard(token)
            if not tokens:
                self._user_sessions.pop(state.user_id, None)

    def _user_is_active_locked(self, user_id: str) -> bool:
        now = self._monotonic()
        wall_now = int(self._wall_clock())
        for token in tuple(self._user_sessions.get(user_id, ())):
            state = self._sessions.get(token)
            if state is None or state.expires_at <= wall_now:
                continue
            if state.client_kind == "browser":
                if state.streams:
                    return True
                if state.connect_deadline is not None and state.connect_deadline > now:
                    return True
                if state.disconnect_deadline is not None and state.disconnect_deadline > now:
                    return True
            elif now - state.last_activity < CLI_OFFLINE_THRESHOLD:
                return True
        return False

    def _write_status_locked(self, user_id: str, active: bool, touch: bool) -> bool:
        status = "active" if active else "inactive"
        now = int(self._wall_clock())
        with self.db.lock:
            row = self.db.fetchone(
                "SELECT status, last_seen FROM users WHERE user_id = ?", (user_id,)
            )
            if not row:
                return False
            changed = row["status"] != status
            update_last_seen = active and (touch or changed)
            if changed or update_last_seen:
                if active:
                    self.db.execute(
                        "UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?",
                        (status, now, user_id),
                    )
                else:
                    self.db.execute(
                        "UPDATE users SET status = ? WHERE user_id = ?",
                        (status, user_id),
                    )
                self.db.commit()
            return changed

    def _notify(self, user_ids):
        if user_ids and self.on_presence_change:
            try:
                self.on_presence_change(list(set(user_ids)))
            except Exception:
                logger.exception("[PRESENCE] Could not publish status change")

    def sweep_once(self):
        """Evaluate expired sessions and deadlines; exposed for deterministic tests."""
        wall_now = int(self._wall_clock())
        changed_user_ids = []
        with self._lock:
            affected_user_ids = set(self._user_sessions)
            for token, state in tuple(self._sessions.items()):
                if state.expires_at <= wall_now:
                    affected_user_ids.add(state.user_id)
                    self._remove_session_locked(token, state)

            for user_id in affected_user_ids:
                if self._write_status_locked(
                    user_id, active=self._user_is_active_locked(user_id), touch=False
                ):
                    changed_user_ids.append(user_id)
        self._notify(changed_user_ids)

    def reset_stale_presence(self):
        """Clear process-local presence left active by a previous app instance."""
        with self.db.lock:
            self.db.execute("UPDATE users SET status = 'inactive' WHERE status != 'inactive'")
            self.db.commit()

    def _sweep_loop(self):
        while not self._stop_event.wait(SWEEP_INTERVAL):
            try:
                self.sweep_once()
            except Exception:
                logger.exception("[PRESENCE] Sweep error")

    def stop(self):
        self._stop_event.set()
        if self._sweeper and self._sweeper is not threading.current_thread():
            self._sweeper.join(timeout=2)
