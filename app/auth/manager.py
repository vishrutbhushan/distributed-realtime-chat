"""
Authentication manager.
Handles user creation, login, logout, token validation, and presence heartbeats.
"""

import hashlib
import logging
import time
import uuid
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

SESSION_TTL = 24 * 60 * 60  # 24 hours in seconds


def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


class AuthManager:
    def __init__(self, db):
        self.db = db

    # ── User management ───────────────────────────────────────────────────────

    def create_user(
        self, username: str, password: str, role: str = "USER"
    ) -> Tuple[bool, str, str]:
        """Create a user. Returns (success, user_id, message)."""
        username = (username or "").strip()
        role = (role or "USER").upper()
        if not username or not password:
            return False, "", "Username and password are required"
        if role not in ("ADMIN", "USER"):
            return False, "", "Role must be ADMIN or USER"
        user_id = str(uuid.uuid4())
        now = int(time.time())
        try:
            with self.db.transaction(immediate=True):
                self.db.execute(
                    "INSERT INTO users (user_id, username, password_hash, role, created_at)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (user_id, username, _hash(password), role, now),
                )
            logger.info("[AUTH] Created user=%s role=%s", username, role)
            return True, user_id, "User created"
        except Exception as exc:
            if "UNIQUE" in str(exc):
                return False, "", "Username already exists"
            logger.error("[AUTH] create_user error: %s", exc)
            return False, "", str(exc)

    def remove_user(self, user_id: str) -> Tuple[bool, str]:
        try:
            with self.db.transaction(immediate=True):
                row = self.db.fetchone(
                    "SELECT active FROM users WHERE user_id = ?", (user_id,)
                )
                if not row or not row["active"]:
                    return False, "User not found or already removed"
                self.db.execute(
                    "UPDATE users SET active = 0 WHERE user_id = ?", (user_id,)
                )
                self.db.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
                self.db.execute("DELETE FROM presence WHERE user_id = ?", (user_id,))
                self.db.execute(
                    "DELETE FROM channel_members WHERE user_id = ?", (user_id,)
                )
            return True, "User deactivated; message history preserved"
        except Exception as exc:
            return False, str(exc)

    def get_user_by_id(self, user_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            "SELECT user_id, username, role FROM users WHERE user_id = ? AND active = 1",
            (user_id,),
        )
        return dict(row) if row else None

    # ── Session management ────────────────────────────────────────────────────

    def login(self, username: str, password: str) -> Tuple[bool, str, str, str]:
        """Returns (success, token, user_id, role)."""
        try:
            with self.db.transaction(immediate=True):
                # Read and create the session under one write lock so a
                # simultaneous account removal cannot create a stale session.
                row = self.db.fetchone(
                    "SELECT user_id, role FROM users "
                    "WHERE username = ? AND password_hash = ? AND active = 1",
                    (username, _hash(password)),
                )
                if not row:
                    logger.warning("[AUTH] Failed login for user=%s", username)
                    return False, "", "", ""
                token = str(uuid.uuid4())
                now = int(time.time())
                self.db.execute(
                    "INSERT INTO sessions (token, user_id, created_at, expires_at)"
                    " VALUES (?, ?, ?, ?)",
                    (token, row["user_id"], now, now + SESSION_TTL),
                )
                self.db.execute(
                    "INSERT INTO presence (user_id, status, last_seen)"
                    " VALUES (?, 'ONLINE', ?)"
                    " ON CONFLICT(user_id) DO UPDATE SET status='ONLINE', last_seen=excluded.last_seen",
                    (row["user_id"], now),
                )
        except Exception:
            raise
        logger.info("[AUTH] Login user=%s", username)
        return True, token, row["user_id"], row["role"]

    def logout(self, token: str) -> bool:
        now = int(time.time())
        try:
            with self.db.transaction(immediate=True):
                row = self.db.fetchone(
                    "SELECT user_id FROM sessions WHERE token = ?", (token,)
                )
                if not row:
                    return False
                self.db.execute("DELETE FROM sessions WHERE token = ?", (token,))
                remaining = self.db.fetchone(
                    "SELECT 1 FROM sessions WHERE user_id = ? AND expires_at > ? LIMIT 1",
                    (row["user_id"], now),
                )
                self.db.execute(
                    "UPDATE presence SET status = ?, last_seen = ? WHERE user_id = ?",
                    ("ONLINE" if remaining else "OFFLINE", now, row["user_id"]),
                )
        except Exception:
            raise
        logger.info("[AUTH] Logout token=...%s", token[-8:])
        return True

    def validate_token(self, token: str) -> Optional[dict]:
        """Return {user_id, username, role} or None if invalid/expired."""
        now = int(time.time())
        row = self.db.fetchone(
            """
            SELECT s.user_id, u.username, u.role
            FROM sessions s
            JOIN users u ON s.user_id = u.user_id
            WHERE s.token = ? AND s.expires_at > ? AND u.active = 1
            """,
            (token, now),
        )
        return dict(row) if row else None

    def heartbeat(self, token: str):
        """Refresh presence timestamp. Called implicitly on any RPC."""
        session = self.validate_token(token)
        if session:
            try:
                with self.db.transaction(immediate=True):
                    self.db.execute(
                        "UPDATE presence SET status = 'ONLINE', last_seen = ? WHERE user_id = ?",
                        (int(time.time()), session["user_id"]),
                    )
            except Exception:
                raise

