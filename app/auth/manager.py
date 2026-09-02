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
        user_id = str(uuid.uuid4())
        now = int(time.time())
        try:
            self.db.execute(
                "INSERT INTO users (user_id, username, password_hash, role, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (user_id, username, _hash(password), role, now),
            )
            self.db.commit()
            logger.info("[AUTH] Created user=%s role=%s", username, role)
            return True, user_id, "User created"
        except Exception as exc:
            if "UNIQUE" in str(exc):
                return False, "", "Username already exists"
            logger.error("[AUTH] create_user error: %s", exc)
            return False, "", str(exc)

    def remove_user(self, user_id: str) -> Tuple[bool, str]:
        try:
            self.db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            self.db.commit()
            return True, "User removed"
        except Exception as exc:
            return False, str(exc)

    def get_user_by_id(self, user_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            "SELECT user_id, username, role FROM users WHERE user_id = ?",
            (user_id,),
        )
        return dict(row) if row else None

    # ── Session management ────────────────────────────────────────────────────

    def login(self, username: str, password: str) -> Tuple[bool, str, str, str]:
        """Returns (success, token, user_id, role)."""
        row = self.db.fetchone(
            "SELECT user_id, role FROM users WHERE username = ? AND password_hash = ?",
            (username, _hash(password)),
        )
        if not row:
            logger.warning("[AUTH] Failed login for user=%s", username)
            return False, "", "", ""

        token = str(uuid.uuid4())
        now   = int(time.time())
        self.db.execute(
            "INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (token, row["user_id"], now, now + SESSION_TTL),
        )
        # Set presence ONLINE
        self.db.execute(
            "INSERT OR REPLACE INTO presence (user_id, status, last_seen) VALUES (?, 'ONLINE', ?)",
            (row["user_id"], now),
        )
        self.db.commit()
        logger.info("[AUTH] Login user=%s", username)
        return True, token, row["user_id"], row["role"]

    def logout(self, token: str) -> bool:
        row = self.db.fetchone("SELECT user_id FROM sessions WHERE token = ?", (token,))
        if not row:
            return False
        self.db.execute("DELETE FROM sessions WHERE token = ?", (token,))
        self.db.execute(
            "UPDATE presence SET status = 'OFFLINE', last_seen = ? WHERE user_id = ?",
            (int(time.time()), row["user_id"]),
        )
        self.db.commit()
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
            WHERE s.token = ? AND s.expires_at > ?
            """,
            (token, now),
        )
        return dict(row) if row else None

    def heartbeat(self, token: str):
        """Refresh presence timestamp. Called implicitly on any RPC."""
        session = self.validate_token(token)
        if session:
            self.db.execute(
                "UPDATE presence SET status = 'ONLINE', last_seen = ? WHERE user_id = ?",
                (int(time.time()), session["user_id"]),
            )
            self.db.commit()

