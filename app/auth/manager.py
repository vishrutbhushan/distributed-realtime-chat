"""
Authentication and User Manager.
Handles user signup, login, logout, active/inactive statuses, and token validation.
"""

import hashlib
import logging
import re
import time
import uuid
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

SESSION_TTL = 7 * 24 * 60 * 60  # 7 days in seconds

# Simple restrictions for username & password
USERNAME_REGEX = re.compile(r"^[a-zA-Z0-9_]{3,20}$")
MIN_PASSWORD_LEN = 4


def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


class AuthManager:
    def __init__(self, db, presence=None):
        self.db = db
        self.presence = presence

    # Ã¢â€â‚¬Ã¢â€â‚¬ User Signup & Login Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

    def signup(
        self, username: str, password: str, client_kind: str = "cli"
    ) -> Tuple[bool, str, str, str, str]:
        """
        Register a new user and its first authenticated session.
        Restrictions:
          - username: 3-20 alphanumeric characters or underscores
          - password: minimum 4 characters
        Returns: (success, token, user_id, username, message)
        """
        username = (username or "").strip()
        if not USERNAME_REGEX.match(username):
            return (
                False,
                "",
                "",
                "",
                "Username must be 3-20 alphanumeric characters or underscores.",
            )

        if not password or len(password) < MIN_PASSWORD_LEN:
            return (
                False,
                "",
                "",
                "",
                f"Password must be at least {MIN_PASSWORD_LEN} characters long.",
            )

        user_id = str(uuid.uuid4())
        now = int(time.time())

        try:
            self.db.execute(
                """
                INSERT INTO users (user_id, username, password_hash, status, last_seen, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    username,
                    _hash(password),
                    "inactive" if self.presence else "active",
                    now,
                    now,
                ),
            )
            # Create active session token
            token = str(uuid.uuid4())
            self.db.execute(
                """
                INSERT INTO sessions (token, user_id, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (token, user_id, now, now + SESSION_TTL),
            )
            self.db.commit()
            if self.presence:
                self.presence.register_session(
                    user_id, token, client_kind, now + SESSION_TTL
                )
            logger.info("[AUTH] User signed up: %s (id=%s)", username, user_id)
            return True, token, user_id, username, "Account created successfully"
        except Exception as exc:
            self.db.rollback()
            if "UNIQUE" in str(exc):
                return False, "", "", "", "Username already exists. Please choose another."
            logger.error("[AUTH] Signup error: %s", exc)
            return False, "", "", "", str(exc)

    def login(
        self, username: str, password: str, client_kind: str = "cli"
    ) -> Tuple[bool, str, str, str, str]:
        """
        Authenticate an existing user.
        Starts a session; PresenceManager records the session's client activity.
        Returns: (success, token, user_id, username, message)
        """
        username = (username or "").strip()
        row = self.db.fetchone(
            "SELECT user_id, username FROM users WHERE username = ? AND password_hash = ?",
            (username, _hash(password)),
        )
        if not row:
            logger.warning("[AUTH] Invalid credentials for username: %s", username)
            return False, "", "", "", "Invalid username or password"

        user_id = row["user_id"]
        now = int(time.time())
        token = str(uuid.uuid4())

        try:
            self.db.execute(
                """
                INSERT INTO sessions (token, user_id, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (token, user_id, now, now + SESSION_TTL),
            )
            # Presence is owned by PresenceManager when configured. Keep the
            # standalone manager behavior for callers without that service.
            if not self.presence:
                self.db.execute(
                    "UPDATE users SET status = 'active', last_seen = ? WHERE user_id = ?",
                    (now, user_id),
                )
            self.db.commit()
            if self.presence:
                self.presence.register_session(
                    user_id, token, client_kind, now + SESSION_TTL
                )
            logger.info("[AUTH] User logged in: %s (status=active)", username)
            return True, token, user_id, username, "Login successful"
        except Exception as exc:
            self.db.rollback()
            logger.error("[AUTH] Login error: %s", exc)
            return False, "", "", "", str(exc)

    def logout(self, token: str) -> bool:
        """
        End this session, then ask PresenceManager to recompute account status.
        """
        now = int(time.time())
        with self.db.lock:
            row = self.db.fetchone("SELECT user_id FROM sessions WHERE token = ?", (token,))
            if not row:
                return False

            user_id = row["user_id"]
            try:
                self.db.execute("DELETE FROM sessions WHERE token = ?", (token,))
                if not self.presence:
                    active_session = self.db.fetchone(
                        "SELECT 1 FROM sessions WHERE user_id = ? AND expires_at > ? LIMIT 1",
                        (user_id, now),
                    )
                    status = "active" if active_session else "inactive"
                    self.db.execute(
                        "UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?",
                        (status, now, user_id),
                    )
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                logger.error("[AUTH] Logout error: %s", exc)
                return False

        if self.presence:
            self.presence.end_session(token)
            row = self.db.fetchone("SELECT status FROM users WHERE user_id = ?", (user_id,))
            status = row["status"] if row else "unknown"
        logger.info("[AUTH] User logged out (user_id=%s, status=%s)", user_id, status)
        return True

    def token_is_valid(self, token: str) -> bool:
        """Check session validity without treating a long-lived stream as activity."""
        if not token:
            return False
        now = int(time.time())
        row = self.db.fetchone(
            "SELECT 1 FROM sessions WHERE token = ? AND expires_at > ?",
            (token, now),
        )
        return bool(row)

    def validate_token(self, token: str) -> Optional[dict]:
        """
        Return the authenticated user and session expiry without changing presence.
        """
        now = int(time.time())
        row = self.db.fetchone(
            """
            SELECT s.user_id, s.expires_at, u.username, u.status
            FROM sessions s
            JOIN users u ON s.user_id = u.user_id
            WHERE s.token = ? AND s.expires_at > ?
            """,
            (token, now),
        )
        if not row:
            return None

        return dict(row)

    def heartbeat(self, token: str):
        """Validate a token and refresh presence only for an eligible CLI session."""
        sess = self.validate_token(token)
        if sess and self.presence:
            self.presence.note_rpc_activity(
                sess["user_id"], token, sess["expires_at"], client_kind="cli"
            )

    def update_presence(self, user_id: str, status: str = "active"):
        """Explicitly update status ('active' | 'inactive') and last_seen."""
        now = int(time.time())
        self.db.execute(
            "UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?",
            (status, now, user_id),
        )
        self.db.commit()

    update_status = update_presence

    # Ã¢â€â‚¬Ã¢â€â‚¬ Directory Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

    def list_users(self, exclude_user_id: Optional[str] = None) -> List[dict]:
        """
        List all users with their active/inactive status and last_seen.
        """
        if exclude_user_id:
            rows = self.db.fetchall(
                """
                SELECT user_id, username, status, last_seen, created_at
                FROM users
                WHERE user_id != ?
                ORDER BY status DESC, username ASC
                """,
                (exclude_user_id,),
            )
        else:
            rows = self.db.fetchall(
                """
                SELECT user_id, username, status, last_seen, created_at
                FROM users
                ORDER BY status DESC, username ASC
                """
            )
        return [dict(r) for r in rows]

    def get_user_by_id(self, user_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            "SELECT user_id, username, status, last_seen FROM users WHERE user_id = ?",
            (user_id,),
        )
        return dict(row) if row else None
