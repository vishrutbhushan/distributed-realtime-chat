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
    def __init__(self, db):
        self.db = db

    # ── User Signup & Login ───────────────────────────────────────────────────

    def signup(
        self, username: str, password: str
    ) -> Tuple[bool, str, str, str, str]:
        """
        Register a new user.
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
                VALUES (?, ?, ?, 'active', ?, ?)
                """,
                (user_id, username, _hash(password), now, now),
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
            logger.info("[AUTH] User signed up: %s (id=%s)", username, user_id)
            return True, token, user_id, username, "Account created successfully"
        except Exception as exc:
            self.db.rollback()
            if "UNIQUE" in str(exc):
                return False, "", "", "", "Username already exists. Please choose another."
            logger.error("[AUTH] Signup error: %s", exc)
            return False, "", "", "", str(exc)

    def login(
        self, username: str, password: str
    ) -> Tuple[bool, str, str, str, str]:
        """
        Authenticate an existing user.
        Sets status to 'active'.
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
            # Set user status to 'active' and update last_seen
            self.db.execute(
                "UPDATE users SET status = 'active', last_seen = ? WHERE user_id = ?",
                (now, user_id),
            )
            self.db.commit()
            logger.info("[AUTH] User logged in: %s (status=active)", username)
            return True, token, user_id, username, "Login successful"
        except Exception as exc:
            self.db.rollback()
            logger.error("[AUTH] Login error: %s", exc)
            return False, "", "", "", str(exc)

    def logout(self, token: str) -> bool:
        """
        End user session and set user status to 'inactive'.
        """
        row = self.db.fetchone("SELECT user_id FROM sessions WHERE token = ?", (token,))
        if not row:
            return False

        user_id = row["user_id"]
        now = int(time.time())
        try:
            self.db.execute("DELETE FROM sessions WHERE token = ?", (token,))
            self.db.execute(
                "UPDATE users SET status = 'inactive', last_seen = ? WHERE user_id = ?",
                (now, user_id),
            )
            self.db.commit()
            logger.info("[AUTH] User logged out (user_id=%s, status=inactive)", user_id)
            return True
        except Exception as exc:
            self.db.rollback()
            logger.error("[AUTH] Logout error: %s", exc)
            return False

    def validate_token(self, token: str) -> Optional[dict]:
        """
        Return user dict if session is valid and active, else None.
        Updates last_seen as implicit heartbeat.
        """
        now = int(time.time())
        row = self.db.fetchone(
            """
            SELECT s.user_id, u.username, u.status
            FROM sessions s
            JOIN users u ON s.user_id = u.user_id
            WHERE s.token = ? AND s.expires_at > ?
            """,
            (token, now),
        )
        if not row:
            return None

        # Refresh last seen and ensure status is active (throttled to at most once per 10s)
        try:
            self.db.execute(
                "UPDATE users SET last_seen = ?, status = 'active' WHERE user_id = ? AND last_seen < ?",
                (now, row["user_id"], now - 10),
            )
        except Exception:
            pass

        return dict(row)

    def update_presence(self, user_id: str, status: str = "active"):
        """Explicitly update status ('active' | 'inactive') and last_seen."""
        now = int(time.time())
        self.db.execute(
            "UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?",
            (status, now, user_id),
        )
        self.db.commit()

    # ── Directory ─────────────────────────────────────────────────────────────

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
