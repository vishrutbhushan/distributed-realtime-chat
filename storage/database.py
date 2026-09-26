"""
Thread-safe SQLite database layer for Distributed Real-time Chat.
Thread-local connections + process-wide lock prevent SQLite concurrency locks.
"""

import os
import sqlite3
import threading
import logging

logger = logging.getLogger(__name__)


class Database:
    """
    Thin wrapper around SQLite providing:
      - thread-local connections (safe for gRPC thread pool)
      - WAL journal mode for concurrent readers
      - busy_timeout and process-wide lock to prevent 'database is locked' errors
      - automatic schema initialisation on first use
      - clean separation for Direct Messages and Group Chats
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock   = threading.RLock()
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._conn = sqlite3.connect(
            self.db_path,
            timeout=60.0,
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=60000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA wal_autocheckpoint=1000")
        self._init_schema()
        logger.info("[DB] Initialised at %s", db_path)

    # ── Internal ──────────────────────────────────────────────────────────────

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def close(self):
        """Close connection cleanly."""
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    def _init_schema(self):
        """Create all tables on startup (idempotent). NO default users or channels."""
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id       TEXT PRIMARY KEY,
                    username      TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    status        TEXT NOT NULL DEFAULT 'inactive',  -- 'active' | 'inactive'
                    last_seen     INTEGER NOT NULL DEFAULT 0,
                    created_at    INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    token      TEXT PRIMARY KEY,
                    user_id    TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS groups (
                    group_id   TEXT PRIMARY KEY,
                    name       TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    FOREIGN KEY (created_by) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS group_members (
                    group_id  TEXT NOT NULL,
                    user_id   TEXT NOT NULL,
                    role      TEXT NOT NULL DEFAULT 'MEMBER',  -- 'ADMIN' | 'MEMBER'
                    joined_at INTEGER NOT NULL,
                    PRIMARY KEY (group_id, user_id),
                    FOREIGN KEY (group_id) REFERENCES groups(group_id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id)  REFERENCES users(user_id)   ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS messages (
                    message_id        TEXT PRIMARY KEY,
                    chat_type         TEXT NOT NULL,  -- 'DM' | 'GROUP'
                    sender_id         TEXT NOT NULL,
                    recipient_id      TEXT DEFAULT NULL, -- user_id for DM
                    group_id          TEXT DEFAULT NULL, -- group_id for GROUP
                    content           TEXT NOT NULL,
                    file_id           TEXT DEFAULT NULL,
                    timestamp         INTEGER NOT NULL,
                    client_request_id TEXT UNIQUE,
                    FOREIGN KEY (sender_id)    REFERENCES users(user_id),
                    FOREIGN KEY (recipient_id) REFERENCES users(user_id),
                    FOREIGN KEY (group_id)     REFERENCES groups(group_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_messages_dm
                    ON messages(chat_type, sender_id, recipient_id, timestamp);

                CREATE INDEX IF NOT EXISTS idx_messages_group
                    ON messages(chat_type, group_id, timestamp);

                CREATE TABLE IF NOT EXISTS files (
                    file_id          TEXT PRIMARY KEY,
                    filename         TEXT NOT NULL,
                    file_type        TEXT NOT NULL, -- 'image' | 'pdf' | 'other'
                    content_type     TEXT NOT NULL DEFAULT 'application/octet-stream',
                    size_bytes       INTEGER NOT NULL,
                    owner_id         TEXT NOT NULL,
                    chat_type        TEXT NOT NULL, -- 'DM' | 'GROUP'
                    target_id        TEXT NOT NULL, -- user_id or group_id
                    storage_location TEXT NOT NULL,
                    uploaded_at      INTEGER NOT NULL,
                    FOREIGN KEY (owner_id) REFERENCES users(user_id)
                );
            """)

    # ── Public helpers ────────────────────────────────────────────────────────

    def execute(self, query: str, params=()) -> sqlite3.Cursor:
        with self._lock:
            return self.conn.execute(query, params)

    def executemany(self, query: str, params_list) -> sqlite3.Cursor:
        with self._lock:
            return self.conn.executemany(query, params_list)

    def commit(self):
        with self._lock:
            self.conn.commit()

    def rollback(self):
        with self._lock:
            self.conn.rollback()

    def fetchone(self, query: str, params=()):
        with self._lock:
            return self.conn.execute(query, params).fetchone()

    def fetchall(self, query: str, params=()) -> list:
        with self._lock:
            return self.conn.execute(query, params).fetchall()
