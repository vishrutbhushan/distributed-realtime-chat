"""
Thread-safe SQLite database layer for Distributed Real-time Chat.
Each Raft node owns its own local SQLite file.
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
        self._local  = threading.local()
        self._lock   = threading.RLock()
        self._all_conns = []
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_schema()
        logger.info("[DB] Initialised at %s", db_path)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        with self._lock:
            self._all_conns.append(conn)
        return conn

    def _get_conn(self) -> sqlite3.Connection:
        """Return (or lazily create) the thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = self._connect()
        return self._local.conn

    @property
    def conn(self) -> sqlite3.Connection:
        return self._get_conn()

    def close(self):
        """Close all connections across threads."""
        with self._lock:
            for conn in self._all_conns:
                try:
                    conn.close()
                except Exception:
                    pass
            self._all_conns.clear()
            if hasattr(self._local, "conn"):
                self._local.conn = None

    def _init_schema(self):
        """Create all tables on startup (idempotent). NO default users or channels."""
        with self._lock:
            conn = self._connect()
            conn.executescript("""
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
                    raft_log_index    INTEGER DEFAULT 0,
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

                -- Raft persistent state (Milestone 2)
                CREATE TABLE IF NOT EXISTS raft_state (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                -- Raft log entries (Milestone 2)
                CREATE TABLE IF NOT EXISTS raft_log (
                    log_index    INTEGER PRIMARY KEY,
                    term         INTEGER NOT NULL,
                    command_type TEXT NOT NULL,
                    payload      TEXT NOT NULL,
                    request_id   TEXT
                );
            """)
            conn.commit()
            conn.close()

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
