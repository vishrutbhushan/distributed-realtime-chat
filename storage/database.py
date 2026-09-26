"""
Thread-safe SQLite database layer.
Each Raft node owns its own local SQLite file.
Thread-local connections avoid SQLite's single-thread limitation.
"""

import os
import sqlite3
import threading
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Database:
    """
    Thin wrapper around SQLite providing:
      - thread-local connections (safe for gRPC thread pool)
      - WAL journal mode for concurrent readers
      - automatic schema initialisation on first use
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local  = threading.local()
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_schema()
        logger.info("[DB] Initialised at %s", db_path)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=5.0,
            isolation_level="DEFERRED",
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _get_conn(self) -> sqlite3.Connection:
        """Return (or lazily create) the thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = self._connect()
        return self._local.conn

    @property
    def conn(self) -> sqlite3.Connection:
        return self._get_conn()

    def _init_schema(self):
        """Create all tables on startup (idempotent)."""
        conn = self._connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id       TEXT PRIMARY KEY,
                username      TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'USER',
                created_at    INTEGER NOT NULL,
                active        INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS channels (
                channel_id TEXT PRIMARY KEY,
                name       TEXT UNIQUE NOT NULL,
                created_by TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channel_members (
                channel_id TEXT NOT NULL,
                user_id    TEXT NOT NULL,
                joined_at  INTEGER NOT NULL,
                PRIMARY KEY (channel_id, user_id),
                FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id)    REFERENCES users(user_id)       ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS messages (
                message_id        TEXT PRIMARY KEY,
                channel_id        TEXT NOT NULL,
                sender_id         TEXT NOT NULL,
                content           TEXT NOT NULL,
                timestamp         INTEGER NOT NULL,
                client_request_id TEXT UNIQUE,
                raft_log_index    INTEGER DEFAULT 0,
                file_id           TEXT DEFAULT NULL,
                FOREIGN KEY (channel_id) REFERENCES channels(channel_id),
                FOREIGN KEY (sender_id)  REFERENCES users(user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_messages_channel_ts
                ON messages(channel_id, timestamp);

            CREATE TABLE IF NOT EXISTS files (
                file_id          TEXT PRIMARY KEY,
                filename         TEXT NOT NULL,
                owner_id         TEXT NOT NULL,
                channel_id       TEXT NOT NULL,
                storage_location TEXT NOT NULL,
                size_bytes       INTEGER NOT NULL,
                content_type     TEXT NOT NULL DEFAULT 'application/octet-stream',
                uploaded_at      INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS presence (
                user_id   TEXT PRIMARY KEY,
                status    TEXT NOT NULL DEFAULT 'OFFLINE',
                last_seen INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
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
        user_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()
        }
        if "active" not in user_columns:
            conn.execute(
                "ALTER TABLE users ADD COLUMN active INTEGER NOT NULL DEFAULT 1"
            )
        conn.commit()
        conn.close()

    # ── Public helpers ────────────────────────────────────────────────────────

    def execute(self, query: str, params=()) -> sqlite3.Cursor:
        return self.conn.execute(query, params)

    @contextmanager
    def transaction(self, immediate: bool = False):
        """Run one unit of work and always commit or roll it back."""
        conn = self.conn
        if conn.in_transaction:
            raise RuntimeError("Nested database transactions are not supported")
        conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

    def executemany(self, query: str, params_list) -> sqlite3.Cursor:
        return self.conn.executemany(query, params_list)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        """Close this thread's connection when a service or test is shutting down."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def fetchone(self, query: str, params=()):
        return self.conn.execute(query, params).fetchone()

    def fetchall(self, query: str, params=()) -> list:
        return self.conn.execute(query, params).fetchall()

