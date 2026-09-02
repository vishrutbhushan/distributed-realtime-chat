"""
Raft log (Milestone 2 stub).

In M1 the app runs in STANDALONE mode with no actual replication.
The log is persisted to SQLite so M2 can build on top of it.
"""

import json
import logging
import time
from typing import List, Optional

logger = logging.getLogger(__name__)


class LogEntry:
    def __init__(
        self,
        index: int,
        term: int,
        command_type: str,
        payload: dict,
        request_id: str = "",
    ):
        self.index = index
        self.term = term
        self.command_type = command_type
        self.payload = payload
        self.request_id = request_id

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "term": self.term,
            "command_type": self.command_type,
            "payload": self.payload,
            "request_id": self.request_id,
        }


class RaftLog:
    """
    Persistent Raft log backed by SQLite.
    Milestone 1: only append() is used (by the state machine stub).
    Milestone 2: full Raft replication uses all methods.
    """

    def __init__(self, db):
        self.db = db

    def append(self, entry: LogEntry) -> int:
        self.db.execute(
            "INSERT OR REPLACE INTO raft_log (log_index, term, command_type, payload, request_id)"
            " VALUES (?, ?, ?, ?, ?)",
            (entry.index, entry.term, entry.command_type,
             json.dumps(entry.payload), entry.request_id),
        )
        self.db.commit()
        return entry.index

    def get(self, index: int) -> Optional[LogEntry]:
        row = self.db.fetchone("SELECT * FROM raft_log WHERE log_index = ?", (index,))
        if not row:
            return None
        return LogEntry(
            index=row["log_index"],
            term=row["term"],
            command_type=row["command_type"],
            payload=json.loads(row["payload"]),
            request_id=row["request_id"] or "",
        )

    def last_index(self) -> int:
        row = self.db.fetchone("SELECT MAX(log_index) AS mx FROM raft_log")
        return row["mx"] if row and row["mx"] is not None else 0

    def last_term(self) -> int:
        row = self.db.fetchone(
            "SELECT term FROM raft_log ORDER BY log_index DESC LIMIT 1"
        )
        return row["term"] if row else 0

    def entries_from(self, start_index: int) -> List[LogEntry]:
        rows = self.db.fetchall(
            "SELECT * FROM raft_log WHERE log_index >= ? ORDER BY log_index",
            (start_index,),
        )
        return [
            LogEntry(
                index=r["log_index"],
                term=r["term"],
                command_type=r["command_type"],
                payload=json.loads(r["payload"]),
                request_id=r["request_id"] or "",
            )
            for r in rows
        ]

    def truncate_from(self, index: int):
        """Remove all entries from index onwards (conflict resolution)."""
        self.db.execute("DELETE FROM raft_log WHERE log_index >= ?", (index,))
        self.db.commit()
