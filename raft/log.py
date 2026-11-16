"""
Persistent Raft Log and Metadata Storage backed by SQLite.
"""

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class LogEntry:
    def __init__(
        self,
        index: int,
        term: int,
        command_type: str,
        payload: Dict[str, Any],
        request_id: str = "",
    ):
        self.index = int(index)
        self.term = int(term)
        self.command_type = command_type
        if isinstance(payload, dict):
            self.payload = payload
        else:
            try:
                self.payload = json.loads(payload)
            except Exception:
                self.payload = {"raw": payload}
        self.request_id = request_id or ""

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "term": self.term,
            "command_type": self.command_type,
            "payload": self.payload,
            "request_id": self.request_id,
        }

    def __repr__(self):
        return f"<LogEntry idx={self.index} term={self.term} cmd={self.command_type} req={self.request_id}>"


class RaftLog:
    """
    Persistent log and metadata store.
    Uses SQLite tables `raft_log` and `raft_meta`.
    """

    def __init__(self, db):
        self.db = db

    # ── Term and Vote Metadata ────────────────────────────────────────────────

    def get_current_term(self) -> int:
        with self.db.lock:
            row = self.db.fetchone("SELECT value FROM raft_meta WHERE key = 'current_term'")
            return int(row["value"]) if row else 0

    def set_current_term(self, term: int):
        with self.db.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('current_term', ?)",
                (str(term),),
            )
            self.db.commit()

    def get_voted_for(self) -> Optional[str]:
        with self.db.lock:
            row = self.db.fetchone("SELECT value FROM raft_meta WHERE key = 'voted_for'")
            return row["value"] if (row and row["value"] != "") else None

    def set_voted_for(self, candidate_id: Optional[str]):
        val = candidate_id or ""
        with self.db.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('voted_for', ?)",
                (val,),
            )
            self.db.commit()

    def update_term_and_vote(self, term: int, voted_for: Optional[str]):
        val = voted_for or ""
        with self.db.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('current_term', ?)",
                (str(term),),
            )
            self.db.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('voted_for', ?)",
                (val,),
            )
            self.db.commit()

    # ── Log Entry Operations ──────────────────────────────────────────────────

    def append(self, term: int, command_type: str, payload: dict, request_id: str = "") -> LogEntry:
        """Append a new entry with auto-incremented index (last_index + 1)."""
        with self.db.lock:
            next_idx = self.last_index() + 1
            payload_str = json.dumps(payload)
            self.db.execute(
                """
                INSERT INTO raft_log (log_index, term, command_type, payload, request_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (next_idx, term, command_type, payload_str, request_id),
            )
            self.db.commit()
            return LogEntry(next_idx, term, command_type, payload, request_id)

    def append_entry(self, entry: LogEntry):
        """Append or overwrite a specific entry at entry.index."""
        with self.db.lock:
            payload_str = json.dumps(entry.payload)
            self.db.execute(
                """
                INSERT OR REPLACE INTO raft_log (log_index, term, command_type, payload, request_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (entry.index, entry.term, entry.command_type, payload_str, entry.request_id),
            )
            self.db.commit()

    def get(self, index: int) -> Optional[LogEntry]:
        if index <= 0:
            return None
        with self.db.lock:
            row = self.db.fetchone("SELECT * FROM raft_log WHERE log_index = ?", (index,))
            if not row:
                return None
            return LogEntry(
                index=row["log_index"],
                term=row["term"],
                command_type=row["command_type"],
                payload=row["payload"],
                request_id=row["request_id"] or "",
            )

    def last_index(self) -> int:
        with self.db.lock:
            row = self.db.fetchone("SELECT MAX(log_index) AS mx FROM raft_log")
            return int(row["mx"]) if (row and row["mx"] is not None) else 0

    def last_term(self) -> int:
        with self.db.lock:
            row = self.db.fetchone("SELECT term FROM raft_log ORDER BY log_index DESC LIMIT 1")
            return int(row["term"]) if row else 0

    def term_at(self, index: int) -> int:
        if index <= 0:
            return 0
        entry = self.get(index)
        return entry.term if entry else 0

    def entries_from(self, start_index: int) -> List[LogEntry]:
        if start_index <= 0:
            start_index = 1
        with self.db.lock:
            rows = self.db.fetchall(
                "SELECT * FROM raft_log WHERE log_index >= ? ORDER BY log_index ASC",
                (start_index,),
            )
            return [
                LogEntry(
                    index=r["log_index"],
                    term=r["term"],
                    command_type=r["command_type"],
                    payload=r["payload"],
                    request_id=r["request_id"] or "",
                )
                for r in rows
            ]

    def truncate_from(self, index: int):
        """Delete all entries from index onwards (used when leader detects log mismatch)."""
        if index <= 0:
            return
        with self.db.lock:
            self.db.execute("DELETE FROM raft_log WHERE log_index >= ?", (index,))
            self.db.commit()
