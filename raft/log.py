"""Persistent SQLite Raft Log and Metadata Store."""

from dataclasses import asdict, dataclass
import json
from typing import Any, Dict, List, Optional


@dataclass
class LogEntry:
    index: int
    term: int
    command_type: str
    payload: Dict[str, Any]
    request_id: str = ""

    def __post_init__(self):
        self.index = int(self.index)
        self.term = int(self.term)
        self.request_id = self.request_id or ""
        if not isinstance(self.payload, dict):
            try:
                self.payload = json.loads(self.payload)
            except Exception:
                self.payload = {"raw": self.payload}

    def to_dict(self) -> dict:
        return asdict(self)


class RaftLog:
    def __init__(self, db):
        self.db = db

    def _get_meta(self, key: str, default: str = "") -> str:
        with self.db.lock:
            row = self.db.fetchone("SELECT value FROM raft_meta WHERE key = ?", (key,))
            return row["value"] if row else default

    def _set_meta(self, **items):
        with self.db.lock:
            for k, v in items.items():
                self.db.execute("INSERT OR REPLACE INTO raft_meta (key, value) VALUES (?, ?)", (k, str(v or "")))
            self.db.commit()

    def get_current_term(self) -> int:
        return int(self._get_meta("current_term", "0") or 0)

    def set_current_term(self, term: int):
        self._set_meta(current_term=term)

    def get_voted_for(self) -> Optional[str]:
        val = self._get_meta("voted_for", "")
        return val if val else None

    def set_voted_for(self, candidate_id: Optional[str]):
        self._set_meta(voted_for=candidate_id or "")

    def update_term_and_vote(self, term: int, voted_for: Optional[str]):
        self._set_meta(current_term=term, voted_for=voted_for or "")

    def append(self, term: int, command_type: str, payload: dict, request_id: str = "") -> LogEntry:
        entry = LogEntry(self.last_index() + 1, term, command_type, payload, request_id)
        self.append_entry(entry)
        return entry

    def append_entry(self, entry: LogEntry):
        with self.db.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO raft_log (log_index, term, command_type, payload, request_id) VALUES (?, ?, ?, ?, ?)",
                (entry.index, entry.term, entry.command_type, json.dumps(entry.payload), entry.request_id),
            )
            self.db.commit()

    def get(self, index: int) -> Optional[LogEntry]:
        if index <= 0:
            return None
        row = self.db.fetchone("SELECT * FROM raft_log WHERE log_index = ?", (index,))
        return LogEntry(row["log_index"], row["term"], row["command_type"], row["payload"], row["request_id"]) if row else None

    def last_index(self) -> int:
        row = self.db.fetchone("SELECT MAX(log_index) AS mx FROM raft_log")
        return int(row["mx"]) if (row and row["mx"] is not None) else 0

    def last_term(self) -> int:
        row = self.db.fetchone("SELECT term FROM raft_log ORDER BY log_index DESC LIMIT 1")
        return int(row["term"]) if row else 0

    def term_at(self, index: int) -> int:
        e = self.get(index)
        return e.term if e else 0

    def entries_from(self, start_index: int) -> List[LogEntry]:
        rows = self.db.fetchall("SELECT * FROM raft_log WHERE log_index >= ? ORDER BY log_index ASC", (max(1, start_index),))
        return [LogEntry(r["log_index"], r["term"], r["command_type"], r["payload"], r["request_id"]) for r in rows]

    def truncate_from(self, index: int):
        if index > 0:
            with self.db.lock:
                self.db.execute("DELETE FROM raft_log WHERE log_index >= ?", (index,))
                self.db.commit()
