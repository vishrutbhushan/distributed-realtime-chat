"""
Chat manager: channel management and message handling.

Idempotency: client_request_id is stored with UNIQUE constraint so that
retried RPCs return the already-committed message instead of inserting a duplicate.
"""

import logging
import queue
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ChatManager:
    def __init__(self, db):
        self.db = db
        # channel_id -> list of queue.Queue (for StreamMessages)
        self._subs: Dict[str, List[queue.Queue]] = {}
        self._sub_lock = threading.Lock()

    # ── Channels ──────────────────────────────────────────────────────────────

    def create_channel(self, name: str, created_by: str) -> Tuple[bool, dict, str]:
        channel_id = str(uuid.uuid4())
        now = int(time.time())
        try:
            self.db.execute(
                "INSERT INTO channels (channel_id, name, created_by, created_at) VALUES (?, ?, ?, ?)",
                (channel_id, name, created_by, now),
            )
            # Creator is automatically a member
            self.db.execute(
                "INSERT OR IGNORE INTO channel_members (channel_id, user_id, joined_at) VALUES (?, ?, ?)",
                (channel_id, created_by, now),
            )
            self.db.commit()
            logger.info("[CHAT] Created channel #%s", name)
            ch = {
                "channel_id": channel_id,
                "name": name,
                "created_by": created_by,
                "created_at": now,
                "member_count": 1,
            }
            return True, ch, "Channel created"
        except Exception as exc:
            if "UNIQUE" in str(exc):
                return False, {}, "Channel name already exists"
            logger.error("[CHAT] create_channel error: %s", exc)
            return False, {}, str(exc)

    def delete_channel(self, channel_id: str) -> Tuple[bool, str]:
        try:
            self.db.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
            self.db.commit()
            return True, "Channel deleted"
        except Exception as exc:
            return False, str(exc)

    def join_channel(self, channel_id: str, user_id: str) -> Tuple[bool, str]:
        ch = self.db.fetchone(
            "SELECT channel_id FROM channels WHERE channel_id = ?", (channel_id,)
        )
        if not ch:
            return False, "Channel not found"
        try:
            self.db.execute(
                "INSERT OR IGNORE INTO channel_members (channel_id, user_id, joined_at) VALUES (?, ?, ?)",
                (channel_id, user_id, int(time.time())),
            )
            self.db.commit()
            return True, "Joined channel"
        except Exception as exc:
            return False, str(exc)

    def leave_channel(self, channel_id: str, user_id: str) -> Tuple[bool, str]:
        self.db.execute(
            "DELETE FROM channel_members WHERE channel_id = ? AND user_id = ?",
            (channel_id, user_id),
        )
        self.db.commit()
        return True, "Left channel"

    def list_channels(self) -> List[dict]:
        rows = self.db.fetchall(
            """
            SELECT c.channel_id, c.name, c.created_by, c.created_at,
                   COUNT(cm.user_id) AS member_count
            FROM channels c
            LEFT JOIN channel_members cm ON c.channel_id = cm.channel_id
            GROUP BY c.channel_id
            ORDER BY c.created_at
            """
        )
        return [dict(r) for r in rows]

    # ── Messages ──────────────────────────────────────────────────────────────

    def send_message(
        self,
        channel_id: str,
        sender_id: str,
        content: str,
        client_request_id: Optional[str] = None,
        file_id: Optional[str] = None,
    ) -> Tuple[bool, dict, str]:
        """
        Insert a message. Idempotent: if client_request_id already exists,
        return the existing row unchanged.
        """
        # Idempotency check
        if client_request_id:
            existing = self.db.fetchone(
                """
                SELECT m.*, u.username AS sender_username
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                WHERE m.client_request_id = ?
                """,
                (client_request_id,),
            )
            if existing:
                logger.info("[CHAT] Idempotent replay request_id=%s", client_request_id)
                return True, dict(existing), ""

        message_id     = str(uuid.uuid4())
        ts             = int(time.time() * 1000)   # millisecond epoch
        raft_log_index = 0                          # set by Raft in Milestone 2

        try:
            self.db.execute(
                """
                INSERT INTO messages
                    (message_id, channel_id, sender_id, content, timestamp,
                     client_request_id, raft_log_index, file_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (message_id, channel_id, sender_id, content, ts,
                 client_request_id, raft_log_index, file_id),
            )
            self.db.commit()
        except Exception as exc:
            logger.error("[CHAT] send_message error: %s", exc)
            return False, {}, str(exc)

        # Fetch sender username for the response
        row = self.db.fetchone(
            "SELECT username FROM users WHERE user_id = ?", (sender_id,)
        )
        msg = {
            "message_id":        message_id,
            "channel_id":        channel_id,
            "sender_id":         sender_id,
            "sender_username":   row["username"] if row else "",
            "content":           content,
            "timestamp":         ts,
            "client_request_id": client_request_id or "",
            "raft_log_index":    raft_log_index,
            "file_id":           file_id or "",
        }
        self._notify_subscribers(channel_id, msg)
        return True, msg, ""

    def get_messages(
        self, channel_id: str, limit: int = 50, before_timestamp: int = 0
    ) -> List[dict]:
        if before_timestamp:
            rows = self.db.fetchall(
                """
                SELECT m.*, u.username AS sender_username
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                WHERE m.channel_id = ? AND m.timestamp < ?
                ORDER BY m.timestamp DESC
                LIMIT ?
                """,
                (channel_id, before_timestamp, limit),
            )
        else:
            rows = self.db.fetchall(
                """
                SELECT m.*, u.username AS sender_username
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                WHERE m.channel_id = ?
                ORDER BY m.timestamp DESC
                LIMIT ?
                """,
                (channel_id, limit),
            )
        msgs = [dict(r) for r in rows]
        msgs.reverse()  # chronological order
        return msgs

    # ── Streaming subscriptions ───────────────────────────────────────────────

    def subscribe(self, channel_id: str, q: queue.Queue):
        with self._sub_lock:
            self._subs.setdefault(channel_id, []).append(q)

    def unsubscribe(self, channel_id: str, q: queue.Queue):
        with self._sub_lock:
            if channel_id in self._subs:
                try:
                    self._subs[channel_id].remove(q)
                except ValueError:
                    pass

    def _notify_subscribers(self, channel_id: str, msg: dict):
        with self._sub_lock:
            for q in list(self._subs.get(channel_id, [])):
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    pass

