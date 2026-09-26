"""Channel, message, and real-time subscription management."""

import logging
import os
import queue
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ChatManager:
    def __init__(self, db):
        self.db = db
        self._subs: Dict[str, List[queue.Queue]] = {}
        self._sub_lock = threading.Lock()

    def get_channel(self, channel_id: str):
        row = self.db.fetchone(
            "SELECT channel_id, name, created_by, created_at FROM channels WHERE channel_id = ?",
            (channel_id,),
        )
        return dict(row) if row else None

    def get_channel_by_name(self, name: str):
        row = self.db.fetchone(
            "SELECT channel_id, name, created_by, created_at FROM channels WHERE name = ?",
            ((name or "").strip(),),
        )
        return dict(row) if row else None

    def is_member(self, channel_id: str, user_id: str) -> bool:
        return self.db.fetchone(
            "SELECT 1 FROM channel_members WHERE channel_id = ? AND user_id = ?",
            (channel_id, user_id),
        ) is not None

    def create_channel(self, name: str, created_by: str) -> Tuple[bool, dict, str]:
        name = (name or "").strip()
        if not name:
            return False, {}, "Channel name is required"
        channel_id = str(uuid.uuid4())
        now = int(time.time())
        try:
            with self.db.transaction(immediate=True):
                self.db.execute(
                    "INSERT INTO channels (channel_id, name, created_by, created_at)"
                    " VALUES (?, ?, ?, ?)",
                    (channel_id, name, created_by, now),
                )
                self.db.execute(
                    "INSERT INTO channel_members (channel_id, user_id, joined_at)"
                    " VALUES (?, ?, ?)",
                    (channel_id, created_by, now),
                )
            logger.info("[CHAT] Created channel #%s", name)
            return True, {
                "channel_id": channel_id,
                "name": name,
                "created_by": created_by,
                "created_at": now,
                "member_count": 1,
            }, "Channel created"
        except Exception as exc:
            if "UNIQUE" in str(exc):
                return False, {}, "Channel name already exists"
            logger.error("[CHAT] create_channel error: %s", exc)
            return False, {}, str(exc)

    def delete_channel(self, channel_id: str) -> Tuple[bool, str]:
        file_rows = []
        try:
            with self.db.transaction(immediate=True):
                channel = self.db.fetchone(
                    "SELECT channel_id FROM channels WHERE channel_id = ?",
                    (channel_id,),
                )
                if not channel:
                    return False, "Channel not found"
                file_rows = self.db.fetchall(
                    "SELECT storage_location FROM files WHERE channel_id = ?",
                    (channel_id,),
                )
                self.db.execute("DELETE FROM messages WHERE channel_id = ?", (channel_id,))
                self.db.execute("DELETE FROM files WHERE channel_id = ?", (channel_id,))
                self.db.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
            for row in file_rows:
                try:
                    os.remove(row["storage_location"])
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    logger.warning("[FILES] Could not remove deleted channel blob: %s", exc)
            with self._sub_lock:
                self._subs.pop(channel_id, None)
            return True, "Channel deleted"
        except Exception as exc:
            logger.error("[CHAT] delete_channel error: %s", exc)
            return False, str(exc)

    def join_channel(self, channel_id: str, user_id: str) -> Tuple[bool, str]:
        try:
            with self.db.transaction(immediate=True):
                if not self.get_channel(channel_id):
                    return False, "Channel not found"
                self.db.execute(
                    "INSERT OR IGNORE INTO channel_members"
                    " (channel_id, user_id, joined_at) VALUES (?, ?, ?)",
                    (channel_id, user_id, int(time.time())),
                )
            return True, "Joined channel"
        except Exception as exc:
            logger.error("[CHAT] join_channel error: %s", exc)
            return False, str(exc)

    def leave_channel(self, channel_id: str, user_id: str) -> Tuple[bool, str]:
        try:
            with self.db.transaction(immediate=True):
                cur = self.db.execute(
                    "DELETE FROM channel_members WHERE channel_id = ? AND user_id = ?",
                    (channel_id, user_id),
                )
                if cur.rowcount == 0:
                    return False, "Channel membership not found"
            return True, "Left channel"
        except Exception as exc:
            logger.error("[CHAT] leave_channel error: %s", exc)
            return False, str(exc)

    def list_channels(self) -> List[dict]:
        rows = self.db.fetchall(
            """
            SELECT c.channel_id, c.name, c.created_by, c.created_at,
                   COUNT(cm.user_id) AS member_count
            FROM channels c
            LEFT JOIN channel_members cm ON c.channel_id = cm.channel_id
            GROUP BY c.channel_id
            ORDER BY c.created_at, c.rowid
            """
        )
        return [dict(r) for r in rows]

    def send_message(
        self,
        channel_id: str,
        sender_id: str,
        content: str,
        client_request_id: Optional[str] = None,
        file_id: Optional[str] = None,
    ) -> Tuple[bool, dict, str]:
        """Insert a message once and notify subscribers only for a new row."""
        message_id = str(uuid.uuid4())
        timestamp = None
        result = None
        try:
            # Serializing the idempotency check and insert closes the concurrent retry race.
            with self.db.transaction(immediate=True):
                if not self.get_channel(channel_id):
                    return False, {}, "Channel not found"
                if not self.is_member(channel_id, sender_id):
                    return False, {}, "Join the channel before sending messages"
                if not (content or "").strip() and not file_id:
                    return False, {}, "Message content or a file attachment is required"
                user = self.db.fetchone(
                    "SELECT username FROM users WHERE user_id = ? AND active = 1",
                    (sender_id,),
                )
                if not user:
                    return False, {}, "Sender account is inactive"
                if file_id:
                    file_row = self.db.fetchone(
                        "SELECT file_id FROM files WHERE file_id = ? AND channel_id = ?",
                        (file_id, channel_id),
                    )
                    if not file_row:
                        return False, {}, "File not found in this channel"
                if client_request_id:
                    existing = self.db.fetchone(
                        """
                        SELECT m.*, u.username AS sender_username
                        FROM messages m JOIN users u ON m.sender_id = u.user_id
                        WHERE m.client_request_id = ?
                        """,
                        (client_request_id,),
                    )
                    if existing:
                        if (existing["channel_id"], existing["sender_id"]) != (channel_id, sender_id):
                            return False, {}, "Request ID was already used for another message"
                        logger.info("[CHAT] Idempotent replay request_id=%s", client_request_id)
                        return True, dict(existing), ""
                last = self.db.fetchone(
                    "SELECT MAX(timestamp) AS latest FROM messages WHERE channel_id = ?",
                    (channel_id,),
                )
                timestamp = max(
                    int(time.time() * 1000),
                    (int(last["latest"]) + 1) if last and last["latest"] is not None else 0,
                )
                self.db.execute(
                    """
                    INSERT INTO messages
                        (message_id, channel_id, sender_id, content, timestamp,
                         client_request_id, raft_log_index, file_id)
                    VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                    (message_id, channel_id, sender_id, content, timestamp,
                     client_request_id, file_id),
                )
                result = {
                    "message_id": message_id,
                    "channel_id": channel_id,
                    "sender_id": sender_id,
                    "sender_username": user["username"],
                    "content": content,
                    "timestamp": timestamp,
                    "client_request_id": client_request_id or "",
                    "raft_log_index": 0,
                    "file_id": file_id or "",
                }
        except Exception as exc:
            if client_request_id:
                existing = self.db.fetchone(
                    """
                    SELECT m.*, u.username AS sender_username
                    FROM messages m JOIN users u ON m.sender_id = u.user_id
                    WHERE m.client_request_id = ?
                    """,
                    (client_request_id,),
                )
                if existing and (existing["channel_id"], existing["sender_id"]) == (channel_id, sender_id):
                    return True, dict(existing), ""
            logger.error("[CHAT] send_message error: %s", exc)
            return False, {}, str(exc)

        self._notify_subscribers(channel_id, result)
        return True, result, ""

    def get_messages(
        self, channel_id: str, limit: int = 50, before_timestamp: int = 0
    ) -> List[dict]:
        limit = max(1, min(int(limit or 50), 500))
        if before_timestamp:
            rows = self.db.fetchall(
                """
                SELECT m.*, u.username AS sender_username
                FROM messages m JOIN users u ON m.sender_id = u.user_id
                WHERE m.channel_id = ? AND m.timestamp < ?
                ORDER BY m.timestamp DESC, m.rowid DESC
                LIMIT ?
                """,
                (channel_id, before_timestamp, limit),
            )
        else:
            rows = self.db.fetchall(
                """
                SELECT m.*, u.username AS sender_username
                FROM messages m JOIN users u ON m.sender_id = u.user_id
                WHERE m.channel_id = ?
                ORDER BY m.timestamp DESC, m.rowid DESC
                LIMIT ?
                """,
                (channel_id, limit),
            )
        msgs = [dict(r) for r in rows]
        msgs.reverse()
        return msgs

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
                if not self._subs[channel_id]:
                    self._subs.pop(channel_id, None)

    def _notify_subscribers(self, channel_id: str, msg: dict):
        with self._sub_lock:
            for subscriber in list(self._subs.get(channel_id, [])):
                try:
                    subscriber.put_nowait(msg)
                except queue.Full:
                    logger.warning("[STREAM] Dropped notification for slow subscriber")
