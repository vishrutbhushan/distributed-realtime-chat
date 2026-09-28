"""
Chat manager: handles 1-on-1 Direct Messaging (DMs) and Group Chats.

Requirements satisfied:
- Send / receive 1-on-1 text messages to/from any registered user.
- Send / receive PDF and image files in DMs and groups.
- Group creation with custom name and user selection during creation (creator is ADMIN).
- Group members immediately see the group.
- Only ADMIN can rename group, add members, remove members, or make another user admin.
- Idempotent message delivery via client_request_id.
- Real-time subscriber notification.
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
        # user_id -> list of queue.Queue (for StreamMessages)
        self._subs: Dict[str, List[queue.Queue]] = {}
        self._sub_lock = threading.Lock()

    # Ã¢â€â‚¬Ã¢â€â‚¬ Real-time Streaming Subscriptions Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

    def subscribe(self, user_id: str, q: queue.Queue):
        with self._sub_lock:
            self._subs.setdefault(user_id, []).append(q)

    def unsubscribe(self, user_id: str, q: queue.Queue):
        with self._sub_lock:
            if user_id in self._subs:
                try:
                    self._subs[user_id].remove(q)
                except ValueError:
                    pass
                if not self._subs[user_id]:
                    del self._subs[user_id]

    def _notify_user(self, user_id: str, msg: dict):
        """Publish one committed chat message to all sessions for a user."""
        self.publish_event(user_id, "NEW_MESSAGE", msg)

    def publish_event(self, user_id: str, event_type: str, message: Optional[dict] = None):
        if not user_id:
            return
        event = {"event_type": event_type}
        if message is not None:
            event["message"] = message
        with self._sub_lock:
            for q in list(self._subs.get(user_id, [])):
                try:
                    q.put_nowait(event)
                except queue.Full:
                    # A slow browser must get an explicit signal that its event
                    # queue lost detail. Drain the stale entries and tell it to
                    # reload authoritative snapshots.
                    while True:
                        try:
                            q.get_nowait()
                        except queue.Empty:
                            break
                    try:
                        q.put_nowait({"event_type": "RESYNC_REQUIRED"})
                    except queue.Full:
                        logger.warning("[STREAM] Could not enqueue resync event for user %s", user_id)

    def publish_directory_changed(self, user_ids):
        for user_id in set(user_ids or []):
            self.publish_event(user_id, "DIRECTORY_CHANGED")

    def publish_directory_changed_for_all(self):
        rows = self.db.fetchall("SELECT user_id FROM users")
        self.publish_directory_changed(row["user_id"] for row in rows)

    def subscription_count(self, user_id: Optional[str] = None) -> int:
        """Expose a small read-only hook for stream lifecycle tests/diagnostics."""
        with self._sub_lock:
            if user_id is not None:
                return len(self._subs.get(user_id, []))
            return sum(len(subscribers) for subscribers in self._subs.values())

    def is_group_member(self, group_id: str, user_id: str) -> bool:
        row = self.db.fetchone(
            "SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?",
            (group_id, user_id),
        )
        return bool(row)

    # Ã¢â€â‚¬Ã¢â€â‚¬ Read Receipts & Unread Tracking Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

    def mark_as_read(
        self,
        user_id: str,
        chat_type: str,
        target_id: str,
        timestamp: Optional[int] = None,
        notify: bool | str = True,
    ):
        """Record that user_id has read messages up to timestamp in chat."""
        if not user_id or not target_id:
            return
        ts = timestamp if timestamp is not None else int(time.time() * 1000)
        with self.db.lock:
            try:
                self.db.execute(
                    """
                    INSERT INTO chat_reads (user_id, chat_type, target_id, last_read_timestamp)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(user_id, chat_type, target_id)
                    DO UPDATE SET last_read_timestamp = MAX(chat_reads.last_read_timestamp, excluded.last_read_timestamp)
                    """,
                    (user_id, chat_type, target_id, ts),
                )
                self.db.commit()
            except Exception as exc:
                logger.error("[CHAT] mark_as_read error: %s", exc)
                self.db.rollback()
                return
        if notify == "local":
            self.publish_event(
                user_id,
                "DIRECTORY_CHANGED",
                {"chat_type": chat_type, "target_id": target_id, "local_only": True},
            )
        elif notify:
            self.publish_event(
                user_id,
                "READ_STATE_CHANGED",
                {"chat_type": chat_type, "target_id": target_id},
            )

    def get_dm_metadata_for_user(self, user_id: str) -> Dict[str, dict]:
        """
        Returns a dict mapping other_user_id -> {
            "last_message_time": int,
            "unread_count": int,
            "last_message": str,
        }
        for all other registered users.
        """
        metadata = {}
        with self.db.lock:
            rows = self.db.fetchall(
                """
                SELECT 
                    u.user_id,
                    COALESCE(MAX(m.timestamp), 0) AS last_message_time,
                    (
                        SELECT CASE 
                            WHEN m2.file_id IS NOT NULL AND (m2.content IS NULL OR m2.content = '') 
                            THEN '[File Attachment]' 
                            ELSE m2.content 
                        END
                        FROM messages m2
                        WHERE m2.chat_type = 'DM' 
                          AND ((m2.sender_id = ? AND m2.recipient_id = u.user_id) OR (m2.sender_id = u.user_id AND m2.recipient_id = ?))
                        ORDER BY m2.timestamp DESC, m2.message_id DESC LIMIT 1
                    ) AS last_message,
                    (
                        SELECT COUNT(*) FROM messages m3
                        WHERE m3.chat_type = 'DM'
                          AND m3.sender_id = u.user_id
                          AND m3.recipient_id = ?
                          AND m3.timestamp > COALESCE((
                              SELECT cr.last_read_timestamp FROM chat_reads cr
                              WHERE cr.user_id = ? AND cr.chat_type = 'DM' AND cr.target_id = u.user_id
                          ), 0)
                    ) AS unread_count
                FROM users u
                LEFT JOIN messages m ON m.chat_type = 'DM' AND (
                    (m.sender_id = ? AND m.recipient_id = u.user_id) OR
                    (m.sender_id = u.user_id AND m.recipient_id = ?)
                )
                WHERE u.user_id != ?
                GROUP BY u.user_id
                """,
                (user_id, user_id, user_id, user_id, user_id, user_id, user_id),
            )
            for r in rows:
                metadata[r["user_id"]] = {
                    "last_message_time": r["last_message_time"] or 0,
                    "unread_count": r["unread_count"] or 0,
                    "last_message": (r["last_message"] or "")[:50],
                }
        return metadata

    def get_group_metadata_for_user(self, user_id: str) -> Dict[str, dict]:
        """
        Returns a dict mapping group_id -> {
            "last_message_time": int,
            "unread_count": int,
            "last_message": str,
        }
        for all groups user_id belongs to.
        """
        metadata = {}
        with self.db.lock:
            rows = self.db.fetchall(
                """
                SELECT 
                    g.group_id,
                    COALESCE(MAX(m.timestamp), g.created_at * 1000) AS last_message_time,
                    (
                        SELECT CASE 
                            WHEN m2.file_id IS NOT NULL AND (m2.content IS NULL OR m2.content = '') 
                            THEN '[File Attachment]' 
                            ELSE m2.content 
                        END
                        FROM messages m2
                        WHERE m2.chat_type = 'GROUP' AND m2.group_id = g.group_id
                        ORDER BY m2.timestamp DESC, m2.message_id DESC LIMIT 1
                    ) AS last_message,
                    (
                        SELECT COUNT(*) FROM messages m3
                        WHERE m3.chat_type = 'GROUP'
                          AND m3.group_id = g.group_id
                          AND m3.sender_id != ?
                          AND m3.timestamp > COALESCE((
                              SELECT cr.last_read_timestamp FROM chat_reads cr
                              WHERE cr.user_id = ? AND cr.chat_type = 'GROUP' AND cr.target_id = g.group_id
                          ), 0)
                    ) AS unread_count
                FROM groups g
                JOIN group_members gm ON g.group_id = gm.group_id AND gm.user_id = ?
                LEFT JOIN messages m ON m.chat_type = 'GROUP' AND m.group_id = g.group_id
                GROUP BY g.group_id
                """,
                (user_id, user_id, user_id),
            )
            for r in rows:
                metadata[r["group_id"]] = {
                    "last_message_time": r["last_message_time"] or 0,
                    "unread_count": r["unread_count"] or 0,
                    "last_message": (r["last_message"] or "")[:50],
                }
        return metadata

    # Ã¢â€â‚¬Ã¢â€â‚¬ Direct Messaging (1-to-1) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

    def send_dm(
        self,
        sender_id: str,
        recipient_id: str,
        content: str,
        client_request_id: Optional[str] = None,
        file_id: Optional[str] = None,
    ) -> Tuple[bool, dict, str]:
        """
        Send a direct message from sender_id to recipient_id.
        Idempotent: if client_request_id exists, returns existing message.
        """
        if not recipient_id or recipient_id == sender_id:
            return False, {}, "Invalid recipient for direct message"

        # Check recipient exists
        rcpt = self.db.fetchone("SELECT user_id, username FROM users WHERE user_id = ?", (recipient_id,))
        if not rcpt:
            return False, {}, "Recipient user not found"

        with self.db.lock:
            # Idempotency check
            if client_request_id:
                existing = self.db.fetchone(
                    """
                    SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                    FROM messages m
                    JOIN users u ON m.sender_id = u.user_id
                    LEFT JOIN files f ON m.file_id = f.file_id
                    WHERE m.client_request_id = ?
                    """,
                    (client_request_id,),
                )
                if existing:
                    logger.info("[CHAT] Idempotent replay for DM request_id=%s", client_request_id)
                    return True, dict(existing), ""

            message_id = str(uuid.uuid4())
            latest = self.db.fetchone(
                """
                SELECT COALESCE(MAX(timestamp), 0) AS timestamp FROM messages
                WHERE chat_type = 'DM' AND
                  ((sender_id = ? AND recipient_id = ?) OR (sender_id = ? AND recipient_id = ?))
                """,
                (sender_id, recipient_id, recipient_id, sender_id),
            )
            ts = max(int(time.time() * 1000), int(latest["timestamp"] or 0) + 1)

            try:
                self.db.execute(
                    """
                    INSERT INTO messages
                        (message_id, chat_type, sender_id, recipient_id, group_id, content,
                         file_id, timestamp, client_request_id)
                    VALUES (?, 'DM', ?, ?, NULL, ?, ?, ?, ?)
                    """,
                    (message_id, sender_id, recipient_id, content,
                     file_id, ts, client_request_id),
                )
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                if "UNIQUE" in str(exc) and client_request_id:
                    existing = self.db.fetchone(
                        """
                        SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                        FROM messages m
                        JOIN users u ON m.sender_id = u.user_id
                        LEFT JOIN files f ON m.file_id = f.file_id
                        WHERE m.client_request_id = ?
                        """,
                        (client_request_id,),
                    )
                    if existing:
                        logger.info("[CHAT] Concurrent replay for DM request_id=%s", client_request_id)
                        return True, dict(existing), ""
                logger.error("[CHAT] send_dm insert error: %s", exc)
                return False, {}, str(exc)

            # Build message dictionary
            row = self.db.fetchone(
                """
                SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                LEFT JOIN files f ON m.file_id = f.file_id
                WHERE m.message_id = ?
                """,
                (message_id,),
            )
            msg = dict(row) if row else {
                "message_id": message_id,
                "chat_type": "DM",
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "content": content,
                "timestamp": ts,
                "client_request_id": client_request_id or "",
                "file_id": file_id or "",
            }

        # Sender has read up to this message
        self.mark_as_read(sender_id, "DM", recipient_id, ts, notify="local")

        # Notify both sender and recipient
        self._notify_user(recipient_id, msg)
        self._notify_user(sender_id, msg)

        return True, msg, ""

    def get_dm_history(
        self,
        user_a: str,
        user_b: str,
        limit: int = 100,
        before_timestamp: int = 0,
    ) -> List[dict]:
        """Fetch chronological 1-on-1 message history between user_a and user_b."""
        if before_timestamp:
            rows = self.db.fetchall(
                """
                SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                LEFT JOIN files f ON m.file_id = f.file_id
                WHERE m.chat_type = 'DM'
                  AND ((m.sender_id = ? AND m.recipient_id = ?) OR (m.sender_id = ? AND m.recipient_id = ?))
                  AND m.timestamp < ?
                ORDER BY m.timestamp DESC, m.message_id DESC
                LIMIT ?
                """,
                (user_a, user_b, user_b, user_a, before_timestamp, limit),
            )
        else:
            rows = self.db.fetchall(
                """
                SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                LEFT JOIN files f ON m.file_id = f.file_id
                WHERE m.chat_type = 'DM'
                  AND ((m.sender_id = ? AND m.recipient_id = ?) OR (m.sender_id = ? AND m.recipient_id = ?))
                ORDER BY m.timestamp DESC, m.message_id DESC
                LIMIT ?
                """,
                (user_a, user_b, user_b, user_a, limit),
            )
        msgs = [dict(r) for r in rows]
        msgs.reverse()
        self.mark_as_read(user_a, "DM", user_b)
        return msgs

    # Ã¢â€â‚¬Ã¢â€â‚¬ Group Management Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

    def create_group(
        self,
        name: str,
        creator_id: str,
        initial_member_ids: List[str],
    ) -> Tuple[bool, dict, str]:
        """
        Create a group. Creator is ADMIN by default.
        initial_member_ids are added as MEMBERS during creation itself.
        """
        name = (name or "").strip()
        if not name:
            return False, {}, "Group name cannot be empty"

        group_id = str(uuid.uuid4())
        now = int(time.time())

        try:
            self.db.execute(
                "INSERT INTO groups (group_id, name, created_by, created_at) VALUES (?, ?, ?, ?)",
                (group_id, name, creator_id, now),
            )
            # Add creator as ADMIN
            self.db.execute(
                "INSERT INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'ADMIN', ?)",
                (group_id, creator_id, now),
            )
            # Add initial members as MEMBER
            added_count = 1
            for mid in set(initial_member_ids):
                if mid and mid != creator_id:
                    self.db.execute(
                        "INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'MEMBER', ?)",
                        (group_id, mid, now),
                    )
                    added_count += 1
            self.db.commit()

            creator_row = self.db.fetchone("SELECT username FROM users WHERE user_id = ?", (creator_id,))
            creator_name = creator_row["username"] if creator_row else ""

            group_dict = {
                "group_id": group_id,
                "name": name,
                "created_by": creator_id,
                "created_by_name": creator_name,
                "created_at": now,
                "member_count": added_count,
                "user_role": "ADMIN",
            }
            member_rows = self.db.fetchall(
                "SELECT user_id FROM group_members WHERE group_id = ?", (group_id,)
            )
            self.publish_directory_changed(row["user_id"] for row in member_rows)
            logger.info("[CHAT] Created group '%s' (id=%s, members=%d)", name, group_id, added_count)
            return True, group_dict, "Group created successfully"
        except Exception as exc:
            self.db.rollback()
            logger.error("[CHAT] create_group error: %s", exc)
            return False, {}, str(exc)

    def update_group(
        self,
        group_id: str,
        requesting_user_id: str,
        action: str,
        target_user_id: Optional[str] = None,
        new_name: Optional[str] = None,
    ) -> Tuple[bool, dict, str]:
        """
        Only ADMIN can:
          - RENAME: rename the group
          - ADD_MEMBER: add users
          - REMOVE_MEMBER: remove users
          - MAKE_ADMIN: promote another user to ADMIN
        """
        # Verify group exists
        grp = self.db.fetchone("SELECT * FROM groups WHERE group_id = ?", (group_id,))
        if not grp:
            return False, {}, "Group not found"

        # Verify requesting user is ADMIN or the group creator
        admin_check = self.db.fetchone(
            "SELECT role FROM group_members WHERE group_id = ? AND user_id = ?",
            (group_id, requesting_user_id),
        )
        is_creator = (grp["created_by"] == requesting_user_id)
        is_admin = is_creator or (admin_check and admin_check["role"] == "ADMIN")
        if not is_admin:
            return False, {}, "Permission denied: Only group admins can perform this action"

        old_members = self.db.fetchall(
            "SELECT user_id FROM group_members WHERE group_id = ?", (group_id,)
        )
        old_member_ids = {row["user_id"] for row in old_members}

        now = int(time.time())
        action = action.upper()

        try:
            if action == "RENAME":
                if not new_name or not new_name.strip():
                    return False, {}, "New group name cannot be empty"
                self.db.execute("UPDATE groups SET name = ? WHERE group_id = ?", (new_name.strip(), group_id))
                self.db.commit()
                logger.info("[CHAT] Group %s renamed to '%s'", group_id, new_name.strip())

            elif action == "ADD_MEMBER":
                if not target_user_id:
                    return False, {}, "Target user ID required"
                u_row = self.db.fetchone(
                    "SELECT user_id FROM users WHERE user_id = ? OR username = ?",
                    (target_user_id, target_user_id),
                )
                if not u_row:
                    return False, {}, f"User '{target_user_id}' not found"
                target_uid = u_row["user_id"]

                self.db.execute(
                    "INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'MEMBER', ?)",
                    (group_id, target_uid, now),
                )
                self.db.commit()
                logger.info("[CHAT] User %s added to group %s", target_uid, group_id)

            elif action == "REMOVE_MEMBER":
                if not target_user_id:
                    return False, {}, "Target user ID required"
                if target_user_id == requesting_user_id:
                    return False, {}, "Cannot remove yourself as admin"
                self.db.execute(
                    "DELETE FROM group_members WHERE group_id = ? AND user_id = ?",
                    (group_id, target_user_id),
                )
                self.db.commit()
                logger.info("[CHAT] User %s removed from group %s", target_user_id, group_id)

            elif action == "MAKE_ADMIN":
                if not target_user_id:
                    return False, {}, "Target user ID required"
                self.db.execute(
                    "UPDATE group_members SET role = 'ADMIN' WHERE group_id = ? AND user_id = ?",
                    (group_id, target_user_id),
                )
                self.db.commit()
                logger.info("[CHAT] User %s promoted to ADMIN in group %s", target_user_id, group_id)

            else:
                return False, {}, f"Unknown action: {action}"

            # Fetch updated group summary
            count_row = self.db.fetchone("SELECT COUNT(*) AS cnt FROM group_members WHERE group_id = ?", (group_id,))
            grp_row = self.db.fetchone("SELECT * FROM groups WHERE group_id = ?", (group_id,))
            creator_row = self.db.fetchone("SELECT username FROM users WHERE user_id = ?", (grp_row["created_by"],))

            updated_group = {
                "group_id": group_id,
                "name": grp_row["name"],
                "created_by": grp_row["created_by"],
                "created_by_name": creator_row["username"] if creator_row else "",
                "created_at": grp_row["created_at"],
                "member_count": count_row["cnt"] if count_row else 1,
                "user_role": "ADMIN",
            }
            new_members = self.db.fetchall(
                "SELECT user_id FROM group_members WHERE group_id = ?", (group_id,)
            )
            new_member_ids = {row["user_id"] for row in new_members}
            if action == "REMOVE_MEMBER" and target_user_id not in new_member_ids:
                self.publish_event(
                    target_user_id,
                    "GROUP_ACCESS_REVOKED",
                    {"chat_type": "GROUP", "group_id": group_id},
                )
            self.publish_directory_changed(old_member_ids | new_member_ids)
            return True, updated_group, f"Group action '{action}' succeeded"
        except Exception as exc:
            self.db.rollback()
            logger.error("[CHAT] update_group error: %s", exc)
            return False, {}, str(exc)

    def list_user_groups(self, user_id: str) -> List[dict]:
        """Return all groups that user_id belongs to."""
        rows = self.db.fetchall(
            """
            SELECT g.group_id, g.name, g.created_by, g.created_at,
                   u.username AS created_by_name,
                   CASE WHEN g.created_by = ? OR gm.role = 'ADMIN' THEN 'ADMIN' ELSE gm.role END AS user_role,
                   (SELECT COUNT(*) FROM group_members WHERE group_id = g.group_id) AS member_count
            FROM groups g
            JOIN group_members gm ON g.group_id = gm.group_id
            JOIN users u ON g.created_by = u.user_id
            WHERE gm.user_id = ?
            ORDER BY g.created_at DESC
            """,
            (user_id, user_id),
        )
        return [dict(r) for r in rows]

    def get_group_members(self, group_id: str, user_id: str) -> Tuple[bool, List[dict], str]:
        """Return members of a group if user_id is a member."""
        member_check = self.db.fetchone(
            "SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?",
            (group_id, user_id),
        )
        if not member_check:
            return False, [], "You are not a member of this group"

        rows = self.db.fetchall(
            """
            SELECT gm.user_id, u.username, gm.role, u.status, gm.joined_at
            FROM group_members gm
            JOIN users u ON gm.user_id = u.user_id
            WHERE gm.group_id = ?
            ORDER BY gm.role ASC, u.username ASC
            """,
            (group_id,),
        )
        return True, [dict(r) for r in rows], ""

    # Ã¢â€â‚¬Ã¢â€â‚¬ Group Messaging Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬

    def send_group_message(
        self,
        sender_id: str,
        group_id: str,
        content: str,
        client_request_id: Optional[str] = None,
        file_id: Optional[str] = None,
    ) -> Tuple[bool, dict, str]:
        """
        Send a message to a group. Sender must be an active group member.
        Broadcasts to all group members.
        """
        # Check sender membership
        member_check = self.db.fetchone(
            "SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?",
            (group_id, sender_id),
        )
        if not member_check:
            return False, {}, "Cannot send message: You are not a member of this group"

        with self.db.lock:
            # Idempotency check
            if client_request_id:
                existing = self.db.fetchone(
                    """
                    SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                    FROM messages m
                    JOIN users u ON m.sender_id = u.user_id
                    LEFT JOIN files f ON m.file_id = f.file_id
                    WHERE m.client_request_id = ?
                    """,
                    (client_request_id,),
                )
                if existing:
                    logger.info("[CHAT] Idempotent replay for group message request_id=%s", client_request_id)
                    return True, dict(existing), ""

            message_id = str(uuid.uuid4())
            latest = self.db.fetchone(
                "SELECT COALESCE(MAX(timestamp), 0) AS timestamp FROM messages WHERE chat_type = 'GROUP' AND group_id = ?",
                (group_id,),
            )
            ts = max(int(time.time() * 1000), int(latest["timestamp"] or 0) + 1)

            try:
                self.db.execute(
                    """
                    INSERT INTO messages
                        (message_id, chat_type, sender_id, recipient_id, group_id, content,
                         file_id, timestamp, client_request_id)
                    VALUES (?, 'GROUP', ?, NULL, ?, ?, ?, ?, ?)
                    """,
                    (message_id, sender_id, group_id, content,
                     file_id, ts, client_request_id),
                )
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                if "UNIQUE" in str(exc) and client_request_id:
                    existing = self.db.fetchone(
                        """
                        SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                        FROM messages m
                        JOIN users u ON m.sender_id = u.user_id
                        LEFT JOIN files f ON m.file_id = f.file_id
                        WHERE m.client_request_id = ?
                        """,
                        (client_request_id,),
                    )
                    if existing:
                        logger.info("[CHAT] Concurrent replay for group message request_id=%s", client_request_id)
                        return True, dict(existing), ""
                logger.error("[CHAT] send_group_message insert error: %s", exc)
                return False, {}, str(exc)

            # Retrieve full message
            row = self.db.fetchone(
                """
                SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                LEFT JOIN files f ON m.file_id = f.file_id
                WHERE m.message_id = ?
                """,
                (message_id,),
            )
            msg = dict(row) if row else {
                "message_id": message_id,
                "chat_type": "GROUP",
                "sender_id": sender_id,
                "group_id": group_id,
                "content": content,
                "timestamp": ts,
                "client_request_id": client_request_id or "",
                "file_id": file_id or "",
            }

            # Broadcast to all members of the group
            members = self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (group_id,))

        # Sender has read up to this message
        self.mark_as_read(sender_id, "GROUP", group_id, ts, notify="local")

        for m in members:
            self._notify_user(m["user_id"], msg)

        return True, msg, ""

    def get_group_history(
        self,
        group_id: str,
        requesting_user_id: str,
        limit: int = 100,
        before_timestamp: int = 0,
    ) -> Tuple[bool, List[dict], str]:
        """Fetch chronological message history for a group."""
        member_check = self.db.fetchone(
            "SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?",
            (group_id, requesting_user_id),
        )
        if not member_check:
            return False, [], "Permission denied: You are not a member of this group"

        if before_timestamp:
            rows = self.db.fetchall(
                """
                SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                LEFT JOIN files f ON m.file_id = f.file_id
                WHERE m.chat_type = 'GROUP' AND m.group_id = ? AND m.timestamp < ?
                ORDER BY m.timestamp DESC, m.message_id DESC
                LIMIT ?
                """,
                (group_id, before_timestamp, limit),
            )
        else:
            rows = self.db.fetchall(
                """
                SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                LEFT JOIN files f ON m.file_id = f.file_id
                WHERE m.chat_type = 'GROUP' AND m.group_id = ?
                ORDER BY m.timestamp DESC, m.message_id DESC
                LIMIT ?
                """,
                (group_id, limit),
            )
        msgs = [dict(r) for r in rows]
        msgs.reverse()
        self.mark_as_read(requesting_user_id, "GROUP", group_id)
        return True, msgs, ""
