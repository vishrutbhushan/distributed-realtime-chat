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

    # ── Real-time Streaming Subscriptions ──────────────────────────────────────

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

    def _notify_user(self, user_id: str, msg: dict):
        with self._sub_lock:
            for q in list(self._subs.get(user_id, [])):
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    pass

    # ── Direct Messaging (1-to-1) ─────────────────────────────────────────────

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
        ts = int(time.time() * 1000)
        raft_log_index = 0

        try:
            self.db.execute(
                """
                INSERT INTO messages
                    (message_id, chat_type, sender_id, recipient_id, group_id, content,
                     file_id, timestamp, client_request_id, raft_log_index)
                VALUES (?, 'DM', ?, ?, NULL, ?, ?, ?, ?, ?)
                """,
                (message_id, sender_id, recipient_id, content,
                 file_id, ts, client_request_id, raft_log_index),
            )
            self.db.commit()
        except Exception as exc:
            self.db.rollback()
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
                ORDER BY m.timestamp DESC
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
                ORDER BY m.timestamp DESC
                LIMIT ?
                """,
                (user_a, user_b, user_b, user_a, limit),
            )
        msgs = [dict(r) for r in rows]
        msgs.reverse()
        return msgs

    # ── Group Management ──────────────────────────────────────────────────────

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

        # Verify requesting user is ADMIN
        admin_check = self.db.fetchone(
            "SELECT role FROM group_members WHERE group_id = ? AND user_id = ?",
            (group_id, requesting_user_id),
        )
        if not admin_check or admin_check["role"] != "ADMIN":
            return False, {}, "Permission denied: Only group admins can perform this action"

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
                self.db.execute(
                    "INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'MEMBER', ?)",
                    (group_id, target_user_id, now),
                )
                self.db.commit()
                logger.info("[CHAT] User %s added to group %s", target_user_id, group_id)

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
                   gm.role AS user_role,
                   (SELECT COUNT(*) FROM group_members WHERE group_id = g.group_id) AS member_count
            FROM groups g
            JOIN group_members gm ON g.group_id = gm.group_id
            JOIN users u ON g.created_by = u.user_id
            WHERE gm.user_id = ?
            ORDER BY g.created_at DESC
            """,
            (user_id,),
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

    # ── Group Messaging ───────────────────────────────────────────────────────

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
        ts = int(time.time() * 1000)
        raft_log_index = 0

        try:
            self.db.execute(
                """
                INSERT INTO messages
                    (message_id, chat_type, sender_id, recipient_id, group_id, content,
                     file_id, timestamp, client_request_id, raft_log_index)
                VALUES (?, 'GROUP', ?, NULL, ?, ?, ?, ?, ?, ?)
                """,
                (message_id, sender_id, group_id, content,
                 file_id, ts, client_request_id, raft_log_index),
            )
            self.db.commit()
        except Exception as exc:
            self.db.rollback()
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
                ORDER BY m.timestamp DESC
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
                ORDER BY m.timestamp DESC
                LIMIT ?
                """,
                (group_id, limit),
            )
        msgs = [dict(r) for r in rows]
        msgs.reverse()
        return True, msgs, ""
