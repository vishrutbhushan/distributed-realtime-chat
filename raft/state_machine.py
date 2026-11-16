"""
Raft Replicated State Machine.

Executes committed Raft log entries sequentially against the local SQLite database
and notifies local real-time subscribers (SSE streams).
"""

import base64
import logging
import os
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)


class StateMachine:
    """
    Applies committed log entries in strict index order to the local node's SQLite DB
    and triggers local ChatManager / PresenceManager events.
    """

    def __init__(self, db, auth_manager=None, chat_manager=None, file_manager=None):
        self.db = db
        self.auth = auth_manager
        self.chat = chat_manager
        self.files = file_manager
        self.last_applied = 0

    def apply(self, entry) -> Dict[str, Any]:
        """
        Apply a committed LogEntry.
        Returns a result dictionary used to fulfill synchronous client RPCs.
        """
        cmd = entry.command_type
        payload = entry.payload
        logger.info("[STATE_MACHINE] Applying log_index=%d cmd=%s", entry.index, cmd)

        result: Dict[str, Any] = {"success": True, "index": entry.index}

        try:
            if cmd == "SIGNUP":
                result = self._apply_signup(payload)

            elif cmd == "LOGIN":
                result = self._apply_login(payload)

            elif cmd == "LOGOUT":
                result = self._apply_logout(payload)

            elif cmd == "UPDATE_PRESENCE":
                result = self._apply_update_presence(payload)

            elif cmd == "SEND_DM":
                result = self._apply_send_dm(payload, log_index=entry.index)

            elif cmd == "SEND_GROUP_MSG":
                result = self._apply_send_group_msg(payload, log_index=entry.index)

            elif cmd == "CREATE_GROUP":
                result = self._apply_create_group(payload)

            elif cmd == "UPDATE_GROUP":
                result = self._apply_update_group(payload)

            elif cmd == "UPLOAD_FILE_META":
                result = self._apply_upload_file_meta(payload)

            elif cmd == "MARK_READ":
                result = self._apply_mark_read(payload)

            elif cmd == "BUSINESS_REQUEST":
                result = {
                    "success": True,
                    "request_id": payload.get("request_id", ""),
                    "status": "COMPLETED",
                    "result": f"Processed business request: {payload.get('payload', '')}",
                }

            else:
                logger.warning("[STATE_MACHINE] Unknown command_type: %s", cmd)
                result = {"success": True, "message": f"Ignored unknown command {cmd}"}

        except Exception as exc:
            logger.error("[STATE_MACHINE] Error applying entry %d (%s): %s", entry.index, cmd, exc)
            result = {"success": False, "error": str(exc), "index": entry.index}

        self.last_applied = entry.index
        return result

    # ── Command Handlers ──────────────────────────────────────────────────────

    def _apply_signup(self, payload: dict) -> dict:
        user_id = payload["user_id"]
        username = payload["username"]
        password_hash = payload["password_hash"]
        created_at = payload.get("created_at", int(time.time()))
        token = payload.get("token")
        expires_at = payload.get("expires_at", created_at + 7 * 24 * 3600)

        with self.db.lock:
            self.db.execute(
                """
                INSERT OR IGNORE INTO users (user_id, username, password_hash, status, last_seen, created_at)
                VALUES (?, ?, ?, 'active', ?, ?)
                """,
                (user_id, username, password_hash, created_at, created_at),
            )
            if token:
                self.db.execute(
                    """
                    INSERT OR REPLACE INTO sessions (token, user_id, created_at, expires_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (token, user_id, created_at, expires_at),
                )
            self.db.commit()

        if self.chat:
            self.chat.publish_directory_changed_for_all()

        return {
            "success": True,
            "user_id": user_id,
            "username": username,
            "token": token or "",
        }

    def _apply_login(self, payload: dict) -> dict:
        user_id = payload["user_id"]
        username = payload.get("username", "")
        token = payload["token"]
        now = payload.get("created_at", int(time.time()))
        expires_at = payload.get("expires_at", now + 7 * 24 * 3600)

        with self.db.lock:
            self.db.execute(
                """
                INSERT OR REPLACE INTO sessions (token, user_id, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (token, user_id, now, expires_at),
            )
            self.db.execute(
                "UPDATE users SET status = 'active', last_seen = ? WHERE user_id = ?",
                (now, user_id),
            )
            self.db.commit()

        if self.chat:
            self.chat.publish_directory_changed_for_all()

        return {
            "success": True,
            "user_id": user_id,
            "username": username,
            "token": token,
        }

    def _apply_logout(self, payload: dict) -> dict:
        token = payload.get("token", "")
        now = payload.get("timestamp", int(time.time()))
        user_id = payload.get("user_id", "")

        with self.db.lock:
            if not user_id and token:
                row = self.db.fetchone("SELECT user_id FROM sessions WHERE token = ?", (token,))
                if row:
                    user_id = row["user_id"]
            if token:
                self.db.execute("DELETE FROM sessions WHERE token = ?", (token,))
            if user_id:
                active_session = self.db.fetchone(
                    "SELECT 1 FROM sessions WHERE user_id = ? AND expires_at > ? LIMIT 1",
                    (user_id, now),
                )
                status = "active" if active_session else "inactive"
                self.db.execute(
                    "UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?",
                    (status, now, user_id),
                )
            self.db.commit()

        if self.auth and self.auth.presence and token:
            self.auth.presence.end_session(token)

        if self.chat:
            self.chat.publish_directory_changed_for_all()

        return {"success": True, "user_id": user_id}

    def _apply_update_presence(self, payload: dict) -> dict:
        user_id = payload.get("user_id", "")
        status = payload.get("status", "active")
        now = payload.get("timestamp", int(time.time()))

        if user_id:
            with self.db.lock:
                if status == "active":
                    self.db.execute(
                        "UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?",
                        (status, now, user_id),
                    )
                else:
                    self.db.execute(
                        "UPDATE users SET status = ? WHERE user_id = ?",
                        (status, user_id),
                    )
                self.db.commit()

            if self.chat:
                self.chat.publish_directory_changed_for_all()

        return {"success": True}

    def _apply_send_dm(self, payload: dict, log_index: int = 0) -> dict:
        message_id = payload["message_id"]
        sender_id = payload["sender_id"]
        recipient_id = payload["recipient_id"]
        content = payload.get("content", "")
        file_id = payload.get("file_id")
        client_request_id = payload.get("client_request_id")

        with self.db.lock:
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
                    msg_dict = dict(existing)
                    msg_dict["raft_log_index"] = log_index
                    return {"success": True, "message": msg_dict}

            latest = self.db.fetchone(
                """
                SELECT COALESCE(MAX(timestamp), 0) AS timestamp FROM messages
                WHERE chat_type = 'DM' AND
                  ((sender_id = ? AND recipient_id = ?) OR (sender_id = ? AND recipient_id = ?))
                """,
                (sender_id, recipient_id, recipient_id, sender_id),
            )
            base_ts = int(payload.get("timestamp", int(time.time() * 1000)))
            ts = max(base_ts, int(latest["timestamp"] or 0) + 1)

            self.db.execute(
                """
                INSERT OR IGNORE INTO messages
                    (message_id, chat_type, sender_id, recipient_id, group_id, content,
                     file_id, timestamp, client_request_id)
                VALUES (?, 'DM', ?, ?, NULL, ?, ?, ?, ?)
                """,
                (message_id, sender_id, recipient_id, content, file_id, ts, client_request_id),
            )
            self.db.commit()

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

        msg_dict = dict(row) if row else {
            "message_id": message_id,
            "chat_type": "DM",
            "sender_id": sender_id,
            "sender_username": payload.get("sender_username", ""),
            "recipient_id": recipient_id,
            "content": content,
            "timestamp": ts,
            "client_request_id": client_request_id or "",
            "file_id": file_id or "",
        }
        msg_dict["raft_log_index"] = log_index

        if self.chat:
            self.chat.mark_as_read(sender_id, "DM", recipient_id, msg_dict["timestamp"], notify="local")
            self.chat._notify_user(recipient_id, msg_dict)
            self.chat._notify_user(sender_id, msg_dict)

        return {"success": True, "message": msg_dict}

    def _apply_send_group_msg(self, payload: dict, log_index: int = 0) -> dict:
        message_id = payload["message_id"]
        sender_id = payload["sender_id"]
        group_id = payload["group_id"]
        content = payload.get("content", "")
        file_id = payload.get("file_id")
        client_request_id = payload.get("client_request_id")

        with self.db.lock:
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
                    msg_dict = dict(existing)
                    msg_dict["raft_log_index"] = log_index
                    return {"success": True, "message": msg_dict}

            latest = self.db.fetchone(
                "SELECT COALESCE(MAX(timestamp), 0) AS timestamp FROM messages WHERE chat_type = 'GROUP' AND group_id = ?",
                (group_id,),
            )
            base_ts = int(payload.get("timestamp", int(time.time() * 1000)))
            ts = max(base_ts, int(latest["timestamp"] or 0) + 1)

            self.db.execute(
                """
                INSERT OR IGNORE INTO messages
                    (message_id, chat_type, sender_id, recipient_id, group_id, content,
                     file_id, timestamp, client_request_id)
                VALUES (?, 'GROUP', ?, NULL, ?, ?, ?, ?, ?)
                """,
                (message_id, sender_id, group_id, content, file_id, ts, client_request_id),
            )
            self.db.commit()

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
            members = self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (group_id,))

        msg_dict = dict(row) if row else {
            "message_id": message_id,
            "chat_type": "GROUP",
            "sender_id": sender_id,
            "sender_username": payload.get("sender_username", ""),
            "group_id": group_id,
            "content": content,
            "timestamp": ts,
            "client_request_id": client_request_id or "",
            "file_id": file_id or "",
        }
        msg_dict["raft_log_index"] = log_index

        if self.chat:
            self.chat.mark_as_read(sender_id, "GROUP", group_id, msg_dict["timestamp"], notify="local")
            for m in members:
                self.chat._notify_user(m["user_id"], msg_dict)

        return {"success": True, "message": msg_dict}

    def _apply_create_group(self, payload: dict) -> dict:
        group_id = payload["group_id"]
        name = payload["name"]
        creator_id = payload["creator_id"]
        initial_members = payload.get("initial_member_ids", [])
        now = payload.get("created_at", int(time.time()))

        with self.db.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO groups (group_id, name, created_by, created_at) VALUES (?, ?, ?, ?)",
                (group_id, name, creator_id, now),
            )
            self.db.execute(
                "INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'ADMIN', ?)",
                (group_id, creator_id, now),
            )
            for mid in set(initial_members):
                if mid and mid != creator_id:
                    self.db.execute(
                        "INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'MEMBER', ?)",
                        (group_id, mid, now),
                    )
            self.db.commit()

            creator_row = self.db.fetchone("SELECT username FROM users WHERE user_id = ?", (creator_id,))
            creator_name = creator_row["username"] if creator_row else ""
            count_row = self.db.fetchone("SELECT COUNT(*) AS cnt FROM group_members WHERE group_id = ?", (group_id,))
            member_rows = self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (group_id,))

        group_dict = {
            "group_id": group_id,
            "name": name,
            "created_by": creator_id,
            "created_by_name": creator_name,
            "created_at": now,
            "member_count": count_row["cnt"] if count_row else 1,
            "user_role": "ADMIN",
        }
        if self.chat:
            self.chat.publish_directory_changed(row["user_id"] for row in member_rows)

        return {"success": True, "group": group_dict}

    def _apply_update_group(self, payload: dict) -> dict:
        group_id = payload["group_id"]
        action = payload["action"].upper()
        target_user_id = payload.get("target_user_id")
        new_name = payload.get("new_name")
        now = int(time.time())

        with self.db.lock:
            old_members = self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (group_id,))
            old_member_ids = {row["user_id"] for row in old_members}

            if action == "RENAME" and new_name:
                self.db.execute("UPDATE groups SET name = ? WHERE group_id = ?", (new_name.strip(), group_id))
            elif action == "ADD_MEMBER" and target_user_id:
                self.db.execute(
                    "INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'MEMBER', ?)",
                    (group_id, target_user_id, now),
                )
            elif action == "REMOVE_MEMBER" and target_user_id:
                self.db.execute("DELETE FROM group_members WHERE group_id = ? AND user_id = ?", (group_id, target_user_id))
            elif action == "MAKE_ADMIN" and target_user_id:
                self.db.execute("UPDATE group_members SET role = 'ADMIN' WHERE group_id = ? AND user_id = ?", (group_id, target_user_id))
            self.db.commit()

            grp_row = self.db.fetchone("SELECT * FROM groups WHERE group_id = ?", (group_id,))
            count_row = self.db.fetchone("SELECT COUNT(*) AS cnt FROM group_members WHERE group_id = ?", (group_id,))
            creator_row = self.db.fetchone("SELECT username FROM users WHERE user_id = ?", (grp_row["created_by"],)) if grp_row else None
            new_members = self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (group_id,))
            new_member_ids = {row["user_id"] for row in new_members}

        updated_group = {
            "group_id": group_id,
            "name": grp_row["name"] if grp_row else "",
            "created_by": grp_row["created_by"] if grp_row else "",
            "created_by_name": creator_row["username"] if creator_row else "",
            "created_at": grp_row["created_at"] if grp_row else 0,
            "member_count": count_row["cnt"] if count_row else 1,
            "user_role": "ADMIN",
        }
        if self.chat:
            if action == "REMOVE_MEMBER" and target_user_id and target_user_id not in new_member_ids:
                self.chat.publish_event(
                    target_user_id,
                    "GROUP_ACCESS_REVOKED",
                    {"chat_type": "GROUP", "group_id": group_id},
                )
            self.chat.publish_directory_changed(old_member_ids | new_member_ids)

        return {"success": True, "group": updated_group}

    def _apply_upload_file_meta(self, payload: dict) -> dict:
        file_id = payload["file_id"]
        storage_location = payload["storage_location"]

        if self.files and getattr(self.files, "storage_path", None):
            os.makedirs(self.files.storage_path, exist_ok=True)
            storage_location = os.path.join(self.files.storage_path, file_id)
            data_b64 = payload.get("data_b64")
            if data_b64 and not os.path.exists(storage_location):
                try:
                    with open(storage_location, "wb") as fh:
                        fh.write(base64.b64decode(data_b64))
                except Exception as exc:
                    logger.warning("[STATE_MACHINE] Failed to write replicated file %s: %s", file_id, exc)

        with self.db.lock:
            self.db.execute(
                """
                INSERT OR IGNORE INTO files
                    (file_id, filename, file_type, content_type, size_bytes,
                     owner_id, chat_type, target_id, storage_location, uploaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_id,
                    payload["filename"],
                    payload["file_type"],
                    payload["content_type"],
                    payload["size_bytes"],
                    payload["owner_id"],
                    payload["chat_type"],
                    payload["target_id"],
                    storage_location,
                    payload.get("uploaded_at", int(time.time())),
                ),
            )
            self.db.commit()

        meta_out = {k: v for k, v in payload.items() if k != "data_b64"}
        meta_out["storage_location"] = storage_location
        return {"success": True, "file": meta_out}

    def _apply_mark_read(self, payload: dict) -> dict:
        if self.chat:
            self.chat.mark_as_read(
                user_id=payload.get("user_id", ""),
                chat_type=payload.get("chat_type", "DM"),
                target_id=payload.get("target_id", ""),
                timestamp=payload.get("timestamp"),
            )
        return {"success": True}
