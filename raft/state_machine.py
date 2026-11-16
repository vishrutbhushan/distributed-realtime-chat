"""Compact Replicated State Machine for Raft Consensus."""

import base64
import logging
import os
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

_MSG_QUERY = """
SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
FROM messages m JOIN users u ON m.sender_id = u.user_id
LEFT JOIN files f ON m.file_id = f.file_id
"""


class StateMachine:
    def __init__(self, db, auth_manager=None, chat_manager=None, file_manager=None):
        self.db, self.auth, self.chat, self.files = db, auth_manager, chat_manager, file_manager
        self.last_applied = 0

    def apply(self, entry) -> Dict[str, Any]:
        cmd, p, idx = entry.command_type, entry.payload, entry.index
        logger.info("[STATE_MACHINE] Applying log_index=%d cmd=%s", idx, cmd)
        try:
            handler = getattr(self, f"_on_{cmd.lower()}", None)
            res = handler(p, idx) if handler else {"success": True, "index": idx}
        except Exception as exc:
            logger.error("[STATE_MACHINE] Error at index %d (%s): %s", idx, cmd, exc)
            res = {"success": False, "error": str(exc), "index": idx}
        self.last_applied = idx
        return res

    def _on_signup(self, p: dict, _idx: int) -> dict:
        now = p.get("created_at", int(time.time()))
        with self.db.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, password_hash, status, last_seen, created_at) VALUES (?, ?, ?, 'active', ?, ?)",
                (p["user_id"], p["username"], p["password_hash"], now, now),
            )
            if p.get("token"):
                self.db.execute(
                    "INSERT OR REPLACE INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
                    (p["token"], p["user_id"], now, p.get("expires_at", now + 604800)),
                )
            self.db.commit()
        if self.chat:
            self.chat.publish_directory_changed_for_all()
        return {"success": True, "user_id": p["user_id"], "username": p["username"], "token": p.get("token", "")}

    def _on_login(self, p: dict, _idx: int) -> dict:
        now = p.get("created_at", int(time.time()))
        with self.db.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
                (p["token"], p["user_id"], now, p.get("expires_at", now + 604800)),
            )
            self.db.execute("UPDATE users SET status = 'active', last_seen = ? WHERE user_id = ?", (now, p["user_id"]))
            self.db.commit()
        if self.chat:
            self.chat.publish_directory_changed_for_all()
        return {"success": True, "user_id": p["user_id"], "username": p.get("username", ""), "token": p["token"]}

    def _on_logout(self, p: dict, _idx: int) -> dict:
        tok, uid, now = p.get("token", ""), p.get("user_id", ""), p.get("timestamp", int(time.time()))
        with self.db.lock:
            if tok:
                self.db.execute("DELETE FROM sessions WHERE token = ?", (tok,))
            if uid:
                active = self.db.fetchone("SELECT 1 FROM sessions WHERE user_id = ? AND expires_at > ? LIMIT 1", (uid, now))
                self.db.execute("UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?", ("active" if active else "inactive", now, uid))
            self.db.commit()
        if self.auth and self.auth.presence and tok:
            self.auth.presence.end_session(tok)
        if self.chat:
            self.chat.publish_directory_changed_for_all()
        return {"success": True}

    def _on_update_presence(self, p: dict, _idx: int) -> dict:
        uid, st, now = p.get("user_id", ""), p.get("status", "active"), p.get("timestamp", int(time.time()))
        if uid:
            with self.db.lock:
                self.db.execute("UPDATE users SET status = ?, last_seen = ? WHERE user_id = ?", (st, now, uid))
                self.db.commit()
            if self.chat:
                self.chat.publish_directory_changed_for_all()
        return {"success": True}

    def _insert_msg(self, chat_type: str, p: dict, idx: int) -> dict:
        req_id = p.get("client_request_id")
        with self.db.lock:
            if req_id:
                ex = self.db.fetchone(_MSG_QUERY + " WHERE m.client_request_id = ?", (req_id,))
                if ex:
                    return {"success": True, "message": {**dict(ex), "raft_log_index": idx}}
            if chat_type == "DM":
                s, r = p["sender_id"], p["recipient_id"]
                lat = self.db.fetchone("SELECT COALESCE(MAX(timestamp),0) AS ts FROM messages WHERE chat_type='DM' AND ((sender_id=? AND recipient_id=?) OR (sender_id=? AND recipient_id=?))", (s, r, r, s))
            else:
                lat = self.db.fetchone("SELECT COALESCE(MAX(timestamp),0) AS ts FROM messages WHERE chat_type='GROUP' AND group_id=?", (p["group_id"],))
            ts = max(int(p.get("timestamp", int(time.time() * 1000))), int(lat["ts"] or 0) + 1)
            self.db.execute(
                "INSERT OR IGNORE INTO messages (message_id, chat_type, sender_id, recipient_id, group_id, content, file_id, timestamp, client_request_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (p["message_id"], chat_type, p["sender_id"], p.get("recipient_id"), p.get("group_id"), p.get("content", ""), p.get("file_id"), ts, req_id),
            )
            self.db.commit()
            row = self.db.fetchone(_MSG_QUERY + " WHERE m.message_id = ?", (p["message_id"],))
        msg = dict(row) if row else {**p, "chat_type": chat_type, "timestamp": ts}
        msg["raft_log_index"] = idx
        return {"success": True, "message": msg}

    def _on_send_dm(self, p: dict, idx: int) -> dict:
        res = self._insert_msg("DM", p, idx)
        if self.chat and res.get("message"):
            m = res["message"]
            self.chat.mark_as_read(p["sender_id"], "DM", p["recipient_id"], m["timestamp"], notify="local")
            self.chat._notify_user(p["recipient_id"], m)
            self.chat._notify_user(p["sender_id"], m)
        return res

    def _on_send_group_msg(self, p: dict, idx: int) -> dict:
        res = self._insert_msg("GROUP", p, idx)
        if self.chat and res.get("message"):
            m = res["message"]
            self.chat.mark_as_read(p["sender_id"], "GROUP", p["group_id"], m["timestamp"], notify="local")
            for r in self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (p["group_id"],)):
                self.chat._notify_user(r["user_id"], m)
        return res

    def _on_create_group(self, p: dict, _idx: int) -> dict:
        gid, name, cid, now = p["group_id"], p["name"], p["creator_id"], p.get("created_at", int(time.time()))
        with self.db.lock:
            self.db.execute("INSERT OR IGNORE INTO groups (group_id, name, created_by, created_at) VALUES (?, ?, ?, ?)", (gid, name, cid, now))
            self.db.execute("INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'ADMIN', ?)", (gid, cid, now))
            for mid in set(p.get("initial_member_ids", [])):
                if mid and mid != cid:
                    self.db.execute("INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'MEMBER', ?)", (gid, mid, now))
            self.db.commit()
        return self._group_summary(gid, notify_members=True)

    def _on_update_group(self, p: dict, _idx: int) -> dict:
        gid, act, tid, now = p["group_id"], p["action"].upper(), p.get("target_user_id"), int(time.time())
        old_m = {r["user_id"] for r in self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (gid,))}
        with self.db.lock:
            if act == "RENAME" and p.get("new_name"):
                self.db.execute("UPDATE groups SET name = ? WHERE group_id = ?", (p["new_name"].strip(), gid))
            elif act == "ADD_MEMBER" and tid:
                self.db.execute("INSERT OR IGNORE INTO group_members (group_id, user_id, role, joined_at) VALUES (?, ?, 'MEMBER', ?)", (gid, tid, now))
            elif act == "REMOVE_MEMBER" and tid:
                self.db.execute("DELETE FROM group_members WHERE group_id = ? AND user_id = ?", (gid, tid))
            elif act == "MAKE_ADMIN" and tid:
                self.db.execute("UPDATE group_members SET role = 'ADMIN' WHERE group_id = ? AND user_id = ?", (gid, tid))
            self.db.commit()
        res = self._group_summary(gid)
        if self.chat:
            new_m = {r["user_id"] for r in self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (gid,))}
            if act == "REMOVE_MEMBER" and tid and tid not in new_m:
                self.chat.publish_event(tid, "GROUP_ACCESS_REVOKED", {"chat_type": "GROUP", "group_id": gid})
            self.chat.publish_directory_changed(old_m | new_m)
        return res

    def _group_summary(self, gid: str, notify_members: bool = False) -> dict:
        grp = self.db.fetchone("SELECT * FROM groups WHERE group_id = ?", (gid,))
        cnt = self.db.fetchone("SELECT COUNT(*) AS c FROM group_members WHERE group_id = ?", (gid,))
        cr = self.db.fetchone("SELECT username FROM users WHERE user_id = ?", (grp["created_by"],)) if grp else None
        if notify_members and self.chat:
            self.chat.publish_directory_changed(r["user_id"] for r in self.db.fetchall("SELECT user_id FROM group_members WHERE group_id = ?", (gid,)))
        return {"success": True, "group": {"group_id": gid, "name": grp["name"] if grp else "", "created_by": grp["created_by"] if grp else "", "created_by_name": cr["username"] if cr else "", "created_at": grp["created_at"] if grp else 0, "member_count": cnt["c"] if cnt else 1, "user_role": "ADMIN"}}

    def _on_upload_file_meta(self, p: dict, _idx: int) -> dict:
        fid, loc = p["file_id"], p["storage_location"]
        if self.files and getattr(self.files, "storage_path", None):
            os.makedirs(self.files.storage_path, exist_ok=True)
            loc = os.path.join(self.files.storage_path, fid)
            if p.get("data_b64") and not os.path.exists(loc):
                with open(loc, "wb") as fh:
                    fh.write(base64.b64decode(p["data_b64"]))
        with self.db.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO files (file_id, filename, file_type, content_type, size_bytes, owner_id, chat_type, target_id, storage_location, uploaded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (fid, p["filename"], p["file_type"], p["content_type"], p["size_bytes"], p["owner_id"], p["chat_type"], p["target_id"], loc, p.get("uploaded_at", int(time.time()))),
            )
            self.db.commit()
        return {"success": True, "file": {k: v for k, v in p.items() if k != "data_b64"}}

    def _on_business_request(self, p: dict, _idx: int) -> dict:
        return {"success": True, "request_id": p.get("request_id", ""), "status": "COMPLETED", "result": f"Processed business request: {p.get('payload', '')}"}
