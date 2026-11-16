"""
gRPC ChatService Servicer implementation.

Handles:
- User signup, login, logout, directory & presence tracking
- 1-on-1 Direct Messaging (DMs) with atomic idempotency
- Group chat creation with member selection checklist and Admin controls
- File sharing (images and PDFs) with byte verification and access control
- LLM smart replies & summarization over full conversation history
- Raft Consensus integration (Milestone 2): leader forwarding & log replication
- Assignment explicit RPC signatures: Post, Get, ProcessBusinessRequest
"""

import base64
import json
import logging
import queue
import threading
import time
import uuid
from typing import Dict, Optional

import grpc

import chat_pb2
import chat_pb2_grpc
import llm_pb2
import llm_pb2_grpc

from app.auth.manager import (
    MIN_PASSWORD_LEN,
    SESSION_TTL,
    USERNAME_REGEX,
    AuthManager,
    _hash,
)
from app.chat.manager import ChatManager
from app.files.manager import FileManager
from app.presence.manager import PresenceManager
from raft.node import NodeState, RaftNode
from storage.database import Database

logger = logging.getLogger(__name__)

STREAM_AUTH_CHECK_INTERVAL_SECONDS = 5.0
STREAM_HEARTBEAT_INTERVAL_SECONDS = 2.0


def create_llm_stub(llm_server_addr: str):
    """Create a gRPC stub to the local LLM server."""
    try:
        ch = grpc.insecure_channel(
            llm_server_addr,
            options=[
                ("grpc.max_send_message_length", 64 * 1024 * 1024),
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ("grpc.enable_retries", 1),
            ],
        )
        return llm_pb2_grpc.LLMServiceStub(ch)
    except Exception as exc:
        logger.warning("[GRPC] Could not create LLM channel: %s", exc)
        return None


class ChatServicer(chat_pb2_grpc.ChatServiceServicer):
    node_id: str = "node-1"
    raft: Optional[RaftNode] = None
    app_peers: Dict[str, str] = {}

    def __init__(
        self,
        db: Database,
        node_id: str,
        file_storage_path: str,
        llm_server_addr: str,
        raft_node: Optional[RaftNode] = None,
        app_peers: Optional[Dict[str, str]] = None,
    ):
        self.db = db
        self.node_id = str(node_id)
        self.chat = ChatManager(db)
        self.presence = PresenceManager(
            db,
            on_presence_change=self._on_presence_change,
        )
        self.auth = AuthManager(db, presence=self.presence)
        self.files = FileManager(db, file_storage_path)
        self.llm = create_llm_stub(llm_server_addr)

        self.raft = raft_node
        self.app_peers = dict(app_peers or {})
        self._peer_app_stubs: Dict[str, chat_pb2_grpc.ChatServiceStub] = {}
        self._stub_lock = threading.Lock()

        if self.raft and self.raft.state_machine:
            self.raft.state_machine.auth = self.auth
            self.raft.state_machine.chat = self.chat
            self.raft.state_machine.files = self.files

        logger.info(
            "[GRPC] ChatServicer initialized on %s (raft=%s)",
            self.node_id,
            bool(self.raft),
        )

    def _on_presence_change(self, user_ids: list):
        self.chat.publish_directory_changed_for_all()
        if self.raft and user_ids:
            def _replicate():
                for uid in user_ids:
                    row = self.db.fetchone("SELECT status FROM users WHERE user_id = ?", (uid,))
                    if not row:
                        continue
                    payload = {
                        "user_id": uid,
                        "status": row["status"],
                        "timestamp": int(time.time()),
                    }
                    if self.raft.state == NodeState.LEADER:
                        self.raft.propose("UPDATE_PRESENCE", payload)
            threading.Thread(target=_replicate, daemon=True).start()

    # ── Raft Leader Forwarding Helper ─────────────────────────────────────────

    def _get_peer_app_stub(self, peer_id: str) -> Optional[chat_pb2_grpc.ChatServiceStub]:
        with self._stub_lock:
            if peer_id in self._peer_app_stubs:
                return self._peer_app_stubs[peer_id]
            addr = self.app_peers.get(peer_id) or (self.raft.app_peers.get(peer_id) if self.raft else None)
            if not addr:
                return None
            ch = grpc.insecure_channel(
                addr,
                options=[
                    ("grpc.max_send_message_length", 64 * 1024 * 1024),
                    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ],
            )
            stub = chat_pb2_grpc.ChatServiceStub(ch)
            self._peer_app_stubs[peer_id] = stub
            return stub

    def _is_follower(self) -> bool:
        return bool(self.raft and len(self.raft.peers) > 1 and self.raft.state != NodeState.LEADER)

    def _forward_to_leader(self, method_name: str, request, context):
        """
        Transparently forwards a client write RPC from a Follower to the current Raft Leader.
        Waits briefly if an election is in progress.
        """
        if not self.raft:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Raft node not initialized")

        metadata = ()
        if context is not None:
            try:
                metadata = tuple(
                    (k, v)
                    for k, v in (
                        (item.key, item.value) if hasattr(item, "key") else item
                        for item in (context.invocation_metadata() or ())
                    )
                    if k.lower() == "x-chat-client-kind"
                )
            except Exception:
                metadata = ()

        for attempt in range(2):
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline:
                if self.raft.state == NodeState.LEADER:
                    return None
                if self.raft.leader_id and self.raft.leader_id != self.node_id:
                    break
                time.sleep(0.05)

            if self.raft.state == NodeState.LEADER:
                return None

            leader_id = self.raft.leader_id
            if not leader_id or leader_id == self.node_id:
                context.abort(grpc.StatusCode.UNAVAILABLE, "Cluster election in progress; no leader available")

            stub = self._get_peer_app_stub(leader_id)
            if not stub:
                context.abort(grpc.StatusCode.UNAVAILABLE, f"Cannot reach leader {leader_id}")

            logger.info("[RAFT-FWD] Forwarding %s from %s -> leader %s", method_name, self.node_id, leader_id)
            rpc_fn = getattr(stub, method_name)
            try:
                resp = rpc_fn(request, timeout=5.0, metadata=metadata)
                # Ensure local follower state machine has applied the new session token
                # before returning Signup/Login response to the browser.
                if method_name in ("Signup", "Login") and getattr(resp, "success", False) and getattr(resp, "token", ""):
                    sync_deadline = time.monotonic() + 0.5
                    while time.monotonic() < sync_deadline:
                        if self.auth.token_is_valid(resp.token):
                            break
                        time.sleep(0.005)
                return resp
            except grpc.RpcError as exc:
                if attempt == 0 and exc.code() == grpc.StatusCode.UNAVAILABLE:
                    with self._stub_lock:
                        self._peer_app_stubs.pop(leader_id, None)
                    time.sleep(0.35)
                    continue
                context.abort(exc.code(), exc.details())

    # ── Authentication helpers ────────────────────────────────────────────────

    @staticmethod
    def _client_kind(context) -> str:
        if context is None:
            return "rpc"
        try:
            for item in context.invocation_metadata() or ():
                key, value = (item.key, item.value) if hasattr(item, "key") else item
                if key.lower() == "x-chat-client-kind" and value:
                    return str(value).lower()
        except Exception:
            logger.debug("[AUTH] Client kind metadata was unavailable", exc_info=True)
        return "rpc"

    def _require_auth(self, token: str, context, *, record_activity: bool = True) -> dict:
        if not token:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing session token")
        sess = self.auth.validate_token(token)
        if not sess and self.raft:
            # Wait briefly in case a newly committed SIGNUP/LOGIN log entry is being applied on this follower
            wait_deadline = time.monotonic() + 0.25
            while time.monotonic() < wait_deadline:
                time.sleep(0.01)
                sess = self.auth.validate_token(token)
                if sess:
                    break
        if not sess:
            self.presence.end_session(token)
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or expired session token")
        if record_activity:
            self.presence.note_rpc_activity(
                sess["user_id"],
                token,
                sess["expires_at"],
                self._client_kind(context),
            )
        return sess

    # ── Authentication RPCs ───────────────────────────────────────────────────

    def Signup(self, request, context):
        if self._is_follower():
            fwd = self._forward_to_leader("Signup", request, context)
            if fwd is not None:
                return fwd

        logger.info("[RPC] Signup request for username: %s", request.username)
        if not self.raft:
            ok, token, uid, uname, msg = self.auth.signup(
                request.username, request.password, self._client_kind(context)
            )
            if ok:
                self.chat.publish_directory_changed_for_all()
            return chat_pb2.SignupResponse(
                success=ok,
                token=token,
                user_id=uid,
                username=uname,
                message=msg,
            )

        username = (request.username or "").strip()
        if not USERNAME_REGEX.match(username):
            return chat_pb2.SignupResponse(
                success=False,
                message="Username must be 3-20 alphanumeric characters or underscores.",
            )
        if not request.password or len(request.password) < MIN_PASSWORD_LEN:
            return chat_pb2.SignupResponse(
                success=False,
                message=f"Password must be at least {MIN_PASSWORD_LEN} characters long.",
            )

        row = self.db.fetchone("SELECT user_id FROM users WHERE username = ?", (username,))
        if row:
            return chat_pb2.SignupResponse(
                success=False,
                message="Username already exists. Please choose another.",
            )

        user_id = str(uuid.uuid4())
        token = str(uuid.uuid4())
        now = int(time.time())
        payload = {
            "user_id": user_id,
            "username": username,
            "password_hash": _hash(request.password),
            "token": token,
            "created_at": now,
            "expires_at": now + SESSION_TTL,
        }
        ok, err, _, _ = self.raft.propose("SIGNUP", payload)
        if not ok:
            return chat_pb2.SignupResponse(success=False, message=f"Consensus error: {err}")
        return chat_pb2.SignupResponse(
            success=True,
            token=token,
            user_id=user_id,
            username=username,
            message="Account created successfully",
        )

    def Login(self, request, context):
        if self._is_follower():
            fwd = self._forward_to_leader("Login", request, context)
            if fwd is not None:
                return fwd

        logger.info("[RPC] Login request for username: %s", request.username)
        if not self.raft:
            ok, token, uid, uname, msg = self.auth.login(
                request.username, request.password, self._client_kind(context)
            )
            if ok:
                self.chat.publish_directory_changed_for_all()
            return chat_pb2.LoginResponse(
                success=ok,
                token=token,
                user_id=uid,
                username=uname,
                message=msg,
            )

        username = (request.username or "").strip()
        row = self.db.fetchone(
            "SELECT user_id, username FROM users WHERE username = ? AND password_hash = ?",
            (username, _hash(request.password or "")),
        )
        if not row:
            return chat_pb2.LoginResponse(
                success=False,
                message="Invalid username or password",
            )

        user_id = row["user_id"]
        token = str(uuid.uuid4())
        now = int(time.time())
        payload = {
            "user_id": user_id,
            "username": row["username"],
            "token": token,
            "created_at": now,
            "expires_at": now + SESSION_TTL,
        }
        ok, err, _, _ = self.raft.propose("LOGIN", payload)
        if not ok:
            return chat_pb2.LoginResponse(success=False, message=f"Consensus error: {err}")
        return chat_pb2.LoginResponse(
            success=True,
            token=token,
            user_id=user_id,
            username=row["username"],
            message="Login successful",
        )

    def Logout(self, request, context):
        if self._is_follower():
            fwd = self._forward_to_leader("Logout", request, context)
            if fwd is not None:
                return fwd

        if not self.raft:
            ok = self.auth.logout(request.token)
            if ok:
                self.chat.publish_directory_changed_for_all()
            return chat_pb2.LogoutResponse(
                success=ok,
                message="Logged out successfully" if ok else "Invalid token or already logged out",
            )

        row = self.db.fetchone("SELECT user_id FROM sessions WHERE token = ?", (request.token,))
        if not row:
            return chat_pb2.LogoutResponse(
                success=False,
                message="Invalid token or already logged out",
            )
        ok, _, _, _ = self.raft.propose(
            "LOGOUT",
            {
                "token": request.token,
                "user_id": row["user_id"],
                "timestamp": int(time.time()),
            },
        )
        return chat_pb2.LogoutResponse(
            success=ok,
            message="Logged out successfully" if ok else "Logout failed",
        )

    # ── Directory and presence RPCs ───────────────────────────────────────────

    def ListUsers(self, request, context):
        sess = self._require_auth(request.token, context)
        users = self.auth.list_users(exclude_user_id=sess["user_id"])
        meta = self.chat.get_dm_metadata_for_user(sess["user_id"])
        return chat_pb2.ListUsersResponse(
            users=[
                chat_pb2.UserItem(
                    user_id=u["user_id"],
                    username=u["username"],
                    status=u["status"],
                    last_seen=u["last_seen"],
                    last_message_time=meta.get(u["user_id"], {}).get("last_message_time", 0),
                    unread_count=meta.get(u["user_id"], {}).get("unread_count", 0),
                    last_message=meta.get(u["user_id"], {}).get("last_message", ""),
                )
                for u in users
            ]
        )

    def GetUser(self, request, context):
        self._require_auth(request.token, context)
        u = self.auth.get_user_by_id(request.user_id)
        if not u:
            context.abort(grpc.StatusCode.NOT_FOUND, "User not found")
        return chat_pb2.GetUserResponse(
            success=True,
            user=chat_pb2.UserItem(
                user_id=u["user_id"],
                username=u["username"],
                status=u["status"],
                last_seen=u["last_seen"],
            ),
        )

    def UpdatePresence(self, request, context):
        sess = self._require_auth(request.token, context)
        return chat_pb2.UpdatePresenceResponse(
            success=self.presence.request_status(request.token, request.status)
        )

    # ── Direct messaging RPCs ─────────────────────────────────────────────────

    def SendDirectMessage(self, request, context):
        if self._is_follower():
            fwd = self._forward_to_leader("SendDirectMessage", request, context)
            if fwd is not None:
                return fwd

        sess = self._require_auth(request.token, context)
        started = time.monotonic()
        logger.info(
            "[CHAT] DM send start sender=%s recipient=%s request_id=%s",
            sess["user_id"], request.recipient_user_id, request.client_request_id or "-",
        )

        if not self.raft:
            ok, msg, err = self.chat.send_dm(
                sender_id=sess["user_id"],
                recipient_id=request.recipient_user_id,
                content=request.content,
                client_request_id=request.client_request_id or None,
                file_id=request.file_id or None,
            )
            if ok:
                logger.info(
                    "[CHAT] DM send complete message_id=%s elapsed_ms=%.1f",
                    msg.get("message_id", "-"), (time.monotonic() - started) * 1000,
                )
                return chat_pb2.SendMessageResponse(
                    success=True,
                    message=self._to_message_proto(msg),
                )
            logger.warning(
                "[CHAT] DM send rejected sender=%s recipient=%s error=%s elapsed_ms=%.1f",
                sess["user_id"], request.recipient_user_id, err, (time.monotonic() - started) * 1000,
            )
            return chat_pb2.SendMessageResponse(success=False, error=err)

        recipient_id = request.recipient_user_id
        if not recipient_id or recipient_id == sess["user_id"]:
            return chat_pb2.SendMessageResponse(success=False, error="Invalid recipient for direct message")

        rcpt = self.db.fetchone("SELECT user_id FROM users WHERE user_id = ?", (recipient_id,))
        if not rcpt:
            return chat_pb2.SendMessageResponse(success=False, error="Recipient user not found")

        c_req_id = request.client_request_id or str(uuid.uuid4())
        payload = {
            "message_id": str(uuid.uuid4()),
            "sender_id": sess["user_id"],
            "sender_username": sess["username"],
            "recipient_id": recipient_id,
            "content": request.content,
            "file_id": request.file_id or None,
            "timestamp": int(time.time() * 1000),
            "client_request_id": c_req_id,
        }
        ok, err, _, res = self.raft.propose("SEND_DM", payload, request_id=c_req_id)
        if not ok or not res.get("success"):
            return chat_pb2.SendMessageResponse(success=False, error=err or res.get("error", "Consensus error"))
        return chat_pb2.SendMessageResponse(
            success=True,
            message=self._to_message_proto(res["message"]),
        )

    def GetDirectMessages(self, request, context):
        sess = self._require_auth(request.token, context)
        msgs = self.chat.get_dm_history(
            user_a=sess["user_id"],
            user_b=request.other_user_id,
            limit=request.limit or 100,
            before_timestamp=request.before_timestamp,
        )
        return chat_pb2.GetMessagesResponse(
            messages=[self._to_message_proto(m) for m in msgs]
        )

    # ── Group management and messaging RPCs ───────────────────────────────────

    def CreateGroup(self, request, context):
        if self._is_follower():
            fwd = self._forward_to_leader("CreateGroup", request, context)
            if fwd is not None:
                return fwd

        sess = self._require_auth(request.token, context)
        member_ids = list(request.initial_member_user_ids)

        if not self.raft:
            ok, g, msg = self.chat.create_group(
                name=request.name,
                creator_id=sess["user_id"],
                initial_member_ids=member_ids,
            )
            if ok:
                return chat_pb2.CreateGroupResponse(
                    success=True,
                    group=chat_pb2.Group(
                        group_id=g["group_id"],
                        name=g["name"],
                        created_by=g["created_by"],
                        created_by_name=g.get("created_by_name", ""),
                        created_at=g["created_at"],
                        member_count=g.get("member_count", 1),
                        user_role=g.get("user_role", "ADMIN"),
                    ),
                    message=msg,
                )
            return chat_pb2.CreateGroupResponse(success=False, message=msg)

        name = (request.name or "").strip()
        if not name:
            return chat_pb2.CreateGroupResponse(success=False, message="Group name cannot be empty")

        payload = {
            "group_id": str(uuid.uuid4()),
            "name": name,
            "creator_id": sess["user_id"],
            "initial_member_ids": member_ids,
            "created_at": int(time.time()),
        }
        ok, err, _, res = self.raft.propose("CREATE_GROUP", payload)
        if not ok or not res.get("success"):
            return chat_pb2.CreateGroupResponse(success=False, message=err or res.get("error", "Consensus error"))
        g = res["group"]
        return chat_pb2.CreateGroupResponse(
            success=True,
            group=chat_pb2.Group(
                group_id=g["group_id"],
                name=g["name"],
                created_by=g["created_by"],
                created_by_name=g.get("created_by_name", ""),
                created_at=g["created_at"],
                member_count=g.get("member_count", 1),
                user_role="ADMIN",
            ),
            message="Group created successfully",
        )

    def UpdateGroup(self, request, context):
        if self._is_follower():
            fwd = self._forward_to_leader("UpdateGroup", request, context)
            if fwd is not None:
                return fwd

        sess = self._require_auth(request.token, context)

        if not self.raft:
            ok, g, msg = self.chat.update_group(
                group_id=request.group_id,
                requesting_user_id=sess["user_id"],
                action=request.action,
                target_user_id=request.target_user_id or None,
                new_name=request.new_name or None,
            )
            if ok:
                return chat_pb2.UpdateGroupResponse(
                    success=True,
                    group=chat_pb2.Group(
                        group_id=g["group_id"],
                        name=g["name"],
                        created_by=g["created_by"],
                        created_by_name=g.get("created_by_name", ""),
                        created_at=g["created_at"],
                        member_count=g.get("member_count", 0),
                        user_role="ADMIN",
                    ),
                    message=msg,
                )
            return chat_pb2.UpdateGroupResponse(success=False, message=msg)

        grp = self.db.fetchone("SELECT * FROM groups WHERE group_id = ?", (request.group_id,))
        if not grp:
            return chat_pb2.UpdateGroupResponse(success=False, message="Group not found")

        admin_check = self.db.fetchone(
            "SELECT role FROM group_members WHERE group_id = ? AND user_id = ?",
            (request.group_id, sess["user_id"]),
        )
        is_creator = grp["created_by"] == sess["user_id"]
        is_admin = is_creator or (admin_check and admin_check["role"] == "ADMIN")
        if not is_admin:
            return chat_pb2.UpdateGroupResponse(
                success=False,
                message="Permission denied: Only group admins can perform this action",
            )

        action = (request.action or "").upper()
        target_uid = request.target_user_id or None
        if action == "RENAME":
            if not request.new_name or not request.new_name.strip():
                return chat_pb2.UpdateGroupResponse(success=False, message="New group name cannot be empty")
        elif action == "ADD_MEMBER":
            if not target_uid:
                return chat_pb2.UpdateGroupResponse(success=False, message="Target user ID required")
            u_row = self.db.fetchone(
                "SELECT user_id FROM users WHERE user_id = ? OR username = ?",
                (target_uid, target_uid),
            )
            if not u_row:
                return chat_pb2.UpdateGroupResponse(success=False, message=f"User '{target_uid}' not found")
            target_uid = u_row["user_id"]
        elif action == "REMOVE_MEMBER":
            if not target_uid:
                return chat_pb2.UpdateGroupResponse(success=False, message="Target user ID required")
            if target_uid == sess["user_id"]:
                return chat_pb2.UpdateGroupResponse(success=False, message="Cannot remove yourself as admin")
        elif action == "MAKE_ADMIN":
            if not target_uid:
                return chat_pb2.UpdateGroupResponse(success=False, message="Target user ID required")
        else:
            return chat_pb2.UpdateGroupResponse(success=False, message=f"Unknown action: {action}")

        payload = {
            "group_id": request.group_id,
            "action": action,
            "target_user_id": target_uid,
            "new_name": request.new_name or None,
        }
        ok, err, _, res = self.raft.propose("UPDATE_GROUP", payload)
        if not ok or not res.get("success"):
            return chat_pb2.UpdateGroupResponse(success=False, message=err or res.get("error", "Consensus error"))
        g = res["group"]
        return chat_pb2.UpdateGroupResponse(
            success=True,
            group=chat_pb2.Group(
                group_id=g["group_id"],
                name=g["name"],
                created_by=g["created_by"],
                created_by_name=g.get("created_by_name", ""),
                created_at=g["created_at"],
                member_count=g.get("member_count", 0),
                user_role="ADMIN",
            ),
            message=f"Group action '{action}' succeeded",
        )

    def ListGroups(self, request, context):
        sess = self._require_auth(request.token, context)
        groups = self.chat.list_user_groups(sess["user_id"])
        meta = self.chat.get_group_metadata_for_user(sess["user_id"])
        return chat_pb2.ListGroupsResponse(
            groups=[
                chat_pb2.Group(
                    group_id=g["group_id"],
                    name=g["name"],
                    created_by=g["created_by"],
                    created_by_name=g.get("created_by_name", ""),
                    created_at=g["created_at"],
                    member_count=g.get("member_count", 1),
                    user_role=g.get("user_role", "MEMBER"),
                    last_message_time=meta.get(g["group_id"], {}).get("last_message_time", 0),
                    unread_count=meta.get(g["group_id"], {}).get("unread_count", 0),
                    last_message=meta.get(g["group_id"], {}).get("last_message", ""),
                )
                for g in groups
            ]
        )

    def GetGroupMembers(self, request, context):
        sess = self._require_auth(request.token, context)
        ok, members, err = self.chat.get_group_members(request.group_id, sess["user_id"])
        if not ok:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, err)
        return chat_pb2.GetGroupMembersResponse(
            members=[
                chat_pb2.GroupMember(
                    user_id=m["user_id"],
                    username=m["username"],
                    role=m["role"],
                    status=m["status"],
                    joined_at=m["joined_at"],
                )
                for m in members
            ]
        )

    def SendGroupMessage(self, request, context):
        if self._is_follower():
            fwd = self._forward_to_leader("SendGroupMessage", request, context)
            if fwd is not None:
                return fwd

        sess = self._require_auth(request.token, context)
        started = time.monotonic()
        logger.info(
            "[CHAT] group send start sender=%s group=%s request_id=%s",
            sess["user_id"], request.group_id, request.client_request_id or "-",
        )

        if not self.raft:
            ok, msg, err = self.chat.send_group_message(
                sender_id=sess["user_id"],
                group_id=request.group_id,
                content=request.content,
                client_request_id=request.client_request_id or None,
                file_id=request.file_id or None,
            )
            if ok:
                logger.info(
                    "[CHAT] group send complete message_id=%s elapsed_ms=%.1f",
                    msg.get("message_id", "-"), (time.monotonic() - started) * 1000,
                )
                return chat_pb2.SendMessageResponse(
                    success=True,
                    message=self._to_message_proto(msg),
                )
            logger.warning(
                "[CHAT] group send rejected sender=%s group=%s error=%s elapsed_ms=%.1f",
                sess["user_id"], request.group_id, err, (time.monotonic() - started) * 1000,
            )
            return chat_pb2.SendMessageResponse(success=False, error=err)

        member_check = self.db.fetchone(
            "SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?",
            (request.group_id, sess["user_id"]),
        )
        if not member_check:
            return chat_pb2.SendMessageResponse(
                success=False,
                error="Cannot send message: You are not a member of this group",
            )

        c_req_id = request.client_request_id or str(uuid.uuid4())
        payload = {
            "message_id": str(uuid.uuid4()),
            "sender_id": sess["user_id"],
            "sender_username": sess["username"],
            "group_id": request.group_id,
            "content": request.content,
            "file_id": request.file_id or None,
            "timestamp": int(time.time() * 1000),
            "client_request_id": c_req_id,
        }
        ok, err, _, res = self.raft.propose("SEND_GROUP_MSG", payload, request_id=c_req_id)
        if not ok or not res.get("success"):
            return chat_pb2.SendMessageResponse(success=False, error=err or res.get("error", "Consensus error"))
        return chat_pb2.SendMessageResponse(
            success=True,
            message=self._to_message_proto(res["message"]),
        )

    def GetGroupMessages(self, request, context):
        sess = self._require_auth(request.token, context)
        ok, msgs, err = self.chat.get_group_history(
            group_id=request.group_id,
            requesting_user_id=sess["user_id"],
            limit=request.limit or 100,
            before_timestamp=request.before_timestamp,
        )
        if not ok:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, err)
        return chat_pb2.GetMessagesResponse(
            messages=[self._to_message_proto(m) for m in msgs]
        )

    def MarkRead(self, request, context):
        sess = self._require_auth(request.token, context)
        self.chat.mark_as_read(sess["user_id"], request.chat_type, request.target_id)
        return chat_pb2.MarkReadResponse(success=True)

    # ── Server-streaming message RPC ──────────────────────────────────────────

    def StreamMessages(self, request, context):
        sess = self._require_auth(request.token, context, record_activity=False)
        user_id = sess["user_id"]
        stream_id = str(uuid.uuid4())
        self.presence.connect_stream(
            user_id, request.token, stream_id, sess["expires_at"]
        )
        q = queue.Queue(maxsize=128)
        self.chat.subscribe(user_id, q)
        logger.info("[STREAM] User %s connected to event stream", sess["username"])

        try:
            # READY lets the browser know its subscription is installed before
            # it loads snapshots; messages arriving during those reads are then
            # reconciled by message_id on the client.
            yield chat_pb2.MessageEvent(event_type="READY")
            last_auth_check = time.monotonic()
            last_heartbeat = last_auth_check
            auth_check_interval = STREAM_AUTH_CHECK_INTERVAL_SECONDS
            heartbeat_interval = STREAM_HEARTBEAT_INTERVAL_SECONDS

            while context.is_active():
                now = time.monotonic()
                if now - last_auth_check >= auth_check_interval:
                    last_auth_check = now
                    if not self.auth.token_is_valid(request.token):
                        self.presence.end_session(request.token)
                        yield chat_pb2.MessageEvent(event_type="AUTH_EXPIRED")
                        return

                if now - last_heartbeat >= heartbeat_interval:
                    last_heartbeat = now
                    yield chat_pb2.MessageEvent(event_type="HEARTBEAT")

                wait_for = min(
                    1.0,  # Bound cancellation latency without issuing browser HTTP polls.
                    auth_check_interval - (now - last_auth_check),
                    heartbeat_interval - (now - last_heartbeat),
                )
                try:
                    event = q.get(timeout=max(0.1, wait_for))
                except queue.Empty:
                    pass
                else:
                    # Validate a session before delivering any queued data so a
                    # logout revokes access promptly, even with a busy stream.
                    if not self.auth.token_is_valid(request.token):
                        self.presence.end_session(request.token)
                        yield chat_pb2.MessageEvent(event_type="AUTH_EXPIRED")
                        return

                    event_type = event.get("event_type", "NEW_MESSAGE")
                    message = event.get("message")
                    if event_type == "NEW_MESSAGE" and message:
                        if message.get("chat_type") == "GROUP" and not self.chat.is_group_member(
                            message.get("group_id", ""), user_id
                        ):
                            # Membership may have been revoked after the event
                            # was queued. Do not disclose group content.
                            continue
                        yield chat_pb2.MessageEvent(
                            event_type=event_type,
                            message=self._to_message_proto(message),
                        )
                    elif event_type == "GROUP_ACCESS_REVOKED" and message:
                        yield chat_pb2.MessageEvent(
                            event_type=event_type,
                            message=chat_pb2.Message(
                                chat_type="GROUP",
                                group_id=message.get("group_id", ""),
                            ),
                        )
                    elif event_type == "READ_STATE_CHANGED" and message:
                        target_field = "group_id" if message.get("chat_type") == "GROUP" else "recipient_id"
                        yield chat_pb2.MessageEvent(
                            event_type=event_type,
                            message=chat_pb2.Message(
                                chat_type=message.get("chat_type", ""),
                                **{target_field: message.get("target_id", "")},
                            ),
                        )
                    elif event_type == "DIRECTORY_CHANGED" and message:
                        yield chat_pb2.MessageEvent(
                            event_type=event_type,
                            message=chat_pb2.Message(
                                chat_type=message.get("chat_type", ""),
                                group_id=message.get("target_id", "") if message.get("chat_type") == "GROUP" else "",
                                recipient_id=message.get("target_id", "") if message.get("chat_type") != "GROUP" else "",
                                content="local_only" if message.get("local_only") else "",
                            ),
                        )
                    else:
                        yield chat_pb2.MessageEvent(event_type=event_type)
        except Exception as exc:
            logger.debug("[STREAM] Stream loop ended: %s", exc)
        finally:
            self.chat.unsubscribe(user_id, q)
            self.presence.disconnect_stream(stream_id)
            logger.info("[STREAM] User %s disconnected from event stream", sess["username"])

    # ── File sharing RPCs ─────────────────────────────────────────────────────

    def _file_access_error(
        self,
        user_id: str,
        chat_type: str,
        target_id: str,
        owner_id: str = "",
    ) -> str:
        """Return an access error unless the user belongs to the file's chat."""
        chat_type = (chat_type or "").upper()
        if chat_type == "DM":
            participants = {owner_id, target_id}
            if not target_id or "" in participants or user_id not in participants:
                return "You do not have access to this direct-message file"
            return ""

        if chat_type == "GROUP":
            group = self.db.fetchone("SELECT 1 FROM groups WHERE group_id = ?", (target_id,))
            if not group:
                return "Group not found"
            if not self.chat.is_group_member(target_id, user_id):
                return "You are not a member of this group"
            return ""

        return "Unsupported file chat type"

    def UploadFile(self, request, context):
        if self._is_follower():
            fwd = self._forward_to_leader("UploadFile", request, context)
            if fwd is not None:
                return fwd

        sess = self._require_auth(request.token, context)
        chat_type = (request.chat_type or "").upper()
        if chat_type == "DM":
            if not self.auth.get_user_by_id(request.target_id):
                return chat_pb2.UploadFileResponse(success=False, message="Direct-message user not found")
        access_error = self._file_access_error(
            sess["user_id"], chat_type, request.target_id, sess["user_id"]
        )
        if access_error:
            return chat_pb2.UploadFileResponse(success=False, message=access_error)

        ok, meta, err = self.files.upload_file(
            owner_id=sess["user_id"],
            chat_type=chat_type,
            target_id=request.target_id,
            filename=request.filename,
            data=request.data,
            content_type=request.content_type,
            request_id=request.request_id,
        )
        if not ok:
            return chat_pb2.UploadFileResponse(success=False, message=err)

        if self.raft:
            raft_meta = dict(meta)
            if request.data:
                raft_meta["data_b64"] = base64.b64encode(request.data).decode("ascii")
            self.raft.propose("UPLOAD_FILE_META", raft_meta, request_id=request.request_id or "")

        return chat_pb2.UploadFileResponse(
            success=True,
            file=self._to_file_proto(meta),
            message="",
        )

    def DownloadFile(self, request, context):
        sess = self._require_auth(request.token, context)
        row = self.db.fetchone(
            "SELECT owner_id, chat_type, target_id FROM files WHERE file_id = ?",
            (request.file_id,),
        )
        if not row:
            return chat_pb2.DownloadFileResponse(success=False, message="File not found")
        access_error = self._file_access_error(
            sess["user_id"], row["chat_type"], row["target_id"], row["owner_id"]
        )
        if access_error:
            return chat_pb2.DownloadFileResponse(success=False, message=access_error)

        ok, data, meta, err = self.files.download_file(request.file_id)
        if not ok:
            return chat_pb2.DownloadFileResponse(success=False, message=err)
        return chat_pb2.DownloadFileResponse(
            success=True,
            data=data,
            file=self._to_file_proto(meta),
            message="",
        )

    # ── LLM-backed RPCs ───────────────────────────────────────────────────────

    def GetSmartReplies(self, request, context):
        sess = self._require_auth(request.token, context)
        rid = request.request_id or str(uuid.uuid4())
        started = time.monotonic()
        logger.info("[LLM] smart-reply start request_id=%s user=%s", rid, sess["user_id"])

        if not self.llm:
            return chat_pb2.SmartReplyResponse(
                request_id=rid, success=False, error="LLM service unreachable"
            )
        try:
            resp = self.llm.GetSmartReplies(
                llm_pb2.SmartReplyRequest(
                    request_id=rid,
                    chat_history=list(request.chat_history),
                    current_message=request.current_message,
                    context_title=request.context_title or "Chat",
                ),
                metadata=(("x-chat-current-user", sess.get("username", "you")),),
                timeout=60,
            )
            logger.info("[LLM] smart-reply complete request_id=%s success=%s elapsed_ms=%.1f", rid, resp.success, (time.monotonic() - started) * 1000)
            return chat_pb2.SmartReplyResponse(
                request_id=resp.request_id,
                suggestions=list(resp.suggestions),
                success=resp.success,
                error=resp.error,
            )
        except Exception as exc:
            logger.error("[LLM] smart-reply failed request_id=%s elapsed_ms=%.1f error=%s", rid, (time.monotonic() - started) * 1000, exc)
            return chat_pb2.SmartReplyResponse(
                request_id=rid, success=False, error=str(exc)
            )

    def SummarizeChat(self, request, context):
        sess = self._require_auth(request.token, context)
        rid = request.request_id or str(uuid.uuid4())
        started = time.monotonic()
        logger.info("[LLM] summary start request_id=%s user=%s", rid, sess["user_id"])

        if not self.llm:
            return chat_pb2.SummarizeChatResponse(
                request_id=rid, success=False, error="LLM service unreachable"
            )
        try:
            history = list(request.chat_history)
            current_user = request.current_user or sess.get("username", "user")

            # Fallback to DB if chat_history was empty
            if not history and request.target_id:
                if request.chat_type == "GROUP":
                    ok, msgs, _ = self.chat.get_group_history(
                        group_id=request.target_id,
                        requesting_user_id=sess["user_id"],
                        limit=50,
                    )
                    if ok and msgs:
                        history = [
                            f"{m.get('sender_username', 'User')}: {m.get('content', '')}"
                            for m in msgs
                            if m.get("content")
                        ]
                elif request.chat_type == "DM":
                    msgs = self.chat.get_dm_history(
                        user_a=sess["user_id"],
                        user_b=request.target_id,
                        limit=50,
                    )
                    if msgs:
                        history = [
                            f"{m.get('sender_username', 'User')}: {m.get('content', '')}"
                            for m in msgs
                            if m.get("content")
                        ]

            resp = self.llm.SummarizeConversation(
                llm_pb2.SummarizeRequest(
                    request_id=rid,
                    chat_history=history,
                    context_title=request.context_title or "Chat",
                    current_user=current_user,
                ),
                timeout=60,
            )
            logger.info(
                "[LLM] summary complete request_id=%s success=%s elapsed_ms=%.1f",
                rid, resp.success, (time.monotonic() - started) * 1000,
            )
            return chat_pb2.SummarizeChatResponse(
                request_id=resp.request_id,
                summary=resp.summary,
                success=resp.success,
                error=resp.error,
            )
        except Exception as exc:
            logger.error("[LLM] summary failed request_id=%s elapsed_ms=%.1f error=%s", rid, (time.monotonic() - started) * 1000, exc)
            return chat_pb2.SummarizeChatResponse(
                request_id=rid, success=False, error=str(exc)
            )

    def GetContextSuggestion(self, request, context):
        sess = self._require_auth(request.token, context)
        rid = request.request_id or str(uuid.uuid4())
        started = time.monotonic()
        logger.info("[LLM] suggestion start request_id=%s user=%s", rid, sess["user_id"])

        if not self.llm:
            return chat_pb2.ContextSuggestionResponse(
                request_id=rid, success=False, error="LLM service unreachable"
            )
        try:
            resp = self.llm.GetContextSuggestion(
                llm_pb2.ContextSuggestionRequest(
                    request_id=rid,
                    chat_history=list(request.chat_history),
                    context_title=request.context_title or "Chat",
                    current_user=sess.get("username", "user"),
                ),
                timeout=60,
            )
            logger.info("[LLM] suggestion complete request_id=%s success=%s elapsed_ms=%.1f", rid, resp.success, (time.monotonic() - started) * 1000)
            return chat_pb2.ContextSuggestionResponse(
                request_id=resp.request_id,
                suggestion=resp.suggestion,
                success=resp.success,
                error=resp.error,
            )
        except Exception as exc:
            logger.error("[LLM] suggestion failed request_id=%s elapsed_ms=%.1f error=%s", rid, (time.monotonic() - started) * 1000, exc)
            return chat_pb2.ContextSuggestionResponse(
                request_id=rid, success=False, error=str(exc)
            )

    # ── Explicit service signatures ───────────────────────────────────────────

    def Post(self, request, context):
        """Generic post: e.g. type='signup', 'login', 'send_dm', 'send_group'."""
        try:
            payload = {}
            if request.data:
                try:
                    payload = json.loads(request.data)
                except Exception:
                    payload = {"raw": request.data}
            t = request.type.lower()
            if t == "signup":
                resp = self.Signup(
                    chat_pb2.SignupRequest(username=payload.get("username", ""), password=payload.get("password", "")),
                    context,
                )
                return chat_pb2.PostResponse(
                    status="OK" if resp.success else "ERROR",
                    data=json.dumps({"token": resp.token, "user_id": resp.user_id, "message": resp.message}),
                )
            elif t == "login":
                resp = self.Login(
                    chat_pb2.LoginRequest(username=payload.get("username", ""), password=payload.get("password", "")),
                    context,
                )
                return chat_pb2.PostResponse(
                    status="OK" if resp.success else "ERROR",
                    data=json.dumps({"token": resp.token, "user_id": resp.user_id, "message": resp.message}),
                )
            elif t == "logout":
                resp = self.Logout(chat_pb2.LogoutRequest(token=request.token), context)
                return chat_pb2.PostResponse(status="OK" if resp.success else "ERROR", data=resp.message)
            return chat_pb2.PostResponse(status="OK", data="Post processed")
        except Exception as exc:
            return chat_pb2.PostResponse(status="ERROR", data=str(exc))

    def Get(self, request, context):
        """Generic get: returns requested data items."""
        sess = self._require_auth(request.token, context)
        t = request.type.lower()
        items = []
        try:
            if t == "users":
                users = self.auth.list_users(exclude_user_id=sess["user_id"])
                for u in users:
                    items.append(chat_pb2.KeyValue(id=u["user_id"], data=json.dumps(u)))
            elif t == "groups":
                groups = self.chat.list_user_groups(sess["user_id"])
                for g in groups:
                    items.append(chat_pb2.KeyValue(id=g["group_id"], data=json.dumps(g)))
            return chat_pb2.GetResponse(status="OK", items=items)
        except Exception as exc:
            return chat_pb2.GetResponse(status="ERROR", items=[])

    def ProcessBusinessRequest(self, request, context):
        """Generic business request: handles domain-specific operations."""
        try:
            if self._is_follower():
                fwd = self._forward_to_leader("ProcessBusinessRequest", request, context)
                if fwd is not None:
                    return fwd

            rid = request.request_id or str(uuid.uuid4())
            payload = {
                "request_id": rid,
                "payload": request.payload,
                "context": request.context,
            }
            if self.raft:
                ok, err, _, res = self.raft.propose("BUSINESS_REQUEST", payload, request_id=rid)
                if not ok:
                    return chat_pb2.BusinessResponse(request_id=rid, status="ERROR", result=err)
                return chat_pb2.BusinessResponse(
                    request_id=rid,
                    status=res.get("status", "OK"),
                    result=res.get("result", ""),
                )

            return chat_pb2.BusinessResponse(
                request_id=rid,
                status="OK",
                result=json.dumps({"processed": True, "context": request.context}),
            )
        except Exception as exc:
            return chat_pb2.BusinessResponse(request_id=request.request_id, status="ERROR", result=str(exc))

    # ── Node status RPC ───────────────────────────────────────────────────────

    def GetNodeStatus(self, request, context):
        if self.raft:
            st = self.raft.status()
            return chat_pb2.GetNodeStatusResponse(
                node_id=st["node_id"],
                state=st["state"],
                term=st["term"],
                leader_id=st["leader_id"],
                commit_index=st["commit_index"],
                last_applied=st["last_applied"],
            )
        return chat_pb2.GetNodeStatusResponse(
            node_id=self.node_id,
            state="STANDALONE",
            term=0,
            leader_id=self.node_id,
            commit_index=0,
            last_applied=0,
        )

    # ── Message and file conversion helpers ───────────────────────────────────

    @staticmethod
    def _to_message_proto(m: dict) -> chat_pb2.Message:
        return chat_pb2.Message(
            message_id=m["message_id"],
            chat_type=m.get("chat_type", "DM"),
            sender_id=m["sender_id"],
            sender_username=m.get("sender_username", ""),
            recipient_id=m.get("recipient_id") or "",
            group_id=m.get("group_id") or "",
            content=m.get("content", ""),
            timestamp=m.get("timestamp", 0),
            client_request_id=m.get("client_request_id") or "",
            raft_log_index=m.get("raft_log_index", 0),
            file_id=m.get("file_id") or "",
            filename=m.get("filename") or "",
            file_type=m.get("file_type") or "",
            file_size=m.get("file_size") or 0,
        )

    @staticmethod
    def _to_file_proto(meta: dict) -> chat_pb2.FileMetadata:
        return chat_pb2.FileMetadata(
            file_id=meta["file_id"],
            filename=meta["filename"],
            file_type=meta.get("file_type", "other"),
            content_type=meta.get("content_type", "application/octet-stream"),
            size_bytes=meta.get("size_bytes", 0),
            owner_id=meta.get("owner_id", ""),
            chat_type=meta.get("chat_type", "DM"),
            target_id=meta.get("target_id", ""),
            uploaded_at=meta.get("uploaded_at", 0),
        )
