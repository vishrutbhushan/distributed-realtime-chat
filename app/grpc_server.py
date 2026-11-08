"""
gRPC ChatService Servicer implementation.

Handles:
- User signup, login, logout, directory & presence tracking
- 1-on-1 Direct Messaging (DMs) with atomic idempotency
- Group chat creation with member selection checklist and Admin controls
- File sharing (images and PDFs) with byte verification
- LLM smart replies & summarization over full conversation history
- Raft Consensus replication for strong consistency and fault tolerance
- Assignment explicit RPC signatures: Post, Get, ProcessBusinessRequest
"""

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
        self.auth = AuthManager(db)
        self.chat = ChatManager(db)
        self.presence = PresenceManager(db)
        self.files = FileManager(db, file_storage_path)
        self.llm = create_llm_stub(llm_server_addr)

        self.raft = raft_node
        self.app_peers = dict(app_peers or {})
        self._app_stubs: Dict[str, chat_pb2_grpc.ChatServiceStub] = {}
        self._stub_lock = threading.Lock()

        logger.info(
            "[GRPC] ChatServicer initialized (node_id=%s, raft=%s, peers=%s)",
            self.node_id,
            bool(self.raft),
            list(self.app_peers.keys()),
        )

    # ── Auth Helper ───────────────────────────────────────────────────────────

    def _require_auth(self, token: str, context) -> dict:
        if not token:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing session token")
        sess = self.auth.validate_token(token)
        if not sess:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or expired session token")
        return sess

    # ── Leader Forwarding ─────────────────────────────────────────────────────

    def _get_app_stub(self, peer_id: str) -> Optional[chat_pb2_grpc.ChatServiceStub]:
        if not self.app_peers or peer_id not in self.app_peers:
            return None
        with self._stub_lock:
            if peer_id not in self._app_stubs:
                addr = self.app_peers[peer_id]
                ch = grpc.insecure_channel(
                    addr,
                    options=[
                        ("grpc.max_send_message_length", 64 * 1024 * 1024),
                        ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                    ],
                )
                self._app_stubs[peer_id] = chat_pb2_grpc.ChatServiceStub(ch)
            return self._app_stubs[peer_id]

    def _forward_to_leader(self, method_name: str, request, context):
        """Transparently forward a write RPC to current Raft leader."""
        if not self.raft:
            return None
        leader_id = self.raft.leader_id
        if not leader_id:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Raft leader election in progress; please retry shortly")
        if leader_id == self.node_id:
            return None  # We are leader

        stub = self._get_app_stub(leader_id)
        if not stub:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"Cannot reach Raft leader node {leader_id}")

        logger.info("[FORWARD] Forwarding %s from follower %s to leader %s", method_name, self.node_id, leader_id)
        try:
            fn = getattr(stub, method_name)
            return fn(request)
        except grpc.RpcError as exc:
            context.abort(exc.code(), exc.details())

    # ── Authentication RPCs ───────────────────────────────────────────────────

    def Signup(self, request, context):
        logger.info("[RPC] Signup request for username: %s", request.username)

        # If clustered and we are follower, forward to leader
        if self.raft and len(self.raft.peers) > 1 and self.raft.state != NodeState.LEADER:
            return self._forward_to_leader("Signup", request, context)

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

        # Duplicate check
        row = self.db.fetchone("SELECT user_id FROM users WHERE username = ?", (username,))
        if row:
            return chat_pb2.SignupResponse(success=False, message="Username already exists. Please choose another.")

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

        if self.raft:
            ok, err, _, res = self.raft.propose("SIGNUP", payload)
            if not ok:
                return chat_pb2.SignupResponse(success=False, message=f"Consensus error: {err}")
            return chat_pb2.SignupResponse(
                success=True,
                token=token,
                user_id=user_id,
                username=username,
                message="Account created successfully",
            )
        else:
            ok, tok, uid, uname, msg = self.auth.signup(request.username, request.password)
            return chat_pb2.SignupResponse(
                success=ok,
                token=tok,
                user_id=uid,
                username=uname,
                message=msg,
            )

    def Login(self, request, context):
        logger.info("[RPC] Login request for username: %s", request.username)
        ok, token, uid, uname, msg = self.auth.login(request.username, request.password)
        return chat_pb2.LoginResponse(
            success=ok,
            token=token,
            user_id=uid,
            username=uname,
            message=msg,
        )

    def Logout(self, request, context):
        ok = self.auth.logout(request.token)
        return chat_pb2.LogoutResponse(
            success=ok,
            message="Logged out successfully" if ok else "Invalid token or already logged out",
        )

    # ── Directory & Presence RPCs ─────────────────────────────────────────────

    def ListUsers(self, request, context):
        sess = self._require_auth(request.token, context)
        users = self.auth.list_users(exclude_user_id=sess["user_id"])
        return chat_pb2.ListUsersResponse(
            users=[
                chat_pb2.UserItem(
                    user_id=u["user_id"],
                    username=u["username"],
                    status=u["status"],
                    last_seen=u["last_seen"],
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
        self.auth.update_status(sess["user_id"], request.status)
        return chat_pb2.UpdatePresenceResponse(success=True)

    # ── 1-on-1 Direct Messaging RPCs ──────────────────────────────────────────

    def SendDirectMessage(self, request, context):
        if self.raft and len(self.raft.peers) > 1 and self.raft.state != NodeState.LEADER:
            return self._forward_to_leader("SendDirectMessage", request, context)

        sess = self._require_auth(request.token, context)
        rcpt = self.db.fetchone("SELECT user_id, username FROM users WHERE user_id = ?", (request.recipient_user_id,))
        if not rcpt:
            return chat_pb2.SendMessageResponse(success=False, error="Recipient user not found")

        c_req_id = request.client_request_id or None
        if c_req_id:
            existing = self.db.fetchone(
                """
                SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                LEFT JOIN files f ON m.file_id = f.file_id
                WHERE m.client_request_id = ?
                """,
                (c_req_id,),
            )
            if existing:
                return chat_pb2.SendMessageResponse(success=True, message=self._to_message_proto(dict(existing)))

        msg_id = str(uuid.uuid4())
        ts = int(time.time() * 1000)
        payload = {
            "message_id": msg_id,
            "sender_id": sess["user_id"],
            "recipient_id": request.recipient_user_id,
            "content": request.content,
            "file_id": request.file_id or None,
            "timestamp": ts,
            "client_request_id": c_req_id,
        }

        if self.raft:
            ok, err, _, res = self.raft.propose("SEND_DM", payload, request_id=c_req_id or "")
            if not ok:
                return chat_pb2.SendMessageResponse(success=False, error=f"Consensus error: {err}")
            msg_data = res.get("message", payload)
            msg_data["sender_username"] = sess["username"]
            return chat_pb2.SendMessageResponse(success=True, message=self._to_message_proto(msg_data))
        else:
            ok, msg, err = self.chat.send_dm(
                sender_id=sess["user_id"],
                recipient_id=request.recipient_user_id,
                content=request.content,
                client_request_id=c_req_id,
                file_id=request.file_id or None,
            )
            return chat_pb2.SendMessageResponse(
                success=ok,
                message=self._to_message_proto(msg) if ok else None,
                error=err,
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

    # ── Group Management & Messaging RPCs ─────────────────────────────────────

    def CreateGroup(self, request, context):
        if self.raft and len(self.raft.peers) > 1 and self.raft.state != NodeState.LEADER:
            return self._forward_to_leader("CreateGroup", request, context)

        sess = self._require_auth(request.token, context)
        name = (request.name or "").strip()
        if not name:
            return chat_pb2.CreateGroupResponse(success=False, message="Group name cannot be empty")

        group_id = str(uuid.uuid4())
        now = int(time.time())
        member_ids = list(request.initial_member_user_ids)

        payload = {
            "group_id": group_id,
            "name": name,
            "creator_id": sess["user_id"],
            "initial_member_ids": member_ids,
            "created_at": now,
        }

        if self.raft:
            ok, err, _, res = self.raft.propose("CREATE_GROUP", payload)
            if not ok:
                return chat_pb2.CreateGroupResponse(success=False, message=f"Consensus error: {err}")
            grp = res.get("group", payload)
            return chat_pb2.CreateGroupResponse(
                success=True,
                group=chat_pb2.Group(
                    group_id=grp["group_id"],
                    name=grp["name"],
                    created_by=grp["creator_id"],
                    created_by_name=sess["username"],
                    created_at=grp["created_at"],
                    member_count=grp.get("member_count", len(member_ids) + 1),
                    user_role="ADMIN",
                ),
                message="Group created successfully",
            )
        else:
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

    def UpdateGroup(self, request, context):
        if self.raft and len(self.raft.peers) > 1 and self.raft.state != NodeState.LEADER:
            return self._forward_to_leader("UpdateGroup", request, context)

        sess = self._require_auth(request.token, context)
        admin_check = self.db.fetchone(
            "SELECT role FROM group_members WHERE group_id = ? AND user_id = ?",
            (request.group_id, sess["user_id"]),
        )
        if not admin_check or admin_check["role"] != "ADMIN":
            return chat_pb2.UpdateGroupResponse(
                success=False,
                message="Permission denied: Only group admins can perform this action",
            )

        payload = {
            "group_id": request.group_id,
            "requesting_user_id": sess["user_id"],
            "action": request.action,
            "target_user_id": request.target_user_id or None,
            "new_name": request.new_name or None,
        }

        if self.raft:
            ok, err, _, res = self.raft.propose("UPDATE_GROUP", payload)
            if not ok:
                return chat_pb2.UpdateGroupResponse(success=False, message=f"Consensus error: {err}")
            g = res.get("group", {})
            return chat_pb2.UpdateGroupResponse(
                success=True,
                group=chat_pb2.Group(
                    group_id=g.get("group_id", request.group_id),
                    name=g.get("name", ""),
                    created_by=g.get("created_by", ""),
                    created_by_name=g.get("created_by_name", ""),
                    created_at=g.get("created_at", 0),
                    member_count=g.get("member_count", 1),
                    user_role="ADMIN",
                ),
                message=f"Group action '{request.action}' succeeded",
            )
        else:
            ok, g, msg = self.chat.update_group(
                group_id=request.group_id,
                requesting_user_id=sess["user_id"],
                action=request.action,
                target_user_id=request.target_user_id or None,
                new_name=request.new_name or None,
            )
            return chat_pb2.UpdateGroupResponse(
                success=ok,
                group=chat_pb2.Group(
                    group_id=g.get("group_id", ""),
                    name=g.get("name", ""),
                    created_by=g.get("created_by", ""),
                    created_by_name=g.get("created_by_name", ""),
                    created_at=g.get("created_at", 0),
                    member_count=g.get("member_count", 0),
                    user_role="ADMIN",
                ) if ok else None,
                message=msg,
            )

    def ListGroups(self, request, context):
        sess = self._require_auth(request.token, context)
        groups = self.chat.list_user_groups(sess["user_id"])
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
        if self.raft and len(self.raft.peers) > 1 and self.raft.state != NodeState.LEADER:
            return self._forward_to_leader("SendGroupMessage", request, context)

        sess = self._require_auth(request.token, context)
        member_check = self.db.fetchone(
            "SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?",
            (request.group_id, sess["user_id"]),
        )
        if not member_check:
            return chat_pb2.SendMessageResponse(success=False, error="Cannot send message: You are not a member of this group")

        c_req_id = request.client_request_id or None
        if c_req_id:
            existing = self.db.fetchone(
                """
                SELECT m.*, u.username AS sender_username, f.filename, f.file_type, f.size_bytes AS file_size
                FROM messages m
                JOIN users u ON m.sender_id = u.user_id
                LEFT JOIN files f ON m.file_id = f.file_id
                WHERE m.client_request_id = ?
                """,
                (c_req_id,),
            )
            if existing:
                return chat_pb2.SendMessageResponse(success=True, message=self._to_message_proto(dict(existing)))

        msg_id = str(uuid.uuid4())
        ts = int(time.time() * 1000)
        payload = {
            "message_id": msg_id,
            "sender_id": sess["user_id"],
            "group_id": request.group_id,
            "content": request.content,
            "file_id": request.file_id or None,
            "timestamp": ts,
            "client_request_id": c_req_id,
        }

        if self.raft:
            ok, err, _, res = self.raft.propose("SEND_GROUP_MSG", payload, request_id=c_req_id or "")
            if not ok:
                return chat_pb2.SendMessageResponse(success=False, error=f"Consensus error: {err}")
            msg_data = res.get("message", payload)
            msg_data["sender_username"] = sess["username"]
            return chat_pb2.SendMessageResponse(success=True, message=self._to_message_proto(msg_data))
        else:
            ok, msg, err = self.chat.send_group_message(
                sender_id=sess["user_id"],
                group_id=request.group_id,
                content=request.content,
                client_request_id=c_req_id,
                file_id=request.file_id or None,
            )
            return chat_pb2.SendMessageResponse(
                success=ok,
                message=self._to_message_proto(msg) if ok else None,
                error=err,
            )

    def GetGroupMessages(self, request, context):
        sess = self._require_auth(request.token, context)
        msgs = self.chat.get_group_history(
            group_id=request.group_id,
            user_id=sess["user_id"],
            limit=request.limit or 100,
            before_timestamp=request.before_timestamp,
        )
        return chat_pb2.GetMessagesResponse(
            messages=[self._to_message_proto(m) for m in msgs]
        )

    # ── Real-time Streaming RPC ───────────────────────────────────────────────

    def StreamMessages(self, request, context):
        sess = self._require_auth(request.token, context)
        user_id = sess["user_id"]
        q = queue.Queue(maxsize=100)
        self.chat.subscribe(user_id, q)
        logger.info("[STREAM] User %s connected to real-time event stream", sess["username"])

        try:
            while context.is_active():
                try:
                    event_data = q.get(timeout=1.0)
                    msg_proto = self._to_message_proto(event_data)
                    yield chat_pb2.MessageEvent(event_type="NEW_MESSAGE", message=msg_proto)
                except queue.Empty:
                    pass
        except Exception as exc:
            logger.debug("[STREAM] Stream loop ended: %s", exc)
        finally:
            self.chat.unsubscribe(user_id, q)
            logger.info("[STREAM] User %s disconnected from event stream", sess["username"])

    # ── File Sharing RPCs ─────────────────────────────────────────────────────

    def UploadFile(self, request, context):
        if self.raft and len(self.raft.peers) > 1 and self.raft.state != NodeState.LEADER:
            return self._forward_to_leader("UploadFile", request, context)

        sess = self._require_auth(request.token, context)
        ok, meta, err = self.files.upload_file(
            owner_id=sess["user_id"],
            chat_type=request.chat_type,
            target_id=request.target_id,
            filename=request.filename,
            data=request.data,
            content_type=request.content_type,
            request_id=request.request_id,
        )
        if not ok:
            return chat_pb2.UploadFileResponse(success=False, message=err)

        if self.raft:
            self.raft.propose("UPLOAD_FILE_META", meta, request_id=request.request_id or "")

        return chat_pb2.UploadFileResponse(
            success=True,
            file=self._to_file_proto(meta),
            message="File uploaded successfully",
        )

    def DownloadFile(self, request, context):
        self._require_auth(request.token, context)
        ok, data, meta, err = self.files.download_file(request.file_id)
        if not ok:
            return chat_pb2.DownloadFileResponse(success=False, message=err)
        return chat_pb2.DownloadFileResponse(
            success=True,
            data=data,
            file=self._to_file_proto(meta),
            message="File downloaded successfully",
        )

    # ── LLM Features (Passing Full Chat History) ──────────────────────────────

    def GetSmartReplies(self, request, context):
        self._require_auth(request.token, context)
        rid = request.request_id or str(uuid.uuid4())

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
                )
            )
            return chat_pb2.SmartReplyResponse(
                request_id=resp.request_id,
                suggestions=list(resp.suggestions),
                success=resp.success,
                error=resp.error,
            )
        except Exception as exc:
            logger.error("[LLM PROXY] GetSmartReplies error: %s", exc)
            return chat_pb2.SmartReplyResponse(
                request_id=rid, success=False, error=str(exc)
            )

    def SummarizeChat(self, request, context):
        self._require_auth(request.token, context)
        rid = request.request_id or str(uuid.uuid4())

        if not self.llm:
            return chat_pb2.SummarizeChatResponse(
                request_id=rid, success=False, error="LLM service unreachable"
            )
        try:
            resp = self.llm.SummarizeConversation(
                llm_pb2.SummarizeRequest(
                    request_id=rid,
                    chat_history=list(request.chat_history),
                    context_title=request.context_title or "Chat",
                )
            )
            return chat_pb2.SummarizeChatResponse(
                request_id=resp.request_id,
                summary=resp.summary,
                success=resp.success,
                error=resp.error,
            )
        except Exception as exc:
            logger.error("[LLM PROXY] SummarizeChat error: %s", exc)
            return chat_pb2.SummarizeChatResponse(
                request_id=rid, success=False, error=str(exc)
            )

    # ── Assignment Explicit Function Signatures (Pages 6-7) ───────────────────

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
                ok, token, uid, uname, msg = self.auth.signup(payload.get("username", ""), payload.get("password", ""))
                return chat_pb2.PostResponse(status="OK" if ok else "ERROR", data=json.dumps({"token": token, "user_id": uid, "message": msg}))
            elif t == "login":
                ok, token, uid, uname, msg = self.auth.login(payload.get("username", ""), payload.get("password", ""))
                return chat_pb2.PostResponse(status="OK" if ok else "ERROR", data=json.dumps({"token": token, "user_id": uid, "message": msg}))
            elif t == "logout":
                ok = self.auth.logout(request.token)
                return chat_pb2.PostResponse(status="OK" if ok else "ERROR", data="Logged out" if ok else "Not found")
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
        """Handles domain-specific operations via Raft consistency (Page 7)."""
        rid = request.request_id or str(uuid.uuid4())
        try:
            if self.raft and len(self.raft.peers) > 1 and self.raft.state != NodeState.LEADER:
                return self._forward_to_leader("ProcessBusinessRequest", request, context)

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
                    status=res.get("status", "COMPLETED"),
                    result=res.get("result", "OK"),
                )
            else:
                return chat_pb2.BusinessResponse(
                    request_id=rid,
                    status="OK",
                    result=json.dumps({"processed": True, "context": request.context}),
                )
        except Exception as exc:
            return chat_pb2.BusinessResponse(request_id=rid, status="ERROR", result=str(exc))

    # ── Node Status ───────────────────────────────────────────────────────────

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

    # ── Helpers ───────────────────────────────────────────────────────────────

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
            file_size=m.get("file_size", 0),
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
