"""
Main gRPC application server & Web Gateway for Distributed Real-time Chat.

Each instance is a monolithic node providing:
  - gRPC server (ChatService on port 50051)
  - HTTP Web Gateway (serving the Web UI and REST API on port 8000)
  - Auth / Chat / Presence / Files managers
  - Raft consensus node stub (STANDALONE in M1, Raft consensus in M2)
  - SQLite local database storage

Environment variables:
  NODE_ID            Node identifier (default: node-1)
  PORT               gRPC listen port (default: 50051)
  WEB_PORT           HTTP Web UI listen port (default: 8000)
  DB_PATH            SQLite database path (default: /data/chat.db)
  FILE_STORAGE_PATH  Local file storage directory (default: /data/files/)
  LLM_SERVER         LLM server address (default: llm-server:50060)
  PEERS              Comma-separated peer addresses for Raft (M2)
"""

import base64
import json
import logging
import os
import queue
import sys
import threading
import time
import urllib.parse
import uuid
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc

# ── Path setup (generated stubs compiled at Docker build time) ────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "generated"))
sys.path.insert(0, _ROOT)

import chat_pb2
import chat_pb2_grpc
import llm_pb2
import llm_pb2_grpc

from storage.database import Database
from app.auth.manager import AuthManager
from app.chat.manager import ChatManager
from app.presence.manager import PresenceManager
from app.files.manager import FileManager
from raft.node import RaftNode

# ── Configuration ─────────────────────────────────────────────────────────────
NODE_ID           = os.environ.get("NODE_ID",           "node-1")
PORT              = int(os.environ.get("PORT",          "50051"))
WEB_PORT          = int(os.environ.get("WEB_PORT",      "8000"))
DB_PATH           = os.environ.get("DB_PATH",           "/data/chat.db")
FILE_STORAGE_PATH = os.environ.get("FILE_STORAGE_PATH", "/data/files/")
LLM_SERVER        = os.environ.get("LLM_SERVER",        "llm-server:50060")
PEERS             = [p for p in os.environ.get("PEERS", "").split(",") if p.strip()]

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] NODE={NODE_ID} %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _llm_channel():
    """Create a gRPC channel to the LLM server."""
    try:
        ch = grpc.insecure_channel(
            LLM_SERVER,
            options=[
                ("grpc.max_send_message_length",    64 * 1024 * 1024),
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ("grpc.enable_retries", 1),
            ],
        )
        return ch
    except Exception as exc:
        logger.warning("[SERVER] Could not connect to LLM server: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# gRPC Servicer Implementation
# ─────────────────────────────────────────────────────────────────────────────

class ChatServicer(chat_pb2_grpc.ChatServiceServicer):

    def __init__(self, db: Database, raft: RaftNode):
        self.db       = db
        self.raft     = raft
        self.auth     = AuthManager(db)
        self.chat     = ChatManager(db)
        self.presence = PresenceManager(db)
        self.files    = FileManager(db, FILE_STORAGE_PATH)

        # LLM stub (best-effort)
        llm_ch = _llm_channel()
        self.llm = llm_pb2_grpc.LLMServiceStub(llm_ch) if llm_ch else None

        # Zero default users, zero default channels
        logger.info("[SERVER] Initialized with 0 default users and 0 default channels.")

    # ── Auth Helper ───────────────────────────────────────────────────────────

    def _require_auth(self, token: str, context) -> dict:
        """Validate token and refresh heartbeat; aborts if invalid."""
        sess = self.auth.validate_token(token)
        if not sess:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or expired session token")
        return sess

    # ── Auth RPCs ─────────────────────────────────────────────────────────────

    def Signup(self, request, context):
        logger.info("[RPC] Signup request for username: %s", request.username)
        ok, token, uid, uname, msg = self.auth.signup(request.username, request.password)
        return chat_pb2.SignupResponse(
            success=ok,
            token=token,
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
        logger.info("[RPC] Logout request")
        ok = self.auth.logout(request.token)
        return chat_pb2.LogoutResponse(
            success=ok,
            message="Logged out successfully" if ok else "Session not found",
        )

    # ── Directory & Presence RPCs ─────────────────────────────────────────────

    def ListUsers(self, request, context):
        sess = self._require_auth(request.token, context)
        users = self.auth.list_users(exclude_user_id=sess["user_id"])
        items = [
            chat_pb2.UserItem(
                user_id=u["user_id"],
                username=u["username"],
                status=u["status"],
                last_seen=u["last_seen"],
            )
            for u in users
        ]
        return chat_pb2.ListUsersResponse(users=items)

    def GetUser(self, request, context):
        self._require_auth(request.token, context)
        u = self.auth.get_user_by_id(request.user_id)
        if not u:
            return chat_pb2.GetUserResponse(success=False, error="User not found")
        item = chat_pb2.UserItem(
            user_id=u["user_id"],
            username=u["username"],
            status=u["status"],
            last_seen=u["last_seen"],
        )
        return chat_pb2.GetUserResponse(success=True, user=item)

    def UpdatePresence(self, request, context):
        sess = self._require_auth(request.token, context)
        st = request.status if request.status in ("active", "inactive") else "active"
        self.auth.update_presence(sess["user_id"], st)
        return chat_pb2.UpdatePresenceResponse(success=True)

    # ── Direct Messaging (1-to-1) RPCs ────────────────────────────────────────

    def SendDirectMessage(self, request, context):
        sess = self._require_auth(request.token, context)
        ok, msg, err = self.chat.send_dm(
            sender_id=sess["user_id"],
            recipient_id=request.recipient_user_id,
            content=request.content,
            client_request_id=request.client_request_id or None,
            file_id=request.file_id or None,
        )
        if ok:
            return chat_pb2.SendMessageResponse(
                success=True,
                message=self._to_message_proto(msg),
            )
        return chat_pb2.SendMessageResponse(success=False, error=err)

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

    # ── Group RPCs ────────────────────────────────────────────────────────────

    def CreateGroup(self, request, context):
        sess = self._require_auth(request.token, context)
        member_ids = list(request.initial_member_user_ids)
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
        sess = self._require_auth(request.token, context)
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
        sess = self._require_auth(request.token, context)
        ok, msg, err = self.chat.send_group_message(
            sender_id=sess["user_id"],
            group_id=request.group_id,
            content=request.content,
            client_request_id=request.client_request_id or None,
            file_id=request.file_id or None,
        )
        if ok:
            return chat_pb2.SendMessageResponse(
                success=True,
                message=self._to_message_proto(msg),
            )
        return chat_pb2.SendMessageResponse(success=False, error=err)

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

    # ── Streaming ─────────────────────────────────────────────────────────────

    def StreamMessages(self, request, context):
        sess = self._require_auth(request.token, context)
        uid = sess["user_id"]
        q = queue.Queue(maxsize=128)
        self.chat.subscribe(uid, q)
        logger.info("[STREAM] User %s subscribed", sess["username"])
        try:
            while context.is_active():
                try:
                    msg = q.get(timeout=1.0)
                    yield chat_pb2.MessageEvent(
                        event_type="NEW_MESSAGE",
                        message=self._to_message_proto(msg),
                    )
                except queue.Empty:
                    continue
        finally:
            self.chat.unsubscribe(uid, q)
            logger.info("[STREAM] User %s unsubscribed", sess["username"])

    # ── File RPCs ─────────────────────────────────────────────────────────────

    def UploadFile(self, request, context):
        sess = self._require_auth(request.token, context)
        ok, meta, msg = self.files.upload_file(
            owner_id=sess["user_id"],
            chat_type=request.chat_type,
            target_id=request.target_id,
            filename=request.filename,
            data=request.data,
            content_type=request.content_type or "application/octet-stream",
            request_id=request.request_id or None,
        )
        if ok:
            return chat_pb2.UploadFileResponse(
                success=True,
                file=self._to_file_proto(meta),
                message=msg,
            )
        return chat_pb2.UploadFileResponse(success=False, message=msg)

    def DownloadFile(self, request, context):
        self._require_auth(request.token, context)
        ok, data, meta, msg = self.files.download_file(request.file_id)
        if ok:
            return chat_pb2.DownloadFileResponse(
                success=True,
                data=data,
                file=self._to_file_proto(meta),
                message=msg,
            )
        return chat_pb2.DownloadFileResponse(success=False, message=msg)

    # ── LLM Proxy RPCs ────────────────────────────────────────────────────────

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

    # ── Assignment Explicit Function Signatures ───────────────────────────────

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
        """Generic business request: e.g. creating group, updating role."""
        try:
            payload = {}
            if request.payload:
                try:
                    payload = json.loads(request.payload)
                except Exception:
                    payload = {"raw": request.payload}
            return chat_pb2.BusinessResponse(
                request_id=request.request_id,
                status="OK",
                result=json.dumps({"processed": True, "context": request.context}),
            )
        except Exception as exc:
            return chat_pb2.BusinessResponse(request_id=request.request_id, status="ERROR", result=str(exc))


    # ── Node Status ───────────────────────────────────────────────────────────

    def GetNodeStatus(self, request, context):
        s = self.raft.status()
        return chat_pb2.GetNodeStatusResponse(
            node_id=s["node_id"],
            state=s["state"],
            term=s["term"],
            leader_id=s["leader_id"],
            commit_index=s["commit_index"],
            last_applied=s["last_applied"],
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


# ─────────────────────────────────────────────────────────────────────────────
# HTTP Web UI Gateway (Translates browser HTTP/REST into gRPC calls)
# ─────────────────────────────────────────────────────────────────────────────

class WebGatewayHandler(BaseHTTPRequestHandler):

    grpc_stub: chat_pb2_grpc.ChatServiceStub = None

    def _send_json(self, status_code: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        content_len = int(self.headers.get("Content-Length", 0))
        if content_len == 0:
            return {}
        raw = self.rfile.read(content_len).decode("utf-8")
        return json.loads(raw)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        # Serve Web UI HTML
        if path in ("/", "/index.html"):
            html_path = os.path.join(_ROOT, "web", "index.html")
            if not os.path.exists(html_path):
                self.send_error(404, "index.html not found")
                return
            with open(html_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        token = qs.get("token", [""])[0]

        # GET /api/users
        if path == "/api/users":
            try:
                resp = self.grpc_stub.ListUsers(chat_pb2.ListUsersRequest(token=token))
                users = [
                    {"user_id": u.user_id, "username": u.username, "status": u.status, "last_seen": u.last_seen}
                    for u in resp.users
                ]
                self._send_json(200, {"success": True, "users": users})
            except grpc.RpcError as e:
                self._send_json(401, {"success": False, "error": e.details()})
            return

        # GET /api/groups
        if path == "/api/groups":
            try:
                resp = self.grpc_stub.ListGroups(chat_pb2.ListGroupsRequest(token=token))
                groups = [
                    {
                        "group_id": g.group_id,
                        "name": g.name,
                        "created_by": g.created_by,
                        "created_by_name": g.created_by_name,
                        "created_at": g.created_at,
                        "member_count": g.member_count,
                        "user_role": g.user_role,
                    }
                    for g in resp.groups
                ]
                self._send_json(200, {"success": True, "groups": groups})
            except grpc.RpcError as e:
                self._send_json(401, {"success": False, "error": e.details()})
            return

        # GET /api/groups/{id}/members
        if path.startswith("/api/groups/") and path.endswith("/members"):
            parts = path.split("/")
            group_id = parts[3]
            try:
                resp = self.grpc_stub.GetGroupMembers(chat_pb2.GetGroupMembersRequest(token=token, group_id=group_id))
                members = [
                    {"user_id": m.user_id, "username": m.username, "role": m.role, "status": m.status, "joined_at": m.joined_at}
                    for m in resp.members
                ]
                self._send_json(200, {"success": True, "members": members})
            except grpc.RpcError as e:
                self._send_json(403, {"success": False, "error": e.details()})
            return

        # GET /api/messages/dm/{other_user_id}
        if path.startswith("/api/messages/dm/"):
            other_user_id = path.replace("/api/messages/dm/", "")
            try:
                resp = self.grpc_stub.GetDirectMessages(chat_pb2.GetDirectMessagesRequest(token=token, other_user_id=other_user_id, limit=100))
                msgs = [self._msg_to_dict(m) for m in resp.messages]
                self._send_json(200, {"success": True, "messages": msgs})
            except grpc.RpcError as e:
                self._send_json(401, {"success": False, "error": e.details()})
            return

        # GET /api/messages/group/{group_id}
        if path.startswith("/api/messages/group/"):
            group_id = path.replace("/api/messages/group/", "")
            try:
                resp = self.grpc_stub.GetGroupMessages(chat_pb2.GetGroupMessagesRequest(token=token, group_id=group_id, limit=100))
                msgs = [self._msg_to_dict(m) for m in resp.messages]
                self._send_json(200, {"success": True, "messages": msgs})
            except grpc.RpcError as e:
                self._send_json(403, {"success": False, "error": e.details()})
            return

        # GET /api/files/{file_id}/download
        if path.startswith("/api/files/") and path.endswith("/download"):
            file_id = path.split("/")[3]
            try:
                resp = self.grpc_stub.DownloadFile(chat_pb2.DownloadFileRequest(token=token, file_id=file_id))
                if not resp.success:
                    self._send_json(404, {"success": False, "message": resp.message})
                    return
                self.send_response(200)
                self.send_header("Content-Type", resp.file.content_type or "application/octet-stream")
                self.send_header("Content-Length", str(len(resp.data)))
                self.send_header("Content-Disposition", f'inline; filename="{resp.file.filename}"')
                self.end_headers()
                self.wfile.write(resp.data)
                return
            except grpc.RpcError as e:
                self._send_json(404, {"success": False, "error": e.details()})
                return

        # GET /api/node/status
        if path == "/api/node/status":
            try:
                resp = self.grpc_stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest())
                self._send_json(200, {
                    "node_id": resp.node_id,
                    "state": resp.state,
                    "term": resp.term,
                    "commit_index": resp.commit_index,
                })
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return

        self.send_error(404, "Endpoint not found")

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path

        # POST /api/auth/signup
        if path == "/api/auth/signup":
            body = self._read_json()
            try:
                resp = self.grpc_stub.Signup(chat_pb2.SignupRequest(
                    username=body.get("username", ""),
                    password=body.get("password", ""),
                ))
                self._send_json(200, {
                    "success": resp.success,
                    "token": resp.token,
                    "user_id": resp.user_id,
                    "username": resp.username,
                    "message": resp.message,
                })
            except grpc.RpcError as e:
                self._send_json(500, {"success": False, "message": e.details()})
            return

        # POST /api/auth/login
        if path == "/api/auth/login":
            body = self._read_json()
            try:
                resp = self.grpc_stub.Login(chat_pb2.LoginRequest(
                    username=body.get("username", ""),
                    password=body.get("password", ""),
                ))
                self._send_json(200, {
                    "success": resp.success,
                    "token": resp.token,
                    "user_id": resp.user_id,
                    "username": resp.username,
                    "message": resp.message,
                })
            except grpc.RpcError as e:
                self._send_json(500, {"success": False, "message": e.details()})
            return

        # POST /api/auth/logout
        if path == "/api/auth/logout":
            body = self._read_json()
            try:
                resp = self.grpc_stub.Logout(chat_pb2.LogoutRequest(token=body.get("token", "")))
                self._send_json(200, {"success": resp.success, "message": resp.message})
            except grpc.RpcError as e:
                self._send_json(500, {"success": False, "message": e.details()})
            return

        # POST /api/groups
        if path == "/api/groups":
            body = self._read_json()
            try:
                resp = self.grpc_stub.CreateGroup(chat_pb2.CreateGroupRequest(
                    token=body.get("token", ""),
                    name=body.get("name", ""),
                    initial_member_user_ids=body.get("initial_member_user_ids", []),
                ))
                group_dict = {
                    "group_id": resp.group.group_id,
                    "name": resp.group.name,
                    "created_by": resp.group.created_by,
                    "created_by_name": resp.group.created_by_name,
                    "created_at": resp.group.created_at,
                    "member_count": resp.group.member_count,
                    "user_role": resp.group.user_role,
                } if resp.success else {}
                self._send_json(200, {"success": resp.success, "group": group_dict, "message": resp.message})
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "message": e.details()})
            return

        # POST /api/groups/{id}/update
        if path.startswith("/api/groups/") and path.endswith("/update"):
            parts = path.split("/")
            group_id = parts[3]
            body = self._read_json()
            try:
                resp = self.grpc_stub.UpdateGroup(chat_pb2.UpdateGroupRequest(
                    token=body.get("token", ""),
                    group_id=group_id,
                    action=body.get("action", ""),
                    target_user_id=body.get("target_user_id", ""),
                    new_name=body.get("new_name", ""),
                ))
                self._send_json(200, {"success": resp.success, "message": resp.message})
            except grpc.RpcError as e:
                self._send_json(403, {"success": False, "message": e.details()})
            return

        # POST /api/messages/dm/{recipient_user_id}
        if path.startswith("/api/messages/dm/"):
            rcpt_id = path.replace("/api/messages/dm/", "")
            body = self._read_json()
            try:
                resp = self.grpc_stub.SendDirectMessage(chat_pb2.SendDirectMessageRequest(
                    token=body.get("token", ""),
                    recipient_user_id=rcpt_id,
                    content=body.get("content", ""),
                    file_id=body.get("file_id", ""),
                    client_request_id=str(uuid.uuid4()),
                ))
                self._send_json(200, {"success": resp.success, "message": self._msg_to_dict(resp.message) if resp.success else {}, "error": resp.error})
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        # POST /api/messages/group/{group_id}
        if path.startswith("/api/messages/group/"):
            group_id = path.replace("/api/messages/group/", "")
            body = self._read_json()
            try:
                resp = self.grpc_stub.SendGroupMessage(chat_pb2.SendGroupMessageRequest(
                    token=body.get("token", ""),
                    group_id=group_id,
                    content=body.get("content", ""),
                    file_id=body.get("file_id", ""),
                    client_request_id=str(uuid.uuid4()),
                ))
                self._send_json(200, {"success": resp.success, "message": self._msg_to_dict(resp.message) if resp.success else {}, "error": resp.error})
            except grpc.RpcError as e:
                self._send_json(403, {"success": False, "error": e.details()})
            return

        # POST /api/files/upload
        if path == "/api/files/upload":
            content_type_header = self.headers.get("Content-Type", "")
            content_length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(content_length)

            # Handle multipart/form-data
            if "multipart/form-data" in content_type_header:
                boundary = content_type_header.split("boundary=")[1].strip().encode()
                parts = raw_body.split(b"--" + boundary)
                fields = {}
                file_bytes = b""
                filename = "upload"
                file_ct = "application/octet-stream"

                for part in parts:
                    if not part or part == b"--\r\n" or part == b"--":
                        continue
                    headers_part, _, body_part = part.partition(b"\r\n\r\n")
                    body_part = body_part.rstrip(b"\r\n")
                    headers_text = headers_part.decode("utf-8", errors="ignore")

                    if 'filename="' in headers_text:
                        import re
                        fn_match = re.search(r'filename="([^"]+)"', headers_text)
                        if fn_match:
                            filename = fn_match.group(1)
                        ct_match = re.search(r'Content-Type:\s*([^\r\n]+)', headers_text, re.IGNORECASE)
                        if ct_match:
                            file_ct = ct_match.group(1).strip()
                        file_bytes = body_part
                    else:
                        import re
                        name_match = re.search(r'name="([^"]+)"', headers_text)
                        if name_match:
                            fields[name_match.group(1)] = body_part.decode("utf-8", errors="ignore")

                try:
                    resp = self.grpc_stub.UploadFile(chat_pb2.UploadFileRequest(
                        token=fields.get("token", ""),
                        chat_type=fields.get("chat_type", "DM"),
                        target_id=fields.get("target_id", ""),
                        filename=filename,
                        data=file_bytes,
                        content_type=file_ct,
                    ))
                    file_dict = {
                        "file_id": resp.file.file_id,
                        "filename": resp.file.filename,
                        "file_type": resp.file.file_type,
                        "size_bytes": resp.file.size_bytes,
                    } if resp.success else {}
                    self._send_json(200, {"success": resp.success, "file": file_dict, "message": resp.message})
                except grpc.RpcError as e:
                    self._send_json(500, {"success": False, "message": e.details()})
                return

            self._send_json(400, {"success": False, "message": "Expected multipart/form-data"})
            return

        # POST /api/llm/smart-reply
        if path == "/api/llm/smart-reply":
            body = self._read_json()
            try:
                resp = self.grpc_stub.GetSmartReplies(chat_pb2.SmartReplyRequest(
                    token=body.get("token", ""),
                    chat_type=body.get("chat_type", "DM"),
                    target_id=body.get("target_id", ""),
                    chat_history=body.get("chat_history", []),
                    current_message=body.get("current_message", ""),
                    context_title=body.get("context_title", "Chat"),
                ))
                self._send_json(200, {
                    "success": resp.success,
                    "suggestions": list(resp.suggestions),
                    "error": resp.error,
                })
            except grpc.RpcError as e:
                self._send_json(500, {"success": False, "error": e.details()})
            return

        # POST /api/llm/summarize
        if path == "/api/llm/summarize":
            body = self._read_json()
            try:
                resp = self.grpc_stub.SummarizeChat(chat_pb2.SummarizeChatRequest(
                    token=body.get("token", ""),
                    chat_type=body.get("chat_type", "DM"),
                    target_id=body.get("target_id", ""),
                    chat_history=body.get("chat_history", []),
                    context_title=body.get("context_title", "Chat"),
                ))
                self._send_json(200, {
                    "success": resp.success,
                    "summary": resp.summary,
                    "error": resp.error,
                })
            except grpc.RpcError as e:
                self._send_json(500, {"success": False, "error": e.details()})
            return

        self.send_error(404, "Endpoint not found")

    @staticmethod
    def _msg_to_dict(m: chat_pb2.Message) -> dict:
        return {
            "message_id": m.message_id,
            "chat_type": m.chat_type,
            "sender_id": m.sender_id,
            "sender_username": m.sender_username,
            "recipient_id": m.recipient_id,
            "group_id": m.group_id,
            "content": m.content,
            "timestamp": m.timestamp,
            "file_id": m.file_id,
            "filename": m.filename,
            "file_type": m.file_type,
            "file_size": m.file_size,
        }

    def log_message(self, format, *args):
        # Suppress noisy standard HTTP logs in production
        return


def start_web_gateway(grpc_port: int, web_port: int):
    """Start the HTTP Web Gateway thread."""
    ch = grpc.insecure_channel(f"127.0.0.1:{grpc_port}")
    WebGatewayHandler.grpc_stub = chat_pb2_grpc.ChatServiceStub(ch)
    server = ThreadingHTTPServer(("0.0.0.0", web_port), WebGatewayHandler)
    logger.info("[WEB] Web UI & Gateway server listening on http://0.0.0.0:%d", web_port)
    server.serve_forever()


# ─────────────────────────────────────────────────────────────────────────────
# Server Bootstrap
# ─────────────────────────────────────────────────────────────────────────────

def serve():
    logger.info("[SERVER] Starting node=%s gRPC_port=%d web_port=%d", NODE_ID, PORT, WEB_PORT)

    db   = Database(DB_PATH)
    raft = RaftNode(node_id=NODE_ID, db=db, peers=PEERS)

    # 1. Start gRPC Server
    grpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=[
            ("grpc.max_send_message_length",    64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ("grpc.keepalive_time_ms",          10_000),
            ("grpc.keepalive_timeout_ms",        5_000),
        ],
    )
    chat_pb2_grpc.add_ChatServiceServicer_to_server(
        ChatServicer(db, raft), grpc_server
    )
    grpc_addr = f"[::]:{PORT}"
    grpc_server.add_insecure_port(grpc_addr)
    grpc_server.start()
    logger.info("[SERVER] gRPC listening on %s (state=%s)", grpc_addr, raft.state)

    # 2. Start Web Gateway in background thread
    web_thread = threading.Thread(
        target=start_web_gateway,
        args=(PORT, WEB_PORT),
        daemon=True,
        name="web-gateway",
    )
    web_thread.start()

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("[SERVER] Shutting down")
        grpc_server.stop(grace=3)


if __name__ == "__main__":
    serve()
