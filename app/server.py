"""
Main gRPC application server.

Each instance is a monolithic node:
  - gRPC server (ChatService)
  - Auth / Chat / Presence / Files managers
  - Raft node stub (STANDALONE in M1, full Raft in M2)
  - SQLite local storage

Environment variables:
  NODE_ID            Node identifier (default: node-1)
  PORT               gRPC listen port (default: 50051)
  DB_PATH            SQLite database path (default: /data/chat.db)
  FILE_STORAGE_PATH  Local file storage directory (default: /data/files/)
  LLM_SERVER         LLM server address (default: llm-server:50060)
  PEERS              Comma-separated peer addresses for Raft (M2)
"""

import logging
import os
import queue
import sys
import time
import uuid
from concurrent import futures

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
    """Create a gRPC channel to the LLM server (best-effort)."""
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


# ── gRPC Servicer ─────────────────────────────────────────────────────────────

class ChatServicer(chat_pb2_grpc.ChatServiceServicer):

    def __init__(self, db: Database, raft: RaftNode):
        self.db       = db
        self.raft     = raft
        self.auth     = AuthManager(db)
        self.chat     = ChatManager(db)
        self.presence = PresenceManager(db)
        self.files    = FileManager(db, FILE_STORAGE_PATH)

        # LLM stub (best-effort; None if server unreachable)
        llm_ch = _llm_channel()
        self.llm = llm_pb2_grpc.LLMServiceStub(llm_ch) if llm_ch else None

        self._seed_defaults()

    # ── Seeding ───────────────────────────────────────────────────────────────

    def _seed_defaults(self):
        """Create default admin/users/channels on first start (idempotent)."""
        # Look up before inserting: failed UNIQUE inserts leave a transaction open
        # on some SQLite configurations and made restarts lock the database.
        for username, password, role in (
            ("admin", "admin123", "ADMIN"),
            ("alice", "alice123", "USER"),
            ("bob", "bob123", "USER"),
        ):
            row = self.db.fetchone(
                "SELECT user_id FROM users WHERE username = ?", (username,)
            )
            if not row:
                ok, _, message = self.auth.create_user(username, password, role)
                if not ok:
                    raise RuntimeError(f"Could not seed {username}: {message}")
                logger.info("[SEED] %s created", username)

        # Default channels (created by admin)
        row = self.db.fetchone("SELECT user_id FROM users WHERE username = 'admin'")
        if row:
            aid = row["user_id"]
            for name in ("general", "aos-project", "testing"):
                if not self.chat.get_channel_by_name(name):
                    ok, _, message = self.chat.create_channel(name, aid)
                    if not ok:
                        raise RuntimeError(f"Could not seed channel #{name}: {message}")

    # ── Auth helpers ──────────────────────────────────────────────────────────

    def _session(self, token: str):
        """Validate token → {user_id, username, role} or None."""
        sess = self.auth.validate_token(token)
        if sess:
            # Implicit presence heartbeat on every authenticated RPC
            self.presence.update_presence(sess["user_id"])
        return sess

    def _require_auth(self, token: str, context):
        """Return session or abort RPC with UNAUTHENTICATED."""
        sess = self._session(token)
        if not sess:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or expired token")
        return sess

    def _require_admin(self, token: str, context):
        sess = self._require_auth(token, context)
        if sess and sess["role"] != "ADMIN":
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "Admin role required")
        return sess

    def _require_channel_member(self, token: str, channel_id: str, context):
        sess = self._require_auth(token, context)
        if not self.chat.get_channel(channel_id):
            context.abort(grpc.StatusCode.NOT_FOUND, "Channel not found")
        if not self.chat.is_member(channel_id, sess["user_id"]):
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "Join the channel first")
        return sess

    def _require_file_member(self, token: str, file_id: str, context):
        sess = self._require_auth(token, context)
        meta = self.files.get_file(file_id)
        if not meta:
            context.abort(grpc.StatusCode.NOT_FOUND, "File not found")
        if not self.chat.get_channel(meta["channel_id"]):
            context.abort(grpc.StatusCode.NOT_FOUND, "File channel no longer exists")
        if not self.chat.is_member(meta["channel_id"], sess["user_id"]):
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "Join the channel first")
        return sess, meta

    # ─────────────────────────────────────────────────────────────────────────
    # Auth RPCs
    # ─────────────────────────────────────────────────────────────────────────

    def Login(self, request, context):
        logger.info("[RPC] Login user=%s", request.username)
        ok, token, uid, role = self.auth.login(request.username, request.password)
        if ok:
            return chat_pb2.LoginResponse(
                success=True, token=token, user_id=uid, role=role,
                message="Login successful",
            )
        return chat_pb2.LoginResponse(success=False, message="Invalid credentials")

    def Logout(self, request, context):
        ok = self.auth.logout(request.token)
        return chat_pb2.LogoutResponse(
            success=ok, message="Logged out" if ok else "Token not found"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Channel RPCs
    # ─────────────────────────────────────────────────────────────────────────

    def CreateChannel(self, request, context):
        sess = self._require_admin(request.token, context)
        if not request.channel_name.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Channel name is required")
        ok, ch, msg = self.chat.create_channel(request.channel_name, sess["user_id"])
        if ok:
            return chat_pb2.CreateChannelResponse(
                success=True,
                channel=chat_pb2.Channel(
                    channel_id=ch["channel_id"],
                    name=ch["name"],
                    created_by=ch["created_by"],
                    created_at=ch["created_at"],
                    member_count=ch.get("member_count", 1),
                ),
                message=msg,
            )
        if msg == "Channel name already exists":
            context.abort(grpc.StatusCode.ALREADY_EXISTS, msg)
        return chat_pb2.CreateChannelResponse(success=False, message=msg)

    def DeleteChannel(self, request, context):
        sess = self._require_admin(request.token, context)
        if not sess:
            return chat_pb2.DeleteChannelResponse(success=False)
        ok, msg = self.chat.delete_channel(request.channel_id)
        if not ok and msg == "Channel not found":
            context.abort(grpc.StatusCode.NOT_FOUND, msg)
        return chat_pb2.DeleteChannelResponse(success=ok, message=msg)

    def JoinChannel(self, request, context):
        sess = self._require_auth(request.token, context)
        if not self.chat.get_channel(request.channel_id):
            context.abort(grpc.StatusCode.NOT_FOUND, "Channel not found")
        ok, msg = self.chat.join_channel(request.channel_id, sess["user_id"])
        return chat_pb2.JoinChannelResponse(success=ok, message=msg)

    def LeaveChannel(self, request, context):
        sess = self._require_auth(request.token, context)
        if not self.chat.get_channel(request.channel_id):
            context.abort(grpc.StatusCode.NOT_FOUND, "Channel not found")
        ok, msg = self.chat.leave_channel(request.channel_id, sess["user_id"])
        if not ok:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)
        return chat_pb2.LeaveChannelResponse(success=ok, message=msg)

    def ListChannels(self, request, context):
        self._require_auth(request.token, context)
        chs = self.chat.list_channels()
        return chat_pb2.ListChannelsResponse(
            channels=[
                chat_pb2.Channel(
                    channel_id=c["channel_id"],
                    name=c["name"],
                    created_by=c["created_by"],
                    created_at=c["created_at"],
                    member_count=c.get("member_count", 0),
                )
                for c in chs
            ]
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Message RPCs
    # ─────────────────────────────────────────────────────────────────────────

    def SendMessage(self, request, context):
        sess = self._require_channel_member(request.token, request.channel_id, context)

        # TODO M2: route through Raft log instead of direct insert
        ok, msg, err = self.chat.send_message(
            channel_id=request.channel_id,
            sender_id=sess["user_id"],
            content=request.content,
            client_request_id=request.client_request_id or None,
            file_id=request.file_id or None,
        )
        if ok:
            return chat_pb2.SendMessageResponse(
                success=True,
                message=chat_pb2.Message(
                    message_id=msg["message_id"],
                    channel_id=msg["channel_id"],
                    sender_id=msg["sender_id"],
                    sender_username=msg.get("sender_username", ""),
                    content=msg["content"],
                    timestamp=msg["timestamp"],
                    client_request_id=msg.get("client_request_id") or "",
                    raft_log_index=msg.get("raft_log_index", 0),
                    file_id=msg.get("file_id") or "",
                ),
            )
        if err == "Channel not found":
            context.abort(grpc.StatusCode.NOT_FOUND, err)
        if "Join the channel" in err:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, err)
        if "File not found" in err:
            context.abort(grpc.StatusCode.NOT_FOUND, err)
        if "required" in err.lower():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, err)
        if "Request ID was already used" in err:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, err)
        if "inactive" in err.lower():
            context.abort(grpc.StatusCode.UNAUTHENTICATED, err)
        return chat_pb2.SendMessageResponse(success=False, error=err)

    def GetMessages(self, request, context):
        self._require_channel_member(request.token, request.channel_id, context)
        msgs = self.chat.get_messages(
            channel_id=request.channel_id,
            limit=request.limit or 50,
            before_timestamp=request.before_timestamp,
        )
        return chat_pb2.GetMessagesResponse(
            messages=[
                chat_pb2.Message(
                    message_id=m["message_id"],
                    channel_id=m["channel_id"],
                    sender_id=m["sender_id"],
                    sender_username=m.get("sender_username", ""),
                    content=m["content"],
                    timestamp=m["timestamp"],
                    client_request_id=m.get("client_request_id") or "",
                    raft_log_index=m.get("raft_log_index", 0),
                    file_id=m.get("file_id") or "",
                )
                for m in msgs
            ]
        )

    def StreamMessages(self, request, context):
        """Server-side streaming: push new messages to subscribed clients."""
        sess = self._require_channel_member(request.token, request.channel_id, context)

        q = queue.Queue(maxsize=256)
        self.chat.subscribe(request.channel_id, q)
        logger.info(
            "[STREAM] %s subscribed to channel %s",
            sess["username"], request.channel_id,
        )
        last_heartbeat = time.monotonic()
        try:
            # A small acknowledgement lets CLI clients synchronize publishers
            # with the point at which the server has installed the subscription.
            yield chat_pb2.MessageEvent(event_type="READY")
            while context.is_active():
                current_session = self.auth.validate_token(request.token)
                if not current_session:
                    context.abort(
                        grpc.StatusCode.UNAUTHENTICATED,
                        "Session expired or logged out",
                    )
                if not self.chat.is_member(request.channel_id, sess["user_id"]):
                    context.abort(
                        grpc.StatusCode.PERMISSION_DENIED,
                        "Channel membership was removed",
                    )
                if time.monotonic() - last_heartbeat >= 20:
                    self.presence.update_presence(sess["user_id"])
                    last_heartbeat = time.monotonic()
                try:
                    msg = q.get(timeout=1.0)
                    yield chat_pb2.MessageEvent(
                        event_type="NEW_MESSAGE",
                        message=chat_pb2.Message(
                            message_id=msg["message_id"],
                            channel_id=msg["channel_id"],
                            sender_id=msg["sender_id"],
                            sender_username=msg.get("sender_username", ""),
                            content=msg["content"],
                            timestamp=msg["timestamp"],
                            client_request_id=msg.get("client_request_id") or "",
                            raft_log_index=msg.get("raft_log_index", 0),
                        ),
                    )
                except queue.Empty:
                    continue
        finally:
            self.chat.unsubscribe(request.channel_id, q)
            logger.info(
                "[STREAM] %s unsubscribed from channel %s",
                sess["username"], request.channel_id,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Presence RPCs
    # ─────────────────────────────────────────────────────────────────────────

    def GetPresence(self, request, context):
        self._require_channel_member(request.token, request.channel_id, context)
        users = self.presence.get_channel_presence(request.channel_id)
        return chat_pb2.GetPresenceResponse(
            users=[
                chat_pb2.UserPresence(
                    user_id=u["user_id"],
                    username=u["username"],
                    status=u["status"],
                    last_seen=u["last_seen"],
                )
                for u in users
            ]
        )

    def UpdatePresence(self, request, context):
        sess = self._require_auth(request.token, context)
        if not sess:
            return chat_pb2.UpdatePresenceResponse(success=False)
        status = (request.status or "ONLINE").upper()
        if status not in ("ONLINE", "OFFLINE"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Status must be ONLINE or OFFLINE")
        self.presence.update_presence(sess["user_id"], status)
        return chat_pb2.UpdatePresenceResponse(success=True)

    # ─────────────────────────────────────────────────────────────────────────
    # File RPCs
    # ─────────────────────────────────────────────────────────────────────────

    def UploadFile(self, request, context):
        sess = self._require_channel_member(request.token, request.channel_id, context)
        if not request.filename.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "A valid filename is required")
        ok, meta, msg = self.files.upload_file(
            channel_id=request.channel_id,
            owner_id=sess["user_id"],
            filename=request.filename,
            data=request.data,
            content_type=request.content_type or "application/octet-stream",
            request_id=request.request_id or None,
        )
        if ok:
            return chat_pb2.UploadFileResponse(
                success=True,
                file=self._file_meta(meta),
                message=msg,
            )
        if "Channel not found" in msg:
            context.abort(grpc.StatusCode.NOT_FOUND, msg)
        if "filename" in msg.lower() or "too large" in msg.lower():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
        if "owner account" in msg.lower():
            context.abort(grpc.StatusCode.UNAUTHENTICATED, msg)
        return chat_pb2.UploadFileResponse(success=False, message=msg)

    def DownloadFile(self, request, context):
        self._require_file_member(request.token, request.file_id, context)
        ok, data, meta, msg = self.files.download_file(request.file_id)
        if ok:
            return chat_pb2.DownloadFileResponse(
                success=True,
                data=data,
                file=self._file_meta(meta),
                message=msg,
            )
        return chat_pb2.DownloadFileResponse(success=False, message=msg)

    def ListFiles(self, request, context):
        self._require_channel_member(request.token, request.channel_id, context)
        files = self.files.list_files(request.channel_id)
        return chat_pb2.ListFilesResponse(files=[self._file_meta(f) for f in files])

    @staticmethod
    def _file_meta(m: dict) -> chat_pb2.FileMetadata:
        return chat_pb2.FileMetadata(
            file_id=m["file_id"],
            filename=m["filename"],
            owner_id=m["owner_id"],
            channel_id=m["channel_id"],
            storage_location=m["storage_location"],
            size_bytes=m["size_bytes"],
            content_type=m["content_type"],
            uploaded_at=m["uploaded_at"],
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Admin RPCs
    # ─────────────────────────────────────────────────────────────────────────

    def AddUser(self, request, context):
        self._require_admin(request.token, context)
        ok, uid, msg = self.auth.create_user(
            request.username, request.password, request.role or "USER"
        )
        if not ok and msg == "Username already exists":
            context.abort(grpc.StatusCode.ALREADY_EXISTS, msg)
        if not ok and ("required" in msg.lower() or "Role must" in msg):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
        return chat_pb2.AddUserResponse(success=ok, user_id=uid, message=msg)

    def RemoveUser(self, request, context):
        self._require_admin(request.token, context)
        ok, msg = self.auth.remove_user(request.user_id)
        if not ok and msg == "User not found or already removed":
            context.abort(grpc.StatusCode.NOT_FOUND, "User not found or already removed")
        return chat_pb2.RemoveUserResponse(success=ok, message=msg)

    # ─────────────────────────────────────────────────────────────────────────
    # Node status
    # ─────────────────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────────────────
    # LLM proxy RPCs  (client → app server → LLM server)
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _llm_error(exc: Exception) -> str:
        if isinstance(exc, grpc.RpcError):
            code = exc.code()
            if code == grpc.StatusCode.UNAVAILABLE:
                return "LLM server unavailable"
            if code == grpc.StatusCode.DEADLINE_EXCEEDED:
                return "LLM inference timed out"
            return f"LLM RPC failed ({code.name}): {exc.details() or 'no details'}"
        return f"LLM request failed: {exc}"

    def GetSmartReplies(self, request, context):
        rid = request.request_id or str(uuid.uuid4())

        if not request.channel_name.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Channel name is required")
        channel = self.chat.get_channel_by_name(request.channel_name)
        if not channel:
            context.abort(grpc.StatusCode.NOT_FOUND, "Channel not found")
        self._require_channel_member(request.token, channel["channel_id"], context)
        recent = self.chat.get_messages(channel["channel_id"], limit=10)
        recent_messages = [
            f"{message['sender_username']}: {message['content']}" for message in recent
        ]
        current_message = request.current_message or (
            recent_messages[-1] if recent_messages else ""
        )

        if not self.llm:
            return chat_pb2.SmartReplyResponse(
                request_id=rid, success=False,
                error="LLM server not reachable",
            )
        try:
            resp = self.llm.GetSmartReplies(
                llm_pb2.SmartReplyRequest(
                    request_id=rid,
                    recent_messages=recent_messages,
                    current_message=current_message,
                    channel_name=channel["name"],
                ),
                timeout=90,
            )
            return chat_pb2.SmartReplyResponse(
                request_id=resp.request_id,
                suggestions=list(resp.suggestions),
                success=resp.success,
                error=resp.error,
            )
        except Exception as exc:
            logger.error("[LLM PROXY] GetSmartReplies: %s", exc)
            return chat_pb2.SmartReplyResponse(
                request_id=rid, success=False, error=self._llm_error(exc)
            )

    def SummarizeChannel(self, request, context):
        sess = self._require_channel_member(request.token, request.channel_id, context)
        rid = request.request_id or str(uuid.uuid4())

        # Fetch recent messages from DB
        msgs = self.chat.get_messages(request.channel_id, limit=request.limit or 30)
        msg_texts = [f"{m.get('sender_username', '?')}: {m['content']}" for m in msgs]

        if not self.llm:
            return chat_pb2.SummarizeResponse(
                request_id=rid, success=False, error="LLM server not reachable"
            )
        try:
            resp = self.llm.SummarizeConversation(
                llm_pb2.SummarizeRequest(
                    request_id=rid,
                    messages=msg_texts,
                    channel_name=request.channel_name or request.channel_id,
                ),
                timeout=90,
            )
            return chat_pb2.SummarizeResponse(
                request_id=resp.request_id,
                summary=resp.summary,
                success=resp.success,
                error=resp.error,
            )
        except Exception as exc:
            logger.error("[LLM PROXY] SummarizeChannel: %s", exc)
            return chat_pb2.SummarizeResponse(
                request_id=rid, success=False, error=self._llm_error(exc)
            )

    def GetContextSuggestion(self, request, context):
        sess = self._require_channel_member(request.token, request.channel_id, context)
        rid = request.request_id or str(uuid.uuid4())

        msgs = self.chat.get_messages(request.channel_id, limit=20)
        msg_texts = [f"{m.get('sender_username', '?')}: {m['content']}" for m in msgs]

        if not self.llm:
            return chat_pb2.ContextSuggestionResponse(
                request_id=rid, success=False, error="LLM server not reachable"
            )
        try:
            resp = self.llm.GetContextSuggestion(
                llm_pb2.ContextSuggestionRequest(
                    request_id=rid,
                    recent_messages=msg_texts,
                    channel_name=request.channel_name or request.channel_id,
                    current_user=sess["username"],
                ),
                timeout=90,
            )
            return chat_pb2.ContextSuggestionResponse(
                request_id=resp.request_id,
                suggestion=resp.suggestion,
                success=resp.success,
                error=resp.error,
            )
        except Exception as exc:
            logger.error("[LLM PROXY] GetContextSuggestion: %s", exc)
            return chat_pb2.ContextSuggestionResponse(
                request_id=rid, success=False, error=self._llm_error(exc)
            )


# ── Server bootstrap ──────────────────────────────────────────────────────────

def serve():
    logger.info("[SERVER] Starting node=%s port=%d", NODE_ID, PORT)

    db   = Database(DB_PATH)
    raft = RaftNode(node_id=NODE_ID, db=db, peers=PEERS)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=[
            ("grpc.max_send_message_length",    64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ("grpc.keepalive_time_ms",          10_000),
            ("grpc.keepalive_timeout_ms",        5_000),
        ],
    )
    servicer = ChatServicer(db, raft)
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, server)

    addr = f"[::]:{PORT}"
    server.add_insecure_port(addr)
    server.start()
    logger.info("[SERVER] Listening on %s  (node_id=%s  state=%s)", addr, NODE_ID, raft.state)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("[SERVER] Shutting down")
        server.stop(grace=5)
    finally:
        servicer.presence.stop()


if __name__ == "__main__":
    serve()

