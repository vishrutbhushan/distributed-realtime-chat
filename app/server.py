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
        # Users
        ok, admin_id, _ = self.auth.create_user("admin", "admin123", "ADMIN")
        if ok:
            logger.info("[SEED] admin / admin123 created")
        self.auth.create_user("alice", "alice123", "USER")
        self.auth.create_user("bob",   "bob123",   "USER")

        # Default channels (created by admin)
        row = self.db.fetchone("SELECT user_id FROM users WHERE username = 'admin'")
        if row:
            aid = row["user_id"]
            for name in ("general", "aos-project", "testing"):
                self.chat.create_channel(name, aid)

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
        if not sess:
            return chat_pb2.CreateChannelResponse(success=False)
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
        return chat_pb2.CreateChannelResponse(success=False, message=msg)

    def DeleteChannel(self, request, context):
        sess = self._require_admin(request.token, context)
        if not sess:
            return chat_pb2.DeleteChannelResponse(success=False)
        ok, msg = self.chat.delete_channel(request.channel_id)
        return chat_pb2.DeleteChannelResponse(success=ok, message=msg)

    def JoinChannel(self, request, context):
        sess = self._require_auth(request.token, context)
        if not sess:
            return chat_pb2.JoinChannelResponse(success=False)
        ok, msg = self.chat.join_channel(request.channel_id, sess["user_id"])
        return chat_pb2.JoinChannelResponse(success=ok, message=msg)

    def LeaveChannel(self, request, context):
        sess = self._require_auth(request.token, context)
        if not sess:
            return chat_pb2.LeaveChannelResponse(success=False)
        ok, msg = self.chat.leave_channel(request.channel_id, sess["user_id"])
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
        sess = self._require_auth(request.token, context)
        if not sess:
            return chat_pb2.SendMessageResponse(success=False)

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
        return chat_pb2.SendMessageResponse(success=False, error=err)

    def GetMessages(self, request, context):
        self._require_auth(request.token, context)
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
        sess = self._require_auth(request.token, context)
        if not sess:
            return

        q = queue.Queue(maxsize=256)
        self.chat.subscribe(request.channel_id, q)
        logger.info(
            "[STREAM] %s subscribed to channel %s",
            sess["username"], request.channel_id,
        )
        try:
            while context.is_active():
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
        self._require_auth(request.token, context)
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
        self.presence.update_presence(sess["user_id"], request.status or "ONLINE")
        return chat_pb2.UpdatePresenceResponse(success=True)

    # ─────────────────────────────────────────────────────────────────────────
    # File RPCs
    # ─────────────────────────────────────────────────────────────────────────

    def UploadFile(self, request, context):
        sess = self._require_auth(request.token, context)
        if not sess:
            return chat_pb2.UploadFileResponse(success=False)
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
        return chat_pb2.UploadFileResponse(success=False, message=msg)

    def DownloadFile(self, request, context):
        self._require_auth(request.token, context)
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
        self._require_auth(request.token, context)
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
        return chat_pb2.AddUserResponse(success=ok, user_id=uid, message=msg)

    def RemoveUser(self, request, context):
        self._require_admin(request.token, context)
        ok, msg = self.auth.remove_user(request.user_id)
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

    def GetSmartReplies(self, request, context):
        sess = self._require_auth(request.token, context)
        if not sess:
            return chat_pb2.SmartReplyResponse(success=False)
        rid = request.request_id or str(uuid.uuid4())

        if not self.llm:
            return chat_pb2.SmartReplyResponse(
                request_id=rid, success=False,
                error="LLM server not reachable",
            )
        try:
            resp = self.llm.GetSmartReplies(
                llm_pb2.SmartReplyRequest(
                    request_id=rid,
                    recent_messages=list(request.recent_messages),
                    current_message=request.current_message,
                    channel_name=request.channel_name,
                )
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
                request_id=rid, success=False, error=str(exc)
            )

    def SummarizeChannel(self, request, context):
        sess = self._require_auth(request.token, context)
        if not sess:
            return chat_pb2.SummarizeResponse(success=False)
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
                )
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
                request_id=rid, success=False, error=str(exc)
            )

    def GetContextSuggestion(self, request, context):
        sess = self._require_auth(request.token, context)
        if not sess:
            return chat_pb2.ContextSuggestionResponse(success=False)
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
                )
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
                request_id=rid, success=False, error=str(exc)
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
    chat_pb2_grpc.add_ChatServiceServicer_to_server(
        ChatServicer(db, raft), server
    )

    addr = f"[::]:{PORT}"
    server.add_insecure_port(addr)
    server.start()
    logger.info("[SERVER] Listening on %s  (node_id=%s  state=%s)", addr, NODE_ID, raft.state)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("[SERVER] Shutting down")
        server.stop(grace=5)


if __name__ == "__main__":
    serve()

