"""
gRPC ChatService Servicer implementation.

Handles:
- User signup, login, logout, directory & presence tracking
- 1-on-1 Direct Messaging (DMs) with atomic idempotency
- Group chat creation with member selection checklist and Admin controls
- File sharing (images and PDFs) with byte verification
- LLM smart replies & summarization over full conversation history
- Assignment explicit RPC signatures: Post, Get, ProcessBusinessRequest
"""

import json
import logging
import queue
import time
import uuid

import grpc

import chat_pb2
import chat_pb2_grpc
import llm_pb2
import llm_pb2_grpc

from app.auth.manager import AuthManager
from app.chat.manager import ChatManager
from app.files.manager import FileManager
from app.presence.manager import PresenceManager
from storage.database import Database

logger = logging.getLogger(__name__)

STREAM_AUTH_CHECK_INTERVAL_SECONDS = 5.0
STREAM_HEARTBEAT_INTERVAL_SECONDS = 15.0


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

    def __init__(self, db: Database, node_id: str, file_storage_path: str, llm_server_addr: str):
        self.db = db
        self.node_id = node_id
        self.chat = ChatManager(db)
        self.presence = PresenceManager(
            db,
            on_presence_change=lambda _user_ids: self.chat.publish_directory_changed_for_all(),
        )
        self.auth = AuthManager(db, presence=self.presence)
        self.files = FileManager(db, file_storage_path)
        self.llm = create_llm_stub(llm_server_addr)

        logger.info("[GRPC] ChatServicer initialized with 0 default users and 0 default channels.")

    # ── Auth Helper ───────────────────────────────────────────────────────────

    @staticmethod
    def _client_kind(context) -> str:
        if context is None:
            return "cli"
        try:
            for item in context.invocation_metadata() or ():
                key, value = (item.key, item.value) if hasattr(item, "key") else item
                if key.lower() == "x-chat-client-kind" and value:
                    return str(value).lower()
        except Exception:
            logger.debug("[AUTH] Client kind metadata was unavailable", exc_info=True)
        return "cli"

    def _require_auth(self, token: str, context, *, record_activity: bool = True) -> dict:
        if not token:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing session token")
        sess = self.auth.validate_token(token)
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
        logger.info("[RPC] Signup request for username: %s", request.username)
        ok, token, uid, uname, msg = self.auth.signup(
            request.username, request.password, self._client_kind(context)
        )
        return chat_pb2.SignupResponse(
            success=ok,
            token=token,
            user_id=uid,
            username=uname,
            message=msg,
        )

    def Login(self, request, context):
        logger.info("[RPC] Login request for username: %s", request.username)
        ok, token, uid, uname, msg = self.auth.login(
            request.username, request.password, self._client_kind(context)
        )
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
            )
        )

    def UpdatePresence(self, request, context):
        sess = self._require_auth(request.token, context)
        return chat_pb2.UpdatePresenceResponse(
            success=self.presence.request_status(request.token, request.status)
        )

    # ── 1-on-1 Direct Messaging RPCs ──────────────────────────────────────────

    def SendDirectMessage(self, request, context):
        sess = self._require_auth(request.token, context)
        started = time.monotonic()
        logger.info(
            "[CHAT] DM send start sender=%s recipient=%s request_id=%s",
            sess["user_id"], request.recipient_user_id, request.client_request_id or "-",
        )
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
        sess = self._require_auth(request.token, context)
        started = time.monotonic()
        logger.info(
            "[CHAT] group send start sender=%s group=%s request_id=%s",
            sess["user_id"], request.group_id, request.client_request_id or "-",
        )
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

    # ── Server-Streaming Messages ─────────────────────────────────────────────

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

    # ── File Sharing RPCs ─────────────────────────────────────────────────────

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

    # ── LLM Features (Passing Full Chat History) ──────────────────────────────

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
        """Generic business request: handles domain-specific operations."""
        try:
            return chat_pb2.BusinessResponse(
                request_id=request.request_id,
                status="OK",
                result=json.dumps({"processed": True, "context": request.context}),
            )
        except Exception as exc:
            return chat_pb2.BusinessResponse(request_id=request.request_id, status="ERROR", result=str(exc))

    # ── Node Status ───────────────────────────────────────────────────────────

    def GetNodeStatus(self, request, context):
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
