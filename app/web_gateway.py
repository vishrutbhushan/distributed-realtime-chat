"""
HTTP Web Gateway for Distributed Real-time Chat.

Translates browser HTTP/REST calls into internal gRPC calls
and serves static web assets (HTML, CSS, JS).
"""

import base64
import collections
import email
import email.policy
import json
import logging
import os
import queue
import select
import socket
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
import chat_pb2
import chat_pb2_grpc

logger = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class _BrowserCallDetails(
    collections.namedtuple(
        "_BrowserCallDetails",
        ("method", "timeout", "metadata", "credentials", "wait_for_ready"),
    ),
    grpc.ClientCallDetails,
):
    pass


class _BrowserPresenceInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
):
    """Mark every gateway RPC as browser traffic without changing protobufs."""

    @staticmethod
    def _with_browser_kind(details):
        metadata = list(details.metadata or ())
        if not any(key.lower() == "x-chat-client-kind" for key, _ in metadata):
            metadata.append(("x-chat-client-kind", "browser"))
        return _BrowserCallDetails(
            details.method,
            details.timeout,
            metadata,
            details.credentials,
            details.wait_for_ready,
        )

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(self._with_browser_kind(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(self._with_browser_kind(client_call_details), request)


class WebGatewayHandler(BaseHTTPRequestHandler):

    grpc_stub: chat_pb2_grpc.ChatServiceStub = None
    stream_stub: chat_pb2_grpc.ChatServiceStub = None
    llm_stub: chat_pb2_grpc.ChatServiceStub = None

    def setup(self):
        super().setup()
        self.connection.settimeout(20)
        try:
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            for option, value in (
                ("TCP_KEEPIDLE", 15),
                ("TCP_KEEPINTVL", 5),
                ("TCP_KEEPCNT", 3),
            ):
                level_option = getattr(socket, option, None)
                if level_option is not None:
                    self.connection.setsockopt(socket.IPPROTO_TCP, level_option, value)
        except OSError:
            logger.debug("[WEB] TCP keepalive tuning unavailable", exc_info=True)

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

        # ── Serve Static Assets (HTML, CSS, JS) ──────────────────────────────
        if path in ("/", "/index.html") or path.startswith("/css/") or path.startswith("/js/"):
            rel = "index.html" if path in ("/", "/index.html") else path.lstrip("/")
            file_path = os.path.join(_ROOT, "web", rel)
            if not os.path.exists(file_path):
                self.send_error(404, f"{rel} not found")
                return

            ctype = "text/html; charset=utf-8"
            if file_path.endswith(".css"):
                ctype = "text/css; charset=utf-8"
            elif file_path.endswith(".js"):
                ctype = "application/javascript; charset=utf-8"

            with open(file_path, "rb") as f:
                content = f.read()

            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        token = qs.get("token", [""])[0]

        # GET /api/events keeps one browser connection open and forwards the
        # existing server-streaming gRPC RPC as Server-Sent Events.
        if path == "/api/events":
            self._serve_event_stream(token)
            return

        # GET /api/users
        if path == "/api/users":
            try:
                resp = self.grpc_stub.ListUsers(chat_pb2.ListUsersRequest(token=token))
                users = [
                    {
                        "user_id": u.user_id,
                        "username": u.username,
                        "status": u.status,
                        "last_seen": u.last_seen,
                        "last_message_time": u.last_message_time,
                        "unread_count": u.unread_count,
                        "last_message": u.last_message,
                    }
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
                        "last_message_time": g.last_message_time,
                        "unread_count": g.unread_count,
                        "last_message": g.last_message,
                    }
                    for g in resp.groups
                ]
                self._send_json(200, {"success": True, "groups": groups})
            except grpc.RpcError as e:
                self._send_json(401, {"success": False, "error": e.details()})
            return

        # GET /api/groups/{id}/members or /api/groups/members?group_id=...
        if (path.startswith("/api/groups/") and path.endswith("/members")) or path == "/api/groups/members":
            group_id = qs.get("group_id", [""])[0]
            if not group_id and path.startswith("/api/groups/"):
                group_id = path.split("/")[3]
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

        # GET /api/messages/dm/{other_user_id} or /api/messages/dm?other_user_id=...
        if path.startswith("/api/messages/dm"):
            other_user_id = qs.get("other_user_id", [""])[0]
            if not other_user_id and path.startswith("/api/messages/dm/"):
                other_user_id = path.replace("/api/messages/dm/", "")
            try:
                resp = self.grpc_stub.GetDirectMessages(
                    chat_pb2.GetDirectMessagesRequest(token=token, other_user_id=other_user_id, limit=100)
                )
                msgs = [self._msg_to_dict(m) for m in resp.messages]
                self._send_json(200, {"success": True, "messages": msgs})
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        # GET /api/messages/group/{group_id} or /api/messages/group?group_id=...
        if path.startswith("/api/messages/group"):
            group_id = qs.get("group_id", [""])[0]
            if not group_id and path.startswith("/api/messages/group/"):
                group_id = path.replace("/api/messages/group/", "")
            try:
                resp = self.grpc_stub.GetGroupMessages(
                    chat_pb2.GetGroupMessagesRequest(token=token, group_id=group_id, limit=100)
                )
                msgs = [self._msg_to_dict(m) for m in resp.messages]
                self._send_json(200, {"success": True, "messages": msgs})
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        # GET /api/files/download or /api/files/{file_id}/download
        if path == "/api/files/download" or (path.startswith("/api/files/") and path.endswith("/download")):
            file_id = qs.get("file_id", [""])[0]
            if not file_id and path.startswith("/api/files/"):
                file_id = path.split("/")[3]
            try:
                resp = self.grpc_stub.DownloadFile(chat_pb2.DownloadFileRequest(token=token, file_id=file_id))
                if not resp.success:
                    self.send_error(404, resp.error or "File not found")
                    return
                self.send_response(200)
                self.send_header("Content-Type", resp.file.content_type)
                self.send_header("Content-Length", str(len(resp.data)))
                self.send_header("Content-Disposition", f'inline; filename="{resp.file.filename}"')
                self.end_headers()
                self.wfile.write(resp.data)
            except grpc.RpcError as e:
                self.send_error(400, e.details())
            return

        self.send_error(404, "Endpoint not found")

    def _serve_event_stream(self, token: str):
        stream = None
        try:
            if not token:
                self._send_json(401, {"success": False, "error": "Missing session token"})
                return

            stream = self.stream_stub.StreamMessages(
                chat_pb2.StreamMessagesRequest(token=token)
            )
            # StreamMessages sends READY only after auth and subscription setup.
            # Fetching this first event lets invalid tokens receive a normal
            # HTTP response before the SSE headers are committed.
            first_event = next(stream)
        except StopIteration:
            self._send_json(503, {"success": False, "error": "Event stream ended during setup"})
            return
        except grpc.RpcError as exc:
            status = 401 if exc.code() == grpc.StatusCode.UNAUTHENTICATED else 503
            self._send_json(status, {"success": False, "error": exc.details() or "Event stream unavailable"})
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        event_queue = queue.Queue(maxsize=128)
        client_closed = threading.Event()
        stream_finished = threading.Event()

        def relay_grpc_events():
            try:
                for event in stream:
                    if client_closed.is_set():
                        break
                    try:
                        event_queue.put_nowait(event)
                    except queue.Full:
                        while True:
                            try:
                                event_queue.get_nowait()
                            except queue.Empty:
                                break
                        try:
                            event_queue.put_nowait(
                                chat_pb2.MessageEvent(event_type="RESYNC_REQUIRED")
                            )
                        except queue.Full:
                            logger.warning("[WEB] Could not enqueue stream resync marker")
            except grpc.RpcError as exc:
                logger.debug("[WEB] Event gRPC stream ended: %s", exc.details())
            finally:
                stream_finished.set()

        relay = threading.Thread(
            target=relay_grpc_events,
            daemon=True,
            name="sse-grpc-relay",
        )
        relay.start()
        try:
            self._write_sse_event(first_event)
            while not stream_finished.is_set() or not event_queue.empty():
                if self._client_disconnected():
                    break
                try:
                    event = event_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                self._write_sse_event(event)
        except (BrokenPipeError, ConnectionResetError, OSError):
            # Expected when a tab closes or its fetch stream is aborted.
            pass
        finally:
            client_closed.set()
            if stream is not None:
                stream.cancel()
            relay.join(timeout=1)

    def _client_disconnected(self) -> bool:
        """Detect a closed browser socket even when no SSE data is arriving."""
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            if not readable:
                return False
            return self.connection.recv(1, socket.MSG_PEEK) == b""
        except (BlockingIOError, InterruptedError):
            return False
        except OSError:
            return True

    def _write_sse_event(self, event):
        event_type = event.event_type or "RESYNC_REQUIRED"
        payload = {}
        if event_type == "NEW_MESSAGE" and event.HasField("message"):
            payload["message"] = self._msg_to_dict(event.message)
        elif event_type == "GROUP_ACCESS_REVOKED" and event.HasField("message"):
            payload["group_id"] = event.message.group_id
        elif event_type == "READ_STATE_CHANGED" and event.HasField("message"):
            payload["chat_type"] = event.message.chat_type
            payload["target_id"] = event.message.group_id or event.message.recipient_id
        elif event_type == "DIRECTORY_CHANGED" and event.HasField("message"):
            payload["chat_type"] = event.message.chat_type
            payload["target_id"] = event.message.group_id or event.message.recipient_id
            payload["local_only"] = event.message.content == "local_only"
        encoded = json.dumps(payload, separators=(",", ":"))
        frame = f"event: {event_type}\ndata: {encoded}\n\n".encode("utf-8")
        self.wfile.write(frame)
        self.wfile.flush()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        # POST /api/auth/signup or /api/signup
        if path in ("/api/auth/signup", "/api/signup"):
            data = self._read_json()
            try:
                resp = self.grpc_stub.Signup(
                    chat_pb2.SignupRequest(username=data.get("username", ""), password=data.get("password", ""))
                )
                self._send_json(200, {
                    "success": resp.success,
                    "token": resp.token,
                    "user_id": resp.user_id,
                    "username": resp.username,
                    "message": resp.message,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "message": e.details()})
            return

        # POST /api/auth/login or /api/login
        if path in ("/api/auth/login", "/api/login"):
            data = self._read_json()
            try:
                resp = self.grpc_stub.Login(
                    chat_pb2.LoginRequest(username=data.get("username", ""), password=data.get("password", ""))
                )
                self._send_json(200, {
                    "success": resp.success,
                    "token": resp.token,
                    "user_id": resp.user_id,
                    "username": resp.username,
                    "message": resp.message,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "message": e.details()})
            return

        # POST /api/auth/logout or /api/logout
        if path in ("/api/auth/logout", "/api/logout"):
            data = self._read_json()
            try:
                resp = self.grpc_stub.Logout(chat_pb2.LogoutRequest(token=data.get("token", "")))
                self._send_json(200, {"success": resp.success, "message": resp.message})
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "message": e.details()})
            return

        # POST /api/messages/dm
        if path.startswith("/api/messages/dm"):
            data = self._read_json()
            recipient_id = data.get("recipient_user_id") or path.replace("/api/messages/dm/", "").replace("/api/messages/dm", "")
            try:
                resp = self.grpc_stub.SendDirectMessage(
                    chat_pb2.SendDirectMessageRequest(
                        token=data.get("token", ""),
                        recipient_user_id=recipient_id,
                        content=data.get("content", ""),
                        client_request_id=data.get("client_request_id") or None,
                        file_id=data.get("file_id") or None,
                    )
                )
                self._send_json(200, {
                    "success": resp.success,
                    "message": self._msg_to_dict(resp.message) if resp.success else None,
                    "error": resp.error,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        # POST /api/messages/group
        if path.startswith("/api/messages/group"):
            data = self._read_json()
            group_id = data.get("group_id") or path.replace("/api/messages/group/", "").replace("/api/messages/group", "")
            try:
                resp = self.grpc_stub.SendGroupMessage(
                    chat_pb2.SendGroupMessageRequest(
                        token=data.get("token", ""),
                        group_id=group_id,
                        content=data.get("content", ""),
                        client_request_id=data.get("client_request_id") or None,
                        file_id=data.get("file_id") or None,
                    )
                )
                self._send_json(200, {
                    "success": resp.success,
                    "message": self._msg_to_dict(resp.message) if resp.success else None,
                    "error": resp.error,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        # POST /api/chat/read or /api/messages/read
        if path in ("/api/chat/read", "/api/messages/read"):
            data = self._read_json()
            try:
                resp = self.grpc_stub.MarkRead(
                    chat_pb2.MarkReadRequest(
                        token=data.get("token", ""),
                        chat_type=data.get("chat_type", "DM"),
                        target_id=data.get("target_id", ""),
                    )
                )
                self._send_json(200, {"success": resp.success})
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        # POST /api/groups (Create group)
        if path == "/api/groups":
            data = self._read_json()
            try:
                resp = self.grpc_stub.CreateGroup(
                    chat_pb2.CreateGroupRequest(
                        token=data.get("token", ""),
                        name=data.get("name", ""),
                        initial_member_user_ids=data.get("initial_member_user_ids", []),
                    )
                )
                self._send_json(200, {
                    "success": resp.success,
                    "group": {
                        "group_id": resp.group.group_id,
                        "name": resp.group.name,
                        "created_by": resp.group.created_by,
                        "created_by_name": resp.group.created_by_name,
                        "member_count": resp.group.member_count,
                        "user_role": resp.group.user_role,
                    } if resp.success else None,
                    "message": resp.message,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "message": e.details()})
            return

        # POST /api/groups/{id}/update or /api/groups/update
        if (path.startswith("/api/groups/") and path.endswith("/update")) or path == "/api/groups/update":
            data = self._read_json()
            group_id = data.get("group_id") or (path.split("/")[3] if path.startswith("/api/groups/") else "")
            try:
                resp = self.grpc_stub.UpdateGroup(
                    chat_pb2.UpdateGroupRequest(
                        token=data.get("token", ""),
                        group_id=group_id,
                        action=data.get("action", ""),
                        target_user_id=data.get("target_user_id") or "",
                        new_name=data.get("new_name") or "",
                    )
                )
                self._send_json(200, {
                    "success": resp.success,
                    "group": {
                        "group_id": resp.group.group_id,
                        "name": resp.group.name,
                    } if resp.success else None,
                    "message": resp.message,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "message": e.details()})
            return

        # POST /api/files/upload
        if path == "/api/files/upload":
            content_type_header = self.headers.get("Content-Type", "")
            content_len = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(content_len)

            token = ""
            chat_type = "DM"
            target_id = ""
            filename = "upload.bin"
            file_bytes = b""
            f_content_type = "application/octet-stream"

            # Check query params for token fallback
            parsed_qs = urllib.parse.parse_qs(parsed.query)
            token = parsed_qs.get("token", [""])[0]

            if "multipart/form-data" in content_type_header:
                try:
                    full_raw = f"Content-Type: {content_type_header}\r\n\r\n".encode("utf-8") + raw_body
                    msg = email.message_from_bytes(full_raw, policy=email.policy.default)
                    if msg.is_multipart():
                        for part in msg.iter_parts():
                            p_name = part.get_param("name", header="content-disposition")
                            if p_name == "token":
                                tok_val = part.get_payload(decode=True)
                                if tok_val:
                                    token = tok_val.decode("utf-8", errors="ignore").strip()
                            elif p_name == "chat_type":
                                ct_val = part.get_payload(decode=True)
                                if ct_val:
                                    chat_type = ct_val.decode("utf-8", errors="ignore").strip()
                            elif p_name == "target_id":
                                tid_val = part.get_payload(decode=True)
                                if tid_val:
                                    target_id = tid_val.decode("utf-8", errors="ignore").strip()
                            elif p_name == "file":
                                fn = part.get_filename()
                                if fn:
                                    filename = fn
                                f_content_type = part.get_content_type() or "application/octet-stream"
                                file_bytes = part.get_payload(decode=True) or b""
                except Exception as parse_err:
                    logger.warning("[WEB] Multipart parse warning: %s", parse_err)

            try:
                resp = self.grpc_stub.UploadFile(
                    chat_pb2.UploadFileRequest(
                        token=token,
                        chat_type=chat_type,
                        target_id=target_id,
                        filename=filename,
                        data=file_bytes,
                        content_type=f_content_type,
                    )
                )
                err_msg = resp.message if hasattr(resp, "message") else ""
                self._send_json(200, {
                    "success": resp.success,
                    "file": {
                        "file_id": resp.file.file_id,
                        "filename": resp.file.filename,
                        "file_type": resp.file.file_type,
                        "size_bytes": resp.file.size_bytes,
                    } if resp.success else None,
                    "message": err_msg,
                    "error": err_msg,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            except Exception as exc:
                logger.error("[WEB] File upload error: %s", exc, exc_info=True)
                self._send_json(500, {"success": False, "error": f"Upload processing error: {str(exc)}"})
            return

        # POST /api/llm/smart-reply
        if path == "/api/llm/smart-reply":
            data = self._read_json()
            try:
                resp = self.llm_stub.GetSmartReplies(
                    chat_pb2.SmartReplyRequest(
                        token=data.get("token", ""),
                        chat_history=data.get("chat_history", []),
                        current_message=data.get("current_message", ""),
                        context_title=data.get("context_title", "Chat"),
                    )
                )
                self._send_json(200, {
                    "success": resp.success,
                    "suggestions": list(resp.suggestions),
                    "error": resp.error,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        # POST /api/llm/summarize
        if path == "/api/llm/summarize":
            data = self._read_json()
            try:
                resp = self.llm_stub.SummarizeChat(
                    chat_pb2.SummarizeChatRequest(
                        token=data.get("token", ""),
                        chat_type=data.get("chat_type", ""),
                        target_id=data.get("target_id", ""),
                        chat_history=data.get("chat_history", []),
                        context_title=data.get("context_title", "Chat"),
                        current_user=data.get("current_user", ""),
                    )
                )
                self._send_json(200, {
                    "success": resp.success,
                    "summary": resp.summary,
                    "error": resp.error,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        # POST /api/llm/suggest
        if path == "/api/llm/suggest":
            data = self._read_json()
            try:
                resp = self.llm_stub.GetContextSuggestion(
                    chat_pb2.ContextSuggestionRequest(
                        token=data.get("token", ""),
                        chat_history=data.get("chat_history", []),
                        context_title=data.get("context_title", "Chat"),
                    )
                )
                self._send_json(200, {
                    "success": resp.success,
                    "suggestion": resp.suggestion,
                    "error": resp.error,
                })
            except grpc.RpcError as e:
                self._send_json(400, {"success": False, "error": e.details()})
            return

        self.send_error(404, "Endpoint not found")

    def _msg_to_dict(self, m) -> dict:
        return {
            "message_id": m.message_id,
            "chat_type": m.chat_type,
            "sender_id": m.sender_id,
            "sender_username": m.sender_username,
            "recipient_id": m.recipient_id,
            "group_id": m.group_id,
            "content": m.content,
            "timestamp": m.timestamp,
            "client_request_id": m.client_request_id,
            "file_id": m.file_id,
            "filename": m.filename,
            "file_type": m.file_type,
            "file_size": m.file_size,
        }

    def log_message(self, format, *args):
        pass


def run_web_gateway(
    port: int,
    grpc_target: str,
    stream_grpc_target: str = None,
    llm_grpc_target: str = None,
) -> ThreadingHTTPServer:
    """Run the threaded HTTP Web Gateway server."""
    interceptor = _BrowserPresenceInterceptor()
    channel = grpc.intercept_channel(grpc.insecure_channel(grpc_target), interceptor)
    stream_channel = grpc.intercept_channel(
        grpc.insecure_channel(stream_grpc_target or grpc_target), interceptor
    )
    llm_channel = grpc.intercept_channel(
        grpc.insecure_channel(llm_grpc_target or grpc_target), interceptor
    )
    WebGatewayHandler.grpc_stub = chat_pb2_grpc.ChatServiceStub(channel)
    WebGatewayHandler.stream_stub = chat_pb2_grpc.ChatServiceStub(stream_channel)
    WebGatewayHandler.llm_stub = chat_pb2_grpc.ChatServiceStub(llm_channel)
    httpd = ThreadingHTTPServer(("0.0.0.0", port), WebGatewayHandler)
    httpd.grpc_channels = (channel, stream_channel, llm_channel)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True, name="web-gateway")
    thread.start()
    logger.info("[WEB] Web Gateway running on http://0.0.0.0:%d", port)
    return httpd
