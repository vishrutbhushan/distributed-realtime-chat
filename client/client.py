#!/usr/bin/env python3
"""
Interactive CLI client for the distributed chat system.

Usage:
  python client/client.py --server localhost:50051
  python client/client.py --server localhost:50051 --demo

The --demo flag runs an automated end-to-end demonstration and exits.
"""

import argparse
import os
import queue
import sys
import threading
import time
import uuid

_ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_GENERATED = os.path.join(_ROOT, "generated")
sys.path.insert(0, _GENERATED)
sys.path.insert(0, _ROOT)

import grpc
import chat_pb2
import chat_pb2_grpc

SERVER = os.environ.get("APP_SERVER", "localhost:50051")


def make_stub(server: str) -> chat_pb2_grpc.ChatServiceStub:
    channel = grpc.insecure_channel(
        server,
        options=[
            ("grpc.max_send_message_length",    64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
    )
    return chat_pb2_grpc.ChatServiceStub(channel)


# ─────────────────────────────────────────────────────────────────────────────
# Demo mode — end-to-end automated walkthrough
# ─────────────────────────────────────────────────────────────────────────────

def run_demo(stub: chat_pb2_grpc.ChatServiceStub):
    def check(condition, message):
        if not condition:
            raise RuntimeError(message)

    def require_success(response, label):
        check(response.success, f"{label} failed: {getattr(response, 'message', '') or getattr(response, 'error', '')}")
        return response

    suffix = uuid.uuid4().hex[:8]
    channel_name = f"m1-demo-{suffix}"
    admin_token = alice_token = bob_token = temp_user_token = None
    demo_id = temp_user_id = file_id = None
    removed_user = False
    stream = None

    print("\n" + "=" * 64)
    print(" DISTRIBUTED CHAT — MILESTONE 1 RECORDING DEMO")
    print(f" Run ID: {suffix}")
    print("=" * 64)
    try:
        status = stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest(), timeout=10)
        print(f"[1] Node status: {status.node_id} / {status.state} / term {status.term}")

        for username, password in (
            ("admin", "admin123"),
            ("alice", "alice123"),
            ("bob", "bob123"),
        ):
            response = stub.Login(
                chat_pb2.LoginRequest(username=username, password=password), timeout=10
            )
            require_success(response, f"Login as {username}")
            if username == "admin":
                admin_token = response.token
            elif username == "alice":
                alice_token = response.token
            else:
                bob_token = response.token
            print(f"[2] Logged in {username} ({response.role})")

        channels = stub.ListChannels(
            chat_pb2.ListChannelsRequest(token=admin_token), timeout=10
        )
        channel_names = {channel.name for channel in channels.channels}
        check(
            {"general", "aos-project", "testing"} <= channel_names,
            "One or more default channels are missing",
        )
        print(f"[3] Channels in persistent database: {', '.join('#' + c.name for c in channels.channels)}")

        created = stub.CreateChannel(
            chat_pb2.CreateChannelRequest(
                token=admin_token,
                channel_name=channel_name,
                request_id=str(uuid.uuid4()),
            ),
            timeout=10,
        )
        require_success(created, "Admin create channel")
        demo_id = created.channel.channel_id
        print(f"[4] Admin created #{channel_name}")

        for username, token in (("alice", alice_token), ("bob", bob_token)):
            joined = stub.JoinChannel(
                chat_pb2.JoinChannelRequest(token=token, channel_id=demo_id), timeout=10
            )
            require_success(joined, f"{username} join channel")
        print("[5] Alice and Bob joined; sending channel conversation")

        conversation = [
            (alice_token, "Hey everyone! Are we still meeting at 5?"),
            (bob_token, "Yes! Meeting at 5 PM — don't forget the slides."),
            (alice_token, "Got it. I'll handle the deployment demo."),
            (bob_token, "The CI pipeline failed again — anyone know why?"),
            (alice_token, "Let me check the logs."),
        ]
        for token, content in conversation:
            sent = stub.SendMessage(
                chat_pb2.SendMessageRequest(
                    token=token,
                    channel_id=demo_id,
                    content=content,
                    client_request_id=str(uuid.uuid4()),
                ),
                timeout=10,
            )
            require_success(sent, "Send message")
            print(f"    {sent.message.sender_username}: {sent.message.content}")

        rid = str(uuid.uuid4())
        retry = chat_pb2.SendMessageRequest(
            token=alice_token,
            channel_id=demo_id,
            content="Idempotency retry: this appears once.",
            client_request_id=rid,
        )
        first = require_success(stub.SendMessage(retry, timeout=10), "First idempotent send")
        second = require_success(stub.SendMessage(retry, timeout=10), "Retried idempotent send")
        check(first.message.message_id == second.message.message_id, "Retry created a duplicate message")
        print("[6] Retried send returned the same message ID")

        print("[7] Live stream: Alice subscribes; Bob publishes")
        ready = threading.Event()
        stream_events = queue.Queue()
        stream = stub.StreamMessages(
            chat_pb2.StreamMessagesRequest(token=alice_token, channel_id=demo_id)
        )

        def read_stream():
            try:
                for event in stream:
                    if event.event_type == "READY":
                        ready.set()
                    else:
                        stream_events.put(event)
            except Exception as exc:
                stream_events.put(exc)

        reader = threading.Thread(target=read_stream, name="demo-stream-reader", daemon=True)
        reader.start()
        check(ready.wait(timeout=10), "Stream subscription did not become ready")
        live_text = f"Live stream check {suffix}"
        live = require_success(
            stub.SendMessage(
                chat_pb2.SendMessageRequest(
                    token=bob_token,
                    channel_id=demo_id,
                    content=live_text,
                    client_request_id=str(uuid.uuid4()),
                ),
                timeout=10,
            ),
            "Publish live stream message",
        )
        try:
            streamed = stream_events.get(timeout=10)
        except queue.Empty as exc:
            raise RuntimeError("Timed out waiting for streamed message") from exc
        if isinstance(streamed, Exception):
            raise RuntimeError(f"Stream failed: {streamed}")
        check(streamed.event_type == "NEW_MESSAGE", f"Unexpected stream event {streamed.event_type}")
        check(streamed.message.message_id == live.message.message_id, "Stream delivered the wrong message")
        print(f"    Alice received Bob's message live: {streamed.message.content}")
        stream.cancel()
        stream = None

        revoked_stream = stub.StreamMessages(
            chat_pb2.StreamMessagesRequest(token=alice_token, channel_id=demo_id), timeout=10
        )
        ready_event = next(revoked_stream)
        check(ready_event.event_type == "READY", "Second stream did not subscribe")
        left = stub.LeaveChannel(
            chat_pb2.LeaveChannelRequest(token=alice_token, channel_id=demo_id), timeout=10
        )
        require_success(left, "Leave channel during stream")
        try:
            next(revoked_stream)
            raise RuntimeError("An open stream remained authorized after membership removal")
        except grpc.RpcError as exc:
            check(exc.code() == grpc.StatusCode.PERMISSION_DENIED,
                  f"Revoked stream returned {exc.code()}, expected PERMISSION_DENIED")
        finally:
            revoked_stream.cancel()
        rejoined = stub.JoinChannel(
            chat_pb2.JoinChannelRequest(token=alice_token, channel_id=demo_id), timeout=10
        )
        require_success(rejoined, "Rejoin after stream revocation test")
        print("    Removing membership revoked Alice's already-open stream")

        history = stub.GetMessages(
            chat_pb2.GetMessagesRequest(token=alice_token, channel_id=demo_id, limit=50),
            timeout=10,
        )
        history_text = [m.content for m in history.messages]
        check(history_text.count("Idempotency retry: this appears once.") == 1, "History contains a duplicate retry")
        check(live_text in history_text, "Live message is missing from persistent history")
        check(history_text.index(conversation[0][1]) < history_text.index(conversation[-1][1]), "History order is incorrect")
        print(f"[8] History contains {len(history.messages)} ordered messages")

        try:
            stub.CreateChannel(
                chat_pb2.CreateChannelRequest(
                    token=alice_token,
                    channel_name=f"unauthorized-{suffix}",
                    request_id=str(uuid.uuid4()),
                ),
                timeout=10,
            )
            raise RuntimeError("A non-admin user was able to create a channel")
        except grpc.RpcError as exc:
            check(exc.code() == grpc.StatusCode.PERMISSION_DENIED,
                  f"Unauthorized admin operation returned {exc.code()}, expected PERMISSION_DENIED")
        print("[9] Non-admin channel creation correctly returned PERMISSION_DENIED")

        presence = stub.GetPresence(
            chat_pb2.GetPresenceRequest(token=alice_token, channel_id=demo_id), timeout=10
        )
        check({u.username for u in presence.users} >= {"alice", "bob"}, "Channel presence omitted a logged-in member")
        print("[10] Channel presence reports Alice and Bob online")

        file_content = (
            f"AOS Milestone 1 demo notes\nRun: {suffix}\n"
            "Meeting: 5 PM; Alice owns the deployment demo; Bob is checking the CI failure.\n"
        ).encode("utf-8")
        uploaded = stub.UploadFile(
            chat_pb2.UploadFileRequest(
                token=alice_token,
                channel_id=demo_id,
                filename="demo-notes.txt",
                data=file_content,
                content_type="text/plain",
                request_id=str(uuid.uuid4()),
            ),
            timeout=10,
        )
        require_success(uploaded, "Upload text file")
        file_id = uploaded.file.file_id
        downloaded = stub.DownloadFile(
            chat_pb2.DownloadFileRequest(token=bob_token, file_id=file_id), timeout=10
        )
        require_success(downloaded, "Download shared file")
        check(downloaded.data == file_content, "Downloaded file bytes did not match uploaded bytes")
        print(f"[11] Shared {uploaded.file.filename}; verified all {len(file_content)} bytes")

        print("[12] LLM smart replies (real local model)")
        inference_started = time.perf_counter()
        replies = stub.GetSmartReplies(
            chat_pb2.SmartReplyRequest(
                token=alice_token,
                request_id=str(uuid.uuid4()),
                current_message=conversation[3][1],
                channel_name=channel_name,
            ),
            timeout=100,
        )
        require_success(replies, "Smart replies")
        print(f"    inference completed in {time.perf_counter() - inference_started:.2f}s")
        check(len(replies.suggestions) == 3, f"Expected 3 suggestions, got {len(replies.suggestions)}")
        check(all(s.strip() and "mock" not in s.lower() for s in replies.suggestions), "Smart replies were empty or mock output")
        for index, suggestion in enumerate(replies.suggestions, 1):
            print(f"    {index}. {suggestion}")

        inference_started = time.perf_counter()
        summary = stub.SummarizeChannel(
            chat_pb2.SummarizeRequest(
                token=alice_token,
                request_id=str(uuid.uuid4()),
                channel_id=demo_id,
                channel_name=channel_name,
                limit=20,
            ),
            timeout=100,
        )
        require_success(summary, "Conversation summary")
        print(f"    inference completed in {time.perf_counter() - inference_started:.2f}s")
        check(summary.summary.strip() and "mock" not in summary.summary.lower(), "Summary was empty or mock output")
        print("[13] LLM conversation summary")
        print(summary.summary)

        inference_started = time.perf_counter()
        suggestion = stub.GetContextSuggestion(
            chat_pb2.ContextSuggestionRequest(
                token=bob_token,
                request_id=str(uuid.uuid4()),
                channel_id=demo_id,
                channel_name=channel_name,
            ),
            timeout=100,
        )
        require_success(suggestion, "Next-step suggestion")
        print(f"    inference completed in {time.perf_counter() - inference_started:.2f}s")
        check(suggestion.suggestion.strip() and "mock" not in suggestion.suggestion.lower(), "Next-step suggestion was empty or mock output")
        print("[14] LLM concrete next step")
        print(suggestion.suggestion)

        username = f"demo-user-{suffix}"
        added = stub.AddUser(
            chat_pb2.AddUserRequest(
                token=admin_token,
                username=username,
                password=f"demo-{suffix}-password",
                role="USER",
                request_id=str(uuid.uuid4()),
            ),
            timeout=10,
        )
        require_success(added, "Add temporary demo user")
        temp_user_id = added.user_id
        temp_login = stub.Login(
            chat_pb2.LoginRequest(username=username, password=f"demo-{suffix}-password"),
            timeout=10,
        )
        require_success(temp_login, "Login temporary demo user")
        temp_user_token = temp_login.token

        def expect_permission_denied(label, operation):
            try:
                operation()
                raise RuntimeError(f"Non-member was able to {label}")
            except grpc.RpcError as exc:
                check(exc.code() == grpc.StatusCode.PERMISSION_DENIED,
                      f"Non-member {label} returned {exc.code()}, expected PERMISSION_DENIED")

        expect_permission_denied(
            "read message history",
            lambda: stub.GetMessages(
                chat_pb2.GetMessagesRequest(token=temp_user_token, channel_id=demo_id), timeout=10
            ),
        )
        expect_permission_denied(
            "read channel presence",
            lambda: stub.GetPresence(
                chat_pb2.GetPresenceRequest(token=temp_user_token, channel_id=demo_id), timeout=10
            ),
        )
        expect_permission_denied(
            "list channel files",
            lambda: stub.ListFiles(
                chat_pb2.ListFilesRequest(token=temp_user_token, channel_id=demo_id), timeout=10
            ),
        )
        expect_permission_denied(
            "download a channel file",
            lambda: stub.DownloadFile(
                chat_pb2.DownloadFileRequest(token=temp_user_token, file_id=file_id), timeout=10
            ),
        )
        denied_stream = stub.StreamMessages(
            chat_pb2.StreamMessagesRequest(token=temp_user_token, channel_id=demo_id), timeout=10
        )
        try:
            next(denied_stream)
            raise RuntimeError("Non-member was able to stream channel messages")
        except grpc.RpcError as exc:
            check(exc.code() == grpc.StatusCode.PERMISSION_DENIED,
                  f"Non-member stream returned {exc.code()}, expected PERMISSION_DENIED")
        print("[15] Non-member history, stream, presence, and file access were denied")

        removed = stub.RemoveUser(
            chat_pb2.RemoveUserRequest(token=admin_token, user_id=temp_user_id), timeout=10
        )
        require_success(removed, "Remove temporary demo user")
        removed_user = True
        login_after_removal = stub.Login(
            chat_pb2.LoginRequest(username=username, password=f"demo-{suffix}-password"),
            timeout=10,
        )
        check(not login_after_removal.success, "Removed user was still able to log in")
        print("[16] Admin removed a user; the removed account cannot log in")

        deleted = stub.DeleteChannel(
            chat_pb2.DeleteChannelRequest(token=admin_token, channel_id=demo_id), timeout=10
        )
        require_success(deleted, "Delete demo channel")
        demo_id = None
        try:
            stub.DownloadFile(
                chat_pb2.DownloadFileRequest(token=bob_token, file_id=file_id), timeout=10
            )
            raise RuntimeError("Deleted channel left its file metadata accessible")
        except grpc.RpcError as exc:
            check(exc.code() == grpc.StatusCode.NOT_FOUND,
                  f"Deleted channel file lookup returned {exc.code()}, expected NOT_FOUND")
        print("[17] Demo channel and file metadata cleaned up")

        print("[18] Logging out")
        for username, token in (("admin", admin_token), ("alice", alice_token), ("bob", bob_token)):
            logout = stub.Logout(chat_pb2.LogoutRequest(token=token), timeout=10)
            require_success(logout, f"Logout {username}")
        print("\n  PASS — all Milestone 1 recording-demo checks completed")
        print("=" * 64 + "\n")
    finally:
        if stream is not None:
            stream.cancel()
        if temp_user_id and not removed_user and admin_token:
            try:
                stub.RemoveUser(
                    chat_pb2.RemoveUserRequest(token=admin_token, user_id=temp_user_id),
                    timeout=10,
                )
            except grpc.RpcError:
                pass
        if demo_id and admin_token:
            try:
                stub.DeleteChannel(
                    chat_pb2.DeleteChannelRequest(token=admin_token, channel_id=demo_id),
                    timeout=10,
                )
            except grpc.RpcError:
                pass
        for token in (admin_token, alice_token, bob_token, temp_user_token):
            if token:
                try:
                    stub.Logout(chat_pb2.LogoutRequest(token=token), timeout=10)
                except grpc.RpcError:
                    pass


# ─────────────────────────────────────────────────────────────────────────────
# Interactive REPL
# ─────────────────────────────────────────────────────────────────────────────

HELP = """
Commands:
  login <user> <pass>   Log in
  logout                Log out
  status                Node status
  channels              List all channels
  join <channel_id>     Join a channel
  leave <channel_id>    Leave a channel
  use <channel_id>      Set active channel
  send <text>           Send message to active channel
  history [n]           Last n messages (default 20)
  presence              Online users in active channel
  smartreply            Smart reply suggestions
  summarize             Summarize active channel
  suggest               Context suggestion
  files                 List files in active channel
  help                  This help text
  quit                  Exit
"""


def run_interactive(stub: chat_pb2_grpc.ChatServiceStub):
    token          = None
    username       = None
    current_channel = None

    print(HELP)

    while True:
        try:
            line = input("chat> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not line:
            continue

        parts = line.split(maxsplit=2)
        cmd   = parts[0].lower()

        try:
            if cmd == "quit":
                break

            elif cmd == "help":
                print(HELP)

            elif cmd == "status":
                s = stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest())
                print(f"  node_id={s.node_id}  state={s.state}  term={s.term}")

            elif cmd == "login":
                if len(parts) < 3:
                    print("  Usage: login <user> <pass>")
                    continue
                r = stub.Login(chat_pb2.LoginRequest(username=parts[1], password=parts[2]))
                if r.success:
                    token, username = r.token, parts[1]
                    print(f"  Logged in as {username} (role={r.role})")
                else:
                    print(f"  ERROR: {r.message}")

            elif cmd == "logout":
                if token:
                    stub.Logout(chat_pb2.LogoutRequest(token=token))
                    token = username = None
                    print("  Logged out")

            elif cmd == "channels":
                r = stub.ListChannels(chat_pb2.ListChannelsRequest(token=token))
                for c in r.channels:
                    print(f"  {c.channel_id[:8]}  #{c.name}  ({c.member_count} members)")

            elif cmd == "join":
                r = stub.JoinChannel(
                    chat_pb2.JoinChannelRequest(token=token, channel_id=parts[1])
                )
                print(f"  {r.message}")

            elif cmd == "leave":
                r = stub.LeaveChannel(
                    chat_pb2.LeaveChannelRequest(token=token, channel_id=parts[1])
                )
                print(f"  {r.message}")

            elif cmd == "use":
                current_channel = parts[1]
                print(f"  Active channel: {current_channel}")

            elif cmd == "send":
                if not current_channel:
                    print("  Set active channel first with: use <channel_id>")
                    continue
                content = " ".join(parts[1:]) if len(parts) > 1 else ""
                r = stub.SendMessage(
                    chat_pb2.SendMessageRequest(
                        token=token,
                        channel_id=current_channel,
                        content=content,
                        client_request_id=str(uuid.uuid4()),
                    )
                )
                if r.success:
                    print(f"  Sent (id: ...{r.message.message_id[-8:]})")
                else:
                    print(f"  ERROR: {r.error}")

            elif cmd == "history":
                if not current_channel:
                    print("  Set active channel first")
                    continue
                limit = int(parts[1]) if len(parts) > 1 else 20
                r = stub.GetMessages(
                    chat_pb2.GetMessagesRequest(
                        token=token, channel_id=current_channel, limit=limit
                    )
                )
                for m in r.messages:
                    print(f"  [{m.sender_username}] {m.content}")

            elif cmd == "presence":
                if not current_channel:
                    print("  Set active channel first")
                    continue
                r = stub.GetPresence(
                    chat_pb2.GetPresenceRequest(token=token, channel_id=current_channel)
                )
                for u in r.users:
                    print(f"  {u.username}  {u.status}")

            elif cmd == "smartreply":
                if not current_channel:
                    print("  Set active channel first")
                    continue
                hist = stub.GetMessages(
                    chat_pb2.GetMessagesRequest(
                        token=token, channel_id=current_channel, limit=10
                    )
                )
                msgs_text = [f"{m.sender_username}: {m.content}" for m in hist.messages]
                r = stub.GetSmartReplies(
                    chat_pb2.SmartReplyRequest(
                        token=token,
                        request_id=str(uuid.uuid4()),
                        recent_messages=msgs_text,
                        current_message=msgs_text[-1] if msgs_text else "",
                        channel_name=current_channel,
                    )
                )
                for i, s in enumerate(r.suggestions, 1):
                    print(f"  [{i}] {s}")

            elif cmd == "summarize":
                if not current_channel:
                    print("  Set active channel first")
                    continue
                r = stub.SummarizeChannel(
                    chat_pb2.SummarizeRequest(
                        token=token,
                        request_id=str(uuid.uuid4()),
                        channel_id=current_channel,
                        channel_name=current_channel,
                        limit=30,
                    )
                )
                print(r.summary)

            elif cmd == "suggest":
                if not current_channel:
                    print("  Set active channel first")
                    continue
                r = stub.GetContextSuggestion(
                    chat_pb2.ContextSuggestionRequest(
                        token=token,
                        request_id=str(uuid.uuid4()),
                        channel_id=current_channel,
                        channel_name=current_channel,
                    )
                )
                print(f"  {r.suggestion}")

            elif cmd == "files":
                if not current_channel:
                    print("  Set active channel first")
                    continue
                r = stub.ListFiles(
                    chat_pb2.ListFilesRequest(token=token, channel_id=current_channel)
                )
                for f in r.files:
                    print(f"  {f.file_id[-8:]}  {f.filename}  ({f.size_bytes} bytes)")

            else:
                print(f"  Unknown command: {cmd}. Type 'help'.")

        except grpc.RpcError as e:
            print(f"  gRPC error: {e.code()} — {e.details()}")
        except Exception as e:
            print(f"  Error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Distributed Chat CLI client")
    parser.add_argument("--server", default=SERVER,
                        help="App server address (host:port)")
    parser.add_argument("--demo",   action="store_true",
                        help="Run automated demo then exit")
    args = parser.parse_args()

    print(f"Connecting to {args.server} ...")
    stub = make_stub(args.server)

    # Wait for server to be ready (critical in Docker Compose)
    for attempt in range(30):
        try:
            stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest())
            print("Connected!")
            break
        except grpc.RpcError:
            print(f"  Waiting for server... ({attempt + 1}/30)")
            time.sleep(2)
    else:
        print("Could not connect to server after 60 s. Exiting.")
        sys.exit(1)

    if args.demo:
        run_demo(stub)
    else:
        run_interactive(stub)


if __name__ == "__main__":
    main()

