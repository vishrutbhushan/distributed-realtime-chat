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
import sys
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
    sep = lambda: print("-" * 60)

    print("\n" + "=" * 60)
    print(" DISTRIBUTED CHAT — MILESTONE 1 DEMO")
    print("=" * 60)

    # 1. Node status ──────────────────────────────────────────────────────────
    sep()
    print("[1] Node Status")
    status = stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest())
    print(f"    node_id      = {status.node_id}")
    print(f"    state        = {status.state}")
    print(f"    term         = {status.term}")
    print(f"    commit_index = {status.commit_index}")

    # 2. Login ─────────────────────────────────────────────────────────────────
    sep()
    print("[2] Login as admin")
    resp = stub.Login(chat_pb2.LoginRequest(username="admin", password="admin123"))
    assert resp.success, f"Login failed: {resp.message}"
    admin_token = resp.token
    print(f"    token = ...{admin_token[-8:]}  role = {resp.role}")

    sep()
    print("[3] Login as alice")
    resp = stub.Login(chat_pb2.LoginRequest(username="alice", password="alice123"))
    assert resp.success
    alice_token = resp.token
    print(f"    token = ...{alice_token[-8:]}")

    sep()
    print("[4] Login as bob")
    resp = stub.Login(chat_pb2.LoginRequest(username="bob", password="bob123"))
    assert resp.success
    bob_token = resp.token
    print(f"    token = ...{bob_token[-8:]}")

    # 5. List channels ─────────────────────────────────────────────────────────
    sep()
    print("[5] List Channels")
    channels = stub.ListChannels(chat_pb2.ListChannelsRequest(token=admin_token))
    for ch in channels.channels:
        print(f"    #{ch.name}  (members: {ch.member_count})")

    general_id = next(
        (ch.channel_id for ch in channels.channels if ch.name == "general"), None
    )

    # 6. Admin creates a channel ───────────────────────────────────────────────
    sep()
    print("[6] Admin creates #milestone1-demo")
    resp = stub.CreateChannel(
        chat_pb2.CreateChannelRequest(
            token=admin_token,
            channel_name="milestone1-demo",
            request_id=str(uuid.uuid4()),
        )
    )
    print(f"    success = {resp.success}  msg = {resp.message}")
    demo_id = resp.channel.channel_id if resp.success else None

    # 7–8. Alice and Bob join ──────────────────────────────────────────────────
    if demo_id:
        sep()
        print("[7] Alice joins #milestone1-demo")
        r = stub.JoinChannel(chat_pb2.JoinChannelRequest(token=alice_token, channel_id=demo_id))
        print(f"    {r.message}")

        sep()
        print("[8] Bob joins #milestone1-demo")
        r = stub.JoinChannel(chat_pb2.JoinChannelRequest(token=bob_token, channel_id=demo_id))
        print(f"    {r.message}")

    # 9. Send messages ─────────────────────────────────────────────────────────
    sep()
    print("[9] Sending messages in #milestone1-demo")
    messages_to_send = [
        (alice_token, "alice", "Hey everyone! Are we still meeting at 5?"),
        (bob_token,   "bob",   "Yes! Meeting at 5 PM — don't forget the slides."),
        (alice_token, "alice", "Got it. I'll handle the deployment demo."),
        (bob_token,   "bob",   "The CI pipeline failed again — anyone know why?"),
        (alice_token, "alice", "Let me check the logs."),
    ]

    for token, who, content in messages_to_send:
        resp = stub.SendMessage(
            chat_pb2.SendMessageRequest(
                token=token,
                channel_id=demo_id,
                content=content,
                client_request_id=str(uuid.uuid4()),
            )
        )
        ts = resp.message.timestamp // 1000
        print(
            f"    [{who}] {content}"
            f"  (id: ...{resp.message.message_id[-8:]}  ts: {ts})"
        )
        time.sleep(0.05)

    # 10. Idempotency demo ─────────────────────────────────────────────────────
    sep()
    print("[10] Idempotency test (same request_id sent twice)")
    rid = str(uuid.uuid4())
    req = chat_pb2.SendMessageRequest(
        token=alice_token,
        channel_id=demo_id,
        content="This message is sent twice (idempotency test)",
        client_request_id=rid,
    )
    r1 = stub.SendMessage(req)
    r2 = stub.SendMessage(req)
    print(f"    r1.message_id = ...{r1.message.message_id[-8:]}")
    print(f"    r2.message_id = ...{r2.message.message_id[-8:]}")
    same = r1.message.message_id == r2.message.message_id
    print(f"    Same ID? {same}  ← should be True")

    # 11. Chat history ─────────────────────────────────────────────────────────
    sep()
    print("[11] Get message history from #milestone1-demo")
    history = stub.GetMessages(
        chat_pb2.GetMessagesRequest(token=alice_token, channel_id=demo_id, limit=20)
    )
    for m in history.messages:
        print(f"    [{m.sender_username}] {m.content}")

    # 12. Presence ─────────────────────────────────────────────────────────────
    sep()
    print("[12] Channel presence")
    pres = stub.GetPresence(
        chat_pb2.GetPresenceRequest(token=alice_token, channel_id=demo_id)
    )
    for u in pres.users:
        print(f"    {u.username}  →  {u.status}")

    # 13. File upload & download ───────────────────────────────────────────────
    sep()
    print("[13] Upload a file")
    file_content = b"This is a sample presentation file for the demo.\n" * 3
    upload_resp = stub.UploadFile(
        chat_pb2.UploadFileRequest(
            token=alice_token,
            channel_id=demo_id,
            filename="presentation.pdf",
            data=file_content,
            content_type="application/pdf",
            request_id=str(uuid.uuid4()),
        )
    )
    print(f"    success  = {upload_resp.success}  msg = {upload_resp.message}")
    if upload_resp.success:
        print(f"    file_id  = ...{upload_resp.file.file_id[-8:]}")
        print(f"    size     = {upload_resp.file.size_bytes} bytes")
        dl = stub.DownloadFile(
            chat_pb2.DownloadFileRequest(token=bob_token, file_id=upload_resp.file.file_id)
        )
        print(f"    download = {'OK' if dl.success else 'FAIL'}")
        if dl.success:
            print(f"    content  = {dl.data[:50].decode()}...")

    # 14. LLM: Smart replies ───────────────────────────────────────────────────
    sep()
    print("[14] LLM Smart Replies")
    msgs_text = [f"{m.sender_username}: {m.content}" for m in history.messages]
    last_msg  = msgs_text[-1] if msgs_text else "Are we still on for 5?"
    llm_resp = stub.GetSmartReplies(
        chat_pb2.SmartReplyRequest(
            token=alice_token,
            request_id=str(uuid.uuid4()),
            recent_messages=msgs_text[-10:],
            current_message=last_msg,
            channel_name="milestone1-demo",
        )
    )
    print(f"    success = {llm_resp.success}")
    for i, s in enumerate(llm_resp.suggestions, 1):
        print(f"    [{i}] {s}")

    # 15. LLM: Summarize ───────────────────────────────────────────────────────
    sep()
    print("[15] LLM Conversation Summary")
    sum_resp = stub.SummarizeChannel(
        chat_pb2.SummarizeRequest(
            token=admin_token,
            request_id=str(uuid.uuid4()),
            channel_id=demo_id,
            channel_name="milestone1-demo",
            limit=20,
        )
    )
    print(f"    success = {sum_resp.success}")
    print(f"    summary =\n{sum_resp.summary}")

    # 16. LLM: Context suggestion ──────────────────────────────────────────────
    sep()
    print("[16] LLM Context Suggestion")
    ctx_resp = stub.GetContextSuggestion(
        chat_pb2.ContextSuggestionRequest(
            token=bob_token,
            request_id=str(uuid.uuid4()),
            channel_id=demo_id,
            channel_name="milestone1-demo",
        )
    )
    print(f"    success    = {ctx_resp.success}")
    print(f"    suggestion = {ctx_resp.suggestion}")

    # 17. Admin: add user ──────────────────────────────────────────────────────
    sep()
    print("[17] Admin adds user 'charlie'")
    add_resp = stub.AddUser(
        chat_pb2.AddUserRequest(
            token=admin_token,
            username="charlie",
            password="charlie123",
            role="USER",
            request_id=str(uuid.uuid4()),
        )
    )
    print(f"    success = {add_resp.success}  msg = {add_resp.message}")

    # 18. Logout ───────────────────────────────────────────────────────────────
    sep()
    print("[18] Logout all users")
    for name, token in [("admin", admin_token), ("alice", alice_token), ("bob", bob_token)]:
        r = stub.Logout(chat_pb2.LogoutRequest(token=token))
        print(f"    {name}: {r.message}")

    sep()
    print("\n  ✓ Milestone 1 demo complete!")
    print("=" * 60 + "\n")


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

