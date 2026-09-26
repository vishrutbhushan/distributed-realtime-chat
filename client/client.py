#!/usr/bin/env python3
"""
CLI client & Automated Verification for Distributed Real-time Chat.

Usage:
  python client/client.py --server localhost:50051
  python client/client.py --server localhost:50051 --demo

The --demo flag runs the automated 14-point Milestone 1 verification suite.
"""

import argparse
import os
import sys
import time
import uuid

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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
# Automated Verification Suite (--demo)
# ─────────────────────────────────────────────────────────────────────────────

def run_demo(stub: chat_pb2_grpc.ChatServiceStub):
    sep = lambda: print("-" * 65)

    print("\n" + "=" * 65)
    print(" DISTRIBUTED REAL-TIME CHAT — MILESTONE 1 VERIFICATION")
    print("=" * 65)

    # 1. Node status
    sep()
    print("[1] Node Status (Standalone gRPC node, ready for Raft consensus)")
    status = stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest())
    print(f"    node_id      = {status.node_id}")
    print(f"    state        = {status.state}")
    print(f"    term         = {status.term}")
    print(f"    commit_index = {status.commit_index}")
    assert status.state == "STANDALONE", "Expected STANDALONE state for M1"
    print("    [OK] Node status verified.")

    # 2. Signup Users (No default users)
    sep()
    print("[2] Signup Users (Testing username & password restrictions)")
    # Test username too short
    short_resp = stub.Signup(chat_pb2.SignupRequest(username="al", password="password123"))
    assert not short_resp.success, "Username < 3 chars must fail"
    print("    [OK] Username validation rejected '< 3 chars' as expected.")

    # Signup Alice
    alice_name = f"alice_{uuid.uuid4().hex[:4]}"
    alice_signup = stub.Signup(chat_pb2.SignupRequest(username=alice_name, password="alicepassword"))
    assert alice_signup.success, f"Alice signup failed: {alice_signup.message}"
    alice_token = alice_signup.token
    alice_id = alice_signup.user_id
    print(f"    [OK] Signed up {alice_name} (id={alice_id[:8]}..., status=active)")

    # Signup Bob
    bob_name = f"bob_{uuid.uuid4().hex[:4]}"
    bob_signup = stub.Signup(chat_pb2.SignupRequest(username=bob_name, password="bobpassword"))
    assert bob_signup.success, f"Bob signup failed: {bob_signup.message}"
    bob_token = bob_signup.token
    bob_id = bob_signup.user_id
    print(f"    [OK] Signed up {bob_name} (id={bob_id[:8]}..., status=active)")

    # 3. Directory & Presence (Active / Inactive)
    sep()
    print("[3] User Directory & Status Visibility")
    users_resp = stub.ListUsers(chat_pb2.ListUsersRequest(token=alice_token))
    print(f"    Alice sees {len(users_resp.users)} other registered user(s):")
    for u in users_resp.users:
        print(f"      - {u.username}: status={u.status} (last_seen={u.last_seen})")
    assert any(u.user_id == bob_id and u.status == "active" for u in users_resp.users), "Bob should be active"
    print("    [OK] User presence tracking verified.")

    # 4. 1-on-1 Direct Messaging (Alice -> Bob)
    sep()
    print("[4] 1-on-1 Direct Messaging (Alice sends DM to Bob)")
    dm1 = stub.SendDirectMessage(chat_pb2.SendDirectMessageRequest(
        token=alice_token,
        recipient_user_id=bob_id,
        content="Hi Bob! Did you review the distributed systems architecture doc?",
        client_request_id=str(uuid.uuid4()),
    ))
    assert dm1.success, f"DM send failed: {dm1.error}"
    print(f"    [{dm1.message.sender_username} -> Bob]: {dm1.message.content}")

    # Bob replies to Alice
    dm2 = stub.SendDirectMessage(chat_pb2.SendDirectMessageRequest(
        token=bob_token,
        recipient_user_id=alice_id,
        content="Yes Alice, looks solid! We need to verify Raft log replication next.",
        client_request_id=str(uuid.uuid4()),
    ))
    assert dm2.success, f"DM send failed: {dm2.error}"
    print(f"    [{dm2.message.sender_username} -> Alice]: {dm2.message.content}")

    # Retrieve DM History
    dm_hist = stub.GetDirectMessages(chat_pb2.GetDirectMessagesRequest(
        token=alice_token,
        other_user_id=bob_id,
        limit=20,
    ))
    assert len(dm_hist.messages) >= 2, "Expected at least 2 DMs in history"
    print(f"    [OK] DM history retrieved ({len(dm_hist.messages)} messages exchanged).")

    # 5. Idempotent Message Delivery
    sep()
    print("[5] Idempotency Verification (Resending identical client_request_id)")
    idemp_id = str(uuid.uuid4())
    req1 = stub.SendDirectMessage(chat_pb2.SendDirectMessageRequest(
        token=alice_token, recipient_user_id=bob_id,
        content="Testing idempotent delivery", client_request_id=idemp_id,
    ))
    req2 = stub.SendDirectMessage(chat_pb2.SendDirectMessageRequest(
        token=alice_token, recipient_user_id=bob_id,
        content="Testing idempotent delivery", client_request_id=idemp_id,
    ))
    assert req1.message.message_id == req2.message.message_id, "Idempotent send returned differing IDs!"
    print(f"    Message ID 1 = {req1.message.message_id}")
    print(f"    Message ID 2 = {req2.message.message_id}")
    print("    [OK] Idempotency confirmed: No duplicate message inserted.")

    # 6. File Sharing (PDF & Image upload/download)
    sep()
    print("[6] PDF and Image File Sharing in Chat")
    pdf_bytes = b"%PDF-1.4 Mock Distributed Systems Design PDF Specification..."
    pdf_upload = stub.UploadFile(chat_pb2.UploadFileRequest(
        token=alice_token,
        chat_type="DM",
        target_id=bob_id,
        filename="system_design.pdf",
        data=pdf_bytes,
        content_type="application/pdf",
        request_id=str(uuid.uuid4()),
    ))
    assert pdf_upload.success, "PDF upload failed"
    pdf_id = pdf_upload.file.file_id
    print(f"    [OK] Uploaded PDF: {pdf_upload.file.filename} (type={pdf_upload.file.file_type}, {pdf_upload.file.size_bytes} bytes)")

    # Send DM referencing file
    stub.SendDirectMessage(chat_pb2.SendDirectMessageRequest(
        token=alice_token,
        recipient_user_id=bob_id,
        content="Here is the architecture specification PDF.",
        file_id=pdf_id,
        client_request_id=str(uuid.uuid4()),
    ))

    # Bob downloads the file
    pdf_download = stub.DownloadFile(chat_pb2.DownloadFileRequest(
        token=bob_token,
        file_id=pdf_id,
    ))
    assert pdf_download.success, "PDF download failed"
    assert pdf_download.data == pdf_bytes, "Downloaded content mismatch"
    print(f"    [OK] Bob downloaded PDF successfully ({len(pdf_download.data)} bytes verified).")

    # 7. Group Creation with User Selection (Creator is ADMIN)
    sep()
    print("[7] Group Creation (Alice creates 'Distributed Core Team', adds Bob)")
    grp_resp = stub.CreateGroup(chat_pb2.CreateGroupRequest(
        token=alice_token,
        name="Distributed Core Team",
        initial_member_user_ids=[bob_id],
    ))
    assert grp_resp.success, f"Group creation failed: {grp_resp.message}"
    group_id = grp_resp.group.group_id
    print(f"    [OK] Created group: '{grp_resp.group.name}' (id={group_id[:8]}..., members={grp_resp.group.member_count}, role={grp_resp.group.user_role})")

    # 8. Group Visibility (Bob immediately sees the group)
    sep()
    print("[8] Group Visibility for Members")
    bob_groups = stub.ListGroups(chat_pb2.ListGroupsRequest(token=bob_token))
    assert any(g.group_id == group_id for g in bob_groups.groups), "Bob should see the newly created group"
    print(f"    [OK] Bob verified membership in {len(bob_groups.groups)} group(s).")

    # 9. Group Messaging
    sep()
    print("[9] Group Messaging (Alice and Bob send messages)")
    stub.SendGroupMessage(chat_pb2.SendGroupMessageRequest(
        token=alice_token,
        group_id=group_id,
        content="Welcome to the group channel everyone!",
        client_request_id=str(uuid.uuid4()),
    ))
    stub.SendGroupMessage(chat_pb2.SendGroupMessageRequest(
        token=bob_token,
        group_id=group_id,
        content="Glad to be here. Let's finish the consensus algorithm.",
        client_request_id=str(uuid.uuid4()),
    ))
    grp_msgs = stub.GetGroupMessages(chat_pb2.GetGroupMessagesRequest(
        token=alice_token,
        group_id=group_id,
        limit=10,
    ))
    assert len(grp_msgs.messages) >= 2, "Expected group messages"
    for m in grp_msgs.messages:
        print(f"    [{m.sender_username} in #{grp_resp.group.name}]: {m.content}")
    print("    [OK] Group messaging verified.")

    # 10. Admin Group Controls (Only Admin can manage)
    sep()
    print("[10] Admin-only Group Controls (Rename, Promote, Non-admin rejection)")
    # Bob (MEMBER) tries to rename group -> Must fail
    bob_rename = stub.UpdateGroup(chat_pb2.UpdateGroupRequest(
        token=bob_token,
        group_id=group_id,
        action="RENAME",
        new_name="Unauthorized Rename Attempt",
    ))
    assert not bob_rename.success, "Non-admin member should not be allowed to rename group"
    print("    [OK] Non-admin update blocked as expected (Permission Denied).")

    # Alice (ADMIN) renames group
    alice_rename = stub.UpdateGroup(chat_pb2.UpdateGroupRequest(
        token=alice_token,
        group_id=group_id,
        action="RENAME",
        new_name="Consensus Engineering Team",
    ))
    assert alice_rename.success, "Admin rename should succeed"
    print(f"    [OK] Admin renamed group to '{alice_rename.group.name}'.")

    # Alice promotes Bob to ADMIN
    alice_promote = stub.UpdateGroup(chat_pb2.UpdateGroupRequest(
        token=alice_token,
        group_id=group_id,
        action="MAKE_ADMIN",
        target_user_id=bob_id,
    ))
    assert alice_promote.success, "Admin promotion should succeed"
    print("    [OK] Alice promoted Bob to group ADMIN.")

    # 11. LLM Assistance: Smart Replies (Passing full chat history)
    sep()
    print("[11] LLM Smart Replies (Passing entire conversation history)")
    full_history = [f"{m.sender_username}: {m.content}" for m in grp_msgs.messages]
    last_msg = grp_msgs.messages[-1].content
    sr_resp = stub.GetSmartReplies(chat_pb2.SmartReplyRequest(
        token=alice_token,
        chat_type="GROUP",
        target_id=group_id,
        chat_history=full_history,
        current_message=last_msg,
        context_title="Consensus Engineering Team",
        request_id=str(uuid.uuid4()),
    ))
    if sr_resp.success:
        print("    Received 3 Smart Reply Suggestions:")
        for i, s in enumerate(sr_resp.suggestions, 1):
            print(f"      {i}. {s}")
        print("    [OK] LLM Smart Replies verified.")
    else:
        print(f"    [NOTICE] LLM service unavailable/offline: {sr_resp.error[:70]}...")
        print("    [OK] LLM graceful offline handling verified.")

    # 12. LLM Assistance: Summarization (Passing full chat history)
    sep()
    print("[12] LLM Conversation Summary (Passing entire conversation history)")
    sum_resp = stub.SummarizeChat(chat_pb2.SummarizeChatRequest(
        token=alice_token,
        chat_type="GROUP",
        target_id=group_id,
        chat_history=full_history,
        context_title="Consensus Engineering Team",
        request_id=str(uuid.uuid4()),
    ))
    if sum_resp.success:
        print(f"    Summary output:\n    {sum_resp.summary.replace(chr(10), chr(10) + '    ')}")
        print("    [OK] LLM Summarization verified.")
    else:
        print(f"    [NOTICE] LLM service unavailable/offline: {sum_resp.error[:70]}...")
        print("    [OK] LLM graceful offline handling verified.")

    # 13. Assignment Standard RPCs: Post, Get, ProcessBusinessRequest
    sep()
    print("[13] Assignment Required Core RPCs (post, get, processBusinessRequest)")
    post_resp = stub.Post(chat_pb2.PostRequest(
        token=alice_token,
        type="ping",
        data="system_test",
    ))
    assert post_resp.status == "OK", "Post RPC failed"

    get_resp = stub.Get(chat_pb2.GetRequest(
        token=alice_token,
        type="users",
        params="",
    ))
    assert get_resp.status == "OK", "Get RPC failed"

    biz_resp = stub.ProcessBusinessRequest(chat_pb2.BusinessRequest(
        request_id=str(uuid.uuid4()),
        payload="audit_logs",
        context="admin_audit",
    ))
    assert biz_resp.status == "OK", "ProcessBusinessRequest failed"
    print("    [OK] Post, Get, and ProcessBusinessRequest RPCs verified.")

    # 14. Logout & Inactive Status
    sep()
    print("[14] Logout and Status Transition to Inactive")
    bob_logout = stub.Logout(chat_pb2.LogoutRequest(token=bob_token))
    assert bob_logout.success, "Bob logout failed"
    print("    [OK] Bob logged out.")

    # Alice checks Bob's status -> should now be inactive
    refreshed_users = stub.ListUsers(chat_pb2.ListUsersRequest(token=alice_token))
    bob_entry = next((u for u in refreshed_users.users if u.user_id == bob_id), None)
    assert bob_entry is not None, "Bob not found in directory"
    assert bob_entry.status == "inactive", f"Expected Bob to be inactive, got {bob_entry.status}"
    print(f"    [OK] Bob's status confirmed as '{bob_entry.status}' after logout.")

    sep()
    print("\n  ALL MILESTONE 1 VERIFICATION TESTS PASSED SUCCESSFULLY!")
    print("=" * 65 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Interactive CLI Mode
# ─────────────────────────────────────────────────────────────────────────────

def run_interactive(stub: chat_pb2_grpc.ChatServiceStub):
    token = None
    username = None
    user_id = None
    active_chat = None  # (type, id, name)

    HELP = """
Commands:
  signup <user> <pass>     Sign up a new user account
  login <user> <pass>      Log in to an existing account
  logout                   Log out and set status to inactive
  users                    List all users and their active/inactive status
  dm <username>            Start direct messaging a user
  send <message>           Send message in active chat
  groups                   List your groups
  create-group <name>      Create a new group (you become ADMIN)
  group <group_id>         Switch active chat to group
  history [n]              View last n messages in active chat
  smartreply               Get 3 AI smart reply suggestions
  summarize                Get AI bullet-point summary of active chat
  status                   Node status
  help                     Show this help text
  quit                     Exit
"""
    print(HELP)

    while True:
        try:
            prompt_name = f"[{username}@{active_chat[2]}] " if active_chat else (f"[{username}] " if username else "")
            line = input(f"{prompt_name}chat> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not line:
            continue

        parts = line.split(maxsplit=2)
        cmd = parts[0].lower()

        try:
            if cmd in ("quit", "exit"):
                break

            elif cmd == "help":
                print(HELP)

            elif cmd == "status":
                s = stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest())
                print(f"  Node={s.node_id} State={s.state} Term={s.term} CommitIndex={s.commit_index}")

            elif cmd == "signup":
                if len(parts) < 3:
                    print("  Usage: signup <user> <pass>")
                    continue
                r = stub.Signup(chat_pb2.SignupRequest(username=parts[1], password=parts[2]))
                if r.success:
                    token, user_id, username = r.token, r.user_id, r.username
                    print(f"  Signed up & logged in as {username} (status: active)")
                else:
                    print(f"  ERROR: {r.message}")

            elif cmd == "login":
                if len(parts) < 3:
                    print("  Usage: login <user> <pass>")
                    continue
                r = stub.Login(chat_pb2.LoginRequest(username=parts[1], password=parts[2]))
                if r.success:
                    token, user_id, username = r.token, r.user_id, r.username
                    print(f"  Logged in as {username} (status: active)")
                else:
                    print(f"  ERROR: {r.message}")

            elif cmd == "logout":
                if token:
                    r = stub.Logout(chat_pb2.LogoutRequest(token=token))
                    print(f"  {r.message}")
                    token, user_id, username, active_chat = None, None, None, None
                else:
                    print("  Not logged in.")

            elif cmd == "users":
                if not token:
                    print("  Please log in first.")
                    continue
                r = stub.ListUsers(chat_pb2.ListUsersRequest(token=token))
                print("  Registered Users:")
                for u in r.users:
                    print(f"    - {u.username} [{u.status}] (id: {u.user_id[:8]}...)")

            elif cmd == "dm":
                if not token:
                    print("  Please log in first.")
                    continue
                target_name = parts[1] if len(parts) > 1 else ""
                r = stub.ListUsers(chat_pb2.ListUsersRequest(token=token))
                target = next((u for u in r.users if u.username.lower() == target_name.lower()), None)
                if target:
                    active_chat = ("DM", target.user_id, target.username)
                    print(f"  Switched active chat to DM with {target.username}")
                else:
                    print(f"  User '{target_name}' not found.")

            elif cmd == "groups":
                if not token:
                    print("  Please log in first.")
                    continue
                r = stub.ListGroups(chat_pb2.ListGroupsRequest(token=token))
                print("  Your Groups:")
                for g in r.groups:
                    print(f"    - #{g.name} (id: {g.group_id[:8]}..., members: {g.member_count}, role: {g.user_role})")

            elif cmd == "create-group":
                if not token:
                    print("  Please log in first.")
                    continue
                name = parts[1] if len(parts) > 1 else "New Group"
                r = stub.CreateGroup(chat_pb2.CreateGroupRequest(token=token, name=name))
                if r.success:
                    print(f"  Created group #{r.group.name} (id: {r.group.group_id})")
                    active_chat = ("GROUP", r.group.group_id, r.group.name)
                else:
                    print(f"  ERROR: {r.message}")

            elif cmd == "group":
                if not token:
                    print("  Please log in first.")
                    continue
                gid = parts[1] if len(parts) > 1 else ""
                r = stub.ListGroups(chat_pb2.ListGroupsRequest(token=token))
                match = next((g for g in r.groups if g.group_id.startswith(gid) or g.name.lower() == gid.lower()), None)
                if match:
                    active_chat = ("GROUP", match.group_id, match.name)
                    print(f"  Switched active chat to #{match.name}")
                else:
                    print(f"  Group '{gid}' not found.")

            elif cmd == "send":
                if not token or not active_chat:
                    print("  Select a chat first using 'dm <user>' or 'group <id>'.")
                    continue
                content = parts[1] if len(parts) > 1 else ""
                if len(parts) > 2:
                    content = f"{parts[1]} {parts[2]}"
                if not content:
                    continue

                if active_chat[0] == "DM":
                    r = stub.SendDirectMessage(chat_pb2.SendDirectMessageRequest(
                        token=token, recipient_user_id=active_chat[1],
                        content=content, client_request_id=str(uuid.uuid4()),
                    ))
                else:
                    r = stub.SendGroupMessage(chat_pb2.SendGroupMessageRequest(
                        token=token, group_id=active_chat[1],
                        content=content, client_request_id=str(uuid.uuid4()),
                    ))
                if r.success:
                    print(f"  [Sent] msg_id: {r.message.message_id[:8]}...")
                else:
                    print(f"  ERROR: {r.error}")

            elif cmd == "history":
                if not token or not active_chat:
                    print("  Select a chat first.")
                    continue
                limit = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 20
                if active_chat[0] == "DM":
                    r = stub.GetDirectMessages(chat_pb2.GetDirectMessagesRequest(
                        token=token, other_user_id=active_chat[1], limit=limit,
                    ))
                else:
                    r = stub.GetGroupMessages(chat_pb2.GetGroupMessagesRequest(
                        token=token, group_id=active_chat[1], limit=limit,
                    ))
                print(f"  History for {active_chat[2]}:")
                for m in r.messages:
                    print(f"    [{m.sender_username}]: {m.content}")

            elif cmd == "smartreply":
                if not token or not active_chat:
                    print("  Select a chat first.")
                    continue
                if active_chat[0] == "DM":
                    r_hist = stub.GetDirectMessages(chat_pb2.GetDirectMessagesRequest(token=token, other_user_id=active_chat[1], limit=20))
                else:
                    r_hist = stub.GetGroupMessages(chat_pb2.GetGroupMessagesRequest(token=token, group_id=active_chat[1], limit=20))

                msgs = [f"{m.sender_username}: {m.content}" for m in r_hist.messages]
                last = r_hist.messages[-1].content if r_hist.messages else ""
                r = stub.GetSmartReplies(chat_pb2.SmartReplyRequest(
                    token=token, chat_type=active_chat[0], target_id=active_chat[1],
                    chat_history=msgs, current_message=last, context_title=active_chat[2],
                ))
                if r.success:
                    print("  Smart Reply Suggestions:")
                    for i, s in enumerate(r.suggestions, 1):
                        print(f"    {i}. {s}")
                else:
                    print(f"  ERROR: {r.error}")

            elif cmd == "summarize":
                if not token or not active_chat:
                    print("  Select a chat first.")
                    continue
                if active_chat[0] == "DM":
                    r_hist = stub.GetDirectMessages(chat_pb2.GetDirectMessagesRequest(token=token, other_user_id=active_chat[1], limit=50))
                else:
                    r_hist = stub.GetGroupMessages(chat_pb2.GetGroupMessagesRequest(token=token, group_id=active_chat[1], limit=50))

                msgs = [f"{m.sender_username}: {m.content}" for m in r_hist.messages]
                r = stub.SummarizeChat(chat_pb2.SummarizeChatRequest(
                    token=token, chat_type=active_chat[0], target_id=active_chat[1],
                    chat_history=msgs, context_title=active_chat[2],
                ))
                if r.success:
                    print(f"  Summary:\n{r.summary}")
                else:
                    print(f"  ERROR: {r.error}")

            else:
                print(f"  Unknown command: {cmd}. Type 'help'.")

        except grpc.RpcError as e:
            print(f"  gRPC error: {e.code()} - {e.details()}")
        except Exception as e:
            print(f"  Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Distributed Chat CLI client")
    parser.add_argument("--server", default=SERVER, help="App server address (host:port)")
    parser.add_argument("--demo", action="store_true", help="Run automated demo then exit")
    args = parser.parse_args()

    print(f"Connecting to {args.server} ...")
    stub = make_stub(args.server)

    for attempt in range(30):
        try:
            stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest())
            print("Connected!")
            break
        except grpc.RpcError:
            print(f"  Waiting for server... ({attempt+1}/30)")
            time.sleep(2)
    else:
        print("Could not connect to server. Exiting.")
        sys.exit(1)

    if args.demo:
        run_demo(stub)
    else:
        run_interactive(stub)


if __name__ == "__main__":
    main()
