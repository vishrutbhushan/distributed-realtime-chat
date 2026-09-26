"""
Interactive CLI REPL for Distributed Real-time Chat.
"""

import uuid
import grpc
import chat_pb2
import chat_pb2_grpc


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
