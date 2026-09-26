#!/usr/bin/env python3
"""
Verify chat operations remain functional and AI endpoints return bounded errors
when the LLM service is unavailable/stopped.
"""

import argparse
import os
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "generated"))
sys.path.insert(0, ROOT)

import grpc
import chat_pb2
import chat_pb2_grpc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default=os.environ.get("APP_SERVER", "localhost:50051"))
    args = parser.parse_args()

    channel = grpc.insecure_channel(args.server)
    stub = chat_pb2_grpc.ChatServiceStub(channel)
    suffix = uuid.uuid4().hex[:6]

    try:
        # 1. Node status check
        status = stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest(), timeout=10)
        print(f"[OK] App node reachable: {status.node_id} (state={status.state})")

        # 2. Signup two test users
        u1_name = f"smoke_a_{suffix}"
        u2_name = f"smoke_b_{suffix}"
        resp1 = stub.Signup(chat_pb2.SignupRequest(username=u1_name, password="password123"), timeout=10)
        resp2 = stub.Signup(chat_pb2.SignupRequest(username=u2_name, password="password123"), timeout=10)
        if not resp1.success or not resp2.success:
            raise RuntimeError(f"Signup failed: u1={resp1.message}, u2={resp2.message}")
        t1, id1 = resp1.token, resp1.user_id
        t2, id2 = resp2.token, resp2.user_id
        print(f"[OK] Users signed up: {u1_name}, {u2_name}")

        # 3. Send direct message while LLM is stopped
        msg_resp = stub.SendDirectMessage(
            chat_pb2.SendDirectMessageRequest(
                token=t1,
                recipient_user_id=id2,
                content="Ordinary chat remains 100% available without the AI service.",
                client_request_id=str(uuid.uuid4()),
            ),
            timeout=10,
        )
        if not msg_resp.success:
            raise RuntimeError(f"Chat send failed while LLM was offline: {msg_resp.error}")
        print("[OK] Sent direct message successfully")

        # 4. Fetch history
        hist = stub.GetDirectMessages(
            chat_pb2.GetDirectMessagesRequest(token=t2, other_user_id=id1, limit=10),
            timeout=10,
        )
        if not hist.messages:
            raise RuntimeError("Failed to retrieve chat history")
        print(f"[OK] Retrieved message history: '{hist.messages[0].content}'")

        # 5. Verify LLM endpoint returns a bounded graceful error
        chat_lines = [f"{m.sender_username}: {m.content}" for m in hist.messages]
        ai = stub.SummarizeChat(
            chat_pb2.SummarizeChatRequest(
                token=t1,
                chat_history=chat_lines,
                context_title="SmokeTest",
                request_id=str(uuid.uuid4()),
            ),
            timeout=10,
        )
        if ai.success:
            raise RuntimeError("Expected the offline LLM to return success=False")
        print(f"[OK] Offline AI returned bounded error as expected: '{ai.error}'")

        # 6. Verify Smart Replies also fails gracefully
        sr = stub.GetSmartReplies(
            chat_pb2.SmartReplyRequest(
                token=t1,
                chat_history=chat_lines,
                current_message="Any thoughts?",
                context_title="SmokeTest",
                request_id=str(uuid.uuid4()),
            ),
            timeout=10,
        )
        if sr.success:
            raise RuntimeError("Expected the offline LLM Smart Reply to return success=False")
        print(f"[OK] Offline Smart Reply returned bounded error: '{sr.error}'")

        print("\nALL LLM DECOUPLING SMOKE CHECKS PASSED!")
    finally:
        channel.close()


if __name__ == "__main__":
    main()
