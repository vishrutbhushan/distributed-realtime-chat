#!/usr/bin/env python3
"""Verify chat still works and AI returns an error when the LLM server is down."""

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
    suffix = uuid.uuid4().hex[:8]
    admin_token = alice_token = channel_id = None
    try:
        status = stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest(), timeout=10)
        print(f"App node reachable: {status.node_id} ({status.state})")

        admin = stub.Login(chat_pb2.LoginRequest(username="admin", password="admin123"), timeout=10)
        alice = stub.Login(chat_pb2.LoginRequest(username="alice", password="alice123"), timeout=10)
        if not admin.success or not alice.success:
            raise RuntimeError("Could not log in with seeded demo accounts")
        admin_token, alice_token = admin.token, alice.token

        created = stub.CreateChannel(
            chat_pb2.CreateChannelRequest(token=admin_token, channel_name=f"offline-check-{suffix}"),
            timeout=10,
        )
        if not created.success:
            raise RuntimeError(f"Could not create test channel: {created.message}")
        channel_id = created.channel.channel_id
        joined = stub.JoinChannel(
            chat_pb2.JoinChannelRequest(token=alice_token, channel_id=channel_id), timeout=10
        )
        if not joined.success:
            raise RuntimeError(f"Could not join test channel: {joined.message}")

        sent = stub.SendMessage(
            chat_pb2.SendMessageRequest(
                token=alice_token,
                channel_id=channel_id,
                content="Ordinary chat remains available without the AI service.",
                client_request_id=str(uuid.uuid4()),
            ),
            timeout=10,
        )
        if not sent.success:
            raise RuntimeError(f"Chat send failed while LLM was unavailable: {sent.error}")
        history = stub.GetMessages(
            chat_pb2.GetMessagesRequest(token=alice_token, channel_id=channel_id), timeout=10
        )
        if not history.messages:
            raise RuntimeError("Chat history failed while the LLM service was unavailable")
        print("Chat send and history succeeded while LLM service was stopped")

        ai = stub.SummarizeChannel(
            chat_pb2.SummarizeRequest(
                token=alice_token,
                request_id=str(uuid.uuid4()),
                channel_id=channel_id,
                channel_name=f"offline-check-{suffix}",
                limit=10,
            ),
            timeout=95,
        )
        if ai.success or "unavailable" not in ai.error.lower():
            raise RuntimeError("Expected the unavailable LLM to return a clear error")
        print(f"AI returned a bounded error: {ai.error}")
    finally:
        if channel_id and admin_token:
            try:
                stub.DeleteChannel(
                    chat_pb2.DeleteChannelRequest(token=admin_token, channel_id=channel_id),
                    timeout=10,
                )
            except grpc.RpcError:
                pass
        for token in (alice_token, admin_token):
            if token:
                try:
                    stub.Logout(chat_pb2.LogoutRequest(token=token), timeout=10)
                except grpc.RpcError:
                    pass
        channel.close()


if __name__ == "__main__":
    main()
