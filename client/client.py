#!/usr/bin/env python3
"""
CLI entrypoint for Distributed Real-time Chat interactive terminal REPL.

Usage:
  python client/client.py --server localhost:50051
"""

import argparse
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_GENERATED = os.path.join(_ROOT, "generated")
sys.path.insert(0, _GENERATED)
sys.path.insert(0, _ROOT)

import grpc
import chat_pb2
import chat_pb2_grpc
from client.repl import run_interactive

SERVER = os.environ.get("APP_SERVER", "localhost:50051")


def make_stub(server: str) -> chat_pb2_grpc.ChatServiceStub:
    channel = grpc.insecure_channel(
        server,
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
    )
    return chat_pb2_grpc.ChatServiceStub(channel)


def main():
    parser = argparse.ArgumentParser(description="Distributed Chat CLI Client")
    parser.add_argument("--server", default=SERVER, help="App server address (host:port)")
    args = parser.parse_args()

    print(f"Connecting to {args.server} ...")
    stub = make_stub(args.server)

    for attempt in range(30):
        try:
            stub.GetNodeStatus(chat_pb2.GetNodeStatusRequest())
            print("Connected to Distributed Chat Server!")
            break
        except grpc.RpcError:
            print(f"  Waiting for server... ({attempt+1}/30)")
            time.sleep(2)
    else:
        print("Could not connect to server. Exiting.")
        sys.exit(1)

    run_interactive(stub)


if __name__ == "__main__":
    main()
