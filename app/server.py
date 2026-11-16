"""
Main gRPC application server & Web Gateway for Distributed Real-time Chat.

Each instance is a monolithic node providing:
  - gRPC server (ChatService on port 50051+)
  - Raft peer consensus server (RaftService on port 50071+)
  - HTTP Web Gateway (serving the Web UI and REST API on port 8000+)
  - Auth / Chat / Presence / Files managers
  - SQLite local database storage

Environment variables:
  NODE_ID            Node identifier (default: node-1)
  PORT               gRPC listen port (default: 50051)
  WEB_PORT           HTTP Web UI listen port (default: 8000)
  RAFT_PORT          Raft peer gRPC port (default: 0 = standalone)
  RAFT_PEERS         Comma-separated node_id=host:raft_port
  APP_PEERS          Comma-separated node_id=host:app_port
  DB_PATH            SQLite database path (default: /data/chat.db)
  FILE_STORAGE_PATH  Local file storage directory (default: /data/files/)
  LLM_SERVER         LLM server address (default: llm-server:50060)
"""

import logging
import os
import sys
from concurrent import futures
from typing import Dict

import grpc

# Generated protobuf modules
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "generated"))
sys.path.insert(0, _ROOT)

import chat_pb2_grpc
import raft_pb2_grpc
from app.grpc_server import ChatServicer
from app.web_gateway import run_web_gateway
from raft.node import RaftNode
from raft.state_machine import StateMachine
from storage.database import Database

# Runtime configuration
NODE_ID           = os.environ.get("NODE_ID",           "node-1")
PORT              = int(os.environ.get("PORT",          "50051"))
STREAM_PORT       = int(os.environ.get("STREAM_PORT",   str(PORT + 100)))
LLM_RPC_PORT      = int(os.environ.get("LLM_RPC_PORT",  str(PORT + 200)))
WEB_PORT          = int(os.environ.get("WEB_PORT",      "8000"))
RAFT_PORT         = int(os.environ.get("RAFT_PORT",     "0"))
RAFT_PEERS_ENV    = os.environ.get("RAFT_PEERS",        "")
APP_PEERS_ENV     = os.environ.get("APP_PEERS",         "")
DB_PATH           = os.environ.get("DB_PATH",           "/data/chat.db")
FILE_STORAGE_PATH = os.environ.get("FILE_STORAGE_PATH", "/data/files/")
LLM_SERVER        = os.environ.get("LLM_SERVER",        "llm-server:50060")

# Logging configuration
_LOG_FILE = os.environ.get("LOG_FILE")
logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] NODE={NODE_ID} %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(_LOG_FILE, encoding="utf-8")] if _LOG_FILE else None,
)
logger = logging.getLogger(__name__)


def _parse_peers(peers_str: str) -> Dict[str, str]:
    peers: Dict[str, str] = {}
    if not peers_str:
        return peers
    for part in peers_str.split(","):
        part = part.strip()
        if "=" in part:
            nid, addr = part.split("=", 1)
            peers[nid.strip()] = addr.strip()
    return peers


def serve():
    logger.info(
        "[SERVER] Starting node=%s gRPC_port=%d web_port=%d raft_port=%d",
        NODE_ID, PORT, WEB_PORT, RAFT_PORT,
    )

    db = Database(DB_PATH)
    raft_peers = _parse_peers(RAFT_PEERS_ENV)
    app_peers = _parse_peers(APP_PEERS_ENV)

    # 1. Start Raft Consensus Node & Server if configured
    raft_node = None
    raft_server = None
    if RAFT_PORT > 0 or raft_peers:
        if not raft_peers:
            raft_peers = {NODE_ID: f"localhost:{RAFT_PORT}"}
        sm = StateMachine(db)
        raft_node = RaftNode(
            node_id=NODE_ID,
            db=db,
            peers=raft_peers,
            state_machine=sm,
        )
        if RAFT_PORT > 0:
            raft_server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=16),
                options=[
                    ("grpc.max_send_message_length", 64 * 1024 * 1024),
                    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ],
            )
            raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_node, raft_server)
            raft_addr = f"[::]:{RAFT_PORT}"
            raft_server.add_insecure_port(raft_addr)
            raft_server.start()
            logger.info("[SERVER] RaftService listening on %s (peers=%s)", raft_addr, list(raft_peers.keys()))

    # 2. Start gRPC App Server
    grpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ("grpc.keepalive_time_ms", 10_000),
            ("grpc.keepalive_timeout_ms", 5_000),
        ],
    )
    servicer = ChatServicer(
        db=db,
        node_id=NODE_ID,
        file_storage_path=FILE_STORAGE_PATH,
        llm_server_addr=LLM_SERVER,
        raft_node=raft_node,
        app_peers=app_peers,
    )
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, grpc_server)
    grpc_addr = f"[::]:{PORT}"
    grpc_server.add_insecure_port(grpc_addr)
    grpc_server.start()
    logger.info(
        "[SERVER] gRPC listening on %s (mode=%s)",
        grpc_addr,
        "RAFT_CLUSTER" if raft_node else "STANDALONE",
    )

    # Browser event streams can occupy one worker for their full lifetime.
    # Keep them on a private listener and a bounded pool so they cannot starve
    # the public unary RPC pool on PORT.
    stream_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=32),
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ("grpc.keepalive_time_ms", 10_000),
            ("grpc.keepalive_timeout_ms", 5_000),
        ],
        maximum_concurrent_rpcs=32,
    )
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, stream_server)
    stream_addr = f"127.0.0.1:{STREAM_PORT}"
    stream_server.add_insecure_port(stream_addr)
    stream_server.start()
    logger.info("[SERVER] Internal event-stream gRPC listening on %s (max=32)", stream_addr)

    # Keep model-backed RPCs off the public worker pool.
    llm_rpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
        maximum_concurrent_rpcs=4,
    )
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, llm_rpc_server)
    llm_rpc_addr = f"127.0.0.1:{LLM_RPC_PORT}"
    llm_rpc_server.add_insecure_port(llm_rpc_addr)
    llm_rpc_server.start()
    logger.info("[SERVER] Internal LLM RPC listening on %s (max=4)", llm_rpc_addr)

    # 3. Start Web Gateway in background thread
    httpd = run_web_gateway(
        port=WEB_PORT,
        grpc_target=f"localhost:{PORT}",
        stream_grpc_target=f"127.0.0.1:{STREAM_PORT}",
        llm_grpc_target=f"127.0.0.1:{LLM_RPC_PORT}",
    )

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("[SERVER] Shutting down")
    finally:
        httpd.shutdown()
        httpd.server_close()
        for channel in getattr(httpd, "grpc_channels", ()):
            channel.close()
        stream_server.stop(grace=3)
        llm_rpc_server.stop(grace=3)
        grpc_server.stop(grace=3)
        if raft_node:
            raft_node.stop()
        if raft_server:
            raft_server.stop(grace=2)
        servicer.presence.stop()
        db.close()


if __name__ == "__main__":
    serve()
