"""
Main gRPC application server & Web Gateway for Distributed Real-time Chat.

Each instance is a monolithic node providing:
  - gRPC server (ChatService on port 50051)
  - HTTP Web Gateway (serving the Web UI and REST API on port 8000)
  - Auth / Chat / Presence / Files managers
  - Raft consensus node stub (STANDALONE in M1, Raft consensus in M2)
  - SQLite local database storage

Environment variables:
  NODE_ID            Node identifier (default: node-1)
  PORT               gRPC listen port (default: 50051)
  WEB_PORT           HTTP Web UI listen port (default: 8000)
  DB_PATH            SQLite database path (default: /data/chat.db)
  FILE_STORAGE_PATH  Local file storage directory (default: /data/files/)
  LLM_SERVER         LLM server address (default: llm-server:50060)
  PEERS              Comma-separated peer addresses for Raft (M2)
"""

import logging
import os
import sys
from concurrent import futures

import grpc

# ── Path setup (generated stubs compiled at Docker build time) ────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "generated"))
sys.path.insert(0, _ROOT)

import chat_pb2_grpc
from app.grpc_server import ChatServicer
from app.web_gateway import run_web_gateway
from raft.node import RaftNode
from storage.database import Database

# ── Configuration ─────────────────────────────────────────────────────────────
NODE_ID           = os.environ.get("NODE_ID",           "node-1")
PORT              = int(os.environ.get("PORT",          "50051"))
WEB_PORT          = int(os.environ.get("WEB_PORT",      "8000"))
DB_PATH           = os.environ.get("DB_PATH",           "/data/chat.db")
FILE_STORAGE_PATH = os.environ.get("FILE_STORAGE_PATH", "/data/files/")
LLM_SERVER        = os.environ.get("LLM_SERVER",        "llm-server:50060")
PEERS             = [p for p in os.environ.get("PEERS", "").split(",") if p.strip()]

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] NODE={NODE_ID} %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def serve():
    logger.info("[SERVER] Starting node=%s gRPC_port=%d web_port=%d", NODE_ID, PORT, WEB_PORT)

    db = Database(DB_PATH)
    raft = RaftNode(node_id=NODE_ID, db=db, peers=PEERS)

    # 1. Start gRPC Server
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
        raft=raft,
        file_storage_path=FILE_STORAGE_PATH,
        llm_server_addr=LLM_SERVER,
    )
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, grpc_server)
    grpc_addr = f"[::]:{PORT}"
    grpc_server.add_insecure_port(grpc_addr)
    grpc_server.start()
    logger.info("[SERVER] gRPC listening on %s (state=%s)", grpc_addr, raft.state)

    # 2. Start Web Gateway in background thread
    httpd = run_web_gateway(port=WEB_PORT, grpc_target=f"localhost:{PORT}")

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("[SERVER] Shutting down")
        httpd.shutdown()
        grpc_server.stop(grace=3)


if __name__ == "__main__":
    serve()
