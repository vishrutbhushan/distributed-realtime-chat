"""
Main gRPC application server & Web Gateway for Distributed Real-time Chat.

Each instance is a monolithic node providing:
  - gRPC server (ChatService on port 50051)
  - HTTP Web Gateway (serving the Web UI and REST API on port 8000)
  - Auth / Chat / Presence / Files managers
  - Raft consensus server (RaftService on RAFT_PORT)
  - SQLite local database storage
"""

import logging
import os
import sys
from concurrent import futures
from typing import Dict

import grpc

# ── Path setup (generated stubs compiled at Docker build time) ────────────────
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

# ── Configuration ─────────────────────────────────────────────────────────────
NODE_ID           = os.environ.get("NODE_ID",           "node-1")
PORT              = int(os.environ.get("PORT",          "50051"))
WEB_PORT          = int(os.environ.get("WEB_PORT",      "8000"))
RAFT_PORT         = int(os.environ.get("RAFT_PORT",     "50071"))
RAFT_PEERS        = os.environ.get("RAFT_PEERS",        "")
APP_PEERS         = os.environ.get("APP_PEERS",         "")
DB_PATH           = os.environ.get("DB_PATH",           "/data/chat.db")
FILE_STORAGE_PATH = os.environ.get("FILE_STORAGE_PATH", "/data/files/")
LLM_SERVER        = os.environ.get("LLM_SERVER",        "llm-server:50060")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] NODE={NODE_ID} %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_peers(peers_str: str) -> Dict[str, str]:
    res = {}
    if not peers_str:
        return res
    for p in peers_str.split(","):
        p = p.strip()
        if "=" in p:
            k, v = p.split("=", 1)
            res[k.strip()] = v.strip()
    return res


def serve():
    logger.info(
        "[SERVER] Starting node=%s app_port=%d web_port=%d raft_port=%d",
        NODE_ID,
        PORT,
        WEB_PORT,
        RAFT_PORT,
    )

    db = Database(DB_PATH)

    # 1. Parse Raft and App peers
    raft_peers = parse_peers(RAFT_PEERS)
    if not raft_peers:
        # Default self as single peer
        raft_peers = {NODE_ID: f"localhost:{RAFT_PORT}"}

    app_peers = parse_peers(APP_PEERS)
    if not app_peers and RAFT_PEERS:
        # Auto-infer app peers by replacing raft port 5007x with 5005x
        for k, v in raft_peers.items():
            app_peers[k] = v.replace(str(RAFT_PORT), str(PORT))

    # 2. Setup State Machine and Raft Node
    state_machine = StateMachine(db=db)
    raft_node = RaftNode(
        node_id=NODE_ID,
        db=db,
        peers=raft_peers,
        state_machine=state_machine,
    )

    # 3. Start Raft gRPC Server
    raft_grpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
    )
    raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_node, raft_grpc_server)
    raft_addr = f"[::]:{RAFT_PORT}"
    raft_grpc_server.add_insecure_port(raft_addr)
    raft_grpc_server.start()
    logger.info("[SERVER] Raft gRPC listening on %s", raft_addr)

    # 4. Start Application gRPC Server
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

    # Link servicer managers to state machine
    state_machine.auth = servicer.auth
    state_machine.chat = servicer.chat
    state_machine.files = servicer.files

    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, grpc_server)
    grpc_addr = f"[::]:{PORT}"
    grpc_server.add_insecure_port(grpc_addr)
    grpc_server.start()
    logger.info("[SERVER] App gRPC listening on %s", grpc_addr)

    # 5. Start Web Gateway in background thread
    httpd = run_web_gateway(port=WEB_PORT, grpc_target=f"localhost:{PORT}")

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("[SERVER] Shutting down")
        httpd.shutdown()
        grpc_server.stop(grace=1)
        raft_node.stop()
        raft_grpc_server.stop(grace=1)


if __name__ == "__main__":
    serve()
