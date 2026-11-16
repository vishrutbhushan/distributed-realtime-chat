"""
Main gRPC application server & Web Gateway for Distributed Real-time Chat.

Supports:
1. Docker DNS Service Discovery (CLUSTER_DNS="app-node")
2. Dynamic Port-Range Discovery (NODE_INDEX / RAFT_PORT_RANGE)
3. Static Peer Lists (RAFT_PEERS / APP_PEERS)
"""

from concurrent import futures
import logging
import os
import re
import signal
import socket
import sys
import threading
import time
from typing import Dict

import grpc

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


def _resolve_container_identity() -> tuple[str, str]:
    """Resolve (node_id, container_ip) automatically via Docker Embedded DNS."""
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except Exception:
        ip = "127.0.0.1"
    node_id = os.environ.get("NODE_ID", "")
    if not node_id:
        idx = int(os.environ.get("NODE_INDEX", "0"))
        if idx > 0:
            node_id = f"node-{idx}"
        elif os.environ.get("CLUSTER_DNS"):
            prefix = os.environ["CLUSTER_NODE_PREFIX"]
            for _attempt in range(10):
                try:
                    names = socket.gethostbyaddr(ip)[:2]
                    for name in (names[0], *names[1]):
                        match = re.fullmatch(re.escape(prefix) + r"([1-9][0-9]*)(?:\..*)?", name)
                        if match:
                            return f"node-{match[1]}", ip
                except OSError:
                    pass
                for i in range(1, 65):
                    try:
                        if socket.gethostbyname(f"{prefix}{i}") == ip:
                            return f"node-{i}", ip
                    except OSError:
                        pass
                time.sleep(0.05)
            raise RuntimeError(f"Cannot map {ip} to a Compose replica with prefix {prefix!r}")
        else:
            node_id = "node-1"
    return node_id, ip


_IDX              = int(os.environ.get("NODE_INDEX", "0"))
NODE_ID, _MY_IP   = _resolve_container_identity()
CLUSTER_DNS       = os.environ.get("CLUSTER_DNS",       "")
PORT              = int(os.environ.get("PORT",          str(50050 + _IDX) if _IDX > 0 else "50051"))
STREAM_PORT       = int(os.environ.get("STREAM_PORT",   str(PORT + 100)))
LLM_RPC_PORT      = int(os.environ.get("LLM_RPC_PORT",  str(PORT + 200)))
WEB_PORT          = int(os.environ.get("WEB_PORT",      str(8000 + _IDX - 1) if _IDX > 0 else "8000"))
RAFT_PORT         = int(os.environ.get("RAFT_PORT",     str(50070 + _IDX) if _IDX > 0 else "0"))
RAFT_PORT_RANGE   = os.environ.get("RAFT_PORT_RANGE",   "")
RAFT_HOST         = os.environ.get("RAFT_HOST",         _MY_IP if CLUSTER_DNS else "localhost")
RAFT_PEERS_ENV    = os.environ.get("RAFT_PEERS",        "")
APP_PEERS_ENV     = os.environ.get("APP_PEERS",         "")
DB_PATH           = os.environ.get("DB_PATH",           f"/data/chat_{NODE_ID}.db" if CLUSTER_DNS else (f"/tmp/chat_{NODE_ID}.db" if _IDX > 0 else "/data/chat.db"))
FILE_STORAGE_PATH = os.environ.get("FILE_STORAGE_PATH", f"/data/files_{NODE_ID}/" if CLUSTER_DNS else (f"/tmp/files_{NODE_ID}" if _IDX > 0 else "/data/files/"))
LLM_SERVER        = os.environ.get("LLM_SERVER",        "llm-server:50060")

_LOG_FILE = os.environ.get("LOG_FILE")
logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] NODE={NODE_ID} %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(_LOG_FILE, encoding="utf-8")] if _LOG_FILE else None,
)
logger = logging.getLogger(__name__)

_GRPC_OPTS = [
    ("grpc.max_send_message_length", 64 * 1024 * 1024),
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 10_000),
    ("grpc.keepalive_timeout_ms", 5_000),
]


def _parse_peers(peers_str: str) -> Dict[str, str]:
    return {
        k.strip(): v.strip()
        for part in (peers_str or "").split(",")
        if "=" in part
        for k, v in [part.split("=", 1)]
    }


def serve():
    logger.info("[SERVER] Starting node=%s ip=%s gRPC=%d web=%d raft=%d", NODE_ID, _MY_IP, PORT, WEB_PORT, RAFT_PORT)
    db = Database(DB_PATH)
    raft_peers = _parse_peers(RAFT_PEERS_ENV)
    app_peers = _parse_peers(APP_PEERS_ENV)

    raft_node = None
    raft_server = None
    if RAFT_PORT > 0 or raft_peers or CLUSTER_DNS:
        if CLUSTER_DNS:
            prefix = os.environ["CLUSTER_NODE_PREFIX"]
            voter_count = int(os.environ.get("RAFT_VOTER_COUNT", "6"))
            if voter_count < 3:
                raise ValueError("RAFT_VOTER_COUNT must be at least 3")
            raft_peers = {f"node-{i}": f"{prefix}{i}:{RAFT_PORT}" for i in range(1, voter_count + 1)}
            app_peers = {f"node-{i}": f"{prefix}{i}:{PORT}" for i in range(1, voter_count + 1)}
        voter_ids = frozenset(raft_peers or {NODE_ID})
        self_raft = raft_peers.get(NODE_ID, f"{RAFT_HOST}:{RAFT_PORT}")
        self_app = app_peers.get(NODE_ID, f"{RAFT_HOST}:{PORT}")
        raft_peers.setdefault(NODE_ID, self_raft)
        app_peers.setdefault(NODE_ID, self_app)

        raft_node = RaftNode(
            node_id=NODE_ID,
            db=db,
            peers=raft_peers,
            state_machine=StateMachine(db),
            app_peers=app_peers,
            self_raft_addr=self_raft,
            self_app_addr=self_app,
            voter_ids=voter_ids,
            # Give incoming heartbeats time to reach a restarted voter before it campaigns.
            startup_grace=2.0 if CLUSTER_DNS else 0.0,
        )

    servicer = ChatServicer(
        db=db,
        node_id=NODE_ID,
        file_storage_path=FILE_STORAGE_PATH,
        llm_server_addr=LLM_SERVER,
        raft_node=raft_node,
        app_peers=app_peers,
    )

    if raft_node and RAFT_PORT > 0:
        raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=16), options=_GRPC_OPTS)
        raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_node, raft_server)
        raft_server.add_insecure_port(f"[::]:{RAFT_PORT}")
        raft_server.start()
        logger.info("[SERVER] RaftService listening on [::]:%d", RAFT_PORT)

        if CLUSTER_DNS:
            threading.Thread(
                target=raft_node.discover_docker_dns,
                args=(CLUSTER_DNS, RAFT_PORT),
                daemon=True,
            ).start()
        elif RAFT_PORT_RANGE and "-" in RAFT_PORT_RANGE:
            p_start, p_end = (int(x) for x in RAFT_PORT_RANGE.split("-", 1))
            threading.Thread(
                target=raft_node.discover_port_range,
                args=(RAFT_HOST, p_start, p_end),
                daemon=True,
            ).start()

    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=16), options=_GRPC_OPTS)
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, grpc_server)
    grpc_server.add_insecure_port(f"[::]:{PORT}")
    grpc_server.start()

    stream_server = grpc.server(futures.ThreadPoolExecutor(max_workers=32), options=_GRPC_OPTS, maximum_concurrent_rpcs=32)
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, stream_server)
    stream_server.add_insecure_port(f"127.0.0.1:{STREAM_PORT}")
    stream_server.start()

    llm_rpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=_GRPC_OPTS[:2], maximum_concurrent_rpcs=4)
    chat_pb2_grpc.add_ChatServiceServicer_to_server(servicer, llm_rpc_server)
    llm_rpc_server.add_insecure_port(f"127.0.0.1:{LLM_RPC_PORT}")
    llm_rpc_server.start()

    from app.web_gateway import WebGatewayHandler
    WebGatewayHandler.node_monitor_fn = raft_node.monitor if raft_node else None
    httpd = run_web_gateway(
        port=WEB_PORT,
        grpc_target=f"localhost:{PORT}",
        stream_grpc_target=f"127.0.0.1:{STREAM_PORT}",
        llm_grpc_target=f"127.0.0.1:{LLM_RPC_PORT}",
    )

    stop_event = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())

    try:
        stop_event.wait()
    finally:
        logger.info("[SERVER] Shutting down node=%s", NODE_ID)
        httpd.shutdown()
        httpd.server_close()
        for channel in getattr(httpd, "grpc_channels", ()):
            channel.close()
        stream_server.stop(grace=0.5)
        llm_rpc_server.stop(grace=0.5)
        grpc_server.stop(grace=0.5)
        if raft_node:
            raft_node.stop()
        if raft_server:
            raft_server.stop(grace=0.5)
        servicer.presence.stop()
        db.close()


if __name__ == "__main__":
    serve()
