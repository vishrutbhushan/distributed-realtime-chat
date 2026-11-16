#!/usr/bin/env python3
"""
Local 3-Node Raft Cluster Runner (No Docker required).

Spins up:
- Node 1: Web http://localhost:8000 | App gRPC :50051 | Raft :50071
- Node 2: Web http://localhost:8001 | App gRPC :50052 | Raft :50072
- Node 3: Web http://localhost:8002 | App gRPC :50053 | Raft :50073
- LLM Mock Service: gRPC :50060

Press Ctrl+C to stop all nodes.
"""

import os
import signal
import subprocess
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_EXE = sys.executable

RAFT_PEERS = "node-1=localhost:50071,node-2=localhost:50072,node-3=localhost:50073"
APP_PEERS = "node-1=localhost:50051,node-2=localhost:50052,node-3=localhost:50053"

procs = []


def cleanup(signum=None, frame=None):
    print("\n[CLUSTER] Shutting down all local cluster nodes...")
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass
    for p in procs:
        try:
            p.wait(timeout=2)
        except Exception:
            p.kill()
    print("[CLUSTER] All nodes stopped cleanly.")
    sys.exit(0)


signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)


def main():
    print("=" * 65)
    print(" STARTING LOCAL 3-NODE RAFT CLUSTER (NO DOCKER NEEDED)")
    print("=" * 65)

    # Terminate any stale cluster processes from previous runs
    subprocess.run(["pkill", "-f", "app/server.py"], check=False)
    subprocess.run(["pkill", "-f", "llm/server.py"], check=False)
    time.sleep(0.5)

    base_env = os.environ.copy()
    venv_site = os.path.join(_ROOT, ".venv", "lib", "python3.13", "site-packages")
    base_env["PYTHONPATH"] = f"{_ROOT}:{_ROOT}/generated:{venv_site}:" + base_env.get("PYTHONPATH", "")

    # 0. Ensure protobuf stubs in generated/ are up to date
    os.makedirs(os.path.join(_ROOT, "generated"), exist_ok=True)
    subprocess.run(
        [
            PYTHON_EXE,
            "-m",
            "grpc_tools.protoc",
            "-I./proto",
            "--python_out=./generated",
            "--grpc_python_out=./generated",
            "./proto/chat.proto",
            "./proto/llm.proto",
            "./proto/raft.proto",
        ],
        cwd=_ROOT,
        env=base_env,
        check=False,
    )

    # 1. Start LLM Server on port 50060
    llm_env = base_env.copy()
    llm_env["LLM_PORT"] = "50060"
    llm_proc = subprocess.Popen([PYTHON_EXE, "llm/server.py"], cwd=_ROOT, env=llm_env)
    procs.append(llm_proc)
    print("Started LLM Service on port 50060")

    time.sleep(0.5)

    # 2. Start the 3 App nodes
    configs = [
        ("node-1", 50051, 8000, 50071, "/tmp/chat_node1.db", "/tmp/files_node1"),
        ("node-2", 50052, 8001, 50072, "/tmp/chat_node2.db", "/tmp/files_node2"),
        ("node-3", 50053, 8002, 50073, "/tmp/chat_node3.db", "/tmp/files_node3"),
    ]

    for nid, port, web_port, raft_port, db_path, file_dir in configs:
        os.makedirs(file_dir, exist_ok=True)
        for ext in ("", "-wal", "-shm"):
            p_file = db_path + ext
            if os.path.exists(p_file):
                try:
                    os.remove(p_file)
                except OSError:
                    pass

        env = base_env.copy()
        env["NODE_ID"] = nid
        env["PORT"] = str(port)
        env["WEB_PORT"] = str(web_port)
        env["RAFT_PORT"] = str(raft_port)
        env["RAFT_PEERS"] = RAFT_PEERS
        env["APP_PEERS"] = APP_PEERS
        env["DB_PATH"] = db_path
        env["FILE_STORAGE_PATH"] = file_dir
        env["LLM_SERVER"] = "localhost:50060"

        p = subprocess.Popen([PYTHON_EXE, "app/server.py"], cwd=_ROOT, env=env)
        procs.append(p)
        print(f"Started {nid} (PID {p.pid}): Web UI -> http://localhost:{web_port} | App gRPC: {port} | Raft: {raft_port}")

    print("\n" + "=" * 65)
    print(" Cluster is live! Open these 3 URLs side-by-side in your browser:")
    print("   Node 1: http://localhost:8000")
    print("   Node 2: http://localhost:8001")
    print("   Node 3: http://localhost:8002")
    print(" Press Ctrl+C in this terminal to shut down.")
    print("=" * 65 + "\n")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
