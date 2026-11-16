#!/usr/bin/env python3
"""
Dynamic Multi-Node Raft Cluster Runner (Port-Range Auto-Discovery).

Port Formula for Node i (1..10):
  - Web UI Port : 8000 + (i - 1)   (e.g. 8000, 8001, 8002, 8003, ...)
  - App gRPC    : 50050 + i        (e.g. 50051, 50052, 50053, 50054, ...)
  - Raft Port   : 50070 + i        (e.g. 50071, 50072, 50073, 50074, ...)

Usage:
  1. Start a cluster with N nodes (default 6):
       python3 scripts/run_local_cluster.py --nodes 6
  2. Dynamically add a new node (e.g. node-7 on http://localhost:8006) to a running cluster:
       python3 scripts/run_local_cluster.py --add-node 7
"""

import argparse
import os
import signal
import subprocess
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_EXE = sys.executable
RAFT_PORT_RANGE = "50071-50080"

procs = []


def cleanup(signum=None, frame=None):
    print("\n[CLUSTER] Shutting down cluster nodes...")
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
    sys.exit(0)


signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)


def _base_env() -> dict:
    env = os.environ.copy()
    venv_site = os.path.join(_ROOT, ".venv", "lib", "python3.13", "site-packages")
    env["PYTHONPATH"] = f"{_ROOT}:{_ROOT}/generated:{venv_site}:" + env.get("PYTHONPATH", "")
    return env


def _spawn_node(idx: int, base_env: dict, voter_count: int):
    nid = f"node-{idx}"
    app_port = 50050 + idx
    web_port = 8000 + idx - 1
    raft_port = 50070 + idx
    db_path = f"/tmp/chat_{nid}.db"
    file_dir = f"/tmp/files_{nid}"

    os.makedirs(file_dir, exist_ok=True)
    env = base_env.copy()
    env["NODE_INDEX"] = str(idx)
    env["RAFT_PORT_RANGE"] = RAFT_PORT_RANGE
    env["LLM_SERVER"] = "localhost:50060"
    env["RAFT_PEERS"] = ",".join(f"node-{j}=localhost:{50070+j}" for j in range(1, voter_count + 1))
    env["APP_PEERS"] = ",".join(f"node-{j}=localhost:{50050+j}" for j in range(1, voter_count + 1))

    p = subprocess.Popen([PYTHON_EXE, "app/server.py"], cwd=_ROOT, env=env)
    procs.append(p)
    print(f"Started {nid} (PID {p.pid}): Web UI -> http://localhost:{web_port} | App gRPC: {app_port} | Raft: {raft_port}")
    return p


def main():
    parser = argparse.ArgumentParser(description="Dynamic Raft Cluster Runner")
    parser.add_argument("--nodes", type=int, default=6, help="Initial number of Raft nodes to launch (default: 6)")
    parser.add_argument("--voters", type=int, default=6, help="Configured voting nodes (default: 6); use the same value when joining")
    parser.add_argument("--add-node", type=int, default=0, help="Dynamically add node-K (e.g. --add-node 7) to a running cluster")
    args = parser.parse_args()
    if not 3 <= args.voters <= 10:
        parser.error("--voters must be between 3 and 10")
    if args.add_node and not 1 <= args.add_node <= 10:
        parser.error("--add-node must be between 1 and 10")
    if not args.add_node and not args.voters <= args.nodes <= 10:
        parser.error("--nodes must be at least --voters and at most 10")

    base_env = _base_env()
    os.makedirs(os.path.join(_ROOT, "generated"), exist_ok=True)
    subprocess.run(
        [PYTHON_EXE, "-m", "grpc_tools.protoc", "-I./proto", "--python_out=./generated", "--grpc_python_out=./generated",
         "./proto/chat.proto", "./proto/llm.proto", "./proto/raft.proto"],
        cwd=_ROOT, env=base_env, check=True,
    )

    if args.add_node > 0:
        print(f"=== Dynamically joining node-{args.add_node} into port range {RAFT_PORT_RANGE} ===")
        _spawn_node(args.add_node, base_env, args.voters)
        while True:
            time.sleep(1)

    print("=" * 68)
    print(f" STARTING {args.nodes}-NODE RAFT CLUSTER (PORT RANGE {RAFT_PORT_RANGE})")
    print("=" * 68)

    llm_env = base_env.copy()
    llm_env["LLM_PORT"] = "50060"
    procs.append(subprocess.Popen([PYTHON_EXE, "llm/server.py"], cwd=_ROOT, env=llm_env))
    print("Started LLM Service on port 50060")
    time.sleep(0.3)

    for i in range(1, args.nodes + 1):
        _spawn_node(i, base_env, args.voters)

    print("\n" + "=" * 68)
    for i in range(1, args.nodes + 1):
        print(f"   Node {i}: http://localhost:{8000 + i - 1}")
    if args.nodes < 10:
        print(f"   Add a replica: python3 scripts/run_local_cluster.py --add-node {args.nodes + 1} --voters {args.voters}")
    print("=" * 68 + "\n")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
