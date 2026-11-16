#!/usr/bin/env bash
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

mkdir -p logs
log_file="logs/app.log"
: > "$log_file"

if command -v python3 >/dev/null 2>&1; then
  if [ ! -x ".venv/bin/python" ]; then
    python3 -m venv .venv
  fi
  .venv/bin/python -m pip install -q --disable-pip-version-check -r requirements.txt
fi

command -v docker >/dev/null 2>&1 || { echo "Docker CLI was not found." >&2; exit 1; }
docker compose version >/dev/null
docker info >/dev/null

echo "Removing previous containers and volumes..."
docker compose down -v --remove-orphans >/dev/null 2>&1

echo "Building Docker images..."
if docker image inspect distributed-realtime-chat-llm-server:latest >/dev/null 2>&1; then
  build_targets="app-node-1 app-node-2 app-node-3"
else
  build_targets=""
fi
if ! docker compose build $build_targets > logs/build.log 2>&1; then
  tail -n 40 logs/build.log
  exit 1
fi

echo "Starting application services (3-Node Raft Cluster + LLM)..."
docker compose up --detach --no-build llm-server app-node-1 app-node-2 app-node-3 >/dev/null

echo
echo "================================================================="
echo " DISTRIBUTED REAL-TIME CHAT & COLLABORATION PLATFORM (M2 RAFT)"
echo "================================================================="
echo " Web UI Node 1:       http://localhost:8000"
echo " Web UI Node 2:       http://localhost:8001"
echo " Web UI Node 3:       http://localhost:8002"
echo " gRPC App Node 1:     localhost:50051 (Raft: 50071)"
echo " gRPC App Node 2:     localhost:50052 (Raft: 50072)"
echo " gRPC App Node 3:     localhost:50053 (Raft: 50073)"
echo " LLM Server port:     localhost:50060"
echo
echo " 3-Node Raft cluster is live. Open http://localhost:8000 in browser."
echo " Logs: logs/app.log"
echo " To stop: bash scripts/stop.sh"
echo "================================================================="
