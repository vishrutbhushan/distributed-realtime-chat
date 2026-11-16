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
  build_targets="app-node"
else
  build_targets="llm-server app-node"
fi
if ! docker compose build $build_targets > logs/build.log 2>&1; then
  tail -n 40 logs/build.log
  exit 1
fi

echo "Starting application services (Nginx Load Balancer + 5 Raft Replicas via Docker DNS + LLM)..."
docker compose up --detach --no-build >/dev/null

echo
echo "================================================================="
echo " DISTRIBUTED REAL-TIME CHAT & COLLABORATION PLATFORM (M2 RAFT)"
echo "================================================================="
echo " Web UI (Nginx Load Balancer):  http://localhost:8000"
echo " Service Discovery:             Docker DNS (service: app-node, 5 replicas)"
echo " Dynamic Scaling Command:       docker compose up -d --scale app-node=N"
echo " LLM Server port:               localhost:50060"
echo
echo " 5-Node Raft cluster is live behind Nginx (least_conn)."
echo " Open multiple tabs at http://localhost:8000 in your browser!"
echo " Logs: logs/app.log"
echo " To stop: bash scripts/stop.sh"
echo "================================================================="
