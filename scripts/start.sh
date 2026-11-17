#!/usr/bin/env bash
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

mkdir -p logs
log_file="logs/app.log"
: > "$log_file"

if command -v python3 >/dev/null 2>&1; then
  if python3 -c "import ensurepip" >/dev/null 2>&1; then
    if [ ! -x ".venv/bin/python" ]; then
      python3 -m venv .venv 2>/dev/null || true
    fi
    if [ -x ".venv/bin/python" ]; then
      .venv/bin/python -m pip install -q --disable-pip-version-check -r requirements.txt 2>/dev/null || true
    fi
  fi
fi

command -v docker >/dev/null 2>&1 || { echo "Docker CLI was not found." >&2; exit 1; }
docker compose version >/dev/null
docker info >/dev/null

echo "Building Docker images..."
if ! docker compose build app-node llm-server dashboard > logs/build.log 2>&1; then
  tail -n 40 logs/build.log
  exit 1
fi

echo "Starting Nginx, six app replicas, the dashboard, and the LLM service..."
docker compose up --detach --no-build >/dev/null

echo
echo "================================================================="
echo " DISTRIBUTED REAL-TIME CHAT & COLLABORATION PLATFORM (M2 RAFT)"
echo "================================================================="
echo " Web UI (Nginx Load Balancer):  http://localhost:8000"
echo " Raft dashboard:               http://localhost:${DASHBOARD_PORT:-8088}"
echo " Service Discovery:             Docker DNS (service: app-node, 6 replicas)"
echo " Dynamic Scaling Command:       docker compose up -d --scale app-node=N"
echo " LLM Server port:               localhost:50060"
echo
echo " Voting membership is configured separately from replica count."
echo " Open multiple tabs at http://localhost:8000 in your browser!"
echo " Logs: logs/app.log"
echo " To stop: bash scripts/stop.sh"
echo "================================================================="
