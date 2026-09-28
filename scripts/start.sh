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
if ! docker compose build > logs/build.log 2>&1; then
  tail -n 40 logs/build.log
  exit 1
fi

echo "Starting application services..."
docker compose up --detach --no-build llm-server app-node-1 >/dev/null

echo
echo "Application started: http://localhost:8000"
echo "Logs: logs/app.log"
echo "To stop: bash scripts/stop.sh"
