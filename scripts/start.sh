#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "$script_dir/.." && pwd)"
cd "$project_root"

# 1. Virtual environment setup and dependency verification
if command -v python3 >/dev/null 2>&1; then
  if [ ! -d ".venv" ]; then
    printf 'Creating virtual environment in .venv...\n'
    python3 -m venv .venv 2>/dev/null || true
  fi
  if [ -f ".venv/bin/python" ]; then
    printf 'Verifying dependencies in virtual environment...\n'
    .venv/bin/python -m pip install -q --disable-pip-version-check -r requirements.txt || true
    mkdir -p generated
    .venv/bin/python -m grpc_tools.protoc -I ./proto --python_out=./generated --grpc_python_out=./generated ./proto/chat.proto ./proto/llm.proto || true
  fi
fi

# 2. Docker verification
if ! command -v docker >/dev/null 2>&1; then
  printf 'Docker CLI was not found. Install Docker Desktop or Docker Engine, then retry.\n' >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  printf 'Docker Compose v2 is unavailable. Update Docker Desktop or install the Compose plugin.\n' >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  printf 'The Docker engine is not ready. Start Docker Desktop (or the Docker service), wait for it to finish starting, then retry.\n' >&2
  exit 1
fi

cleanup() {
  printf '\nShutting down services and cleaning up volumes...\n'
  docker compose down -v --remove-orphans || true
}
trap cleanup EXIT INT TERM

printf 'Cleaning up previous containers and volumes (clean slate)...\n'
docker compose down -v --remove-orphans || {
  printf 'Failed to clean up existing Docker Compose containers and volumes.\n' >&2
  exit 1
}

printf 'Building project images (static tests run during build)...\n'
docker compose build || {
  printf 'Docker Compose could not build the project images or static tests failed.\n' >&2
  exit 1
}

printf 'Starting the app and local model services...\n'
docker compose up --no-build --detach llm-server app-node-1 || {
  printf 'Docker Compose could not start the app and model services.\n' >&2
  exit 1
}

wait_for_service() {
  service="$1"
  container_id="$(docker compose ps --quiet "$service")" || return 1
  if [ -z "$container_id" ]; then
    return 2
  fi

  health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id")" || return 1
  case "$health" in
    healthy|running)
      return 0
      ;;
    unhealthy|exited|dead)
      printf "The '%s' service reported '%s'. Recent logs follow:\n" "$service" "$health" >&2
      docker compose logs --no-color --tail=100 "$service" >&2 || true
      return 3
      ;;
    *)
      return 2
      ;;
  esac
}

services=(llm-server app-node-1)
deadline=$((SECONDS + 600))
while true; do
  all_healthy=1
  for service in "${services[@]}"; do
    if ! wait_for_service "$service"; then
      status=$?
      if [ "$status" -eq 3 ]; then
        exit 1
      fi
      all_healthy=0
    fi
  done

  if [ "$all_healthy" -eq 1 ]; then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    printf 'Timed out waiting for service health. Recent logs follow:\n' >&2
    docker compose logs --no-color --tail=100 llm-server app-node-1 >&2 || true
    exit 1
  fi
  sleep 3
done

printf '\n'
printf '=================================================================\n'
printf ' DISTRIBUTED REAL-TIME CHAT & COLLABORATION PLATFORM\n'
printf '=================================================================\n'
printf ' Web UI available at: http://localhost:8000\n'
printf ' gRPC Server port:    localhost:50051\n'
printf ' LLM Server port:     localhost:50060\n\n'
printf ' Cluster is live and running. Open http://localhost:8000 in browser.\n'
printf ' Press Ctrl+C to shut down.\n'
printf '=================================================================\n\n'

# Stream service logs live until user presses Ctrl+C
docker compose logs -f --tail=20 app-node-1 llm-server
