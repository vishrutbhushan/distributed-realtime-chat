#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "$script_dir/.." && pwd)"
cd "$project_root"

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
  sleep 5
done

printf 'Both services are healthy.\n'
printf 'Web UI available at: http://localhost:8000\n'
printf 'Starting the self-checking demo suite...\n'
docker compose up --no-build --abort-on-container-exit --exit-code-from client-runner
exit_code=$?

if [ "$exit_code" -eq 0 ]; then
  printf 'Demo succeeded (exit code 0).\n'
else
  printf 'Demo failed (exit code %s). Review the service output above.\n' "$exit_code" >&2
fi

exit "$exit_code"
