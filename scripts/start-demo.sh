#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  printf 'Usage: bash scripts/start-demo.sh [--rebuild]\n'
  printf '  --rebuild  Rebuild all project images before starting the demo.\n'
}

rebuild=0
case "${1:-}" in
  "") ;;
  --rebuild)
    if [ "$#" -ne 1 ]; then
      usage >&2
      exit 2
    fi
    rebuild=1
    ;;
  --help|-h)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

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

printf 'Checking the Compose images...\n'
image_list="$(docker compose config --images)" || {
  printf 'Could not read the Docker Compose configuration.\n' >&2
  exit 1
}
if [ -z "$image_list" ]; then
  printf 'Docker Compose did not report any service images to prepare.\n' >&2
  exit 1
fi

needs_build="$rebuild"
if [ "$needs_build" -eq 0 ]; then
  for image in $image_list; do
    if ! docker image inspect "$image" >/dev/null 2>&1; then
      needs_build=1
      break
    fi
  done
fi

if [ "$needs_build" -eq 1 ]; then
  printf 'Building project images. If the local model image is missing, this step needs internet access and downloads the pinned model.\n'
  docker compose build || {
    printf 'Docker Compose could not build the project images.\n' >&2
    exit 1
  }
else
  printf 'Project images are already available locally; skipping the build.\n'
fi

for image in $image_list; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    printf "Required image '%s' is still unavailable after image preparation.\n" "$image" >&2
    exit 1
  fi
done

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
    starting|created|restarting)
      return 2
      ;;
    *)
      printf "The '%s' service reported '%s'. Recent logs follow:\n" "$service" "$health" >&2
      docker compose logs --no-color --tail=100 "$service" || true
      return 1
      ;;
  esac
}

deadline=$((SECONDS + 600))
while :; do
  pending=0
  for service in llm-server app-node-1; do
    if wait_for_service "$service"; then
      :
    else
      wait_status=$?
      if [ "$wait_status" -eq 1 ]; then
        printf "Could not confirm that '%s' became healthy.\n" "$service" >&2
        exit 1
      fi
      pending=1
    fi
  done

  if [ "$pending" -eq 0 ]; then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    printf 'Timed out waiting for service health. Recent logs follow:\n' >&2
    docker compose logs --no-color --tail=100 llm-server app-node-1 || true
    printf 'The services did not become healthy within 10 minutes.\n' >&2
    exit 1
  fi
  sleep 5
done

printf 'Both services are healthy.\n'
printf 'Web UI available at: http://localhost:8000\n'
printf 'Starting the self-checking demo suite...\n'
if docker compose up --no-build --abort-on-container-exit --exit-code-from client-runner; then
  printf 'Demo succeeded (exit code 0).\n'
else
  demo_status=$?
  printf 'Demo failed (exit code %s). Review the service output above.\n' "$demo_status" >&2
  exit "$demo_status"
fi
