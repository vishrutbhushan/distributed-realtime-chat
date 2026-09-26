#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "$script_dir/.." && pwd)"
cd "$project_root"

printf 'Stopping application cluster and cleaning up resources...\n'
docker compose down -v --remove-orphans
printf '\n=================================================================\n'
printf ' Cluster stopped successfully. No lingering containers remain.\n'
printf '=================================================================\n'
exit 0
