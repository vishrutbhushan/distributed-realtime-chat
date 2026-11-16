#!/bin/sh
set -eu

# Use one stable DNS name per replica so the hash ring has distinct members.
# Nginx resolves slots as they appear and withdraws them when they disappear.
slots=${APP_REPLICA_SLOTS:-16}
case "$slots" in
  ""|*[!0-9]*) echo "APP_REPLICA_SLOTS must be an integer" >&2; exit 1 ;;
esac
if [ "$slots" -lt 3 ] || [ "$slots" -gt 64 ]; then
  echo "APP_REPLICA_SLOTS must be between 3 and 64" >&2
  exit 1
fi

i=1
while [ "$i" -le "$slots" ]; do
  printf 'server %s%s:8000 resolve max_fails=1 fail_timeout=2s;\n' "$CLUSTER_NODE_PREFIX" "$i"
  i=$((i + 1))
done > /etc/nginx/raft-replicas.conf

exec nginx -g 'daemon off;'
