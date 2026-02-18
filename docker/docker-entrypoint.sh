#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" = "0" ]; then
  default_uid=10001
  default_gid=10001

  mkdir -p /data

  run_uid="${RS_RUN_UID:-}"
  run_gid="${RS_RUN_GID:-}"

  if [ -z "$run_uid" ]; then
    run_uid="$(stat -c '%u' /data 2>/dev/null || true)"
  fi
  if [ -z "$run_gid" ]; then
    run_gid="$(stat -c '%g' /data 2>/dev/null || true)"
  fi

  run_uid="${run_uid:-$default_uid}"
  run_gid="${run_gid:-$default_gid}"

  if [ "$run_uid" = "0" ] && [ "$run_gid" = "0" ]; then
    exec "$@"
  fi

  exec setpriv --reuid "$run_uid" --regid "$run_gid" --clear-groups "$@"
fi

exec "$@"
