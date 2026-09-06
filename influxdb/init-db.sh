#!/bin/sh
# Idempotently create the InfluxDB 3 database once the server is reachable.
#
# Intended to run inside the influxdb3 image (it has the `influxdb3` CLI and
# `curl`). The docker-compose `influxdb3-init` service invokes this after the
# server reports healthy. Safe to run repeatedly: an already-existing database
# is treated as success.
#
# Env:
#   INFLUXDB3_HOST_URL       e.g. http://influxdb3:8181 (in-network) or
#                            http://localhost:8181 (host)
#   INFLUXDB3_DATABASE_NAME  database to create (default GEItemPrices)
#   INFLUXDB3_AUTH_TOKEN     token for CLI calls (any non-empty value when the
#                            server runs --without-auth)
set -eu

HOST_URL="${INFLUXDB3_HOST_URL:-http://localhost:8181}"
DB_NAME="${INFLUXDB3_DATABASE_NAME:-GEItemPrices}"
TOKEN="${INFLUXDB3_AUTH_TOKEN:-local-dev-token}"

echo "[init] waiting for InfluxDB 3 at ${HOST_URL} to become healthy..."
i=0
until curl -fsS "${HOST_URL}/health" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "[init] ERROR: server did not become healthy in time at ${HOST_URL}" >&2
    exit 1
  fi
  sleep 2
done
echo "[init] server healthy."

echo "[init] ensuring database '${DB_NAME}' exists..."
# `influxdb3 create database` errors if it already exists; capture output and
# treat an "already exists" error as success so this stays idempotent.
set +e
OUT="$(influxdb3 create database "${DB_NAME}" \
        --host "${HOST_URL}" \
        --token "${TOKEN}" 2>&1)"
RC=$?
set -e

if [ "$RC" -eq 0 ]; then
  echo "[init] database '${DB_NAME}' created."
elif printf '%s' "$OUT" | grep -qiE 'already exists|409|conflict'; then
  echo "[init] database '${DB_NAME}' already exists; nothing to do."
else
  echo "[init] ERROR creating database '${DB_NAME}':" >&2
  printf '%s\n' "$OUT" >&2
  exit "$RC"
fi

echo "[init] done."
