#!/usr/bin/env bash
# One-command bring-up of the InfluxDB 3 Enterprise database on a fresh machine.
#
# Pulls the official Enterprise image, starts the server (persisting to a named
# volume), waits for it to be healthy, and creates the database. Re-running is
# safe/idempotent.
#
# NOTE: on the very first start Enterprise emails INFLUXDB3_LICENSE_EMAIL a
# verification link and waits ("Waiting for verification...") until you click it,
# so the first `up` may block on `influxdb3-init` until the license is verified.
# Set INFLUXDB3_LICENSE_EMAIL in .env before running.
#
# Usage:
#   ./run.sh            # up + init
#   ./run.sh down       # stop (keeps data volume)
#   ./run.sh destroy    # stop AND delete the data volume (irreversible)
#   ./run.sh logs       # follow server logs
#   ./run.sh status     # show container + health
set -euo pipefail

cd "$(dirname "$0")"

# Prefer the modern `docker compose` plugin; fall back to legacy docker-compose.
if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  echo "ERROR: Docker Compose not found. Install Docker + the compose plugin." >&2
  exit 1
fi

# Ensure a .env exists (compose reads it for ports/db name/token/license email).
if [ ! -f .env ]; then
  echo "No .env found; creating one from .env.example."
  echo "IMPORTANT: edit .env and set INFLUXDB3_LICENSE_EMAIL before continuing."
  cp .env.example .env
fi

cmd="${1:-up}"
case "$cmd" in
  up)
    echo ">> Pulling image and starting InfluxDB 3 Enterprise..."
    $DC up -d
    echo ">> Creating database (idempotent; may wait on email verification on first run)..."
    $DC run --rm influxdb3-init
    echo ">> Ready."
    $DC ps
    ;;
  down)
    $DC down
    echo ">> Stopped. Data volume 'influxdb3_data' preserved."
    ;;
  destroy)
    read -r -p "This DELETES all database data (volume influxdb3_data). Continue? [y/N] " ans
    case "$ans" in
      [yY]|[yY][eE][sS]) $DC down -v; echo ">> Removed containers and data volume." ;;
      *) echo "Aborted." ;;
    esac
    ;;
  logs)
    $DC logs -f influxdb3
    ;;
  status)
    $DC ps
    ;;
  *)
    echo "Usage: $0 {up|down|destroy|logs|status}" >&2
    exit 2
    ;;
esac
