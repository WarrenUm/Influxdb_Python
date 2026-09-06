#!/usr/bin/env bash
# One-command bring-up of the InfluxDB 3 Core database on a fresh machine.
#
# Builds the image, starts the server (persisting to a named volume), waits for
# it to be healthy, and creates the database. Re-running is safe/idempotent.
#
# Usage:
#   ./run.sh            # build + up + init
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

# Ensure a .env exists (compose reads it for ports/db name/token).
if [ ! -f .env ]; then
  echo "No .env found; creating one from .env.example (edit it to customize)."
  cp .env.example .env
fi

cmd="${1:-up}"
case "$cmd" in
  up)
    echo ">> Building image and starting InfluxDB 3 Core..."
    $DC up -d --build
    echo ">> Creating database (idempotent)..."
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
