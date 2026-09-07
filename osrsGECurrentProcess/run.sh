#!/usr/bin/env bash
# One-command control of the osrsGECurrentProcess container: the 5-minute
# catch-up ingestion daemon that keeps InfluxDB 3 up to date with the latest
# OSRS GE prices.
#
# It builds the image (bundling the ge_pipeline package from the repo root) and
# runs `python -m ge_pipeline.scheduler`, which triggers catch-up ingestion every
# 5 minutes. The container is stateless (state lives in InfluxDB), so it can be
# recreated freely.
#
# Usage:
#   ./run.sh            # build + start (up)
#   ./run.sh up         # same as above
#   ./run.sh build      # (re)build the image only
#   ./run.sh down       # stop and remove the container
#   ./run.sh restart    # recreate the container (picks up .env / image changes)
#   ./run.sh logs       # follow the scheduler logs
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

# Ensure a .env exists (compose reads it for the InfluxDB target + token).
if [ ! -f .env ]; then
  echo "No .env found; creating one from .env.example."
  echo "IMPORTANT: edit .env and set INFLUXDB3_HOST_URL / INFLUXDB3_AUTH_TOKEN before continuing."
  cp .env.example .env
fi

cmd="${1:-up}"
case "$cmd" in
  up)
    echo ">> Building image and starting osrsGECurrentProcess..."
    $DC up -d --build
    echo ">> Ready. Catch-up ingestion runs every 5 minutes."
    $DC ps
    ;;
  build)
    echo ">> Building image..."
    $DC build
    ;;
  down)
    $DC down
    echo ">> Stopped and removed. (Container is stateless; nothing to preserve.)"
    ;;
  restart)
    echo ">> Recreating container..."
    $DC up -d --build --force-recreate
    $DC ps
    ;;
  logs)
    $DC logs -f osrs-ge-current
    ;;
  status)
    $DC ps
    ;;
  *)
    echo "Usage: $0 {up|build|down|restart|logs|status}" >&2
    exit 2
    ;;
esac
