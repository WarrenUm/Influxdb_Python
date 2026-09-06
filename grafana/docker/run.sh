#!/usr/bin/env bash
# One-command bring-up of Grafana for the OSRS GE dashboards.
#
# Starts Grafana (auto-provisioning the InfluxDB 3 datasource, dashboards, and
# alert rule) and waits for it to be healthy. Grafana state persists in a named
# volume, so re-running is safe. Assumes the InfluxDB 3 container is already
# running (see ../../influxdb/).
#
# Usage:
#   ./run.sh            # up + wait for health
#   ./run.sh down       # stop (keeps Grafana data volume)
#   ./run.sh destroy    # stop AND delete the data volume (irreversible)
#   ./run.sh logs       # follow Grafana logs
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

# Ensure a .env exists (compose reads it for ports/host/token/login).
if [ ! -f .env ]; then
  echo "No .env found; creating one from .env.example (edit it to customize)."
  cp .env.example .env
fi

cmd="${1:-up}"
case "$cmd" in
  up)
    echo ">> Starting Grafana..."
    $DC up -d
    echo ">> Waiting for Grafana to become healthy..."
    # Poll the compose health status for up to ~90s.
    i=0
    until [ "$($DC ps --format '{{.Health}}' grafana 2>/dev/null)" = "healthy" ]; do
      i=$((i + 1))
      if [ "$i" -ge 30 ]; then
        echo ">> WARNING: Grafana not healthy yet; check './run.sh logs'." >&2
        break
      fi
      sleep 3
    done
    echo ">> Ready. Open http://localhost:${GRAFANA_HOST_PORT:-3001} (folder: OSRS GE)."
    $DC ps
    ;;
  down)
    $DC down
    echo ">> Stopped. Data volume 'grafana_ge_data' preserved."
    ;;
  destroy)
    read -r -p "This DELETES all Grafana data (volume grafana_ge_data). Continue? [y/N] " ans
    case "$ans" in
      [yY]|[yY][eE][sS]) $DC down -v; echo ">> Removed containers and data volume." ;;
      *) echo "Aborted." ;;
    esac
    ;;
  logs)
    $DC logs -f grafana
    ;;
  status)
    $DC ps
    ;;
  *)
    echo "Usage: $0 {up|down|destroy|logs|status}" >&2
    exit 2
    ;;
esac
