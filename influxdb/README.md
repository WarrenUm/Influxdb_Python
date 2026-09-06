# Database container (`influxdb/`)

Everything needed to host the OSRS GE price database — **InfluxDB 3 Core** — on
a fresh machine. Clone the repo, install Docker, and run one command.

## Contents

| File | Purpose |
|------|---------|
| `Dockerfile` | Builds the InfluxDB 3 Core image with a container healthcheck and baked-in default `serve` args. |
| `docker-compose.yml` | Runs the server with a persistent named volume, plus a one-shot `influxdb3-init` service that creates the database. |
| `.env.example` | Configurable settings (host port, database name, node id, token). Copy to `.env`. |
| `init-db.sh` | Waits for the server to be healthy, then idempotently creates the database. |
| `run.sh` | Convenience wrapper: build + up + init (and `down`/`destroy`/`logs`/`status`). |

## Prerequisites

- Docker Engine with the Compose plugin (`docker compose`). No Python or other
  local tooling is required just to host the database.

## Quick start (fresh machine)

```bash
cd influxdb
./run.sh
```

That builds the image, starts InfluxDB 3 Core, waits for it to be healthy, and
creates the `GEItemPrices` database. When it finishes, the database is reachable
at `http://localhost:8181`.

Verify:

```bash
curl -s http://localhost:8181/health   # -> OK
```

### Manual equivalent (if you prefer not to use run.sh)

```bash
cp .env.example .env                      # edit if desired
docker compose up -d --build              # start the server (persistent volume)
docker compose run --rm influxdb3-init    # create the database (idempotent)
```

### Deploying changes to the remote host (`192.168.1.85`)

This folder is deployed to the `.85` host at `/opt/stacks/influxdb/`. To push edits from
the dev workstation, don't copy by hand — use the repo's deploy script, which rsyncs this
folder up and force-recreates the container:

```bash
scripts/deploy.sh influxdb            # from the repo root
scripts/deploy.sh influxdb --dry-run  # preview first
```

See the top-level `README.md` → "Updating the deployment from the workstation" and
`.kiro/steering/development-workflow.md` for prerequisites and `--delete` behavior.

## Managing the database

```bash
./run.sh status     # container status + health
./run.sh logs       # follow server logs
./run.sh down       # stop the server; DATA IS PRESERVED in the volume
./run.sh destroy    # stop AND delete the data volume (irreversible; prompts first)
```

`docker compose down` (or `./run.sh down`) keeps your data — it lives in the
named Docker volume `influxdb3_data`. Only `./run.sh destroy` /
`docker compose down -v` deletes it.

## Configuration

Copy `.env.example` to `.env` and adjust:

| Variable | Default | Meaning |
|----------|---------|---------|
| `INFLUXDB3_HOST_PORT` | `8181` | Host port the API/Flight endpoint is published on. |
| `INFLUXDB3_CONTAINER_PORT` | `8181` | Port inside the container. |
| `INFLUXDB3_NODE_ID` | `ge-node` | Logical node id for this single-node instance. |
| `INFLUXDB3_DATABASE_NAME` | `GEItemPrices` | Database created by the init step. |
| `INFLUXDB3_AUTH_TOKEN` | `local-dev-token` | Token used by the init CLI calls. |

## Where the data lives

- **Endpoint:** `http://localhost:${INFLUXDB3_HOST_PORT}` (HTTP API + Flight/gRPC on the same port).
- **Container name:** `ge-influxdb3`.
- **Persistence:** Docker named volume `influxdb3_data`, mounted at
  `/var/lib/influxdb3` inside the container. On the host, Docker stores it under
  `/var/lib/docker/volumes/influxdb3_data/_data` (Linux default). Data survives
  restarts and container recreation; it is removed only by
  `docker compose down -v` / `./run.sh destroy`.

## Auth

For local/dev use the server runs with `--without-auth`, so any non-empty token
is accepted by clients (matching the pipeline's `INFLUXDB3_AUTH_TOKEN`). For a
networked/production deployment, provision a real token and change the compose
`command:` to an authenticated mode; do not expose `--without-auth` beyond a
trusted host.

## Connecting the pipeline

Point the `ge_pipeline` app at this database via the repo-root `.env`:

```
INFLUXDB3_HOST_URL=http://localhost:8181
INFLUXDB3_AUTH_TOKEN=local-dev-token
INFLUXDB3_DATABASE_NAME=GEItemPrices
```

Then `ge-pipeline setup` (also idempotent) / `ge-pipeline ingest` from the repo
root. See the top-level `README.md` for ingestion and the full-history backfill
script.
