---
inclusion: manual
---

# Development Workflow

## Setup

```bash
# Start InfluxDB v2
influxd

# Install the package with test extras into a virtualenv
pip install -e ".[test]"

# Configure environment
cp .env.example .env   # then fill in INFLUX_URL / INFLUX_TOKEN / INFLUX_ORG / INFLUX_BUCKET

# Prepare InfluxDB (health check, create bucket, verify write access)
ge-pipeline setup
```

## CLI (`ge-pipeline`)

```bash
ge-pipeline ingest                       # async catch-up ingestion up to now
ge-pipeline backfill                     # fill gaps from prior failed runs (same catch-up path)
ge-pipeline serve --host 127.0.0.1 --port 8000   # FastAPI query service (uvicorn)
ge-pipeline export --items 554,565 --range 7d --interval 1h --format parquet -o prices.parquet
ge-pipeline rollup --interval 1h --range 30d      # downsample raw -> itemPrice_1h (Core file-limit relief)
```

The CLI entry point is defined in `pyproject.toml` (`ge-pipeline = "ge_pipeline.cli:app"`).
Missing configuration prints remediation guidance and exits non-zero.

## Scheduling

`ge_pipeline/scheduler.py` runs an APScheduler daemon that triggers catch-up ingestion
every 5 minutes; deployable as a long-running daemon (e.g. via systemd).

## Backend Tests

```bash
# Unit + property-based tests (Hypothesis, >=100 examples per property)
pytest

# Integration tests use testcontainers (InfluxDB) and FastAPI TestClient
```

Tests live in `tests/`. Property tests (`*_property.py`) encode the design's correctness
properties. The Hypothesis "ci" profile (registered in `tests/conftest.py`) enforces the
minimum example count. Avoid Jupyter notebooks for verification — the notebooks are
documentation/scratchpads only.

## Frontend (React SPA, `web/`)

```bash
cd web
npm install
npm run dev         # Vite dev server (run manually in your terminal)
npm run build       # tsc --noEmit && vite build
npm run typecheck
npm run test        # vitest run
```

The SPA reads the FastAPI service through the typed client in `web/src/api/`. Configure the
API base URL via `web/.env` (see `web/.env.example`).

## Grafana

Provisioning lives under `grafana/`:
- `provisioning/datasources/influxdb.yaml` — InfluxDB **v3 FlightSQL/SQL** data source
  (`influxdata-flightsql-datasource`, uid `influxdb-v3-ge`)
- `provisioning/dashboards/` + `dashboards/ge-prices.json` — line / candlestick / gauge panels
- `dashboards/ge-db-stats.json` — DB health + ingestion stats (no `$itemID` variable)
- `provisioning/alerting/deviation-alert.yaml` — price-deviation alert (suppressed on zero/undefined)

### Split-host deployment note

The live Grafana runs on server `192.168.1.85:3001` (managed via Dockge) and reads the
InfluxDB on that **same host** (`192.168.1.85:8181`). The ingestion/backfill code runs on a
separate workstation (`pop-os`, `192.168.1.7`) and writes over the LAN to `.85`. See
`.kiro/steering/project-overview.md` → "Deployment topology" for the full picture.

- Datasource `host` on the `.85` Grafana must be `192.168.1.85:8181` (bare `host:port`, no
  `http://`), `mode: 1` (SQL), database `GEItemPrices`. Verify with
  `GET /api/datasources/uid/influxdb-v3-ge/health` (expect `status: OK`).
- Dashboards can be deployed to that Grafana via the HTTP API (see `grafana/README.md`
  "Copying the dashboards into a Dockge-managed Grafana", Option C).
- Whole-store stat queries (`COUNT(*)`, `COUNT(DISTINCT ...)`, `MIN/MAX(time)`) are
  full-table scans and can time out while a backfill is hammering the same InfluxDB; prefer
  time-bounded queries when spot-checking.

### InfluxDB 3 Core query-file-limit (empty-dashboard cause)

If dashboards are empty even for a loaded time range, suspect the Core Parquet
**file-scan limit** (`--query-file-limit`, default 432), not the datasource. Core
doesn't compact, so the backfill's many small files push wide/unbounded queries over
the cap (`Query would scan N Parquet files, exceeding the file limit`). Raise it in
`influxdb/docker-compose.yml` and **recreate** the InfluxDB container
(`docker compose up -d --force-recreate influxdb3`) — a plain restart won't apply a
changed `serve` arg. Never set it to `0` (Core reads that as a literal 0-file limit
and every query fails); use a high finite integer. See
`.kiro/steering/project-overview.md` → "InfluxDB 3 Core query file limit". A quick
progress/health probe: `python scripts/check_backfill_progress.py`.

Raising the cap is a symptom fix; the structural mitigation is the **rollup**
(`ge-pipeline rollup` / `scripts/rollup_history.py`), which downsamples raw `itemPrice`
into a compact `itemPrice_1h` measurement that scans far fewer files. Point historical
dashboards/exports at the rollup and keep raw for recent data. See the README "Rollups /
downsampling" section and `.kiro/steering/project-overview.md` → "Operational scripts".

Core cannot make raw files bigger (gen1 duration caps at 10m, no compaction, no custom
partitioning — batch size is irrelevant). The only way to keep the **raw 5-minute** data
*and* compact files automatically is the free **InfluxDB 3 Enterprise At-Home** license
(single-node, 2-CPU, non-commercial), a one-way upgrade over the same data dir. Full
steps: README "Upgrading to InfluxDB 3 Enterprise (free At-Home license)".

## Deployment (Docker host `192.168.1.85`, managed by Dockge)

The project's services run in Docker on the **`.85` host**, managed by **Dockge** (a
web UI that runs each stack from a plain compose folder under `/opt/stacks`). The dev
workstation (`pop-os`, `192.168.1.7`) is where the repo is edited.

### Host resources (see `project-overview.md` → "Host hardware / resources")

| Role | Host | CPU | RAM | GPU | OS |
|------|------|-----|-----|-----|-----|
| Docker host | `192.168.1.85` | 16c / 32t | 96 GB | RX 7900 XTX | Pop!_OS 24.04 |
| Dev workstation | `192.168.1.7` (`pop-os`) | 12c / 24t | 64 GB | RX 7900 XTX | Pop!_OS 24.04 |

The `.85` host is the larger box; it absorbs wide InfluxDB scans, a high
`--query-file-limit`, and (if upgraded) the Enterprise compaction service comfortably.

### Container layout: one folder per container

Each container has its own top-level folder with everything it needs:
- **`influxdb/`** — InfluxDB 3 Core (Dockerfile, `docker-compose.yml`, `.env.example`,
  `init-db.sh`, `run.sh`). Renamed from the old `docker files/`.
- **`grafana/`** — Grafana; compose in `grafana/docker/`, bind-mounting the sibling
  `grafana/provisioning/` and `grafana/dashboards/` (paths relative to `grafana/docker/`).

Add any new service as a new top-level folder following the same pattern.

### Current workflow: `scripts/deploy.sh` (automated, verified working)

Deployment is now **one command per service** from the workstation — Option 2 below,
implemented and verified end-to-end against `.85` (both `influxdb` and `grafana`
redeploy to healthy, datasource health `OK`):

```bash
scripts/deploy.sh influxdb        # edit influxdb/ then push + recreate
scripts/deploy.sh grafana         # edit grafana/ then push + recreate
scripts/deploy.sh all             # both
scripts/deploy.sh all --status    # remote `docker compose ps`
scripts/deploy.sh grafana --dry-run   # preview before pushing
```

It rsyncs each container's repo folder(s) into the matching `/opt/stacks` folder(s) on
`.85`, then `ssh`es in and runs `docker compose up -d --force-recreate <svc>`. Because
recreate is always used, `serve`-arg changes (e.g. `--query-file-limit`) actually apply
— no more "restarted but nothing changed". Full details, host layout, and the SSH/chown
prerequisites are under "Proposed automated deployment workflow → Option 2" below.

The old **manual** flow (hand `scp` into `/opt/stacks/<stack>/`, then recreate in the
Dockge UI) still works as a fallback but is superseded by the script — it was
error-prone (easy to skip the recreate; `/opt/stacks` drifted from the repo).

## Proposed automated deployment workflow

Goal: manage the `.85` docker host **100% from the workstation**, with the repo as the
single source of truth — no hand copying, no forgetting to recreate. Options below are
ordered simplest → most capable; you can adopt them incrementally.

### Prerequisite: passwordless SSH + Docker context (foundation for all options)

1. **SSH key to the host** so no interactive password/copy is needed:
   ```bash
   ssh-copy-id warren@192.168.1.85          # once, from pop-os
   ssh warren@192.168.1.85 docker version   # verify
   ```
2. **A Docker context** that points the local `docker`/`docker compose` CLI at the
   remote engine over SSH — this lets you drive `.85`'s Docker *from the workstation*
   with ordinary commands:
   ```bash
   docker context create ge85 --docker "host=ssh://warren@192.168.1.85"
   docker --context ge85 ps                 # lists containers running on .85
   ```
   (Requires the workstation user's key in the host's `~/.ssh/authorized_keys` and the
   user in the host's `docker` group.)

### Option 1 — remote compose over the Docker context (smallest change)

Keep the compose files in the repo; deploy without copying by targeting the remote
context. From the repo on `pop-os`:

```bash
docker --context ge85 compose -f influxdb/docker-compose.yml up -d --force-recreate
docker --context ge85 compose -f grafana/docker/docker-compose.yml up -d
```

Wrap these in a `scripts/deploy.sh <service>` helper so "deploy" is one command that
always includes `--force-recreate` (killing the "forgot to recreate" bug). Caveats:
build context and bind-mount **paths are resolved on the host side** for some
operations, and `.env` still needs to exist next to each compose file on the host — so
this works cleanly when images are pulled (Grafana) but the InfluxDB image, which is
`build:`-based, is better pushed as a prebuilt image (see Option 2) or built on the host.

### Option 2 — `rsync` push + remote recreate (IMPLEMENTED: `scripts/deploy.sh`)

**This is the adopted workflow.** It automates exactly today's manual flow so Dockge
still owns the stacks but you never copy by hand or forget to recreate. Implemented as
`scripts/deploy.sh` (+ git-ignored `scripts/deploy.env` for host details).

```bash
# one-time: create your local config from the example and edit if needed
cp scripts/deploy.env.example scripts/deploy.env

# deploy a service (rsync up + remote force-recreate)
scripts/deploy.sh influxdb
scripts/deploy.sh grafana
scripts/deploy.sh all

# helpers
scripts/deploy.sh influxdb --dry-run      # preview the rsync + remote command
scripts/deploy.sh grafana  --no-recreate  # sync only, recreate later
scripts/deploy.sh all       --status      # remote `docker compose ps`
```

What it does per service: `rsync -az --delete` the repo folder(s) into their host
stack folder(s), then `ssh` in and `docker compose up -d --force-recreate <service>`.

**Verified host layout under `/opt/stacks` on `.85`** (this is what the script's
defaults map to — the Grafana stack is *flat*, not nested):

| Repo source | Host destination | Notes |
|-------------|------------------|-------|
| `influxdb/` | `/opt/stacks/influxdb/` | compose at root; recreate svc `influxdb3` |
| `grafana/docker/` | `/opt/stacks/grafana/` | compose at root; recreate svc `grafana` |
| `grafana/provisioning/` | `/opt/stacks/provisioning/` | sibling stack (compose mounts `../provisioning`) |
| `grafana/dashboards/` | `/opt/stacks/dashboards/` | sibling stack (compose mounts `../dashboards`) |

The Grafana compose on the host mounts `../provisioning` and `../dashboards`, which are
**sibling** folders of `/opt/stacks/grafana`, so Grafana maps to three host folders.
Stack names on the host are `influxdb` / `grafana` (not `ge-*`).

- `.env` (and `.git`, `__pycache__`, `*.pyc`) are **excluded** from the rsync, so the
  host's secrets are created once on the host and left in place — never copied from the
  workstation, never in git. (Note: a local `grafana/docker/.env` exists in the working
  tree and is git-ignored; the exclude means it is never pushed and the host's own
  `grafana/.env` is never deleted.)
- `--delete` keeps each host folder an exact mirror of its repo folder (stale files on
  the host are removed) — the repo becomes the source of truth for each stack's files.

#### What `--delete` removes on the host (and what it never touches)

For each mapping, `rsync --delete` removes anything in the **host** folder that has no
match in the corresponding **repo** folder, *except* the excluded patterns. As deployed
today the host folders already mirror the repo, so a `--delete` dry-run reports **zero
deletions**. What it would remove on a future run:

- **Would be deleted:** files/dirs created *directly on the host* under
  `/opt/stacks/{influxdb,grafana,provisioning,dashboards}` that aren't in the matching
  repo folder — e.g. a dashboard JSON hand-dropped into `/opt/stacks/dashboards/`, a
  scratch `notes.txt`, or an extra compose override. Next deploy mirrors the repo and
  removes them.
- **Never deleted (excluded):** `.env`, `.git`, `__pycache__`, `*.pyc`. The important
  one is the host's `grafana/.env` (admin password + token) — safe in both directions.
- **Never touched (outside `/opt/stacks`):** Docker named volumes `influxdb3_data` and
  `grafana_ge_data` (the actual DB data and Grafana state live in Docker's volume store,
  not in the stack folders), and every other Dockge stack (`llama`, `open-webui`,
  `dockge`, …).

Practical rule: **don't hand-edit files in those four stack folders on `.85`** (except
`.env`); make changes in the repo and redeploy. Use `scripts/deploy.sh <svc> --dry-run`
to preview exactly what a run would copy and delete before committing to it.
- `--force-recreate` is always used, so `serve`-arg changes (e.g. `--query-file-limit`)
  actually apply — the classic "I restarted but nothing changed" trap is gone.
- Dockge keeps showing/managing the stack because you write into its stacks folder.

Config defaults (in `scripts/deploy.sh`, override in `scripts/deploy.env`): host
`192.168.1.85`, user = your local username, port `22`, stacks root `/opt/stacks`,
host stack folders `influxdb` / `grafana` (+ sibling `provisioning` / `dashboards`).
Each service is defined by a list of `REPO_SUBDIR::HOST_DIR` rsync mappings
(`<svc>_MAPPINGS`) plus `<svc>_COMPOSE_DIR` and `<svc>_RECREATE_SVC`.

#### Prerequisites — both DONE and verified

Recorded here so the setup is reproducible on a fresh host; on the current `.85` both
are already satisfied (both services redeploy to healthy).

1. **Key-based SSH into `.85`** (done). The script needs non-interactive SSH and fails
   fast with these instructions if it can't connect. On a fresh host:
   ```bash
   # ON THE HOST:
   sudo apt update && sudo apt install -y openssh-server
   sudo systemctl enable --now ssh
   sudo usermod -aG docker "$USER"      # so `docker compose` needs no sudo; re-login after
   # ON THE WORKSTATION:
   ssh-keygen -t ed25519                # only if you don't already have a key
   ssh-copy-id warren@192.168.1.85      # add -p PORT if sshd is on a custom port
   ssh warren@192.168.1.85 docker ps    # verify key login + docker usable
   ```
   If sshd runs on a non-standard port or the username differs, set `DEPLOY_SSH_PORT` /
   `DEPLOY_USER` in `scripts/deploy.env`.

2. **Stack folders owned by your login user** (done). rsync runs as your user, so it can
   only write where that user owns the folder. The `/opt/stacks` folders were originally
   `root`-owned; they were chowned once so deploys need no sudo:
   ```bash
   # ON THE HOST (one-time):
   sudo chown -R warren:warren /opt/stacks/influxdb /opt/stacks/grafana \
                               /opt/stacks/provisioning /opt/stacks/dashboards
   ```
   (Alternative if you'd rather not chown: give the user passwordless sudo for rsync and
   run rsync via `--rsync-path="sudo rsync"`; chowning is simpler and keeps the deploy
   fully unprivileged.)

### Option 3 — Git-based / GitOps (single source of truth, fully hands-off)

Make `.85` pull from the repo instead of receiving pushes:
- Put each stack's compose under a path the host checks out, and either (a) run a tiny
  cron/systemd timer on `.85` that does `git pull && docker compose up -d
  --force-recreate` when the branch changes, or (b) use a watcher like **Watchtower**
  (auto-updates running containers when a new image is pushed) for image-based services.
- Pair with a CI step (or a local `make release`) that **builds and pushes the InfluxDB
  image** to a registry (even a local one on `.85`) so the host only pulls, never builds.
- Result: `git push` (or merge) is the deploy; the host converges itself. This is the
  most robust but the biggest setup step, and overkill unless deploys get frequent.

### Recommendation

**Option 2 is implemented and adopted** (`scripts/deploy.sh`): it removes every manual
step, keeps Dockge, keeps secrets on the host, and needs no registry. The only remaining
one-time setup is enabling SSH on `.85` (see the prerequisite above). Move to Option 3
only if deployments become frequent enough to want full GitOps. Whichever option, always
use `--force-recreate` so `serve`-arg changes (like `--query-file-limit`) actually apply.

## API Reference

- RuneScape Wiki Prices API: https://prices.runescape.wiki/api/v1/osrs
- 5-minute endpoint: `/5m?timestamp={unix_timestamp}`
- Requires a `User-Agent` header (`GEoutlier-detection`)

## FastAPI Endpoints

```
GET /api/health
GET /api/items?query={substr}
GET /api/items/{item_id}/prices?range&interval
GET /api/items/{item_id}/prices/page?cursor&limit
GET /api/items/{item_id}/outliers?range&method
GET /api/datasets/export?items&range&interval&format   # chunked streaming
GET /api/datasets/features?items&range&interval
GET /api/outlier-methods
```
