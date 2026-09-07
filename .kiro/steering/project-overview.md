# Project Overview

This project collects Old School RuneScape (OSRS) Grand Exchange item price data
from the RuneScape Wiki Prices API and stores it in a local **InfluxDB 3** (Enterprise,
free At-Home license) time-series database. The backend is a modernized Python package
(`ge_pipeline`) with async ingestion, schema validation, a CLI, scheduling, a
scale-oriented data access layer, and a FastAPI query service. Two front ends visualize
and detect price outliers: a low-code Grafana dashboard and a custom React + TypeScript
single-page app.

> **InfluxDB version:** the live deployment now runs **InfluxDB 3 Enterprise** (free
> At-Home license), upgraded in place from **InfluxDB 3 Core** against the same data
> volume. Enterprise is a strict superset of Core (same SQL over Flight/gRPC, same
> line-protocol writes, same host/token/database client) and adds the compaction service,
> so nothing in `ge_pipeline` changed. It was originally migrated from InfluxDB v2. The v2
> client and Flux read logic survive **only** inside `ge_pipeline/migrate.py`, used to
> copy legacy v2 data into v3. A static test (`tests/test_import_confinement.py`) enforces
> that the v2 `influxdb_client` package is imported nowhere else in the package.

## Architecture

InfluxDB 3 (Enterprise) is the single source of truth. Both UIs read from the same store:
Grafana reads InfluxDB directly (v3 FlightSQL/SQL datasource), while the React SPA reads
through the FastAPI query layer.

```
RuneScape Wiki API ─► ge-pipeline ingest ─► InfluxDB 3 Enterprise ─► ge-pipeline serve (FastAPI) ─► React SPA
   (5-min snapshots)      (ingestion)        (localhost:8181)       (api)                       (web/)
                                                   │
                                                   └─► Grafana dashboards / ge-pipeline export
```

## Where the database runs and where it lives

- **Engine:** InfluxDB 3 Enterprise (free At-Home license), run as a Docker container.
- **Endpoint:** `http://localhost:8181` (HTTP API + Flight/gRPC on the same port).
- **Container:** name `ge-influxdb3-enterprise`, image `influxdb:3-enterprise`, started
  with `influxdb3 serve --node-id ge-node --cluster-id ge-cluster --license-type home
  --object-store file --data-dir /var/lib/influxdb3 --http-bind 0.0.0.0:8181 --without-auth
  --query-file-limit 2000`. Requires `INFLUXDB3_LICENSE_EMAIL` (git-ignored `.env`); the
  license JWT lives in the volume at `/var/lib/influxdb3/ge-cluster/trial_or_home_license`.
- **Database:** `GEItemPrices`.
- **Auth:** `--without-auth` for local dev, so any non-empty token is accepted.
- **Persistence:** Docker named volume `influxdb3_data` mounted at `/var/lib/influxdb3`
  in the container; on the host at `/var/lib/docker/volumes/influxdb3_data/_data`. Data
  survives restarts/removal unless the volume is deleted.

### Deployment topology (may be split across hosts)

`localhost:8181` is the portable single-host default, but the store need not live on the
same machine as the ingestion code. The current live deployment is **split across two
hosts** and is worth knowing when debugging "no data" issues:

- **Server host `192.168.1.85`** runs both the target **InfluxDB 3 Enterprise** (`:8181`)
  and the **Grafana** instance (`:3001`, managed via Dockge). Grafana reads this local InfluxDB.
- **Workstation `192.168.1.7`** (hostname `pop-os`) runs the **ingestion / backfill** code,
  writing over the LAN to `INFLUXDB3_HOST_URL=http://192.168.1.85:8181`. It also happens to
  have its own unrelated local InfluxDB on `localhost:8181` — do **not** confuse that stale
  local instance with the real target on `.85`.

```
workstation pop-os (192.168.1.7)              server (192.168.1.85)
  backfill_history.py / ge-pipeline ingest ─────► InfluxDB 3 Enterprise :8181 (GEItemPrices)
                                                        ▲
                                                        └── Grafana :3001 (Dockge) reads it
```

Always confirm which InfluxDB a process targets by its `INFLUXDB3_HOST_URL`, not by
assuming `localhost`. The Grafana FlightSQL datasource on `.85` must point at that host's
InfluxDB (`192.168.1.85:8181`), database `GEItemPrices`.

> **Default host when the user talks about "the deployment", "the dashboard", "the
> database", "the server", or "Grafana": assume the REMOTE server `192.168.1.85`** (its
> InfluxDB 3 Enterprise on `:8181` and Grafana on `:3001`), **not** the local workstation. The
> workstation's own `localhost:8181` InfluxDB and any local Grafana are stale/unrelated and
> should be ignored unless the user explicitly says "local" or "workstation". When
> debugging, inspect and query the `.85` instance (e.g. over the LAN / on that host), not
> the local containers on `pop-os`.

### Host hardware / resources

Both machines run **Pop!_OS 24.04** and have an **AMD Radeon RX 7900 XTX** GPU.

| Role | Host | CPU | RAM | GPU | OS |
|------|------|-----|-----|-----|-----|
| **Docker host** (services) | `192.168.1.85` | 16 cores / 32 threads | 96 GB | RX 7900 XTX | Pop!_OS 24.04 |
| **Dev workstation** (`pop-os`) | `192.168.1.7` | 12 cores / 24 threads | 64 GB | RX 7900 XTX | Pop!_OS 24.04 |

Implications:
- The `.85` docker host is the larger box (more cores, 96 GB RAM), so it comfortably
  absorbs wide InfluxDB scans and the Enterprise compaction service (now running; the free
  At-Home license caps Enterprise at 2 CPU cores, well within this host).
- Backfill/ingestion runs on the dev workstation and is network/API-bound, not
  resource-constrained, so its 12c/64 GB are ample.
- Both GPUs (RX 7900 XTX) are ROCm-capable, relevant only if the outlier-detection work
  later moves to GPU compute; the pipeline itself is CPU/IO-bound.

### Container layout: one folder per container

Each Docker service lives in its **own top-level folder** holding everything that
container needs (Dockerfile if any, `docker-compose.yml`, `.env.example`, provisioning,
helper `run.sh`). This mirrors how the services are deployed on the `.85` host and keeps
each container self-contained and copyable on its own.

```
python_InfluxDB/
├── influxdb/                 # InfluxDB 3 Enterprise container (was "docker files/")
│   ├── docker-compose.yml    # uses the official influxdb:3-enterprise image (no Dockerfile)
│   ├── .env.example          # -> .env (git-ignored); includes INFLUXDB3_LICENSE_EMAIL
│   ├── init-db.sh            # idempotent DB creation
│   └── run.sh                # up / down / destroy / logs / status
└── grafana/                  # Grafana container + its provisioning-as-code
    ├── docker/
    │   ├── docker-compose.yml
    │   ├── .env.example       # -> .env (git-ignored)
    │   └── run.sh
    ├── provisioning/          # datasource, dashboard provider, alert rule
    └── dashboards/            # dashboard JSON mounted into the container
```

- **`influxdb/`** is the InfluxDB 3 Enterprise container folder. It was renamed from the old
  `docker files/` (the space in that name was awkward and it didn't identify the
  container). All docs/scripts now reference `influxdb/`.
- **`grafana/`** is the Grafana container folder. Its compose lives in `grafana/docker/`
  and bind-mounts the sibling `provisioning/` and `dashboards/` folders (paths are
  relative to `grafana/docker/`), so those stay inside the grafana folder rather than
  being split out. Do not move `provisioning/`/`dashboards/` without updating the
  bind-mount paths in `grafana/docker/docker-compose.yml`.
- **Rule for new services:** add a new top-level folder named after the container, with
  its own compose + `.env.example` + `run.sh`, following the same pattern.

### Deploy workflow: `scripts/deploy.sh` (automated, from the workstation)

Services on `.85` are managed by **Dockge** (a web UI over per-stack compose folders,
stacks root `/opt/stacks`, one folder per stack). Deployment is automated and driven
entirely from the dev workstation via `scripts/deploy.sh` (the adopted "Option 2" — see
`.kiro/steering/development-workflow.md` → "Proposed automated deployment workflow" for
full detail). It is implemented and verified end-to-end (both services redeploy to
healthy):

```bash
scripts/deploy.sh influxdb    # rsync influxdb/ -> host, then force-recreate
scripts/deploy.sh grafana     # rsync grafana/ folders -> host, then force-recreate
scripts/deploy.sh all         # both
scripts/deploy.sh all --status
```

Per service it `rsync -az --delete`s the repo folder(s) into the matching `/opt/stacks`
folder(s) over SSH, then `docker compose up -d --force-recreate <svc>` on the host.
Verified host mapping (the Grafana stack is *flat* — its compose mounts sibling
`../provisioning` and `../dashboards`, so it spans three host folders):

| Repo source | Host destination |
|-------------|------------------|
| `influxdb/` | `/opt/stacks/influxdb/` |
| `grafana/docker/` | `/opt/stacks/grafana/` |
| `grafana/provisioning/` | `/opt/stacks/provisioning/` |
| `grafana/dashboards/` | `/opt/stacks/dashboards/` |

Key properties:
- `--force-recreate` is always used, so a changed `serve` arg / compose `command:`
  (e.g. `--query-file-limit`) actually takes effect — a plain restart would keep the old
  args.
- `.env` / `.git` / `__pycache__` / `*.pyc` are excluded, so the host's secrets
  (notably `grafana/.env`) are never overwritten or deleted.
- `--delete` makes each host folder an exact mirror of its repo folder → **the repo is
  the source of truth**; do not hand-edit files in those stack folders on `.85` (except
  `.env`). Docker named volumes (`influxdb3_data`, `grafana_ge_data`) live outside
  `/opt/stacks` and are never touched, so data/state are safe.

Prerequisites (both one-time, already done on `.85`): key-based SSH from the workstation,
and the four stack folders chowned to the login user so rsync needs no sudo. The old
manual `scp` + Dockge-UI-restart flow still works as a fallback but is superseded.

### InfluxDB 3 query file limit (`--query-file-limit`)

InfluxDB 3 caps how many Parquet files a single query may scan (`--query-file-limit`,
env `INFLUXDB3_QUERY_FILE_LIMIT`, **default 432**). A large history backfill produces
many small gen1 files, so wide or **unbounded** queries could exceed the cap and fail
with:

```
Query would scan N Parquet files, exceeding the file limit.
```

**Now that the deployment runs Enterprise, the compaction service merges those files
automatically**, so the file count stays low and this cap sits near the default —
`--query-file-limit ${INFLUXDB3_QUERY_FILE_LIMIT:-2000}` in `influxdb/docker-compose.yml`.
It can still bite **transiently** during a large fresh backfill before compaction has
caught up: the Grafana `$itemID` variable (`SELECT DISTINCT "itemID" FROM "itemPrice"`,
no time filter) and whole-store stat panels (`COUNT(*)`, `COUNT(DISTINCT ...)`,
`MIN/MAX(time)`) scan the whole store; time-bounded queries are fine.

If you hit it, raise the value temporarily and **recreate** the container (a plain
restart keeps the old `serve` arg). Critical gotcha: **`0` does NOT mean unlimited** —
it is taken literally (allow 0 files) and makes *every* query fail with "scan 0 Parquet
files". Always use a finite integer.

The Core-era structural mitigation, the **rollup** (`ge_pipeline/rollup.py` /
`scripts/rollup_history.py`, `ge-pipeline rollup`), is now **optional**: it re-reads the
raw `itemPrice` data in bounded windows, downsamples with `date_bin` (default hourly),
and writes a far smaller `itemPrice_1h` measurement. Still handy for cheap pre-aggregated
long-range dashboards, but no longer required for correctness now that compaction runs.
See "Operational scripts" below and the README "Rollups / downsampling" section.

### File-size background and the Enterprise upgrade (done)

Historical context (kept because it explains the structure): on **Core**, batch size does
**not** control file size — Core forms gen1 Parquet files by **timestamp**, spanning
`--gen1-duration` (`1m`/`5m`/`10m`, default `10m`), has **no compaction**, and **no custom
partitioning**. So the coarsest a raw file got on Core was a 10-minute block — a fixed
floor (~a quarter-million files for ~4.5y of 5-minute data) that files re-grew toward.
Never advise "write bigger batches so files are larger".

Because the project must keep the **raw 5-minute resolution** for statistics, the durable
fix was **InfluxDB 3 Enterprise** (adds the compaction service), and **it is now in
place** on `.85`. The **At-Home** license is free, never expires, single-node, 2-CPU,
non-commercial — fits the `.85` host. Enterprise is a superset of Core on the **same
object store / data dir** (no data migration). Gotchas that were handled and matter if
reproducing on a new host: the upgrade is **one-way** (catalog changes; back up the data
volume first — no downgrade), Enterprise needs a `--cluster-id` (`ge-cluster` here), and
Docker activation is non-interactive (`INFLUXDB3_LICENSE_EMAIL` + `--license-type=home` +
click the email verification link; the license JWT is a secret, never commit it). Pin the
image tag (`influxdb:3-enterprise`), not `latest`. On 3.11.x the license check is
`influxdb3 show license info` (no `--host`). Full step-by-step is in the README
"Upgrading to InfluxDB 3 Enterprise (free At-Home license)". Ranking when asked about file
pressure: **Enterprise At-Home** (in place; keeps 5m + auto-compacts) > **rollup companion**
(optional, keeps 5m raw + fast `itemPrice_1h`) > raise `--query-file-limit` (transient
symptom knob) > clean reload (niche cleanup).

### Ingestion status on the live deployment

The `.85` store was upgraded in place from Core to **Enterprise** (compaction now runs).
As of this deployment, only `scripts/backfill_history.py` is running against `.85`, loading
history **oldest→newest** (resumes from `scripts/backfill_checkpoint.json`; was in the 2021+
range). No 5-minute catch-up / scheduler is running there yet, so the store has **no recent
(near-now) data** — dashboards defaulting to `now-24h`/`now-7d` render empty until either the
backfill reaches present day or a catch-up is started against `.85`. See the TODO in
`README.md` / `grafana/README.md`. Check progress with
`PYTHONPATH=. .venv/bin/python scripts/check_backfill_progress.py`.

## Backend package: `ge_pipeline/`

| Module | Purpose |
|--------|---------|
| `config.py` | Lazy, cached env-backed settings via `get_settings()` (no import-time side effects). Exposes v3 target settings and lazily-validated v2 source settings (`require_migration_source`). |
| `models.py` | pydantic v2 models (`ItemPricePoint`, `FiveMinuteSnapshot`) validating raw API JSON |
| `influx.py` | InfluxDB 3 client lifecycle (cached `InfluxDBClient3`), batched line-protocol writes, parameterized **SQL** queries with `date_bin` downsampling |
| `admin.py` | InfluxDB 3 database creation (`setup_v3_database`) via the HTTP management API (idempotent) |
| `migrate.py` | One-pass v2→v3 migration (SOLE surviving v2/Flux reader): stream from v2, copy 1:1 into v3 |
| `retry.py` | Retry with exponential backoff for transient failures |
| `ingestion.py` | Async catch-up ingestion (lazy timestamp gaps, null filtering, batching) |
| `data_access.py` | Scale-oriented reads: time chunking, streaming export, pagination, feature frames |
| `rollup.py` | Downsample raw `itemPrice` into a coarser rollup measurement (default `itemPrice_1h`): lazy windowed read → `date_bin` aggregate → bounded write. Was the DIY substitute for compaction on Core; optional now that Enterprise compaction runs. |
| `outliers.py` | Pluggable outlier-detection registry (z-score, IQR) |
| `api.py` | FastAPI query/export service consumed by the SPA and ML clients |
| `scheduler.py` | APScheduler daemon triggering catch-up every 5 minutes |
| `cli.py` | Typer CLI entry point (`ge-pipeline`): `ingest`, `backfill`, `rollup`, `serve`, `export`, `setup`, `migrate` |
| `errors.py` | Shared exceptions: `ConfigError`, `TransientError`, `NonTransientError` |

## Operational scripts: `scripts/`

- `scripts/backfill_history.py` — chunked, resumable full-history backfill driver (NOT part
  of the library). Walks history oldest→newest in fixed-size chunks reusing the pipeline's
  fetch/validate/write logic; checkpoints after each committed chunk; writes in bounded
  sub-batches (default 50k records) to stay under InfluxDB 3's 10 MB request limit; does not
  advance the checkpoint on a failed write. Run with `PYTHONPATH=.`. Key flags:
  `--chunk-windows` (default 2016 = 1 week), `--max-chunks` (0 = all), `--start-ts`,
  `--stop-ts`, `--concurrency` (default 4), `--pause` (default 2.0),
  `--max-records-per-write` (default 50000), `--checkpoint`.
- `scripts/rollup_history.py` — chunked, resumable rollup/downsample driver (NOT part of the
  library). Walks the raw `itemPrice` history in fixed-size time windows, downsamples each to
  a `date_bin` interval (default hourly), and writes the far smaller result into a separate
  rollup measurement (default `itemPrice_1h`). Its purpose is to relieve Core's small-file /
  `--query-file-limit` pressure (Core can't auto-compact) without touching Parquet files
  directly — it rewrites *through* the DB so the catalog stays authoritative. Checkpoints per
  committed window (`scripts/rollup_checkpoint.json`); enumerates items per window with a
  time-bounded `DISTINCT` so enumeration stays under the file cap; writes in bounded
  sub-batches and does not advance the checkpoint on a failed write. Run with `PYTHONPATH=.`.
  Key flags: `--interval` (default `1h`), `--measurement` (default `itemPrice_1h`),
  `--window-seconds` (default 604800 = 1 week), `--start-ts`, `--stop-ts`, `--max-windows`
  (0 = all), `--pause` (default 0.5), `--max-records-per-write` (default 50000),
  `--checkpoint`.

## Front ends

- **React SPA** — `web/` (Vite + React + TypeScript, Lightweight Charts, dark theme).
  Item search, candlestick + volume charts, and outlier highlighting over the FastAPI API.
- **Grafana** — `grafana/` provisioned InfluxDB **v3 FlightSQL/SQL** datasource, dashboards,
  and a deviation alert rule.

## Data Flow (ingestion)

1. Query InfluxDB for the latest stored timestamp (reference item "554")
2. Lazily generate missing 5-minute timestamps from latest → now (`iter_missing_timestamps`)
3. Fetch each snapshot from the Wiki API concurrently (bounded semaphore, async httpx)
4. Validate JSON into a `FiveMinuteSnapshot`; build records, filtering fully-null items
5. Batch-write records to the InfluxDB 3 database "GEItemPrices"

For a full historical load on an empty database, use `scripts/backfill_history.py` (chunked,
resumable) rather than a single `ge-pipeline ingest`, which would try to backfill from 2021 in
one run.

## Key Dependencies

- Backend: `influxdb3-python`, `pandas`, `numpy`, `python-dotenv`, `httpx`, `pydantic>=2`,
  `typer`, `apscheduler`, `fastapi`, `uvicorn`, `pyarrow` (pinned in `pyproject.toml`)
- Migration only (optional extra `[migration]`): `influxdb-client` (v2), imported solely by
  `ge_pipeline/migrate.py`
- Tests: `pytest`, `hypothesis`, `testcontainers`
- Frontend: `react`, `typescript`, `vite`, `lightweight-charts`, `vitest`
- InfluxDB 3 Enterprise (free At-Home license) running at localhost:8181 (`.85` in the live deployment)

## Configuration

- **v3 target (required for all commands):** `INFLUXDB3_HOST_URL`, `INFLUXDB3_AUTH_TOKEN`,
  `INFLUXDB3_DATABASE_NAME` — validated eagerly on first `get_settings()`.
- **v2 source (required only for `migrate`):** `V2_INFLUX_URL`, `V2_INFLUX_TOKEN`,
  `V2_INFLUX_ORG`, `V2_INFLUX_BUCKET` — validated lazily via `require_migration_source`.
- `get_settings()` caches the result and raises `ConfigError` naming the first missing var.
- Keep `.env` git-ignored; `.env.example` documents the keys with placeholders.

## Storage Schema

- Table / measurement: `itemPrice`
- Tag: `itemID` (string)
- Fields: `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`
- Write precision: seconds; granularity: 5 minutes
- Database / (legacy v2 org): `GEItemPrices` / `Ge-data-project`
- API User-Agent: `GEoutlier-detection`
- Schema preserved 1:1 from v2 (camelCase, unchanged). InfluxDB 3 SQL requires the camelCase
  identifiers to be double-quoted (e.g. `"itemPrice"`, `"itemID"`, `"avgHighPrice"`) so they
  stay case-sensitive; user values (item ids, time bounds) are always bound as query
  parameters, never interpolated.

## Notebooks (documentation / experimentation only)

- `DatabaseSetup.ipynb` — InfluxDB setup guide
- `testFunctions.ipynb`, `testGettingandwritingtimestamps.ipynb` — API/query scratchpads
