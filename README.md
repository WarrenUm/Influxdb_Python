# OSRS Grand Exchange Price Collector

A data ingestion pipeline that fetches Old School RuneScape (OSRS) Grand Exchange item price data from the [RuneScape Wiki Prices API](https://prices.runescape.wiki/api/v1/osrs) and stores it in a local **InfluxDB 3 Core** time-series database. It also serves the data over a FastAPI query service and exports bulk datasets for a GE outlier-detection project.

> **InfluxDB version:** the pipeline targets **InfluxDB 3 Core** (SQL over Flight/gRPC, line-protocol writes, host/token/database). It was migrated from InfluxDB v2; the v2 client and Flux read logic survive only inside `ge_pipeline/migrate.py`, which is used to copy legacy v2 data into v3.

> **Core vs Enterprise (storage):** InfluxDB 3 **Core** does not compact its Parquet
> files, so a large backfill accumulates many small files and wide queries hit the
> `--query-file-limit`. Two mitigations exist and are documented below: the DIY
> [rollup](#rollups--downsampling-ge-pipeline-rollup-scriptsrollup_historypy) (keeps a
> compact `itemPrice_1h` alongside the raw 5-minute data) and, for automatic ongoing
> compaction while keeping full 5-minute resolution, the free
> [At-Home Enterprise upgrade](#upgrading-to-influxdb-3-enterprise-free-at-home-license).

## Table of contents

- [Architecture](#architecture)
- [Data model](#data-model)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Running the database](#where-the-database-runs-and-where-it-lives)
- [Deployment: hosting the database on `192.168.1.85`](#deployment-hosting-the-database-on-192168185-split-host)
- [Backfilling full history](#backfilling-full-history-scriptsbackfill_historypy)
- [Rollups / downsampling](#rollups--downsampling-ge-pipeline-rollup-scriptsrollup_historypy)
- [Managing storage and the Core file limit](#managing-storage-and-the-core-file-limit)
- [Upgrading to InfluxDB 3 Enterprise (free At-Home license)](#upgrading-to-influxdb-3-enterprise-free-at-home-license)
- [Migrating from InfluxDB v2](#migrating-from-influxdb-v2)
- [API reference](#api-reference)

## Deployment: hosting the database on `192.168.1.85` (split-host)

The database runs on the remote host **`192.168.1.85`** (port `8181`) and is
reached over the LAN from this workstation (`pop-os`, `192.168.1.7`), which
fetches from the Wiki API and writes to that remote DB. **Grafana also runs on
`192.168.1.85`** (`:3001`, managed via Dockge) and reads that same InfluxDB
directly. Work top to bottom.

> **Don't confuse InfluxDB instances.** This workstation also has its own,
> unrelated InfluxDB on `localhost:8181`. The pipeline target is the remote
> `192.168.1.85:8181` — always confirm by the running process's
> `INFLUXDB3_HOST_URL`, not by assuming `localhost`.

### Updating the deployment from the workstation (`scripts/deploy.sh`)

Once the host is set up (the checklist below is the one-time bootstrap), you push
changes to `.85` **entirely from this workstation** with `scripts/deploy.sh` — no
manual copying, no forgetting to recreate:

```bash
scripts/deploy.sh influxdb        # after editing influxdb/
scripts/deploy.sh grafana         # after editing grafana/
scripts/deploy.sh all             # both
scripts/deploy.sh all --status    # remote `docker compose ps`
scripts/deploy.sh grafana --dry-run   # preview the rsync + remote command
```

Per service it `rsync -az --delete`s the container's repo folder(s) into the matching
`/opt/stacks` folder(s) on `.85` over SSH, then runs `docker compose up -d
--force-recreate <svc>` there. Because recreate is always used, changed `serve` args
(e.g. `--query-file-limit`) actually apply. Host mapping (the Grafana stack is flat, so
it spans three host folders):

| Repo source | Host destination |
|-------------|------------------|
| `influxdb/` | `/opt/stacks/influxdb/` |
| `grafana/docker/` | `/opt/stacks/grafana/` |
| `grafana/provisioning/` | `/opt/stacks/provisioning/` |
| `grafana/dashboards/` | `/opt/stacks/dashboards/` |

- `.env` (and `.git`, `__pycache__`, `*.pyc`) are excluded, so the host's
  `grafana/.env` (admin password + token) is never pushed or deleted.
- `--delete` mirrors each host folder to its repo folder, so **the repo is the source of
  truth** — don't hand-edit files in those stack folders on `.85` (except `.env`); any
  file created there that isn't in the repo is removed on the next deploy. Docker named
  volumes (`influxdb3_data`, `grafana_ge_data`) live outside `/opt/stacks` and are never
  touched.
- One-time prerequisites (already done on `.85`, needed on a fresh host): key-based SSH
  from the workstation, and the four `/opt/stacks` stack folders chowned to your login
  user so rsync needs no sudo. Config lives in `scripts/deploy.env` (copy from
  `scripts/deploy.env.example`); defaults match the current `.85` layout.

### On the database host (`192.168.1.85`)

- [ ] Install Docker Engine + the Compose plugin.
- [ ] Copy the `influxdb/` folder to the host (clone the repo or `scp` the folder).
- [ ] `cd influxdb` and (optionally) `cp .env.example .env` to customize the
      database name/port. Keep `INFLUXDB3_HOST_PORT=8181`.
- [ ] Start the database: `./run.sh` (builds the image, starts the server, creates
      the `GEItemPrices` database). This binds `0.0.0.0:8181`, so it is reachable
      across the LAN.
- [ ] Open the firewall for TCP `8181` if the host has one enabled, e.g.
      `sudo ufw allow 8181/tcp`.
- [ ] Verify locally on the host: `curl -s http://localhost:8181/health` → `OK`.

> **Security note:** the compose stack runs `--without-auth`, so *any* host on the
> LAN can read/write this database. That is fine on a trusted home network; do not
> expose port `8181` to the internet. To lock it down, switch the compose
> `command:` to an authenticated mode and set a real `INFLUXDB3_AUTH_TOKEN`.

### On this machine (the ingester)

- [ ] Confirm the remote DB is reachable over the LAN:
      `curl -s http://192.168.1.85:8181/health` → `OK`.
- [ ] Point the pipeline at the remote host by editing the repo-root `.env`:
      ```
      INFLUXDB3_HOST_URL=http://192.168.1.85:8181
      INFLUXDB3_AUTH_TOKEN=local-dev-token
      INFLUXDB3_DATABASE_NAME=GEItemPrices
      ```
- [ ] Ensure the database exists on the remote (idempotent): `ge-pipeline setup`.
- [ ] Continue the chunked history backfill against the remote DB (resumes from the
      checkpoint automatically):
      ```bash
      PYTHONPATH=. .venv/bin/python scripts/backfill_history.py --chunk-windows 2016
      ```
      Run a bounded slice first if you want to sanity-check the remote write path,
      e.g. add `--max-chunks 4`. See
      [Backfilling full history](#backfilling-full-history-scriptsbackfill_historypy)
      for all parameters.
- [ ] Keep recent data current going forward with `ge-pipeline ingest` (or schedule
      it). It writes to whatever `INFLUXDB3_HOST_URL` points at.

> The backfill is resumable: the checkpoint at `scripts/backfill_checkpoint.json`
> tracks the last committed window (`next_start_ts`). If you switch the target host,
> the same checkpoint still applies — the *windows* fetched are independent of which
> DB they are written to. If you want the remote DB to start its history from
> scratch, delete the checkpoint file first.

### TODO: run the 5-minute catch-up against the remote DB

Right now only `scripts/backfill_history.py` is running against `192.168.1.85`,
and it loads history **oldest→newest** (currently still in the 2021 range). That
means the remote DB has **no recent (near-now) data yet**, so the Grafana
dashboards on `.85` look empty for default `now-24h`/`now-7d` ranges — they only
show data if you set the time range back to a period the backfill has loaded.

- [ ] **Start the 5-minute catch-up ingestion pointed at `192.168.1.85:8181`** so
      the dashboards get recent/live prices, not just backfilled history:
      ```bash
      # with the repo-root .env pointing INFLUXDB3_HOST_URL at 192.168.1.85:8181
      ge-pipeline ingest          # one catch-up pass (fills forward to now)
      # or run the scheduler daemon for continuous 5-minute catch-up:
      PYTHONPATH=. .venv/bin/python -m ge_pipeline.scheduler
      ```
      This is safe to run **alongside** the historical backfill: the catch-up
      fills forward from the latest stored timestamp while the backfill fills
      older history behind it. Both write to whatever `INFLUXDB3_HOST_URL` points
      at, so double-check it targets `.85` and not this workstation's local
      InfluxDB.

### Troubleshooting: Grafana/queries return no data (InfluxDB 3 Core file limit)

> This is the operational quick-fix. For the full picture of Core's storage limits
> and the two lasting mitigations, see
> [Managing storage and the Core file limit](#managing-storage-and-the-core-file-limit).

InfluxDB 3 **Core** caps how many Parquet files a single query may scan
(`--query-file-limit`, default **432**). Core doesn't auto-compact, so a large
backfill creates thousands of small files and wide/unbounded queries — whole-store
`COUNT(*)`/`COUNT(DISTINCT ...)`, the Grafana `$itemID` dropdown, all-history stat
panels — exceed the cap and fail with:

```
Query would scan N Parquet files, exceeding the file limit.
```

The compose stack raises this via `--query-file-limit ${INFLUXDB3_QUERY_FILE_LIMIT:-1000000}`
(see `influxdb/`). To apply a change you must **recreate** the container (a plain
restart keeps the old `serve` args):

```bash
cd influxdb                                        # on the InfluxDB host
docker compose up -d --force-recreate influxdb3
docker inspect ge-influxdb3 --format '{{json .Args}}' | tr ',' '\n' | grep -A1 query-file-limit
```

> **Never set the limit to `0`.** On Core that is a literal "0 files allowed", so
> *every* query fails ("scan 0 Parquet files"). Use a high finite integer. Higher
> values cost more memory and can OOM the server on a small host — drop to e.g.
> `50000` (still well above 432) if RAM is tight.

Check the backfill's progress at any time with:

```bash
python scripts/check_backfill_progress.py           # reads the checkpoint + queries the DB
```

## Architecture

```
RuneScape Wiki API ──► ge-pipeline ingest ──► InfluxDB 3 Core ──► ge-pipeline serve (FastAPI)
   (5-min price snapshots)   (ge_pipeline.ingestion)  (host:8181)     (ge_pipeline.api)
                                     │                                          │
                              ge_pipeline.influx                        ge-pipeline export
                            (client, writes, SQL queries)              (bulk NDJSON/CSV/Parquet)
```

> `host:8181` is `localhost:8181` for a single-host setup, or a remote LAN host
> (currently `192.168.1.85:8181`) — see the task checklist above. The target is
> always whatever `INFLUXDB3_HOST_URL` points at.

The modernized backend lives in the `ge_pipeline` package:

| Module | Purpose |
|--------|---------|
| `ge_pipeline/config.py` | Environment-backed settings (`get_settings`); v3 target + lazy v2 source |
| `ge_pipeline/influx.py` | InfluxDB 3 client, batched line-protocol writes, parameterized SQL queries |
| `ge_pipeline/ingestion.py` | Async catch-up ingestion (timestamp gaps, null filtering, batching) |
| `ge_pipeline/admin.py` | InfluxDB 3 database creation (`setup_v3_database`) |
| `ge_pipeline/migrate.py` | One-pass v2→v3 migration tool (sole surviving v2/Flux reader) |
| `ge_pipeline/api.py` | FastAPI query service |
| `ge_pipeline/data_access.py` | Streaming bulk export (NDJSON/CSV/Parquet) |
| `ge_pipeline/rollup.py` | Downsample raw `itemPrice` into a coarser rollup measurement (e.g. `itemPrice_1h`) to relieve Core's small-file pressure |
| `ge_pipeline/scheduler.py` | APScheduler periodic ingestion |
| `ge_pipeline/cli.py` | Typer CLI entry point (`ge-pipeline`) |

## Data Model

- **Table / measurement**: `itemPrice`
- **Tag**: `itemID` (string)
- **Fields**: `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`
- **Write precision**: seconds
- **Granularity**: 5-minute intervals

> The schema is preserved 1:1 from the original v2 layout (camelCase names unchanged). Because the identifiers are camelCase, every SQL identifier is double-quoted (e.g. `SELECT "avgHighPrice" FROM "itemPrice" WHERE "itemID" = $item_id`) so InfluxDB 3 SQL treats them as case-sensitive.

## Where the database runs and where it lives

The database is **InfluxDB 3 Core**, run as a local Docker container.

- **Endpoint:** `http://localhost:8181` (HTTP API + Flight/gRPC on the same port)
- **Container name:** `ge-influxdb3` (image `quay.io/influxdb/influxdb3-core:latest`)
- **Database name:** `GEItemPrices`
- **Auth:** started with `--without-auth` for local use, so any non-empty token is accepted
- **Data persistence:** stored in the Docker named volume `influxdb3_data`, mounted at
  `/var/lib/influxdb3` inside the container. On the host this resolves to
  `/var/lib/docker/volumes/influxdb3_data/_data` (data survives container restarts/removal
  as long as the volume is not deleted).

Start (or recreate) the container:

```bash
docker volume create influxdb3_data
docker run -d \
  --name ge-influxdb3 \
  -p 8181:8181 \
  -v influxdb3_data:/var/lib/influxdb3 \
  quay.io/influxdb/influxdb3-core:latest \
  serve --node-id ge-node --object-store file --data-dir /var/lib/influxdb3 \
        --http-bind 0.0.0.0:8181 --without-auth

# Health check
curl -s http://localhost:8181/health   # -> OK
```

Common operations:

```bash
docker ps --filter name=ge-influxdb3        # status
docker stop ge-influxdb3                     # stop (data persists in the volume)
docker start ge-influxdb3                    # resume
docker logs --tail 50 ge-influxdb3           # logs
```

## Prerequisites

- Python 3.10+
- Docker (to run InfluxDB 3 Core), or an InfluxDB 3 Core server reachable at the configured host

## Setup

1. Start InfluxDB 3 Core (see [Where the database runs](#where-the-database-runs-and-where-it-lives) above).

2. Install the package (with test extras) into a virtualenv:
   ```bash
   pip install -e ".[test]"
   ```
   Dependencies are pinned in `pyproject.toml` (mirrored in `requirements.txt`). The v2
   client needed only for migration is an optional extra: `pip install -e ".[migration]"`.

3. Configure environment variables. Copy the example file and fill in your values:
   ```bash
   cp .env.example .env
   ```
   Required (v3 target, all commands): `INFLUXDB3_HOST_URL`, `INFLUXDB3_AUTH_TOKEN`, `INFLUXDB3_DATABASE_NAME`
   Required only for `ge-pipeline migrate` (v2 source): `V2_INFLUX_URL`, `V2_INFLUX_TOKEN`, `V2_INFLUX_ORG`, `V2_INFLUX_BUCKET`
   (`.env` is git-ignored so tokens never land in source control.)

4. Create the InfluxDB 3 database (idempotent):
   ```bash
   ge-pipeline setup
   ```

## Usage

```bash
# Catch the database up to the current time (async, batched)
ge-pipeline ingest

# Backfill any windows missed by prior runs
ge-pipeline backfill

# Serve the query API (FastAPI + uvicorn)
ge-pipeline serve --host 127.0.0.1 --port 8000

# Export a bulk dataset to a file
ge-pipeline export --items 554,565 --range 7d --interval 1h --format parquet -o prices.parquet

# Downsample raw data into a coarser rollup measurement (relieves Core's file limit)
ge-pipeline rollup --interval 1h --range 30d

# Copy existing InfluxDB v2 data into the v3 database (requires V2_* env vars)
ge-pipeline migrate
```

`ge-pipeline ingest` queries InfluxDB for the most recent data point (reference item `554`), calculates the gap to now, and backfills all missing 5-minute intervals from the RuneScape Wiki API. When the database is empty it would otherwise backfill from March 2021; for large historical loads use the chunked backfill script below.

## Backfilling full history (`scripts/backfill_history.py`)

`ge-pipeline ingest` always catches up to *now* from the latest stored timestamp, so on an
empty database it attempts the entire history (~470k five-minute windows) in one run. To pull
the full history politely and resumably, use the chunked backfill driver. It walks history
oldest→newest in fixed-size chunks, reusing the pipeline's own fetch/validate/write logic
(bounded concurrency, retry/backoff, null filtering, seconds precision), and writes a JSON
checkpoint after each fully committed chunk so it is safe to interrupt and resume.

Writes are split into bounded sub-batches (default 50,000 records per request) to stay under
InfluxDB 3's 10 MB request limit. If a chunk's write fails after retries, the checkpoint is
**not** advanced, so a resume re-fetches that chunk rather than leaving a silent gap.

Run it (set `PYTHONPATH` to the repo root so `ge_pipeline` imports):

```bash
# Trial: 4 one-week chunks starting from the project's earliest snapshot
PYTHONPATH=. .venv/bin/python scripts/backfill_history.py \
  --chunk-windows 2016 --max-chunks 4 --concurrency 4 --pause 2.0

# Resume and run the full remaining history (checkpointed; long-running)
PYTHONPATH=. .venv/bin/python scripts/backfill_history.py --chunk-windows 2016
```

### Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--chunk-windows` | `2016` | 5-minute windows per chunk before checkpointing (2016 = 1 week; 288 = 1 day; ~8640 = ~1 month). |
| `--max-chunks` | `0` | Max chunks to process this invocation; `0` means run until `--stop-ts` (i.e. all remaining history). |
| `--start-ts` | `1615733100` | Backfill start (unix seconds). Default is the project's earliest snapshot (2021-03-14). Ignored when a checkpoint exists (it resumes instead). |
| `--stop-ts` | `0` | Backfill stop (unix seconds, exclusive). `0` means "now". |
| `--concurrency` | `4` | Max concurrent Wiki API fetches in flight (kept modest to be polite to the API). |
| `--pause` | `2.0` | Seconds to sleep between chunks. |
| `--max-records-per-write` | `50000` | Max records per InfluxDB 3 write request; keeps each write under the 10 MB limit. |
| `--checkpoint` | `scripts/backfill_checkpoint.json` | Path to the resumable checkpoint file. Delete it to start over. |

**Resuming:** on startup the script reads the checkpoint and continues from `next_start_ts`.
To restart from scratch, delete the checkpoint file. The checkpoint records the last committed
position plus running totals.

**Rough cost:** ~1.5 minutes per one-week chunk at `--concurrency 4`; the full ~4.5-year
history (~235 weekly chunks) is on the order of a few hours of wall time and ~470k API calls.

## Rollups / downsampling (`ge-pipeline rollup`, `scripts/rollup_history.py`)

### Why

InfluxDB 3 **Core** does not compact its Parquet files (compaction is Enterprise-only), so a
long backfill leaves thousands of small gen1 files. Wide or unbounded queries then trip the
`--query-file-limit` cap (default 432) and fail with `Query would scan N Parquet files,
exceeding the file limit` — which is why whole-store dashboards look empty. Raising the cap
(see [the file-limit troubleshooting section](#troubleshooting-grafanaqueries-return-no-data-influxdb-3-core-file-limit))
treats the symptom but the file count keeps growing.

A **rollup** is the structural mitigation you can run yourself: it re-reads the raw `itemPrice`
data in bounded time windows, downsamples each window with `date_bin` (hourly by default), and
writes the far smaller result into a **separate measurement** (default `itemPrice_1h`). A query
over `itemPrice_1h` scans dramatically fewer files, so historical dashboards and exports stay
fast even on Core.

> This is a rewrite **through the database**, not a file-level merge. The engine's catalog
> stays authoritative and no Parquet files are touched directly — the only safe DIY option on
> Core. (You cannot compact the raw files by hand: the catalog would still reference the old
> files and you'd corrupt the store.)

### Where it applies

- **Use the rollup** when you have a large backfilled history on Core and want fast long-range
  reads without upgrading to Enterprise. Point Grafana panels and `ge-pipeline export` at the
  rollup measurement for old ranges, and keep the raw `itemPrice` measurement for recent,
  fine-grained (5-minute) data.
- The rollup **reduces query file-scan cost**; it does **not** shrink the existing raw files on
  disk. To actually cut disk/file usage, pair it with a retention policy on the raw measurement
  (or drop raw ranges you've already rolled up).
- If you truly want automatic, ongoing compaction **while keeping the raw 5-minute
  data**, that only comes with **InfluxDB 3 Enterprise** — which has a free At-Home
  license for single-host use. See
  [Upgrading to InfluxDB 3 Enterprise](#upgrading-to-influxdb-3-enterprise-free-at-home-license).

### How

For a one-shot pass over a bounded range (in-process, not resumable):

```bash
# Roll up the last 30 days of raw data into itemPrice_1h at hourly resolution
ge-pipeline rollup --interval 1h --range 30d

# Roll up everything, a specific window, or specific items
ge-pipeline rollup --interval 1h --range all
ge-pipeline rollup --interval 1d --start 1615733100 --stop 1620000000
ge-pipeline rollup --interval 1h --range 7d --items 554,565
```

For a **full-history rollup** that is checkpointed and safe to interrupt (recommended for the
big backlog), use the driver script — same resumable discipline as the backfill:

```bash
# Roll up all raw history into itemPrice_1h, one week per window (resumable)
PYTHONPATH=. .venv/bin/python scripts/rollup_history.py --interval 1h

# Resume automatically from scripts/rollup_checkpoint.json; delete it to start over
```

#### `scripts/rollup_history.py` parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--interval` | `1h` | `date_bin` downsample interval (e.g. `1h`, `1d`). |
| `--measurement` | `itemPrice_1h` | Rollup measurement to write into (`[A-Za-z0-9_]` identifier). |
| `--window-seconds` | `604800` | Width of each read/aggregate/write window (1 week). Keeps each window's scan under the file cap. |
| `--start-ts` | `1615733100` | Rollup start (unix seconds); the project's earliest snapshot. Ignored when a checkpoint exists. |
| `--stop-ts` | `0` | Rollup stop (unix seconds, exclusive). `0` means "now". |
| `--max-windows` | `0` | Max windows to process this invocation; `0` = all up to `--stop-ts`. |
| `--pause` | `0.5` | Seconds to sleep between windows. |
| `--max-records-per-write` | `50000` | Max records per InfluxDB 3 write request; keeps each write under the 10 MB limit. |
| `--checkpoint` | `scripts/rollup_checkpoint.json` | Path to the resumable checkpoint file. Delete it to start over. |

**Aggregation detail:** price fields (`avgHighPrice`, `avgLowPrice`) keep their floating-point
mean; the volume fields (`highPriceVolume`, `lowPriceVolume`) are rounded to whole integers.
Rows whose every field is null are skipped. The rollup measurement is schema-identical to the
raw one, so the same query builders read it (`ge-pipeline export`/query helpers accept a
measurement override).

## Managing storage and the Core file limit

This section is the "why" behind the file-limit errors and lays out every option, so
you can pick one deliberately. It matters here because the project keeps **5-minute
resolution** for statistics, which rules out "just downsample and delete the raw data".

### How Core forms files (the fixed floor)

InfluxDB 3 Core writes data into **generation-1 (gen1) Parquet files** whose time span
is set by `--gen1-duration`, which on Core accepts only `1m`, `5m`, or `10m` (default
`10m`). Rows are placed into a file **by timestamp**, not by how large your write
request is — so writing "bigger batches" does **not** produce bigger files. Core also
has **no compaction** (merging gen1 files into larger generations is an
Enterprise-only service) and **no custom partitioning** (the Core `create database`
command exposes only `--retention-period`; week/month partition templates are
Dedicated/Clustered/Enterprise features).

Net effect: on Core, the coarsest a raw file can get is a 10-minute block. For ~4.5
years of 5-minute data that's a fixed floor of roughly a quarter-million files. A clean
one-shot reload can get you *down to* that floor if a resumable/interrupted backfill
fragmented some 10-minute blocks, but it cannot go below it, and small files re-grow as
ingestion continues.

### Your options (ranked, given the 5-minute requirement)

1. **Upgrade to InfluxDB 3 Enterprise (free At-Home license).** The only option that
   keeps full 5-minute fidelity **and** compacts small files automatically and
   ongoing. Same object store, no data migration. See
   [Upgrading to InfluxDB 3 Enterprise](#upgrading-to-influxdb-3-enterprise-free-at-home-license).
2. **Stay on Core, raise `--query-file-limit`, query time-bounded ranges.** Already in
   place in the compose stack. Treats the symptom; small files keep accumulating. See
   the [file-limit troubleshooting](#troubleshooting-grafanaqueries-return-no-data-influxdb-3-core-file-limit).
3. **Add the rollup as a companion (not a replacement).** Keep raw 5-minute data for
   statistics; point dashboards / long-range browsing at `itemPrice_1h` so the wide
   queries that were erroring hit far fewer files. See
   [Rollups / downsampling](#rollups--downsampling-ge-pipeline-rollup-scriptsrollup_historypy).
4. **Clean reload into a fresh database.** Only worth it if diagnostics show the store
   is fragmented well above the ~10-minute-block floor. Modest, one-shot payoff; files
   re-grow. Batch size is irrelevant to the result (see the floor above).

> **Rule of thumb:** if you need fast queries over long ranges *and* the raw 5-minute
> points, Enterprise At-Home is the clean answer. The rollup is the best Core-only
> companion. A reload is a niche cleanup, not a real fix.

## Upgrading to InfluxDB 3 Enterprise (free At-Home license)

InfluxDB 3 **Enterprise** is a strict superset of Core: it runs against the *same*
object store / data directory (no data migration, no schema change) and adds the
**compaction service** that merges Core's small gen1 files into larger generations
automatically. That is what makes long-range queries fast **while keeping the full
5-minute raw data** — the thing the Core-only rollup can't do on its own.

InfluxData offers a free **At-Home** license for hobbyist, non-commercial use. Findings
from the InfluxData docs ([license](https://docs.influxdata.com/influxdb3/enterprise/admin/license/),
[upgrade guide](https://docs.influxdata.com/influxdb3/core/admin/upgrade-to-enterprise/)),
rephrased for compliance:

| Aspect | At-Home license |
|--------|-----------------|
| Price | Free |
| Expiration | Never |
| CPU limit | 2 CPU cores (per cluster) |
| Topology | Single node only |
| Use restriction | At-home / non-commercial only |
| Features | All Enterprise features except multi-node and commercial use — **including compaction** |

The `.85` box is a single-host home deployment, so it fits the At-Home terms. The 2-CPU
cap is the main practical constraint — Enterprise will use at most 2 cores on that host.

### Important caveats before you start

- **No downgrade.** Once Enterprise starts against your data dir it makes catalog
  changes that Core cannot read. You **cannot go back to Core** except by restoring a
  backup taken *before* the upgrade. **Back up the data directory / volume first.**
- **Enterprise needs a `--cluster-id`** that Core does not use. Pick one (e.g.
  `ge-cluster`); it becomes part of the storage path.
- **License activation is by email.** In Docker the interactive prompt doesn't work, so
  you must pass `INFLUXDB3_LICENSE_EMAIL` and `INFLUXDB3_LICENSE_TYPE=home`, then click
  the verification link in the email InfluxData sends. After verification the license
  (a JWT) is written into the object store at
  `<data-dir>/<cluster-id>/trial_or_home_license`.
- **Treat the license file as a secret** — do not commit it or paste its contents into
  logs/issues.
- **The `latest` Docker tag is changing.** Pin a specific tag; use
  `influxdb:3-enterprise` (or a version tag), not `latest`.

### Step by step (this project's Docker Compose stack on `.85`)

The current stack builds a Core image and stores data in the named volume
`influxdb3_data` at `/var/lib/influxdb3` (see `influxdb/docker-compose.yml`). The
upgrade points an Enterprise container at that **same volume**.

1. **Back up first (required — no downgrade).** On the `.85` host, with the DB stopped:
   ```bash
   cd influxdb
   docker compose stop influxdb3
   # Copy the whole data volume to a timestamped tarball you can restore from.
   docker run --rm -v influxdb3_data:/data -v "$PWD":/backup alpine \
     tar czf "/backup/influxdb3_data.pre-enterprise.$(date +%s).tar.gz" -C /data .
   ```
   Keep that tarball until you're confident the upgrade is good.

2. **Add a Compose service for Enterprise** (a separate service keeps the Core one
   intact for reference; only one runs at a time). In `influxdb/docker-compose.yml`,
   add alongside `influxdb3`:
   ```yaml
     influxdb3-enterprise:
       image: influxdb:3-enterprise            # pin a version tag in production
       container_name: ge-influxdb3-enterprise
       restart: unless-stopped
       command:
         - influxdb3
         - serve
         - --node-id=${INFLUXDB3_NODE_ID:-ge-node}     # same node id as Core
         - --cluster-id=${INFLUXDB3_CLUSTER_ID:-ge-cluster}
         - --license-type=home
         - --object-store=file
         - --data-dir=/var/lib/influxdb3               # SAME data dir as Core
         - --http-bind=0.0.0.0:${INFLUXDB3_CONTAINER_PORT:-8181}
         - --without-auth
         - --query-file-limit=${INFLUXDB3_QUERY_FILE_LIMIT:-1000000}
       environment:
         INFLUXDB3_LICENSE_EMAIL: ${INFLUXDB3_LICENSE_EMAIL:?set your license email}
       ports:
         - "${INFLUXDB3_HOST_PORT:-8181}:${INFLUXDB3_CONTAINER_PORT:-8181}"
       volumes:
         - influxdb3_data:/var/lib/influxdb3           # reuse the Core volume
   ```
   Add `INFLUXDB3_LICENSE_EMAIL=you@example.com` and `INFLUXDB3_CLUSTER_ID=ge-cluster`
   to `influxdb/.env` (both are git-ignored — do not commit the email if you'd
   rather not).

3. **Start Enterprise (Core stays stopped — never run both on the same volume at once):**
   ```bash
   docker compose up -d influxdb3-enterprise
   docker compose logs -f influxdb3-enterprise      # watch for the license prompt/notice
   ```

4. **Verify your email.** Check the inbox for the address you set and click the
   verification link. Until you do, the server won't have an active license.

5. **Confirm the upgrade and license:**
   ```bash
   docker compose exec influxdb3-enterprise influxdb3 --version
   docker compose exec influxdb3-enterprise influxdb3 show license --host http://localhost:8181
   curl -s http://localhost:8181/health           # -> OK
   ```
   Then run a normal query (or `ge-pipeline` command from the workstation) to confirm
   your existing `GEItemPrices` data is intact and readable.

6. **Let compaction run.** With Enterprise active, the compactor begins merging the
   backlog of small gen1 files in the background. Wide/long-range queries get faster
   over time and you can lower `--query-file-limit` back toward defaults once compaction
   has caught up — all while the raw 5-minute points remain queryable.

### What this changes for the pipeline

Nothing in `ge_pipeline` needs to change: the client still connects to
`http://<host>:8181` with a token and database name, the schema is identical, and all
CLI commands (`ingest`, `backfill`, `rollup`, `export`, …) work unchanged. Grafana's
FlightSQL datasource on `.85` also stays as-is. The rollup remains useful but becomes
optional once compaction keeps raw-data queries fast.

## Migrating from InfluxDB v2

If you have an existing InfluxDB v2 server, `ge-pipeline migrate` copies its `itemPrice` data
1:1 into the v3 database (no schema change). Set the `V2_*` variables in `.env`, ensure both
servers are reachable, then:

```bash
ge-pipeline migrate   # reports records read from v2 and written to v3
```

The v2 client is an optional dependency; install it with `pip install -e ".[migration]"`.

## API Reference

- Base URL: `https://prices.runescape.wiki/api/v1/osrs`
- 5-minute endpoint: `/5m?timestamp={unix_timestamp}`
- Requires `User-Agent` header (set to `GEoutlier-detection`)
