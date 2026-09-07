# Grafana low-code dashboard (OSRS GE prices)

This directory is the low-code front end of the OSRS Grand Exchange price
pipeline. It runs a **containerized Grafana** that reads the price data
**directly from InfluxDB 3 over SQL (FlightSQL/gRPC)** — no application
code in between. It is the sibling of the React SPA, which reads the same data
through the FastAPI query layer (`ge-pipeline serve`); Grafana instead talks to
the database itself. (The store is now InfluxDB 3 **Enterprise**, upgraded from Core;
the FlightSQL datasource is unchanged — Enterprise is a superset of Core.)

```
RuneScape Wiki API ─► ge-pipeline ingest ─► InfluxDB 3 Enterprise ─┬─► Grafana (this folder, direct SQL)
                                             (localhost:8181)       └─► ge-pipeline serve ─► React SPA
```

Everything here is **provisioning-as-code**: starting the container wires up the
data source, dashboard, and alert automatically, so the dashboard is
reproducible and version-controlled rather than clicked together in the UI.

## Live deployment (split across two hosts)

The `localhost:8181` in the diagram above is the portable single-host default.
The **current live deployment is split across two machines**, which matters when
debugging empty dashboards:

- **Server `192.168.1.85`** runs both **InfluxDB 3 Enterprise** (`:8181`) and **Grafana**
  (`:3001`, managed via Dockge). Grafana reads the InfluxDB on that same host.
- **Workstation `192.168.1.7`** (`pop-os`) runs the ingestion / backfill, writing
  over the LAN to `192.168.1.85:8181`.

So on the `.85` Grafana the FlightSQL datasource `host` is `192.168.1.85:8181`
(bare `host:port`), database `GEItemPrices`, uid `influxdb-v3-ge`. The two
dashboards (`ge-prices`, `ge-db-stats`) were deployed there via the HTTP API
(see "Copying the dashboards into a Dockge-managed Grafana" → Option C).

> **Why dashboards may look empty:** only `scripts/backfill_history.py` is
> currently running against `.85`, and it loads history **oldest→newest** (still
> in the 2021 range). There is **no near-now data** yet, so panels defaulting to
> `now-24h`/`now-7d` render empty. To see data now, set the time range to a window
> the backfill has already loaded (e.g. 2021). The whole-store stat panels show
> data regardless of the time picker (but are slow while the backfill runs).

### If panels are empty even on a loaded time range: the query file limit

InfluxDB 3 caps how many Parquet files one query may scan (`--query-file-limit`,
default **432**). Since the upgrade to **Enterprise** the compaction service keeps the
file count low, so this sits near the default (`2000`) and rarely bites. It can still
appear **transiently** during a large fresh backfill before compaction catches up: wide
queries fail with `Query would scan N Parquet files, exceeding the file limit`, which
breaks the `$itemID` variable (empty dropdown → every price panel blank) and the
whole-store stat panels, even though bounded queries work.

Fix (set in `../influxdb/docker-compose.yml`): raise the limit temporarily and
**recreate** the InfluxDB container so the new `serve` arg takes effect:

```bash
cd influxdb                                        # on the InfluxDB host (192.168.1.85)
docker compose up -d --force-recreate influxdb3    # a plain restart does NOT apply it
docker inspect ge-influxdb3-enterprise --format '{{json .Args}}' | tr ',' '\n' | grep -A1 query-file-limit
```

**Do not set it to `0`** — that means a literal 0-file limit and *every* query fails
("scan 0 Parquet files"). Use a finite integer (`INFLUXDB3_QUERY_FILE_LIMIT`, default
`2000`). Higher = more memory / slower wide scans.

## TODO

- [ ] **Run the 5-minute catch-up ingestion against the remote InfluxDB
  (`192.168.1.85:8181`)** so the dashboards get recent/live prices instead of only
  backfilled history. Run `ge-pipeline ingest` (or the `ge_pipeline/scheduler.py`
  APScheduler daemon) with `INFLUXDB3_HOST_URL=http://192.168.1.85:8181` and
  `INFLUXDB3_DATABASE_NAME=GEItemPrices`. This can run alongside the historical
  backfill; the catch-up fills forward from the latest stored timestamp while the
  backfill fills older history.

## Folder layout

```
grafana/
├── docker/                            # containerized Grafana (run it from here)
│   ├── docker-compose.yml             # Grafana service, mounts provisioning + dashboards
│   ├── .env.example                   # ports, InfluxDB host/token/db, admin login
│   ├── run.sh                         # up / down / destroy / logs / status helper
│   └── .gitignore                     # ignores local .env
├── provisioning/
│   ├── datasources/influxdb.yaml      # InfluxDB 3 FlightSQL (SQL) data source
│   ├── dashboards/dashboards.yaml     # dashboard provider -> /var/lib/grafana/dashboards
│   └── alerting/deviation-alert.yaml  # price-deviation alert rule (v3 SQL)
├── dashboards/
│   ├── ge-prices.json                 # time series + candlestick + gauge, $itemID variable
│   └── ge-db-stats.json               # DB health + ingestion stats (no variable)
└── README.md
```

## Prerequisites

- Docker Engine with the Compose plugin (`docker compose`).
- The **InfluxDB 3 container already running** on this machine — see
  [`../influxdb/`](../influxdb/). Start it first (`cd ../influxdb && ./run.sh`)
  and confirm `curl -s http://localhost:8181/health` returns `OK`.
- Some data ingested (`ge-pipeline ingest` or the backfill script), otherwise the
  dashboards render empty and the `$itemID` dropdown has no options.

## Quick start

```bash
cd grafana/docker
./run.sh
```

That copies `.env.example` to `.env` (if needed), starts Grafana, installs the
FlightSQL datasource plugin, and waits for it to become healthy. When it
finishes, open the UI and find the **OSRS GE Prices** dashboard under the
**OSRS GE** folder. Default login is `admin` / `admin` (Grafana prompts you to
change it on first sign-in).

- On the host itself: **http://localhost:3001**
- From another machine on the LAN: **http://192.168.1.85:3001** (this host's
  address). Grafana binds to `0.0.0.0`, so it is reachable on the LAN with no
  extra config; just make sure the host firewall allows inbound TCP 3001.

### Manual equivalent (if you prefer not to use run.sh)

```bash
cd grafana/docker
cp .env.example .env          # edit if desired
docker compose up -d          # start Grafana with provisioning mounted
docker compose ps             # wait until healthy
```

### Deploying changes to the remote host (`192.168.1.85`)

On `.85` this stack is *flat*: `grafana/docker/` deploys to `/opt/stacks/grafana/`, and
`grafana/provisioning/` + `grafana/dashboards/` deploy to the sibling `/opt/stacks/
provisioning/` and `/opt/stacks/dashboards/` (the host compose mounts `../provisioning`
and `../dashboards`). Push edits from the dev workstation with the repo's deploy script,
which handles all three folders and force-recreates the container:

```bash
scripts/deploy.sh grafana             # from the repo root
scripts/deploy.sh grafana --dry-run   # preview first
```

The script excludes `.env`, so the host's Grafana admin password/token is never
overwritten or deleted; `--delete` otherwise mirrors these folders to the repo (the repo
is the source of truth — don't hand-edit them on `.85`). To add a dashboard, drop the
JSON in `dashboards/` here and redeploy. See the top-level `README.md` → "Updating the
deployment from the workstation" and `.kiro/steering/development-workflow.md` for
prerequisites and full `--delete` behavior.

## Managing Grafana

```bash
cd grafana/docker
./run.sh status     # container status + health
./run.sh logs       # follow Grafana logs
./run.sh down       # stop Grafana; DATA IS PRESERVED in the volume
./run.sh destroy    # stop AND delete the data volume (irreversible; prompts first)
```

Grafana's own state (users, UI edits, alert state) lives in the named volume
`grafana_ge_data` and survives `./run.sh down`. Only `./run.sh destroy` /
`docker compose down -v` deletes it. The dashboards and alert themselves are
provisioned from these files, so they come back on every start regardless.

## How it connects to InfluxDB

Both containers run on the same host. **InfluxDB publishes its port (8181) on
the host**, and Grafana — in its own container — reaches it through the host
gateway:

```
┌─ ge-grafana container ─┐        host              ┌─ ge-influxdb3-enterprise ─┐
│  Grafana :3000 ►:3001  │  host.docker.internal    │  InfluxDB 3 Enterprise    │
│  FlightSQL datasource ─┼──────────► :8181 ────────┼─► :8181 (HTTP + Flight)   │
└────────────────────────┘   (published port)       └───────────────────────────┘
```

- The compose file adds `extra_hosts: host.docker.internal:host-gateway` so the
  name resolves to the host on Linux (as well as macOS/Windows).
- The datasource (`provisioning/datasources/influxdb.yaml`) connects to the bare
  `host:port` in `${GRAFANA_INFLUXDB3_HOST}`, which defaults to
  `host.docker.internal:8181`. The FlightSQL plugin needs a bare `host:port`,
  **not** an `http://` URL. InfluxDB 3 serves its HTTP API and the
  FlightSQL/gRPC endpoint on the same port.
- Query `mode: 1` selects **SQL**. The target database
  (`${INFLUXDB3_DATABASE_NAME}`, default `GEItemPrices`) is passed via FlightSQL
  connection metadata, and the token comes from `${INFLUXDB3_AUTH_TOKEN}`.
- **Auth:** while InfluxDB runs with `--without-auth` (the local/dev default),
  any non-empty token is accepted. For a networked deployment, set a real token
  here and switch InfluxDB to an authed mode.

Because InfluxDB runs on this same host (`192.168.1.85`), the default
`host.docker.internal:8181` works as-is and keeps the config portable. If you
prefer to pin it to the machine's LAN address, set
`GRAFANA_INFLUXDB3_HOST=192.168.1.85:8181` in `grafana/docker/.env`. Use an
explicit address like this if InfluxDB ever moves to a different host.

## Copying the dashboards into a Dockge-managed Grafana

If your Grafana isn't the `grafana/docker` stack in this repo but a container you
run through **Dockge**, the dashboards in `dashboards/` still work unchanged —
they only depend on a FlightSQL datasource with uid `influxdb-v3-ge`. What
differs is *how the JSON reaches the container*. Pick the option that matches how
your Dockge stack mounts Grafana.

Dockge stores each stack as a plain folder of compose files on the host (default
stacks root `/opt/stacks`, set by `DOCKGE_STACKS_DIR`), e.g.
`/opt/stacks/grafana/compose.yaml`. Nothing is hidden — you edit the same files
Dockge runs. The examples below assume a stack named `grafana`; adjust the path
to your stack's name.

### Option A — bind-mount a dashboards folder (recommended, provisioned)

This mirrors how the repo's own compose works and makes dashboards
reproducible: Grafana auto-loads every JSON in a folder, and adding a file is all
it takes. You need the datasource + dashboard *providers* mounted too, so Grafana
knows about the FlightSQL datasource and where to read dashboards.

1. On the Docker host, create folders next to your Grafana stack and copy this
   repo's `provisioning/` and `dashboards/` into them:

   ```bash
   # on the Docker host, for a Dockge stack at /opt/stacks/grafana
   mkdir -p /opt/stacks/grafana/provisioning /opt/stacks/grafana/dashboards
   # copy from wherever you have this repo checked out on that host:
   cp -r /path/to/repo/grafana/provisioning/* /opt/stacks/grafana/provisioning/
   cp /path/to/repo/grafana/dashboards/*.json /opt/stacks/grafana/dashboards/
   ```

2. In Dockge, edit the `grafana` stack's compose so the service mounts them
   (paths on the left are relative to the stack folder, which Dockge sets as the
   compose working directory):

   ```yaml
   services:
     grafana:
       image: grafana/grafana:latest
       environment:
         # the datasource provisioning file expands these at startup
         GRAFANA_INFLUXDB3_HOST: host.docker.internal:8181
         INFLUXDB3_DATABASE_NAME: GEItemPrices
         INFLUXDB3_AUTH_TOKEN: local-dev-token
         GF_INSTALL_PLUGINS: influxdata-flightsql-datasource
       extra_hosts:
         - "host.docker.internal:host-gateway"
       volumes:
         - ./provisioning:/etc/grafana/provisioning:ro
         - ./dashboards:/var/lib/grafana/dashboards:ro
   ```

3. Redeploy the stack from the Dockge UI (**Restart**/**Update**). The dashboard
   provider (`provisioning/dashboards/dashboards.yaml`) scans
   `/var/lib/grafana/dashboards` every 30s, so the **OSRS GE Database & Ingestion
   Stats** and **OSRS GE Prices** dashboards appear under the **OSRS GE** folder.
   To add or update a dashboard later, just drop/replace the JSON in
   `/opt/stacks/grafana/dashboards/` — no redeploy needed.

### Option B — copy straight into a running container (quick, one-off)

If you don't want to touch the compose and the datasource already exists in that
Grafana, copy the JSON into the container's dashboards path with `docker cp` and
let the provider pick it up. This only persists if that path is on a volume;
otherwise it's lost on container recreation (use Option A to make it durable).

```bash
# 'ge-grafana' is the container name; use yours from `docker ps`
docker cp grafana/dashboards/ge-db-stats.json ge-grafana:/var/lib/grafana/dashboards/ge-db-stats.json
```

If that Grafana has no dashboard *provider* configured, this path won't be
scanned — use Option C instead.

### Option C — import via the HTTP API (no file mounts, fully durable)

This writes the dashboard into Grafana's own database, so it survives restarts
regardless of mounts. It needs the JSON wrapped in an import envelope and a login
or API token. Run against your Grafana's URL (Dockge typically publishes it on a
host port you chose):

```bash
GRAFANA_URL=http://localhost:3001         # your Dockge Grafana address
# wrap the dashboard file and POST it (jq builds the {dashboard:...} envelope)
jq '{dashboard: ., overwrite: true, folderUid: ""}' grafana/dashboards/ge-db-stats.json \
  | curl -s -u admin:admin -H "Content-Type: application/json" \
      -X POST "$GRAFANA_URL/api/dashboards/db" -d @-
```

You can also import by hand: Grafana UI -> **Dashboards -> New -> Import ->
Upload JSON file**, then pick the FlightSQL datasource when prompted.

> Whichever option you choose, that Grafana must have a FlightSQL datasource the
> dashboards can bind to. Option A provisions it for you. For B and C, either
> provision the datasource (copy `provisioning/datasources/influxdb.yaml`) or
> create one in the UI. If its uid isn't `influxdb-v3-ge`, either set that uid on
> the datasource or edit the `datasource.uid` fields in the dashboard JSON to
> match, otherwise panels show "datasource not found".

## Configuration

Copy `docker/.env.example` to `docker/.env` and adjust:

| Variable | Default | Meaning |
|----------|---------|---------|
| `GRAFANA_HOST_PORT` | `3001` | Host port the Grafana UI is published on. |
| `GRAFANA_INFLUXDB3_HOST` | `host.docker.internal:8181` | Bare `host:port` of the InfluxDB 3 FlightSQL endpoint, as seen from inside the Grafana container. |
| `INFLUXDB3_DATABASE_NAME` | `GEItemPrices` | The v3 database the dashboards query. |
| `INFLUXDB3_AUTH_TOKEN` | `local-dev-token` | Token sent to InfluxDB (any non-empty value in `--without-auth` mode). |
| `GF_SECURITY_ADMIN_USER` | `admin` | Initial Grafana admin username. |
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Initial Grafana admin password (change beyond local dev). |
| `GF_INSTALL_PLUGINS` | `influxdata-flightsql-datasource` | Plugins installed on startup (FlightSQL is required for v3 SQL). |

`docker/.env` is git-ignored so tokens and passwords never land in source control.

## Data assumptions

These match what `ge-pipeline` writes (see the project storage schema):

- **Engine:** InfluxDB 3 Enterprise at `http://localhost:8181` (HTTP + FlightSQL/gRPC
  on the same port).
- **Database:** `GEItemPrices`.
- **Table / measurement:** `itemPrice`.
- **Tag:** `itemID` (string).
- **Fields:** `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`.

All queries use these camelCase identifiers double-quoted so SQL keeps them
case-sensitive (e.g. `"itemPrice"`, `"itemID"`, `"avgHighPrice"`).

## Dashboard contents (`dashboards/ge-prices.json`)

The **OSRS GE Prices** dashboard (uid `ge-prices`). All panels filter on the
selected item and use Grafana's `$__timeFrom()` / `$__timeTo()` time macros.

- **`$itemID` variable** — a dropdown populated from
  `SELECT DISTINCT "itemID" FROM "itemPrice"`.
- **Avg High / Low Price** (time series) — `avgHighPrice` and `avgLowPrice` over
  the panel time range.
- **Candlestick** — the same two fields mapped to candles (`avgHighPrice` →
  high/close, `avgLowPrice` → low/open).
- **Latest Combined Volume** (gauge) — the most recent
  `highPriceVolume + lowPriceVolume` for the item, with green/yellow/red
  thresholds.

## Stats dashboard (`dashboards/ge-db-stats.json`)

The **OSRS GE Database & Ingestion Stats** dashboard (uid `ge-db-stats`) is a
health/operations view of the store itself, not of any single item. It needs no
`$itemID` variable. Use it to watch the database grow as the 5-minute catch-up
and the chunked history backfill run.

It queries the same `itemPrice` table over the same FlightSQL datasource, using
the double-quoted camelCase identifiers and the `date_bin(...)` binning that
InfluxDB 3 SQL uses elsewhere in the project. Panels:

- **Database overview** (whole store, unbounded by the time picker):
  - **Total records stored** — `COUNT(*)` over `itemPrice`.
  - **Distinct items tracked** — `COUNT(DISTINCT "itemID")`.
  - **Snapshots captured** — `COUNT(DISTINCT time)` (each 5-min snapshot fans out
    to ~3k item rows).
  - **Data freshness** — `MAX(time)` rendered as age-from-now, green/yellow/red at
    10 min / 30 min so a stalled ingest turns red.
  - **Earliest / Latest snapshot stored** — the loaded history window; the
    earliest edge advances backward as the backfill fills older weeks.
- **Ingestion activity** (bounded by the dashboard time range, so these stay
  fast): **records per hour**, **distinct items per hour**, and **snapshots per
  hour** (12 = a fully covered hour at 5-minute granularity).
- **History coverage & per-item volume** (whole store): **records per day across
  all stored history** (the full historical footprint the backfill has loaded),
  **top 20 items by rows stored**, and **most recently updated items**.

Note on cost: the overview and full-history panels are deliberate full-table
scans, so they can be slow while a heavy backfill is running against the same
InfluxDB instance. They refresh on the dashboard interval (default 1m); the
per-hour panels are cheap because the time picker bounds them. Widen or narrow
the time range to trade detail for speed.

## Deviation alert (`provisioning/alerting/deviation-alert.yaml`)

An alerting rule (**GE avgHighPrice deviation**) that fires when the latest
`avgHighPrice` for the selected item deviates from its trailing moving average by
more than a configured factor (default `0.25`, i.e. 25%). It is written in **v3
SQL** against the same FlightSQL datasource:

- Query **A** computes the fractional deviation `|latest - avg| / avg` over the
  most recent 12 samples (~1h at 5-minute granularity).
- Expression **C** thresholds that deviation (`> 0.25`).

To avoid false positives, the SQL only emits a row when there is a usable latest
value and a non-zero average (`NULLIF` guards divide-by-zero); combined with
`noDataState: OK`, the alert cannot fire on missing or degenerate data.

Tune it by editing the file: change `LIMIT 12` in the `recent` CTE for the
moving-average window, or the threshold `params: [0.25]` in expression C for
sensitivity.
