# osrsGECurrentProcess

A Docker container that keeps the InfluxDB 3 store current with the latest OSRS
Grand Exchange prices by running **catch-up ingestion every 5 minutes**.

It runs the `ge_pipeline` APScheduler daemon (`python -m ge_pipeline.scheduler`).
Each scheduled run:

1. Queries InfluxDB 3 for the latest stored timestamp (reference item `554`).
2. Lazily generates every missing 5-minute window up to now.
3. Fetches those snapshots from the RuneScape Wiki API (bounded concurrency).
4. Validates and writes the records to the `GEItemPrices` database.

Because each run recomputes the gap from the latest stored timestamp, a run that
follows a failed run automatically backfills the windows the failure left behind
— the scheduler needs no gap-tracking of its own.

## How it differs from the other ingestion paths

| Path | What it does | When to use |
|------|--------------|-------------|
| **osrsGECurrentProcess** (this container) | 5-minute catch-up, forever | keep the DB current (near-now data) |
| `scripts/backfill_history.py` | chunked, resumable deep-history load | one-time load of years of history |
| `ge-pipeline ingest` | a single manual catch-up run | ad-hoc / debugging |

## Layout

This folder follows the repo's one-folder-per-container convention:

```
osrsGECurrentProcess/
├── Dockerfile           # installs the ge_pipeline package, runs the scheduler
├── docker-compose.yml   # build context is the REPO ROOT (../)
├── .env.example         # -> .env (git-ignored): InfluxDB target + token
├── run.sh               # up / build / down / restart / logs / status
└── README.md
```

> The compose `build.context` is the repository root (`..`) because the image
> installs the `ge_pipeline` package (it needs `pyproject.toml` and the
> `ge_pipeline/` source). The compose/`.env`/`run.sh` files still live here.

## Run

```bash
cp .env.example .env         # then set INFLUXDB3_HOST_URL / token / db
./run.sh up                  # build + start (detached)
./run.sh logs                # follow the scheduler logs
./run.sh status              # container + health
./run.sh down                # stop + remove (stateless; nothing to preserve)
```

## Configuration

All three InfluxDB 3 target variables are required (read lazily by
`ge_pipeline.config.get_settings`):

- `INFLUXDB3_HOST_URL` — where to write. In the split live deployment the
  ingestion writes over the LAN to `http://192.168.1.85:8181`. If you run this
  container on the **same** host as InfluxDB via compose, put both on a shared
  docker network and use the service name (`http://influxdb3:8181`) instead.
- `INFLUXDB3_AUTH_TOKEN` — any non-empty value while the server runs
  `--without-auth`.
- `INFLUXDB3_DATABASE_NAME` — `GEItemPrices`.
- `LOG_LEVEL` — daemon log verbosity (default `INFO`).

## Deploy

Wired into the workstation deploy script:

```bash
scripts/deploy.sh osrs-current           # rsync + force-recreate on the host
scripts/deploy.sh osrs-current --status
scripts/deploy.sh all                    # influxdb + grafana + osrs-current
```
