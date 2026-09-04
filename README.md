# OSRS Grand Exchange Price Collector

A data ingestion pipeline that fetches Old School RuneScape (OSRS) Grand Exchange item price data from the [RuneScape Wiki Prices API](https://prices.runescape.wiki/api/v1/osrs) and stores it in a local InfluxDB v2 time-series database. It also serves the data over a FastAPI query service and exports bulk datasets for a GE outlier-detection project.

## Architecture

```
RuneScape Wiki API ──► ge-pipeline ingest ──► InfluxDB v2 ──► ge-pipeline serve (FastAPI)
   (5-min price snapshots)   (ge_pipeline.ingestion)   (localhost:8086)   (ge_pipeline.api)
                                     │                                            │
                              ge_pipeline.influx                          ge-pipeline export
                            (client, writes, queries)                    (bulk NDJSON/CSV/Parquet)
```

The modernized backend lives in the `ge_pipeline` package:

| Module | Purpose |
|--------|---------|
| `ge_pipeline/config.py` | Environment-backed settings (`get_settings`) |
| `ge_pipeline/influx.py` | InfluxDB client, batched writes, parameterized queries |
| `ge_pipeline/ingestion.py` | Async catch-up ingestion (timestamp gaps, null filtering, batching) |
| `ge_pipeline/api.py` | FastAPI query service |
| `ge_pipeline/data_access.py` | Streaming bulk export (NDJSON/CSV/Parquet) |
| `ge_pipeline/scheduler.py` | APScheduler periodic ingestion |
| `ge_pipeline/cli.py` | Typer CLI entry point (`ge-pipeline`) |

## Data Model

- **Measurement**: `itemPrice`
- **Tag**: `itemID` (string)
- **Fields**: `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`
- **Write precision**: seconds
- **Granularity**: 5-minute intervals

## Prerequisites

- Python 3.10+
- InfluxDB v2 running locally

## Setup

1. Install and start InfluxDB v2:
   ```bash
   influxd
   ```

2. Install the package (with test extras) into a virtualenv:
   ```bash
   pip install -e ".[test]"
   ```
   Dependencies are pinned in `pyproject.toml` (mirrored in `requirements.txt`).

3. Configure environment variables. Copy the example file and fill in your values:
   ```bash
   cp .env.example .env
   ```
   Required keys: `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_BUCKET`
   (`.env` is git-ignored so tokens never land in source control).

4. Prepare InfluxDB (health check, create the bucket, verify write access):
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
```

`ge-pipeline ingest` queries InfluxDB for the most recent data point, calculates the gap to now, and backfills all missing 5-minute intervals from the RuneScape Wiki API.

## Notebooks

- `DatabaseSetup.ipynb` — InfluxDB setup guide and experimentation
- `testFunctions.ipynb` — query testing notebook
- `testGettingandwritingtimestamps.ipynb` — API response testing notebook

## API Reference

- Base URL: `https://prices.runescape.wiki/api/v1/osrs`
- 5-minute endpoint: `/5m?timestamp={unix_timestamp}`
- Requires `User-Agent` header (set to `GEoutlier-detection`)
