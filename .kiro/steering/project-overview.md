# Project Overview

This project collects Old School RuneScape (OSRS) Grand Exchange item price data
from the RuneScape Wiki Prices API and stores it in a local InfluxDB v2 time-series
database. The backend is a modernized Python package (`ge_pipeline`) with async
ingestion, schema validation, a CLI, scheduling, a scale-oriented data access layer,
and a FastAPI query service. Two front ends visualize and detect price outliers: a
low-code Grafana-on-InfluxDB dashboard and a custom React + TypeScript single-page app.

## Architecture

InfluxDB v2 is the single source of truth. Both UIs read from the same store: Grafana
reads InfluxDB directly, while the React SPA reads through the FastAPI query layer.

```
RuneScape Wiki API ─► ge-pipeline ingest ─► InfluxDB v2 ─► ge-pipeline serve (FastAPI) ─► React SPA
   (5-min snapshots)      (ingestion)        (localhost:8086)      (api)                    (web/)
                                                   │
                                                   └─► Grafana dashboards / ge-pipeline export
```

## Backend package: `ge_pipeline/`

| Module | Purpose |
|--------|---------|
| `config.py` | Lazy, cached env-backed settings via `get_settings()` (no import-time side effects) |
| `models.py` | pydantic v2 models (`ItemPricePoint`, `FiveMinuteSnapshot`) validating raw API JSON |
| `influx.py` | InfluxDB client lifecycle, batched writes, parameterized Flux queries |
| `retry.py` | Retry with exponential backoff for transient failures |
| `ingestion.py` | Async catch-up ingestion (lazy timestamp gaps, null filtering, batching) |
| `data_access.py` | Scale-oriented reads: time chunking, streaming export, pagination, feature frames |
| `outliers.py` | Pluggable outlier-detection registry (z-score, IQR) |
| `api.py` | FastAPI query/export service consumed by the SPA and ML clients |
| `scheduler.py` | APScheduler daemon triggering catch-up every 5 minutes |
| `cli.py` | Typer CLI entry point (`ge-pipeline`) |
| `errors.py` | Shared exceptions: `ConfigError`, `TransientError`, `NonTransientError` |

## Front ends

- **React SPA** — `web/` (Vite + React + TypeScript, Lightweight Charts, dark theme).
  Item search, candlestick + volume charts, and outlier highlighting over the FastAPI API.
- **Grafana** — `grafana/` provisioned InfluxDB Flux data source, dashboards, and a
  deviation alert rule.

## Data Flow (ingestion)

1. Query InfluxDB for the latest stored timestamp (reference item "554")
2. Lazily generate missing 5-minute timestamps from latest → now (`iter_missing_timestamps`)
3. Fetch each snapshot from the Wiki API concurrently (bounded semaphore, async httpx)
4. Validate JSON into a `FiveMinuteSnapshot`; build records, filtering fully-null items
5. Batch-write records to the InfluxDB bucket "GEItemPrices"

## Key Dependencies

- Backend: `influxdb-client`, `pandas`, `numpy`, `python-dotenv`, `httpx`, `pydantic>=2`,
  `typer`, `apscheduler`, `fastapi`, `uvicorn`, `pyarrow` (pinned in `pyproject.toml`)
- Tests: `pytest`, `hypothesis`, `testcontainers`
- Frontend: `react`, `typescript`, `vite`, `lightweight-charts`, `vitest`
- InfluxDB v2 running at localhost:8086

## Storage Schema

- Measurement: `itemPrice`
- Tags: `itemID` (string)
- Fields: `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`
- Write precision: seconds; granularity: 5 minutes
- Bucket / Org: `GEItemPrices` / `Ge-data-project`
- API User-Agent: `GEoutlier-detection`

## Notebooks (documentation / experimentation only)

- `DatabaseSetup.ipynb` — InfluxDB setup guide
- `testFunctions.ipynb`, `testGettingandwritingtimestamps.ipynb` — API/query scratchpads
