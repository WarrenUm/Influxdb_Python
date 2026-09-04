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
- `provisioning/datasources/influxdb.yaml` — InfluxDB v2 Flux data source
- `provisioning/dashboards/` + `dashboards/ge-prices.json` — line / candlestick / gauge panels
- `provisioning/alerting/deviation-alert.yaml` — price-deviation alert (suppressed on zero/undefined)

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
