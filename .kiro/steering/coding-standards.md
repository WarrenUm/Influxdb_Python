# Coding Standards

## Python Style

- Follow PEP 8 naming conventions (snake_case for functions and variables)
- Use type hints on all public function signatures (use `X | None` unions)
- Add Google-style docstrings to all public functions (summary, Args, Returns, Raises)
- Keep functions focused and single-purpose
- Prefer lazy imports inside CLI commands / entry points to keep module import lightweight

## Configuration

- Never hardcode secrets or tokens in source files
- Load all settings lazily through `get_settings()` (python-dotenv); no import-time env reads
- Required env vars: `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_BUCKET`
- `get_settings()` caches the result and raises `ConfigError` naming the first missing var
- Keep `.env` git-ignored; `.env.example` documents the required keys with placeholders

## Error Handling

- Classify failures: `TransientError` (timeout, connection error, HTTP 5xx/429) vs
  `NonTransientError` (non-429 4xx); raise `ConfigError` for missing config
- Retry transient failures with exponential backoff (`retry.py`); honor `Retry-After` on 429
- Validate raw API JSON through the pydantic models; centralize null handling there
- On per-snapshot failure, log the offending timestamp and continue; never abort the whole run
- On final batch-write failure, log and drop the batch so ingestion can proceed

## Database Patterns

- Reuse a single InfluxDB client across all operations in a run (never open/close per iteration)
- Close the client even on partial failure (try/finally or context manager)
- Use parameterized Flux queries only — never interpolate user item IDs or ranges into query strings
- Apply server-side `aggregateWindow` downsampling when an interval is provided
- Batch writes (`batch_size`, default 50) and write with seconds precision
- Never write a record with an empty `fields` dict

## Scale and Memory

- Generate timestamps and time chunks lazily (generators, O(1) memory) — never materialize
  the full range since 2021
- Stream bulk exports (NDJSON/CSV/Parquet) chunk-by-chunk; hold at most one chunk in memory
- Offer cursor-based pagination for large/interactive reads

## Logging

- Use the `logging` module across all layers; no `print()` for operational messages
- DEBUG: request URLs / intermediate values; INFO: progress; WARNING: null data / transient
  failures; ERROR: failures after retries are exhausted
- Ingestion logging must not abort the run if a logging call itself fails (availability first)

## Frontend (React + TypeScript)

- Keep TypeScript transport types in sync with the FastAPI response models
- Consume the API only through the typed fetch wrappers in `web/src/api/`
- Pin JS dependencies via `package-lock.json`

## Project Hygiene

- Keep `pyproject.toml` the source of truth for pinned deps; `requirements.txt` mirrors it
- Remove dead/commented-out code — use git history instead
- Legacy modules (`InfluxAdmin.py`, `RunTimestampFetch.py`, `smallFcns.py`,
  `setup_influxdb.py`, `latestTime.txt`) have been folded into `ge_pipeline` and removed;
  do not reintroduce them
