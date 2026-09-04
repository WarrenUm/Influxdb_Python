# Implementation Plan: Modernization and Dashboard

## Overview

This plan converts the approved design into incremental, test-driven coding steps.
Work proceeds bottom-up: package scaffolding, lazy config, and pydantic models land
first, followed by the influx service and retry/backoff, then async ingestion. The
scale-oriented data access layer, pluggable outlier registry, CLI, scheduler, and
FastAPI service build on that core. The two UI paths (Grafana low-code and the React
+ TypeScript SPA) come after the API contract is stable, and packaging cleanup plus
the integration/round-trip test suite are wired throughout. Existing working logic in
`InfluxAdmin.py`, `RunTimestampFetch.py`, and `setup_influxdb.py` is folded into the
`ge_pipeline` package and its CLI, then the superseded legacy files are removed.

Property-based tests (Hypothesis, >=100 iterations) are placed next to the code they
validate so correctness regressions surface early. Each property test task cites its
design property number and the requirement clause it checks.

## Tasks

- [x] 1. Scaffold the `ge_pipeline` package and dependency manifest
  - Create the `ge_pipeline/` package directory with `__init__.py` (no import-time side effects) and submodule stubs (`config.py`, `models.py`, `influx.py`, `retry.py`, `ingestion.py`, `data_access.py`, `outliers.py`, `cli.py`, `scheduler.py`, `api.py`, `errors.py`)
  - Create `pyproject.toml` declaring the package and pinned backend deps: `influxdb-client`, `pandas`, `python-dotenv`, `httpx`, `pydantic>=2`, `typer`, `apscheduler`, `fastapi`, `uvicorn`, `pytest`, `hypothesis`, `pyarrow`
  - Create `tests/` directory with `__init__.py` and a `conftest.py` placeholder; configure `pytest` (and a Hypothesis profile with `max_examples>=100`) in `pyproject.toml`
  - Define shared exception types in `errors.py`: `ConfigError`, `TransientError`, `NonTransientError`
  - _Requirements: 20.1, 20.3, 21.2_

- [x] 2. Implement lazy, cached configuration service
  - [x] 2.1 Implement `Settings` dataclass and `get_settings()` in `config.py`
    - Define the frozen `Settings` dataclass (`influx_url`, `influx_token`, `influx_org`, `influx_bucket`, `user_agent`, `api_base_url`, `max_concurrency`, `batch_size`) with documented defaults
    - Implement `get_settings()` to load `.env`/environment on first call only, cache the result, and raise `ConfigError` naming the first missing required variable; ensure no reads occur at import time
    - Add type hints and docstrings per PEP 8
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 20.3_
  - [x] 2.2 Write unit tests for configuration loading
    - Assert importing modules performs no env reads and raises nothing
    - Assert caching returns the identical object without re-reading env, and that a missing var raises `ConfigError` naming the variable
    - _Requirements: 1.1, 1.3, 1.4, 21.1_

- [x] 3. Implement pydantic v2 validation models
  - [x] 3.1 Implement `ItemPricePoint` and `FiveMinuteSnapshot` in `models.py`
    - Define `ItemPricePoint` with `avgHighPrice`/`avgLowPrice`/`highPriceVolume`/`lowPriceVolume` aliases coerced to snake_case fields, all `int | None` with `ge=0`, `populate_by_name=True`, `extra="ignore"`
    - Define `FiveMinuteSnapshot` (`timestamp: int`, `data: dict[str, ItemPricePoint]`); raise descriptive validation errors on malformed input
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 8.1_
  - [x] 3.2 Write property test for schema validation round-trip
    - **Property 8: pydantic validation round-trip**
    - **Validates: Requirements 2.1, 2.3, 21.5**
    - Mandatory schema round-trip test: well-formed API JSON validates and every non-null numeric field is an int `>= 0`
  - [x] 3.3 Write unit tests for alias coercion, null preservation, and unknown-key ignoring
    - Cover alias mapping, `None` preservation for absent/null fields, unknown-key ignoring, and negative-value rejection
    - _Requirements: 2.2, 2.4, 2.5, 2.6, 21.1_

- [x] 4. Implement retry with exponential backoff
  - [x] 4.1 Implement the retry helper in `retry.py`
    - Provide a decorator/util that retries on `TransientError` with exponential backoff and honors an explicit wait for HTTP 429 `Retry-After`
    - Log each retry and log a final error after retries are exhausted
    - _Requirements: 6.1, 6.2, 8.1_
  - [x] 4.2 Write unit tests for backoff behavior
    - Assert exponential delay growth, `Retry-After` honoring, and final-failure logging/propagation
    - _Requirements: 6.1, 6.2, 21.1_

- [x] 5. Implement the influx service (client, writes, parameterized queries)
  - [x] 5.1 Implement client lifecycle and batched writes in `influx.py`
    - Port `get_db_client`/`write_to_db` from `InfluxAdmin.py` into `get_client(settings)` and `write_batch(client, bucket, records)`; reuse a single client across operations and write with seconds precision
    - Wrap write failures as `TransientError`, retry via the retry helper, and log-and-drop the batch on final failure
    - _Requirements: 7.3, 7.5, 8.1, 8.3_
  - [x] 5.2 Implement `get_latest_timestamp` and parameterized single-item query
    - Implement `get_latest_timestamp(client, item_id)` returning the most recent stored timestamp or `None`
    - Implement `query_price_series(client, item_id, start, stop, interval)` using parameterized Flux with server-side `aggregateWindow` when an interval is provided
    - _Requirements: 9.1, 9.2, 9.4, 9.5_
  - [x] 5.3 Implement multi-item query and item listing
    - Implement `query_chunk(client, item_ids, start, stop, interval)` returning a combined multi-item `DataFrame` via parameterized Flux + `aggregateWindow` + pivot
    - Implement `list_item_ids(client)` for distinct `itemID` tag values
    - _Requirements: 9.1, 9.3, 9.4_
  - [x] 5.4 Write unit tests for the influx service with a mocked client
    - Assert parameterization (no user values interpolated into query strings), `aggregateWindow` application, `get_latest_timestamp` None-on-empty, and write retry/drop behavior
    - _Requirements: 9.1, 9.4, 9.5, 7.5, 21.1_

- [x] 6. Implement ingestion record building and lazy timestamp generation
  - [x] 6.1 Implement `build_price_records` in `ingestion.py`
    - Port and adapt null-filtering logic from `RunTimestampFetch.py` to accept a validated `FiveMinuteSnapshot`; emit one record per item with >=1 non-null field, each with `measurement="itemPrice"`, `tags={"itemID": <str>}`, `time=snapshot.timestamp`, and a non-empty `fields` dict of only non-null values
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  - [x] 6.2 Write property test for record null-exclusion invariant
    - **Property 1: build_price_records excludes fully-null items**
    - **Validates: Requirements 3.3**
  - [x] 6.3 Write property test for record preservation count
    - **Property 2: build_price_records preserves non-null items**
    - **Validates: Requirements 3.1, 3.2**
  - [x] 6.4 Write property test for snapshot timestamp/measurement stamping
    - **Property 3: build_price_records stamps the snapshot timestamp**
    - **Validates: Requirements 3.4**
  - [x] 6.5 Implement `iter_missing_timestamps` generator
    - Yield strictly increasing, `interval`-aligned timestamps in `[start, now)` lazily (one at a time), where `start = earliest` if `latest is None` else `latest + interval`
    - _Requirements: 4.1, 4.2, 4.3, 4.4_
  - [x] 6.6 Write property test for timestamp alignment/increasing
    - **Property 4: iter_missing_timestamps is strictly increasing and aligned**
    - **Validates: Requirements 4.2, 4.3**
  - [x] 6.7 Write property test for timestamp completeness
    - **Property 5: iter_missing_timestamps completeness**
    - **Validates: Requirements 4.3, 4.4**

- [x] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement async fetch and catch-up orchestration
  - [x] 8.1 Implement `fetch_snapshot` with httpx and error classification
    - Implement `async def fetch_snapshot(client, ts)` using the configured `user_agent`; validate the body into `FiveMinuteSnapshot`
    - Raise `TransientError` on timeout/connection error/HTTP 5xx/HTTP 429 (honoring `Retry-After`) and `NonTransientError` on non-429 4xx
    - _Requirements: 5.2, 5.3, 5.4, 6.2_
  - [x] 8.2 Write unit tests for `fetch_snapshot` with mocked httpx
    - Assert error classification for timeout/5xx/429/4xx, `Retry-After` handling, `User-Agent` header, and successful validation path
    - _Requirements: 5.2, 5.3, 5.4, 21.1_
  - [x] 8.3 Implement `run_catch_up` with bounded concurrency and batching
    - Orchestrate per the design pseudocode: fetch latest timestamp, iterate `iter_missing_timestamps`, bound concurrent fetches with a semaphore (`max_concurrency`), retry on transient failure and skip after exhaustion (log offending timestamp, increment failures), build records, batch-write at `batch_size` and flush the final partial batch, always close the client, and return an accurate `IngestionResult`
    - Ensure logging continues even if a logging call itself fails (availability over audit trail)
    - _Requirements: 5.1, 5.5, 5.6, 5.7, 6.3, 7.1, 7.2, 7.4, 8.1, 8.2, 8.3_
  - [x] 8.4 Write unit tests for `run_catch_up` orchestration
    - Use mocked fetch/write to assert concurrency ceiling, exactly-once attempt per window, accurate result counts, final-batch flush, and client close on partial failure
    - _Requirements: 5.1, 5.5, 5.6, 5.7, 7.1, 7.2, 21.1_

- [x] 9. Implement the data access layer (chunking, streaming, pagination, features)
  - [x] 9.1 Implement `iter_time_chunks` in `data_access.py`
    - Yield contiguous, non-overlapping half-open windows whose union is exactly `[start, stop)`, each with `0 < b - a <= chunk_seconds`, none when `start == stop`, using O(1) memory
    - _Requirements: 10.1, 10.2, 10.3_
  - [x] 9.2 Write property test for time-chunk partitioning
    - **Property 10: iter_time_chunks partitions the range exactly**
    - **Validates: Requirements 10.1, 10.2**
  - [x] 9.3 Implement `stream_dataset` streaming encoder
    - Stream NDJSON/CSV/Parquet byte chunks by iterating `iter_time_chunks` and calling `query_chunk`, holding at most one chunk's rows in memory; emit rows time-ascending per item with no boundary duplication; reject unsupported formats and time-ordering violations with descriptive errors
    - _Requirements: 10.4, 11.1, 11.2, 11.3, 11.4, 8.1_
  - [x] 9.4 Write property test for stream_dataset round-trip
    - **Property 12: stream_dataset round-trips its rows**
    - **Validates: Requirements 10.4, 11.2, 11.3**
  - [x] 9.5 Implement `get_price_page` cursor pagination
    - Return at most `limit` time-ascending points starting strictly after `cursor` (or at `start` when `None`); set `nextCursor` to the last point's time when more data exists else `None`; validate `0 < limit <= MAX_PAGE_LIMIT` with a descriptive error otherwise
    - _Requirements: 12.1, 12.2, 12.4_
  - [x] 9.6 Write property test for lossless pagination
    - **Property 11: get_price_page pagination is a lossless partition**
    - **Validates: Requirements 12.1, 12.2, 12.3**
  - [x] 9.7 Implement `build_feature_frame`
    - Return a tidy, time-indexed, ML-ready `DataFrame` with per-item columns for the requested items/range/interval
    - _Requirements: 11.5_
  - [x] 9.8 Write unit tests for build_feature_frame and format rejection
    - Assert frame shape/indexing and that unsupported export formats raise descriptive errors
    - _Requirements: 11.4, 11.5, 21.1_

- [x] 10. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Implement the pluggable outlier detection registry
  - [x] 11.1 Implement the detector protocol, registry, and z-score/IQR detectors in `outliers.py`
    - Define the `OutlierDetector` protocol and a name-keyed registry with `get_detector(method, **params)` (descriptive error for unregistered names)
    - Implement `detect_outliers`/z-score detector (trailing window, `|values[i]-μ|/σ > threshold`, `False` when `σ==0`, `None`→`False`) and an IQR detector; both return `list[bool]` equal in length to input
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 8.1_
  - [x] 11.2 Write property test for detector length and None-safety
    - **Property 6: detect_outliers length and None-safety**
    - **Validates: Requirements 13.2, 13.3**
  - [x] 11.3 Write property test for zero-variance stability
    - **Property 7: detect_outliers zero-variance stability**
    - **Validates: Requirements 13.4, 13.5**
  - [x] 11.4 Write property test for registry contract across all registered detectors
    - **Property 13: get_detector returns a length-preserving detector**
    - **Validates: Requirements 13.2, 13.3, 13.6**
  - [x] 11.5 Write unit test for unregistered-method error
    - Assert `get_detector` raises a descriptive error for an unknown method name
    - _Requirements: 13.7, 21.1_

- [x] 12. Implement the FastAPI query service
  - [x] 12.1 Implement app setup, CORS, and conditional auth in `api.py`
    - Create the FastAPI app, restrict CORS to the React_SPA origin, validate query params, and apply conditional auth (no auth for localhost single-user; API-key/reverse-proxy auth dependency when exposed beyond localhost)
    - Implement `GET /api/health` and `GET /api/outlier-methods` (registered detector names)
    - _Requirements: 16.1, 13.8, 19.1, 19.2, 19.3, 19.4, 8.1_
  - [x] 12.2 Implement item search and price-series endpoints
    - Implement `GET /api/items` (substring search) and `GET /api/items/{item_id}/prices` returning `itemId`/`interval`/`points` with OHLC+volume and an `isOutlier` boolean per point in strictly ascending time order; return 404 with a typed error body when the item has no data
    - Enforce max-items and max-un-downsampled-range guardrails; return 4xx with descriptive body on invalid params
    - _Requirements: 16.2, 16.3, 16.5, 16.6, 16.7_
  - [x] 12.3 Write property test for price-series API contract
    - **Property 9: PriceSeriesResponse time ordering (API contract)**
    - **Validates: Requirements 16.2, 16.3**
  - [x] 12.4 Implement pagination, outliers, and dataset export/features endpoints
    - Implement `GET /api/items/{item_id}/prices/page` (cursor/limit), `GET /api/items/{item_id}/outliers` (method-selectable, outlier points only), `GET /api/datasets/export` (always chunked `StreamingResponse`), and `GET /api/datasets/features`
    - _Requirements: 16.1, 16.4, 16.5, 16.6, 16.7, 12.1, 11.1, 11.5_
  - [x] 12.5 Write API unit tests with TestClient and mocked services
    - Assert route contracts, 404 on missing data, 4xx on invalid params, guardrail enforcement, CORS restriction, and that export uses chunked streaming
    - _Requirements: 16.1, 16.4, 16.5, 16.6, 16.7, 19.1, 21.1_

- [x] 13. Implement the Typer CLI (fold in setup logic)
  - [x] 13.1 Implement CLI commands in `cli.py`
    - Implement `ingest` (run `run_catch_up`, echo written/processed/failures), `backfill`, `serve` (start uvicorn on host/port), `export` (stream a bulk dataset to a file with bounded memory), and `setup` (fold in `setup_influxdb.py`: health check, ensure bucket, verify write access)
    - On missing configuration, print remediation guidance (`cp .env.example .env`) and exit non-zero
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 8.1_
  - [x] 13.2 Write CLI unit tests with Typer's runner
    - Assert command wiring, `ingest` count output, `export` file streaming, and non-zero exit with remediation on missing config
    - _Requirements: 14.2, 14.4, 14.5, 21.1_

- [x] 14. Implement the APScheduler daemon
  - [x] 14.1 Implement the scheduler in `scheduler.py`
    - Configure APScheduler to trigger `run_catch_up` every 5 minutes (not continuously) as a long-running daemon; next run backfills gaps left by prior failed runs; provide a systemd-friendly entrypoint
    - _Requirements: 15.1, 15.2, 15.3, 8.1_
  - [x] 14.2 Write unit test for scheduler configuration
    - Assert a 5-minute interval trigger is registered against the catch-up job (with the scheduler mocked)
    - _Requirements: 15.1, 21.1_

- [x] 15. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 16. Author the Grafana low-code dashboard artifacts
  - Create a provisioned InfluxDB v2 Flux data source config (org `Ge-data-project`, bucket `GEItemPrices`, token from env) and a dashboard JSON with an `$itemID` template variable (distinct `itemID` tag values), line panel (`avgHighPrice`/`avgLowPrice`), candlestick panel, and gauge panel (latest combined volume), all using parameterized Flux for `$itemID`
  - Define the deviation alert rule that suppresses firing when latest price, moving average, or the configured factor is zero/undefined
  - _Requirements: 18.1, 18.2, 18.3, 18.4, 18.5_

- [x] 17. Scaffold the React + TypeScript SPA
  - [x] 17.1 Initialize the Vite React + TypeScript app and typed API client
    - Create the Vite project, pin JS deps via `package-lock.json`, add the charting lib (Lightweight Charts), and implement `api/types.ts` (`PricePoint`, `PriceSeriesResponse`, `ItemSearchResult`, `PricePage`, `ExportFormat`) plus `api/client.ts` typed fetch wrappers matching the transport interfaces
    - _Requirements: 17.6, 20.2_
  - [x] 17.2 Implement item search with debounced requests
    - Implement `components/ItemSearch.tsx` issuing debounced `/api/items` requests and displaying matches
    - _Requirements: 17.1_
  - [x] 17.3 Implement the price chart, outlier highlighting, and data hook
    - Implement `hooks/usePriceSeries.ts`, `components/PriceChart.tsx` (candlestick + volume overlay), and `components/OutlierBadge.tsx` to highlight points where `isOutlier` is true; show an empty-state message when the API returns no data
    - _Requirements: 17.2, 17.3, 17.5_
  - [x] 17.4 Implement dark theme with fallback and wire the app together
    - Apply a default dark theme with a fallback to a default theme if it fails to load, and compose search + chart into the top-level app
    - _Requirements: 17.4_
  - [x] 17.5 Write SPA unit tests for client and components
    - Test the typed API client against the transport interfaces, debounced search behavior, outlier highlighting, and empty-state rendering
    - _Requirements: 17.1, 17.3, 17.5, 17.6_

- [x] 18. Packaging cleanup and legacy removal
  - Verify `pyproject.toml` pins are complete and public functions have type hints and docstrings (PEP 8 snake_case); confirm `.env` remains git-ignored and no secrets are in source
  - Remove dead/duplicate legacy files superseded by the package (`InfluxAdmin.py`, `RunTimestampFetch.py`, `setup_influxdb.py`, and any `smallFcns.py`/`latestTime.txt` remnants) after their logic is fully folded in; update `requirements.txt`/README references accordingly
  - _Requirements: 20.1, 20.3, 20.4, 19.6_

- [x] 19. Implement integration tests
  - [x] 19.1 Write InfluxDB test-container round-trip integration test
    - Spin up an InfluxDB test container; write then query to assert schema/data round-trip through `influx.write_batch` and the query functions
    - _Requirements: 21.4_
  - [x] 19.2 Write FastAPI TestClient integration tests against a seeded store
    - Exercise the endpoints against a seeded store to assert end-to-end contracts (prices, pagination, outliers, export streaming, 404s)
    - _Requirements: 21.4_

- [x] 20. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a faster MVP, but they encode the design's correctness properties and are recommended.
- Each task references specific requirement sub-clauses for traceability.
- Property-based tests (Hypothesis, >=100 iterations) validate Properties 1-13; the schema round-trip test (Task 3.2) is mandatory per Requirement 21.5.
- Checkpoints ensure incremental validation at natural boundaries.
- Legacy files are removed only after their working logic is folded into `ge_pipeline`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "3.1", "4.1"] },
    { "id": 2, "tasks": ["2.2", "3.2", "3.3", "4.2", "5.1", "6.1", "6.5", "11.1"] },
    { "id": 3, "tasks": ["5.2", "5.3", "6.2", "6.3", "6.4", "6.6", "6.7", "9.1", "9.5", "9.7", "11.2", "11.3", "11.4", "11.5"] },
    { "id": 4, "tasks": ["5.4", "8.1", "9.2", "9.3", "9.6", "9.8", "14.1"] },
    { "id": 5, "tasks": ["8.2", "8.3", "9.4", "12.1", "14.2", "16"] },
    { "id": 6, "tasks": ["8.4", "12.2", "12.4", "13.1"] },
    { "id": 7, "tasks": ["12.3", "12.5", "13.2", "18"] },
    { "id": 8, "tasks": ["17.1"] },
    { "id": 9, "tasks": ["17.2", "17.3", "17.4"] },
    { "id": 10, "tasks": ["17.5", "19.1", "19.2"] }
  ]
}
```
