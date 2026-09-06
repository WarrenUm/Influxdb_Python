# Implementation Plan: InfluxDB v3 Migration

## Overview

This plan cuts the `ge_pipeline` pipeline over from InfluxDB v2 to InfluxDB 3 Core while keeping the storage schema identical to v2 (a 1:1 copy, no rename). Work proceeds bottom-up so the storage seam is stable before the layers above it move: dependencies and configuration first, then the storage-schema constants (unchanged v2 names), the `influx.py` write and query rewrite, ingestion verification, the v3 database setup helper, the one-pass migration tool, the CLI wiring, the Grafana datasource/dashboard update, and finally the full test-suite/cutover verification (including the static import-scan that keeps the v2 client confined to `migrate.py`).

The guiding principle from the design is a narrow blast radius: only the internals of `influx.py` change while its public call shapes stay fixed, so `ingestion.py`, `data_access.py`, `api.py`, and `cli.py` follow along with minimal churn. Property tests are drawn directly from the design's 14 correctness properties and placed next to the code they validate. All test sub-tasks are marked optional with `*`.

## Tasks

- [x] 1. Swap dependencies and document v3 environment
  - [x] 1.1 Replace the v2 client dependency with `influxdb3-python`
    - In `pyproject.toml` and `requirements.txt`, remove `influxdb-client` from the runtime dependencies and add `influxdb3-python`
    - Keep `influxdb-client` available only where the migration tool needs it (it is imported solely in `migrate.py`); confirm Python 3.10+ requirement is retained
    - Add `testcontainers` to the test/dev dependency group for the InfluxDB 3 Core integration tests
    - _Requirements: 1.1, 1.4, 1.5_

  - [x] 1.2 Update `.env.example` for v3 and migration variables
    - Document the v3 target variables `INFLUXDB3_HOST_URL`, `INFLUXDB3_AUTH_TOKEN`, `INFLUXDB3_DATABASE_NAME`
    - Document the v2/source migration variables `V2_INFLUX_URL`, `V2_INFLUX_TOKEN`, `V2_INFLUX_ORG`, `V2_INFLUX_BUCKET`
    - _Requirements: 2.5_

- [x] 2. Migrate configuration to v3 target + lazy v2 source
  - [x] 2.1 Add v3 target and v2 source settings to `config.py`
    - Extend `Settings` with `influx3_host`, `influx3_token`, `influx3_database` and optional `v2_url`, `v2_token`, `v2_org`, `v2_bucket`
    - In `get_settings`, validate `_REQUIRED_V3_VARS` in declared order and raise a `ConfigError` naming the first missing variable; cache the result
    - Add `require_migration_source(settings)` that validates `_REQUIRED_V2_VARS` in declared order and raises a `ConfigError` naming the first missing `V2_*` variable, returning a `MigrationSource`
    - Preserve `load_dotenv(override=False)` so the process environment wins over `.env`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.6_

  - [x] 2.2 Write property test for first-missing target variable
    - **Property 2: Config names the first missing target variable**
    - **Validates: Requirements 2.1, 2.2**

  - [x] 2.3 Write property test for lazy migration-source validation
    - **Property 3: Migration source validated only on demand**
    - **Validates: Requirements 2.4**

  - [x] 2.4 Write property test for env-over-dotenv precedence
    - **Property 4: Process environment overrides `.env`**
    - **Validates: Requirements 2.6**

- [x] 3. Rewrite the storage seam (`influx.py`) for InfluxDB 3
  - [x] 3.1 Keep the existing v2 schema constants and add the cached `InfluxDBClient3`
    - Keep `MEASUREMENT = "itemPrice"` unchanged and keep `PRICE_FIELDS` as the existing camelCase names `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume` (no snake_case rename); keep `WRITE_PRECISION = "s"`
    - Import `InfluxDBClient3` (and `Point`) from `influxdb_client_3`; rewrite `get_client` to build/cache the client keyed by `(host, database, token)`
    - _Requirements: 1.2, 1.3, 3.1_

  - [x] 3.2 Write property test for client caching identity
    - **Property 1: Client caching identity**
    - **Validates: Requirements 1.3**

  - [x] 3.3 Rewrite the write path to line protocol / `Point`
    - Add `_record_to_point` that maps a record dict (measurement/tags/fields/time) to a `Point` using the unchanged v2 schema (measurement `itemPrice`, tag `itemID`, camelCase fields)
    - Rewrite `write_batch(client, database, records)`: empty batch is a no-op; non-empty batch writes with `write_precision="s"` (targeting the v3 write endpoint); wrap failures as `TransientError` and retry via `retry_with_backoff()`; on exhaustion, log and drop without raising
    - _Requirements: 3.3, 7.1, 7.2, 7.3, 7.4_

  - [x] 3.4 Write property test for write/read round-trip preservation
    - **Property 5: Write/read round-trip preserves the snapshot**
    - **Validates: Requirements 3.1, 3.2, 3.3, 7.1**

  - [x] 3.5 Rewrite `get_latest_timestamp` and `list_item_ids` with parameterized SQL
    - `get_latest_timestamp`: `SELECT max(time) AS latest FROM "itemPrice" WHERE "itemID" = $item_id`, binding `item_id` via `query_parameters`; return `int(latest.timestamp())` or `None`
    - `list_item_ids`: `SELECT DISTINCT "itemID" FROM "itemPrice" ORDER BY "itemID"`, returning the distinct string ids
    - Double-quote the camelCase measurement/tag/field identifiers so InfluxDB 3 SQL treats them as case-sensitive rather than folding to lowercase
    - _Requirements: 6.1, 6.2, 6.6, 6.7_

  - [x] 3.6 Write property tests for latest-timestamp and distinct listing
    - **Property 6: Latest timestamp equals the maximum stored**
    - **Property 10: Distinct item listing**
    - **Validates: Requirements 6.2, 6.6**

  - [x] 3.7 Rewrite `query_price_series` with SQL and `date_bin` downsampling
    - Without interval: `SELECT time, "avgHighPrice", "avgLowPrice", "highPriceVolume", "lowPriceVolume" FROM "itemPrice" WHERE "itemID" = $item_id AND time >= $start AND time < $stop ORDER BY time`, binding `item_id`, `start`, `stop` as parameters
    - With interval: `date_bin($interval, time)` window with `avg("avgHighPrice")` etc. per field, `GROUP BY date_bin(...)`, `ORDER BY time`; double-quote every camelCase identifier so it stays case-sensitive
    - Preserve the existing `list[dict]` return shape (each point has `time` in unix seconds plus the four camelCase fields)
    - _Requirements: 6.1, 6.3, 6.4, 6.7_

  - [x] 3.8 Write property tests for series ordering and interval mean
    - **Property 7: Price series is time-ascending and complete**
    - **Property 8: Interval downsampling is the per-window mean**
    - **Validates: Requirements 6.3, 6.4**

  - [x] 3.9 Rewrite `query_chunk` with a parameterized `IN` list
    - Bind each item id as `$id0, $id1, ...` in an SQL `WHERE "itemID" IN (...)` clause (never concatenated into query text); select the double-quoted camelCase fields from `"itemPrice"`; optional `date_bin` downsampling on interval; return a `pandas.DataFrame` with one row per `("itemID", time)` and a column per field; empty `item_ids` returns an empty DataFrame
    - _Requirements: 6.1, 6.4, 6.5, 6.7_

  - [x] 3.10 Write property tests for chunk shape and parameter binding
    - **Property 9: Chunk frame has one row per item-and-timestamp**
    - **Property 11: Query parameters are bound, not interpolated**
    - **Validates: Requirements 6.5, 6.7**

  - [x] 3.11 Verify no caller changes are needed for the preserved field names
    - Confirm `data_access.py` and `api.py` keep working unchanged: because the query path returns the same camelCase field keys (`avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`) as v2, no field-name edits are required in callers
    - _Requirements: 6.3, 6.5_

- [x] 4. Confirm ingestion needs no schema/field changes
  - [x] 4.1 Verify `build_price_records` and `_FIELD_NAME_MAP` stay as-is
    - Because the schema is preserved (measurement `itemPrice`, tag `itemID`, camelCase fields), the write path already emits the correct v2 names; confirm `build_price_records` and `_FIELD_NAME_MAP` require no changes and continue to filter out null fields (skip points with no non-null fields)
    - _Requirements: 3.1, 3.2, 3.3, 7.1_

  - [x] 4.2 Write unit tests asserting the unchanged v2 record shape
    - Assert `build_price_records` still emits measurement `itemPrice`, tag `itemID`, the four camelCase fields, null-field filtering, and unix-second timestamps
    - _Requirements: 3.1, 3.2, 7.1, 7.2_

- [x] 5. Add the v3 database setup helper and repoint CLI `setup`
  - [x] 5.1 Implement `setup_v3_database(settings)`
    - Preflight the server with a lightweight health/query call; on failure raise a connection error naming `influx3_host`
    - Create `influx3_database` if absent; catch the "already exists" response and report `"exists"` without creating a duplicate; return `"created"` or `"exists"`
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 5.2 Write unit tests for setup idempotence and unreachable host
    - Cover created/exists outcomes and the connection-error message naming the host
    - _Requirements: 4.1, 4.2, 4.3_

- [x] 6. Build the one-pass migration tool (`migrate.py`)
  - [x] 6.1 Implement the v2 reader as a 1:1 copy (no name transformation)
    - Create `migrate.py` as the sole module importing `influxdb_client` (v2) and Flux read logic
    - Implement `iter_v2_records` streaming v2 rows (time-chunked or per-item) and copying each 1:1 preserving the v2 names: measurement `itemPrice`, tag `itemID`, camelCase fields, and the record timestamp; the `itemID` string, the four numeric values, and the timestamp are preserved exactly with no rename or transform
    - Add `MigrationResult(records_read, records_written)`
    - _Requirements: 1.5, 5.1, 5.2_

  - [x] 6.2 Write property test for the 1:1 migration copy
    - **Property 12: Migration copies records 1:1 preserving names**
    - **Validates: Requirements 5.1, 5.2, 10.2**

  - [x] 6.3 Implement `run_migration` with preflight, batching, retry, and counts
    - Resolve source via `require_migration_source`; open the v2 client and the cached v3 client
    - Preflight both endpoints and, if either is unreachable, raise a connection error naming the unreachable server and stop before any write
    - Stream in a single pass, writing through `influx.write_batch` in batches of `settings.batch_size`, flushing the remainder at the end
    - Surface a batch that exhausts retries as a failure so `records_written` stays accurate; return and report `records_read` / `records_written`
    - _Requirements: 5.1, 5.3, 5.4, 5.5, 5.6_

  - [x] 6.4 Write property tests for batching and count accuracy
    - **Property 13: Migration batching partitions the records**
    - **Property 14: Migration counts are accurate**
    - **Validates: Requirements 5.3, 5.5**

  - [x] 6.5 Write a unit test for preflight-stops-before-write
    - Assert an unreachable source or target raises a naming error with zero writes attempted
    - _Requirements: 5.6_

- [x] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Wire the CLI to v3
  - [x] 8.1 Repoint `setup` and update v3 remediation messaging
    - `setup` calls `setup_v3_database` and reports created/exists/connection-error, exiting non-zero when the server is unreachable
    - Update `_require_settings`/remediation text to name the `INFLUXDB3_*` variables; ensure `ingest`, `backfill`, `serve`, `export` operate against v3 through the updated `influx.py` seam
    - Ensure any command needing v3 that cannot reach it prints a message naming `influx3_host` and exits with code 1
    - _Requirements: 8.1, 8.2, 8.4_

  - [x] 8.2 Add the `migrate` command
    - Register a `migrate` subcommand that wraps `run_migration` and echoes the read/written counts
    - _Requirements: 5.5, 8.3_

  - [x] 8.3 Write unit tests for CLI command registration and exit codes
    - Assert `migrate` is registered, `setup` reports outcomes, and unreachable-v3 commands exit non-zero naming the host
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

- [x] 9. Update Grafana for v3
  - [x] 9.1 Move the datasource to the v3 SQL/FlightSQL mode
    - Rewrite `grafana/provisioning/datasources/influxdb.yaml` to the InfluxDB v3 (FlightSQL/SQL) datasource referencing `host`, `token` (injected via `${INFLUXDB3_AUTH_TOKEN}`, never hardcoded), and `database`
    - _Requirements: 9.1, 9.2_

  - [x] 9.2 Rewrite dashboard queries to v3 SQL with double-quoted camelCase fields
    - In `grafana/dashboards/ge-prices.json`, replace Flux panel queries with SQL selecting the four double-quoted camelCase fields from `"itemPrice"`; change the `$itemID` template variable query to `SELECT DISTINCT "itemID" FROM "itemPrice"`; keep candlestick/gauge field mappings on the existing camelCase names
    - _Requirements: 9.3_

  - [x] 9.3 Write config-assertion tests for the Grafana files
    - Assert the datasource YAML uses the v3 mode and env-injected token, and the dashboard JSON queries reference the double-quoted camelCase fields under the `"itemPrice"` measurement
    - _Requirements: 9.1, 9.2, 9.3_

- [x] 11. Verify v3 integration and enforce the v2-client cutover
  - [x] 11.1 Stand up the InfluxDB 3 Core testcontainer fixture
    - Add a `testcontainers`-based fixture serving a live InfluxDB 3 Core instance for the query/write and setup/migration integration tests
    - _Requirements: 10.1, 10.2_

  - [x] 11.2 Exercise the v3 query/write paths and migration against the container
    - Run the write→read round-trip, latest-timestamp, series, interval, chunk, and distinct-listing checks plus the migration transform preservation against the live container
    - _Requirements: 10.1, 10.2, 10.3_

  - [x] 11.3 Add the static import-scan enforcing v2-client confinement
    - Assert `influxdb_client` (v2) is imported only inside `ge_pipeline/migrate.py` and nowhere else in the runtime package
    - _Requirements: 10.4_

- [x] 12. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional (test sub-tasks) and can be skipped for a faster cutover; core implementation tasks are never optional.
- The v3 schema is kept identical to v2 (measurement `itemPrice`, tag `itemID`, camelCase fields), so migration is a straight 1:1 copy and callers/ingestion need no field-name changes. The one accepted cost is that InfluxDB 3 SQL requires the camelCase identifiers to be double-quoted (`"itemPrice"`, `"itemID"`, `"avgHighPrice"`) so they stay case-sensitive.
- Each task references specific requirement sub-clauses for traceability, and property test sub-tasks reference the exact design property they validate.
- Property tests are placed next to the code they exercise so schema/query mistakes surface early; the Hypothesis `ge` profile enforces ≥100 examples per property.
- Checkpoints (tasks 7 and 12) ensure incremental validation at natural boundaries.
- The v2 `influxdb-client` and Flux logic survive only inside `migrate.py`; task 11.3's import-scan enforces this after cutover.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "2.1"] },
    { "id": 1, "tasks": ["2.2", "2.3", "2.4", "3.1"] },
    { "id": 2, "tasks": ["3.2", "3.3", "3.5", "3.7", "3.9"] },
    { "id": 3, "tasks": ["3.4", "3.6", "3.8", "3.10", "3.11", "4.1", "5.1"] },
    { "id": 4, "tasks": ["4.2", "5.2", "6.1", "9.1", "9.2"] },
    { "id": 5, "tasks": ["6.2", "6.3", "8.1", "9.3"] },
    { "id": 6, "tasks": ["6.4", "6.5", "8.2"] },
    { "id": 7, "tasks": ["8.3", "11.1", "11.3"] },
    { "id": 8, "tasks": ["11.2"] }
  ]
}
```
