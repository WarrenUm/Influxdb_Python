# Requirements Document

## Introduction

This feature modernizes the OSRS Grand Exchange price collection pipeline and adds two
complementary front ends for visualizing and detecting price outliers. The backend is
repackaged as a Python package (`ge_pipeline`) with lazy/cached configuration, async
ingestion, schema validation, a CLI, scheduling, a scale-oriented data access layer, a
FastAPI query service, and a pluggable outlier-detection registry. Two UI paths are
delivered: a low-code Grafana-on-InfluxDB dashboard, and a custom FastAPI + React/TypeScript
single-page application. The requirements below are derived from the approved design document
and trace to its components, function specifications, and the thirteen correctness properties
defined in Part B. Foundational code-quality work (env-var configuration, retry with
exponential backoff, batched writes, structured logging, null handling, type hints,
docstrings, pinned dependency packaging) is folded into the modernized package and verified
by the same test suite.

## Glossary

- **ge_pipeline**: The modernized Python package containing all backend components.
- **Configuration_Service**: The `config` component exposing `get_settings()`, which lazily
  loads and caches `Settings` from environment/`.env`.
- **Settings**: The frozen dataclass holding `influx_url`, `influx_token`, `influx_org`,
  `influx_bucket`, `user_agent`, `api_base_url`, `max_concurrency`, and `batch_size`.
- **Ingestion_Service**: The `ingestion` component performing async catch-up ingestion.
- **Validation_Model**: The pydantic v2 models (`ItemPricePoint`, `FiveMinuteSnapshot`) that
  validate and coerce raw Wiki API JSON.
- **Influx_Service**: The `influx` component owning the InfluxDB client lifecycle, batched
  writes, and parameterized Flux queries.
- **Data_Access_Layer**: The scale-oriented read component (chunking, pagination,
  downsampling, export) serving the API and ML/research clients.
- **Outlier_Service**: The pluggable outlier-detection component with a named-detector registry.
- **CLI**: The Typer command-line application (`ingest`, `backfill`, `setup`, `serve`, `export`).
- **Scheduler_Service**: The APScheduler-based daemon that triggers catch-up ingestion every 5 minutes.
- **Query_API**: The FastAPI service exposing read/export endpoints consumed by the SPA and ML clients.
- **React_SPA**: The custom React + TypeScript single-page application.
- **Grafana_Dashboard**: The low-code Grafana-on-InfluxDB dashboard path.
- **Wiki_API**: The RuneScape Wiki price API at `prices.runescape.wiki/api/v1/osrs`.
- **InfluxDB**: The InfluxDB v2 store (bucket `GEItemPrices`, org `Ge-data-project`).
- **Snapshot**: A validated `FiveMinuteSnapshot` for one 5-minute window.
- **Price_Record**: An InfluxDB line-protocol record built from a snapshot item.
- **Transient_Error**: A retryable failure (timeout, connection error, HTTP 5xx, HTTP 429).
- **Non_Transient_Error**: A non-retryable failure (non-429 4xx and similar).
- **Config_Error**: The descriptive error raised when a required setting is missing at first access.
- **Export_Format**: One of `ndjson`, `csv`, or `parquet`.

## Requirements

### Requirement 1: Configuration and Secrets Handling

**User Story:** As an operator, I want configuration loaded lazily from environment/`.env`
with no hardcoded secrets, so that importing the package has no side effects and misconfiguration
is reported clearly.

#### Acceptance Criteria

1. WHEN a `ge_pipeline` module is imported, THE Configuration_Service SHALL perform no
   environment reads and SHALL raise no configuration errors.
2. WHEN `get_settings()` is called for the first time, THE Configuration_Service SHALL load
   settings from environment variables and `.env` and return a fully populated Settings object.
3. WHEN `get_settings()` is called after the first call has completed, THE Configuration_Service
   SHALL return the identical cached Settings object without re-reading the environment.
4. IF a required environment variable among `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, or
   `INFLUX_BUCKET` is unset or empty at first access, THEN THE Configuration_Service SHALL raise
   a Config_Error naming the first missing variable.
5. THE Configuration_Service SHALL read all secrets and connection values from environment
   variables or `.env` and SHALL NOT contain hardcoded secret values in source.

### Requirement 2: Wiki API Schema Validation and Null Handling

**User Story:** As a data engineer, I want raw Wiki API JSON validated and coerced through
pydantic v2 models, so that malformed responses are rejected early and null handling is centralized.

#### Acceptance Criteria

1. WHEN a well-formed `/5m` API response is provided, THE Validation_Model SHALL validate it
   into a FiveMinuteSnapshot successfully. (Validates: Property 8)
2. WHEN validating an item entry, THE Validation_Model SHALL coerce API field aliases
   (`avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`) to the snake_case domain fields.
3. WHERE a numeric price field is non-null, THE Validation_Model SHALL require the value to be
   an integer greater than or equal to 0. (Validates: Property 8)
4. WHERE an item price field is absent or null, THE Validation_Model SHALL preserve the value as
   `None` for centralized downstream filtering.
5. WHEN a response contains unknown keys, THE Validation_Model SHALL ignore the unknown keys and
   validate the recognized fields.
6. IF a response fails validation, THEN THE Validation_Model SHALL raise a descriptive validation error.

### Requirement 3: Price Record Construction and Null Filtering

**User Story:** As a data engineer, I want records built only for items with real price data,
so that no empty or null-valued records are written to InfluxDB.

#### Acceptance Criteria

1. WHEN `build_price_records` processes a validated Snapshot, THE Ingestion_Service SHALL produce
   one Price_Record for each item that has at least one non-null price field. (Validates: Property 2)
2. WHEN an item has every price field equal to `None`, THE Ingestion_Service SHALL exclude that
   item from the produced records. (Validates: Property 2)
3. THE Ingestion_Service SHALL produce each Price_Record with a non-empty `fields` dictionary
   containing only non-null values. (Validates: Property 1)
4. THE Ingestion_Service SHALL stamp each Price_Record with `measurement` equal to `"itemPrice"`,
   `tags` equal to `{"itemID": <item id string>}`, and `time` equal to the snapshot timestamp.
   (Validates: Property 3)

### Requirement 4: Lazy Missing-Timestamp Generation

**User Story:** As a data engineer, I want missing 5-minute timestamps generated lazily,
so that memory stays flat regardless of how large the backfill range is.

#### Acceptance Criteria

1. WHEN `iter_missing_timestamps` is invoked, THE Ingestion_Service SHALL yield timestamps as a
   generator holding at most one timestamp in memory at a time.
2. THE Ingestion_Service SHALL yield timestamps that are strictly increasing and aligned such
   that consecutive yielded values differ by exactly `interval`. (Validates: Property 4)
3. THE Ingestion_Service SHALL yield every value `t` satisfying `start <= t < now`, where `start`
   equals `earliest` when `latest` is `None` and equals `latest + interval` otherwise.
   (Validates: Property 4, Property 5)
4. THE Ingestion_Service SHALL yield exactly the set `{ start + k*interval : k >= 0 and start + k*interval < now }`
   with no expected timestamp skipped and no extra timestamp produced. (Validates: Property 5)

### Requirement 5: Async Ingestion with Bounded Concurrency and Rate-Limit Respect

**User Story:** As an operator, I want async ingestion with bounded concurrency that respects
rate limits, so that large backfills complete quickly without overwhelming the Wiki API.

#### Acceptance Criteria

1. WHILE `run_catch_up` is fetching snapshots, THE Ingestion_Service SHALL limit active concurrent
   fetches to at most `settings.max_concurrency`.
2. WHEN fetching a snapshot, THE Ingestion_Service SHALL send requests over `httpx` asynchronously
   using the configured `user_agent`.
3. IF a fetch encounters a timeout, connection error, HTTP 5xx, or HTTP 429, THEN THE
   Ingestion_Service SHALL raise a Transient_Error and honor the `Retry-After` header on HTTP 429.
4. IF a fetch encounters a non-429 4xx response, THEN THE Ingestion_Service SHALL raise a Non_Transient_Error.
5. WHEN `run_catch_up` completes, THE Ingestion_Service SHALL attempt every missing 5-minute window
   between `latest` and `now` exactly once.
6. WHEN `run_catch_up` completes, THE Ingestion_Service SHALL return an IngestionResult that
   accurately counts processed, written, skipped, and failed timestamps.
7. WHEN `run_catch_up` finishes, THE Ingestion_Service SHALL close the InfluxDB client even if a
   partial failure occurred.

### Requirement 6: Retry with Exponential Backoff

**User Story:** As an operator, I want transient network and database failures retried with
exponential backoff, so that temporary problems do not abort ingestion.

#### Acceptance Criteria

1. IF a transient network or database failure occurs during a fetch or write, THEN THE ge_pipeline
   SHALL retry the operation using exponential backoff.
2. WHEN an HTTP 429 response is received, THE Ingestion_Service SHALL wait for the duration
   indicated by the `Retry-After` header and THEN retry the request.
3. IF a fetch exhausts its retry attempts, THEN THE Ingestion_Service SHALL log the failure with
   the offending timestamp, increment the failure count, and continue with the next timestamp.

### Requirement 7: Batched Writes to InfluxDB

**User Story:** As an operator, I want records written in batches through a reused client,
so that network round-trips are amortized and connections are reused per steering rules.

#### Acceptance Criteria

1. WHEN the number of processed timestamps in the current batch reaches `settings.batch_size`,
   THE Ingestion_Service SHALL write the batch to InfluxDB and reset the batch.
2. WHEN ingestion finishes with a non-empty partial batch, THE Ingestion_Service SHALL write the
   remaining records before completing.
3. THE Influx_Service SHALL reuse a single InfluxDB client across all operations within a run
   rather than opening a client per iteration.
4. THE Ingestion_Service SHALL ensure every batch submitted for writing contains only records with
   a non-empty `fields` dictionary.
5. IF an InfluxDB write fails transiently, THEN THE Influx_Service SHALL retry, and on final failure
   SHALL log the error and drop that batch so ingestion can continue.

### Requirement 8: Structured Logging

**User Story:** As an operator, I want structured logging across all layers, so that I can
troubleshoot ingestion, query, and export behavior.

#### Acceptance Criteria

1. THE ge_pipeline SHALL emit log messages through the Python `logging` module across the
   configuration, ingestion, influx, data access, outlier, and API layers.
2. IF a snapshot fails to fetch or validate, THEN THE Ingestion_Service SHALL log a warning that
   includes the offending timestamp and the error, AND SHALL continue processing even if the
   logging operation itself fails, prioritizing availability over the audit trail.
3. IF a batch write is dropped after final failure, THEN THE Influx_Service SHALL log an error
   describing the dropped batch.

### Requirement 9: Parameterized InfluxDB Queries and Multi-Item Support

**User Story:** As a developer, I want all Flux queries parameterized and multi-item capable,
so that item IDs and ranges cannot cause injection and bulk reads are supported.

#### Acceptance Criteria

1. THE Influx_Service SHALL construct all Flux queries using parameterization and SHALL NOT
   interpolate user-supplied item IDs or ranges directly into query strings.
2. WHEN querying a single item, THE Influx_Service SHALL return a PriceSeries for the requested
   item, start, stop, and interval.
3. WHEN querying multiple items, THE Influx_Service SHALL return a combined result covering all
   requested item IDs.
4. WHERE a downsample interval is provided, THE Influx_Service SHALL always apply server-side
   `aggregateWindow` downsampling at that interval.
5. WHEN `get_latest_timestamp` is called for a reference item, THE Influx_Service SHALL return the
   most recent stored timestamp for that item, or `None` when no data exists.

### Requirement 10: Time-Chunked Streaming and Bounded Memory

**User Story:** As an ML/research client, I want long ranges chunked by time and streamed,
so that exporting years of multi-item data never loads the full result set into memory.

#### Acceptance Criteria

1. WHEN `iter_time_chunks` is invoked with `start`, `stop`, and `chunk_seconds`, THE
   Data_Access_Layer SHALL yield contiguous, non-overlapping half-open windows whose union is
   exactly `[start, stop)`. (Validates: Property 10)
2. THE Data_Access_Layer SHALL yield each chunk `(a, b)` such that `0 < b - a <= chunk_seconds`,
   and SHALL yield no chunks when `start` equals `stop`. (Validates: Property 10)
3. THE Data_Access_Layer SHALL generate time chunks lazily using O(1) memory regardless of range length.
4. WHILE streaming a dataset, THE Data_Access_Layer SHALL hold in memory at most the rows of two
   time-chunks, and only transiently during the transition between consecutive chunks.
   (Validates: Property 12)

### Requirement 11: Bulk Dataset Export in Multiple Formats

**User Story:** As an ML/research client, I want to export bulk datasets in NDJSON, CSV, or
Parquet, so that I can feed model training and analysis tools.

#### Acceptance Criteria

1. WHEN `stream_dataset` is invoked with a supported Export_Format, THE Data_Access_Layer SHALL
   yield the dataset as a stream of byte chunks in the requested format.
2. WHEN the streamed output is decoded and concatenated, THE Data_Access_Layer SHALL reproduce
   exactly the rows returned by the underlying chunked queries with no loss and no duplication
   across chunk boundaries. (Validates: Property 12)
3. THE Data_Access_Layer SHALL emit rows in strictly time-ascending order per item and SHALL
   reject any dataset containing a timestamp-ordering violation with a descriptive error.
   (Validates: Property 12)
4. IF the requested Export_Format is not one of `ndjson`, `csv`, or `parquet`, THEN THE
   Data_Access_Layer SHALL reject the request with a descriptive error.
5. WHEN `build_feature_frame` is invoked, THE Data_Access_Layer SHALL return a tidy, time-indexed,
   ML-ready frame with per-item columns for the requested items, range, and interval.

### Requirement 12: Cursor-Based Pagination

**User Story:** As a client, I want cursor-based pagination over price points, so that
interactive and large reads can page through data without gaps or overlaps.

#### Acceptance Criteria

1. WHEN `get_price_page` is called, THE Data_Access_Layer SHALL return at most `limit`
   time-ascending points beginning strictly after `cursor`, or at `start` when `cursor` is `None`.
   (Validates: Property 11)
2. WHEN one or more actual points are returned and more data exists beyond the returned page, THE
   Data_Access_Layer SHALL set `nextCursor` to the `time` of the last returned point; otherwise,
   including when no points are returned (for example at a limit boundary), THE Data_Access_Layer
   SHALL set `nextCursor` to `None`. (Validates: Property 11)
3. WHEN a client follows returned cursors from `cursor=None` until `nextCursor` is `None`, THE
   Data_Access_Layer SHALL visit every point exactly once with no gaps and no overlaps.
   (Validates: Property 11)
4. IF `limit` is not within `0 < limit <= MAX_PAGE_LIMIT`, THEN THE Data_Access_Layer SHALL
   reject the request with a descriptive error.

### Requirement 13: Pluggable Outlier Detection

**User Story:** As a data scientist, I want a registry of swappable outlier detectors,
so that new algorithms can be added without touching the API.

#### Acceptance Criteria

1. WHEN `get_detector` is called with a registered method name, THE Outlier_Service SHALL return a
   detector that conforms to the OutlierDetector protocol.
2. WHEN a detector's `detect` is applied to a value sequence, THE Outlier_Service SHALL return a
   list of booleans equal in length to the input. (Validates: Property 6, Property 13)
3. WHERE an input value is `None`, THE Outlier_Service SHALL map that index to `False`.
   (Validates: Property 6, Property 13)
4. WHEN `method` is `"zscore"` with a trailing window, THE Outlier_Service SHALL flag index `i` as
   `True` if and only if `|values[i] - μ_window| / σ_window > threshold`, and SHALL flag `False`
   when `σ_window` equals 0. (Validates: Property 6, Property 7)
5. WHEN the input is a constant non-null sequence, THE Outlier_Service SHALL flag no index as an
   outlier under the `"zscore"` method regardless of threshold. (Validates: Property 7)
6. THE Outlier_Service SHALL support registering new detectors by name such that any registered
   detector satisfies the same length and None-safety contract. (Validates: Property 13)
7. IF `get_detector` is called with an unregistered method name, THEN THE Outlier_Service SHALL
   raise a descriptive error.
8. WHEN `/api/outlier-methods` is requested, THE Query_API SHALL return the list of registered
   detector names.

### Requirement 14: Command-Line Interface

**User Story:** As an operator, I want a Typer CLI, so that I can run ingestion, setup,
serving, and export from the command line.

#### Acceptance Criteria

1. THE CLI SHALL expose `ingest`, `backfill`, `setup`, `serve`, and `export` commands.
2. WHEN `ingest` is invoked, THE CLI SHALL run catch-up ingestion and echo the counts of records
   written, timestamps processed, and failures.
3. WHEN `serve` is invoked, THE CLI SHALL start the FastAPI query service on the specified host and port.
4. WHEN `export` is invoked, THE CLI SHALL stream a bulk dataset for the specified items, range,
   interval, and format to the specified output file using bounded memory.
5. IF a required configuration value is missing when a CLI command runs, THEN THE CLI SHALL print
   remediation guidance and exit with a non-zero status.

### Requirement 15: Automated Scheduling

**User Story:** As an operator, I want catch-up ingestion scheduled automatically,
so that the database stays current without manual runs.

#### Acceptance Criteria

1. WHILE the Scheduler_Service is running, THE Scheduler_Service SHALL trigger catch-up ingestion
   on a defined schedule of every 5 minutes rather than continuously.
2. WHEN a scheduled run detects a gap left by a prior failed run, THE Scheduler_Service SHALL
   backfill the missing windows on its next run.
3. THE Scheduler_Service SHALL be deployable as a long-running daemon (for example via systemd).

### Requirement 16: FastAPI Query and Export Endpoints

**User Story:** As a frontend and research consumer, I want typed REST endpoints for items,
price series, outliers, and exports, so that the SPA and ML clients read consistent data.

#### Acceptance Criteria

1. THE Query_API SHALL expose the routes `GET /api/health`, `GET /api/items`,
   `GET /api/items/{item_id}/prices`, `GET /api/items/{item_id}/prices/page`,
   `GET /api/items/{item_id}/outliers`, `GET /api/datasets/export`, `GET /api/datasets/features`,
   and `GET /api/outlier-methods`.
2. WHEN a price series is requested, THE Query_API SHALL return JSON containing `itemId`,
   `interval`, and a `points` array where each point includes OHLC/volume fields and an
   `isOutlier` flag. (Validates: Property 9)
3. THE Query_API SHALL return `points` in strictly ascending time order with `isOutlier` present
   as a boolean on every point. (Validates: Property 9)
4. WHEN a bulk export is requested, THE Query_API SHALL always stream the response using chunked
   transfer rather than buffering the full result set, regardless of whether the response could
   fit in a single payload.
5. WHEN a requested item has data, THE Query_API SHALL return a status code appropriate to the
   request type (for example, a `200` JSON body for a price series versus a streamed `200` for a
   bulk export) rather than a single uniform response; IF a requested item has no data, THEN THE
   Query_API SHALL return HTTP 404 with a typed error body.
6. THE Query_API SHALL validate all query parameters and reject requests exceeding the configured
   maximum items per request or maximum un-downsampled range unless an explicit `interval` is provided.
7. IF a query parameter is invalid, THEN THE Query_API SHALL return an HTTP 4xx response with a
   descriptive error body.

### Requirement 17: React + TypeScript SPA

**User Story:** As a user, I want a tailored dark-themed OSRS GE web app, so that I can search
items and view candlestick and volume charts with outlier highlighting.

#### Acceptance Criteria

1. WHEN a user types in the search box, THE React_SPA SHALL issue debounced item-search requests
   and display matching items.
2. WHEN a user selects an item, THE React_SPA SHALL render a candlestick chart with a volume overlay
   for the selected item and range.
3. WHEN a price point is flagged with `isOutlier` true, THE React_SPA SHALL visually highlight that point.
4. THE React_SPA SHALL render its interface using a dark theme by default, AND IF the dark theme
   fails to load, THEN THE React_SPA SHALL fall back to a default theme rather than failing to render.
5. WHEN the Query_API returns no data for a requested item, THE React_SPA SHALL display an
   empty-state message.
6. THE React_SPA SHALL consume the Query_API through typed fetch wrappers matching the defined
   TypeScript transport interfaces.

### Requirement 18: Grafana Dashboard Path

**User Story:** As an operator, I want a low-code Grafana dashboard on InfluxDB, so that I can
visualize prices and receive alerts without building application UI.

#### Acceptance Criteria

1. THE Grafana_Dashboard SHALL connect to InfluxDB v2 as a Flux data source for org
   `Ge-data-project` and bucket `GEItemPrices` using a token supplied from the environment.
2. THE Grafana_Dashboard SHALL provide an `$itemID` templating variable populated from distinct
   `itemID` tag values.
3. THE Grafana_Dashboard SHALL provide a line panel of `avgHighPrice` and `avgLowPrice`, a
   candlestick panel, and a gauge panel of latest combined volume for the selected item.
4. WHEN the latest `avgHighPrice` deviates from its moving average by more than a configured factor,
   THE Grafana_Dashboard SHALL fire an alert; IF the latest price, its moving average, or the
   configured factor is zero or undefined, THEN THE Grafana_Dashboard SHALL NOT fire the alert,
   avoiding false positives.
5. THE Grafana_Dashboard SHALL use parameterized Flux queries for the selected `$itemID`.

### Requirement 19: Security and Authentication

**User Story:** As an operator, I want the network-exposed services secured appropriately,
so that data is not unintentionally exposed beyond its intended audience.

#### Acceptance Criteria

1. THE Query_API SHALL validate all query parameters and SHALL restrict CORS to the React_SPA origin.
2. THE Query_API SHALL use parameterized Flux queries so that item IDs and ranges cannot be used for injection.
3. WHERE the deployment is a local single-user setup on localhost, THE Query_API MAY operate without
   an authentication layer.
4. WHERE the Query_API is exposed beyond localhost, THE Query_API SHALL be protected by an
   authentication layer such as an API key or reverse-proxy authentication.
5. THE Grafana_Dashboard SHALL retain its built-in login authentication.
6. THE ge_pipeline SHALL keep all secrets out of source control by loading them from environment or
   `.env`, which remains git-ignored.

### Requirement 20: Packaging and Dependencies

**User Story:** As a maintainer, I want the backend delivered as a proper package with pinned
dependencies, so that the environment is reproducible and dead code is removed.

#### Acceptance Criteria

1. THE ge_pipeline SHALL be organized as a Python package with a `pyproject.toml` declaring pinned
   backend dependency versions including `influxdb-client`, `pandas`, `python-dotenv`, `httpx`,
   `pydantic>=2`, `typer`, `apscheduler`, `fastapi`, `uvicorn`, `pytest`, `hypothesis`, and `pyarrow`.
2. THE React_SPA SHALL pin its JavaScript dependencies via `package-lock.json`.
3. THE ge_pipeline SHALL provide type hints and docstrings on all public functions following PEP 8
   snake_case naming.
4. THE ge_pipeline SHALL remove dead and duplicate legacy files superseded by the modernized package.

### Requirement 21: Testing Strategy

**User Story:** As a maintainer, I want a combined unit, property-based, and integration test
suite, so that the modernized backend's correctness is guaranteed by CI.

#### Acceptance Criteria

1. THE ge_pipeline SHALL include `pytest` unit tests covering config loading, record building,
   outlier math, and query builders using mocked `httpx` and mocked InfluxDB clients.
2. THE ge_pipeline SHALL include Hypothesis property-based tests encoding the design's correctness
   properties, each running at least 100 iterations.
3. THE ge_pipeline SHALL include property tests for `build_price_records` null filtering,
   `iter_missing_timestamps` correctness, `iter_time_chunks` partitioning, `get_price_page`
   lossless pagination, `stream_dataset` round-trip, and outlier detector length/None-safety.
   (Validates: Property 1, Property 2, Property 3, Property 4, Property 5, Property 6, Property 7,
   Property 10, Property 11, Property 12, Property 13)
4. THE ge_pipeline SHALL include integration tests that round-trip write then query against an
   InfluxDB test container and exercise FastAPI endpoints via `TestClient` against a seeded store.
5. THE ge_pipeline SHALL include a mandatory schema validation round-trip test, and WHENEVER a
   schema validation round-trip is tested, THE ge_pipeline SHALL verify that well-formed API JSON
   validates and that non-null numeric fields are integers greater than or equal to 0.
   (Validates: Property 8)
