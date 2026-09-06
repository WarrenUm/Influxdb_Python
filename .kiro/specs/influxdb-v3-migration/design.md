# Design Document

## Overview

This design cuts the `ge_pipeline` OSRS Grand Exchange price pipeline over from InfluxDB v2 to **InfluxDB 3 Core** (open-source, self-hosted). The cutover swaps the v2 client (`influxdb-client`, Flux, org/bucket) for the v3 client (`influxdb3-python`, SQL over Flight+gRPC, line-protocol writes, host/token/database), preserves the existing schema unchanged for the price data, adds a `setup` path that creates the target database, and provides a one-pass `migrate` command that reads the existing v2 server and copies records 1:1 into v3.

The guiding principle is a **narrow blast radius**: the query and write helpers in `ge_pipeline/influx.py` are the seam between "the rest of the pipeline" and "the storage engine". `ingestion.py`, `data_access.py`, `cli.py`, and `api.py` talk only to those helpers and to `config.py`. If the helpers keep their existing signatures and return shapes, the layers above them move to v3 for free. That is the strategy here:

- `influx.py` is rewritten to use `InfluxDBClient3`, SQL, and line protocol, but keeps `get_client`, `write_batch`, `get_latest_timestamp`, `query_price_series`, `query_chunk`, and `list_item_ids` with the same call shapes and return types (Requirements 6, 7).
- `config.py` gains v3 target settings and (lazily validated) v3 source settings for the migration tool (Requirement 2).
- The V3_Schema is intentionally kept identical to the v2 schema (same measurement, tag, field names, types, and timestamp semantics), so migration and ingestion change only the storage engine, not the data layout (Requirement 3).
- A new `migrate.py` module owns the *only* surviving v2 client + Flux read logic, used purely to read the source during migration (Requirements 1.5, 5).
- `cli.py` gains a `migrate` command and repoints `setup` at v3 database creation (Requirement 8).
- The Grafana datasource and dashboards move to a v3 SQL datasource (Requirement 9).

The pipeline continues to target Python 3.10+ (Requirement 1.4).

### How InfluxDB 3 Core changes the model

| Concept | InfluxDB v2 (today) | InfluxDB 3 Core (target) |
| --- | --- | --- |
| Connection identity | `url` + `org` + `token`, data in a **bucket** | `host` + `token` + **database** |
| Client | `InfluxDBClient` (`influxdb-client`) | `InfluxDBClient3` (`influxdb3-python`) |
| Write | line protocol via `write_api`, precision `s` | line protocol / `Point` via `client.write(...)`, precision `s` |
| Read | Flux (`from(bucket) |> range |> filter ...`) | **SQL** (or InfluxQL) over Flight+gRPC via `client.query(...)` |
| Downsample | `aggregateWindow(every, fn: mean)` | SQL `date_bin(interval, time) ... GROUP BY` with `avg(...)` |
| Tag listing | `schema.tagValues(...)` | `SELECT DISTINCT "itemID" FROM "itemPrice"` |
| Parameter binding | `params=` on the query API | `query_parameters={...}` with `$name` placeholders in SQL |
| Measurement | "measurement" | "table" (same wire concept; a table per measurement) |

The connection model change (host/token/database) drives the config change (Requirement 2.1) and the client cache key change (Requirement 1.3). The query language change (Flux to SQL) drives the whole query-path rewrite (Requirement 6) and the Grafana datasource change (Requirement 9).

## Architecture

### Component and data flow

```
                         ┌───────────────────────────────────────────────┐
                         │                   CLI (cli.py)                  │
                         │  ingest  backfill  serve  export  setup  migrate│
                         └───┬───────────┬──────────┬─────────┬───────┬────┘
                             │           │          │         │       │
             run_catch_up ───┘           │  stream_ │  setup_ │       │ run_migration
                                         │  dataset │  v3_db  │       │
              ┌──────────────────────────▼───┐  ┌───▼─────────▼──┐ ┌──▼───────────────┐
              │        ingestion.py           │  │  data_access.py│ │   migrate.py     │
              │  fetch snapshots (Wiki API)   │  │ export / paging│ │  (v2 read ONLY)  │
              │  build_price_records          │  └───────┬────────┘ │ influxdb-client  │
              └───────────────┬───────────────┘          │          │ + Flux read      │
                              │                           │          └──┬────────────┬──┘
                              │      Query_Path / Write_Path            │            │
                              ▼                           ▼             │ reads       │ writes
                    ┌─────────────────────────────────────────────┐    │ V2_Source   │ V3_Target
                    │                 influx.py                     │◄───┘ (v2)        │ (via influx.write_batch)
                    │  get_client (InfluxDBClient3, cached)         │                 │
                    │  write_batch (line protocol)                  │                 │
                    │  get_latest_timestamp / query_price_series /  │                 │
                    │  query_chunk / list_item_ids  (SQL + params)  │◄────────────────┘
                    └───────────────────────┬───────────────────────┘
                                            │ Flight+gRPC (SQL) / HTTP (line protocol)
                                            ▼
                              ┌──────────────────────────────┐
                              │      InfluxDB 3 Core          │
                              │  database = INFLUXDB3_DATABASE│
                              │  table    = itemPrice         │
                              └──────────────────────────────┘

  ┌───────────────┐   SQL (FlightSQL) datasource   ┌──────────────────────────────┐
  │    Grafana    │ ──────────────────────────────►│      InfluxDB 3 Core          │
  └───────────────┘                                └──────────────────────────────┘
```

Key structural points, mapped to requirements:

- **Single seam.** Everything above `influx.py` (ingestion, data access, API, CLI) is unchanged in shape; only the internals of `influx.py` change (Requirement 8.1). The migration tool is the sole exception: it holds its own v2 client (Requirement 1.5) and writes through `influx.write_batch` to reach v3.
- **The migration tool is one-directional and one-pass.** It reads v2 (Flux) and writes v3 (line protocol) as a straight 1:1 copy in a single streaming pass with batching + retry (Requirement 5.1, 5.3, 5.4).
- **Preflight before writes.** `run_migration` verifies both endpoints are reachable before writing anything, so a dead source or target fails fast with a naming error and zero partial writes (Requirement 5.6).

### Client connection model (InfluxDB 3)

`influxdb3-python` exposes `InfluxDBClient3(host=..., token=..., database=...)`. Writes accept line protocol or `Point` objects via `client.write(record=..., write_precision="s")`; reads use `client.query(query=<sql>, language="sql", query_parameters={...})`, which returns a PyArrow object we convert with `.to_pandas()` / `read_all()`. This is documented in the [influxdb3-python README](https://github.com/InfluxCommunity/influxdb3-python/blob/main/README.md) and [InfluxDB 3 Core Python docs](https://docs.influxdata.com/influxdb3/core/reference/client-libraries/v3/python/). *Content was rephrased for compliance with licensing restrictions.*

The client is cached exactly as today, but keyed by the v3 connection identity `(host, database, token)` instead of `(url, org, token)` (Requirement 1.3).

## Components and Interfaces

### 1. Configuration (`ge_pipeline/config.py`) — Requirement 2

`Settings` gains v3 target fields and a nested/optional group of v3 source fields for migration. Target values are validated eagerly on first `get_settings()`; source values are validated **lazily**, only when the migration tool asks for them, so `ingest`/`serve`/`export` never require source config (Requirements 2.2, 2.4).

```python
@dataclass(frozen=True)
class Settings:
    # --- V3 target (required for all commands) ---
    influx3_host: str            # INFLUXDB3_HOST_URL, e.g. http://localhost:8181
    influx3_token: str           # INFLUXDB3_AUTH_TOKEN
    influx3_database: str         # INFLUXDB3_DATABASE_NAME

    # --- V3 source / v2 (required ONLY for migration) ---
    v2_url: str | None = None     # V2_INFLUX_URL
    v2_token: str | None = None   # V2_INFLUX_TOKEN
    v2_org: str | None = None     # V2_INFLUX_ORG
    v2_bucket: str | None = None  # V2_INFLUX_BUCKET

    # --- unchanged tuning / Wiki API ---
    user_agent: str = "GEoutlier-detection"
    api_base_url: str = "https://prices.runescape.wiki/api/v1/osrs"
    max_concurrency: int = 8
    batch_size: int = 50


# Required for every command; checked in declared order so the error names the
# FIRST missing variable (Requirement 2.2).
_REQUIRED_V3_VARS = ("INFLUXDB3_HOST_URL", "INFLUXDB3_AUTH_TOKEN", "INFLUXDB3_DATABASE_NAME")

# Required only when the migration tool runs (Requirement 2.4).
_REQUIRED_V2_VARS = ("V2_INFLUX_URL", "V2_INFLUX_TOKEN", "V2_INFLUX_ORG", "V2_INFLUX_BUCKET")


def get_settings() -> Settings: ...          # validates _REQUIRED_V3_VARS, caches

def require_migration_source(settings: Settings) -> MigrationSource:
    """Validate and return v2 source settings, raising ConfigError naming the
    first missing V2_* variable. Called only by the migration command."""
```

`load_dotenv(override=False)` is preserved so process environment wins over `.env` (Requirement 2.6). Legacy `INFLUX_*` names are intentionally **not** reused for the v3 target to avoid ambiguity with the retained v2 source variables; the v3 client's own env conventions (`INFLUXDB3_*`) are adopted for clarity.

### 2. Storage seam (`ge_pipeline/influx.py`) — Requirements 6, 7

Same public API, v3 internals. Module constants keep the existing v2 schema unchanged (see Data Models):

```python
from influxdb_client_3 import InfluxDBClient3, Point

MEASUREMENT = "itemPrice"                 # unchanged from v2
PRICE_FIELDS = (                          # camelCase field names, unchanged from v2
    "avgHighPrice", "avgLowPrice", "highPriceVolume", "lowPriceVolume",
)
WRITE_PRECISION = "s"

_client_cache: dict[tuple[str, str, str], InfluxDBClient3] = {}   # (host, database, token)


def get_client(settings: Settings) -> InfluxDBClient3:
    key = (settings.influx3_host, settings.influx3_database, settings.influx3_token)
    client = _client_cache.get(key)
    if client is None:
        client = InfluxDBClient3(
            host=settings.influx3_host,
            token=settings.influx3_token,
            database=settings.influx3_database,
        )
        _client_cache[key] = client
    return client
```

**Write path** (`write_batch`) — Requirement 7. Unchanged contract: empty batch is a no-op (7.2); non-empty batch is written with seconds precision (7.1); failures are wrapped as `TransientError`, retried via the existing `retry_with_backoff()` helper (7.3), and on exhaustion the batch is logged and dropped without raising (7.4). Records are converted from the existing record dict shape to `Point` objects (or line protocol) using the unchanged v2 schema; `write_use_v2_api=False` is used so InfluxDB 3 Core's write endpoint is targeted.

```python
def _record_to_point(record: dict) -> Point:
    p = Point(record["measurement"])          # MEASUREMENT = "itemPrice"
    for tag, val in record["tags"].items():    # {"itemID": "..."} -> tag "itemID"
        p = p.tag(tag, val)
    for field, val in record["fields"].items():  # camelCase field names
        p = p.field(field, val)
    return p.time(record["time"])               # unix seconds

def write_batch(client: InfluxDBClient3, database: str, records: list[dict]) -> None:
    if not records:
        return
    @retry_with_backoff()
    def _do_write() -> None:
        try:
            client.write(record=[_record_to_point(r) for r in records],
                         write_precision=WRITE_PRECISION)
        except Exception as exc:               # network / write errors
            raise TransientError(f"InfluxDB 3 write failed: {exc}") from exc
    try:
        _do_write()
    except TransientError as exc:
        logger.error("Dropping batch of %d after retries: %s", len(records), exc)
```

> Note: `write_batch`'s `bucket` parameter is renamed to `database` to match v3 terminology. The incoming record shape is unchanged: measurement `itemPrice`, tag `itemID`, and the four camelCase fields. Because the schema is preserved, `ingestion.build_price_records` and its `_FIELD_NAME_MAP` need no changes. See Data Models.

**Query path** — Requirement 6. Flux is replaced with parameterized SQL. All user-supplied item IDs and time bounds are passed via `query_parameters` using `$name` placeholders, never string-interpolated (Requirement 6.7). Timestamps are compared using SQL `to_timestamp($start_s)` (seconds) or by binning; the four fields are selected as columns.

Because the schema keeps the v2 camelCase names, every SQL identifier for the measurement, tag, and fields must be **double-quoted** so InfluxDB 3's SQL engine treats it as a case-sensitive identifier rather than folding it to lowercase. This is the cost of preserving the schema, and it is accepted per the decision to keep the v2 layout unchanged: unquoted `itemPrice`/`avgHighPrice` would resolve to `itemprice`/`avghighprice` and fail, so `"itemPrice"`, `"itemID"`, and `"avgHighPrice"` appear quoted in every query, dashboard, and ad-hoc SQL statement.

`get_latest_timestamp` (6.2):

```sql
SELECT max(time) AS latest FROM "itemPrice" WHERE "itemID" = $item_id
```
Returns `int(latest.timestamp())` or `None` when the item has no rows.

`query_price_series` (6.3, 6.4) — without interval, select ordered points; with interval, bin and average:

```sql
-- no interval
SELECT time, "avgHighPrice", "avgLowPrice", "highPriceVolume", "lowPriceVolume"
FROM "itemPrice"
WHERE "itemID" = $item_id AND time >= $start AND time < $stop
ORDER BY time

-- with interval (mean per window, Requirement 6.4)
SELECT date_bin($interval, time) AS time,
       avg("avgHighPrice")    AS "avgHighPrice",
       avg("avgLowPrice")     AS "avgLowPrice",
       avg("highPriceVolume") AS "highPriceVolume",
       avg("lowPriceVolume")  AS "lowPriceVolume"
FROM "itemPrice"
WHERE "itemID" = $item_id AND time >= $start AND time < $stop
GROUP BY date_bin($interval, time)
ORDER BY time
```

Returns the same `list[dict]` shape as today: each point has `time` (unix seconds) plus the four fields. The dicts continue to use the existing camelCase field names, so callers that key by field name (`data_access._extract_points` keys only on `time`; `api` transport) need no changes.

`query_chunk` (6.5) matches multiple items via an SQL `IN` list bound as parameters, one row per `("itemID", time)`, one column per field, returned as a `pandas.DataFrame` via `query_dataframe`. Empty `item_ids` returns an empty DataFrame (preserved).

`list_item_ids` (6.6):

```sql
SELECT DISTINCT "itemID" FROM "itemPrice" ORDER BY "itemID"
```

**Parameter binding note (6.7):** InfluxDB 3's Python client accepts a `query_parameters` mapping and substitutes `$name` placeholders server-side, so an `itemID` such as `"1'; DROP ..."` is treated purely as a value and simply matches no rows. The `IN` clause for `query_chunk` binds each id as `$id0, $id1, ...` rather than concatenating them into the query text. Note that parameter placeholders (`$item_id`) are distinct from quoted identifiers (`"itemID"`): the placeholder binds a value, the quoted name selects a case-sensitive column.

### 3. Database setup (`ge_pipeline/cli.py::setup` + helper) — Requirement 4

InfluxDB 3 Core creates databases via its management API / `influxdb3 create database` semantics. The client exposes database administration through the HTTP management endpoint; the setup helper:

1. Preflights the server with a lightweight query/health call; on failure reports a connection error naming `influx3_host` and exits non-zero (Requirements 4.3, 8.4).
2. Creates `influx3_database` if absent (Requirement 4.1). Creation is **idempotent**: if the database already exists, the "already exists" response is caught and reported as success without creating a duplicate (Requirement 4.2).

```python
def setup_v3_database(settings: Settings) -> str:
    """Create the target database if absent; return one of {"created","exists"}.
    Raises ConfigError/ConnectionError naming the host when unreachable."""
```

### 4. Migration tool (`ge_pipeline/migrate.py` + `cli.py::migrate`) — Requirements 1.5, 5

This module is the **only** place the v2 `influxdb-client` and Flux survive (Requirement 1.5, 10.4). It reads the v2 source and writes into v3 through `influx.write_batch`, in a single streaming pass.

```python
from influxdb_client import InfluxDBClient   # retained ONLY here

@dataclass
class MigrationResult:
    records_read: int
    records_written: int

def iter_v2_records(v2_client, bucket) -> Iterator[dict]:
    """Stream v2 rows via Flux, time-chunked (or per-item) to bound memory,
    yielding record dicts that preserve the v2 names 1:1 (the V3_Schema is
    identical to v2):
    {"measurement": "itemPrice",
     "tags": {"itemID": <id>},
     "time": <unix seconds>,
     "fields": {"avgHighPrice": ..., ...non-null...}}"""

def run_migration(settings: Settings) -> MigrationResult:
    source = require_migration_source(settings)          # Req 2.4
    v2 = InfluxDBClient(url=source.url, token=source.token, org=source.org, ...)
    v3 = influx.get_client(settings)
    _preflight(v2, v3, settings)                          # Req 5.6: raise+stop, no writes
    batch, read, written = [], 0, 0
    for rec in iter_v2_records(v2, source.bucket):        # Req 5.1 single pass
        read += 1
        batch.append(rec)
        if len(batch) >= settings.batch_size:             # Req 5.3 batched
            written += _write_batch_counting(v3, settings.influx3_database, batch)
            batch.clear()
    written += _write_batch_counting(v3, settings.influx3_database, batch)  # final flush
    return MigrationResult(records_read=read, records_written=written)      # Req 5.5
```

- **1:1 copy — no name transformation (5.2):** each v2 point (measurement `itemPrice`, tag `itemID`, camelCase fields, unix-second `_time`) is copied into v3 with the same measurement, tag, field names, and timestamp. Only the storage engine and connection change; the `itemID` string, the four numeric field values, and the timestamp are preserved exactly.
- **Batched writes with retry (5.3, 5.4):** writes go through `influx.write_batch`, which already retries with exponential backoff via the shared helper. For migration, a batch that exhausts retries is surfaced as a failure rather than silently dropped so the operator sees an accurate written count.
- **Streaming (5.1):** reads are time-chunked (reusing the `iter_time_chunks` idea) or per-item so memory stays bounded regardless of history length.
- **Reporting (5.5):** returns and the CLI echoes `records_read` and `records_written`.
- **Preflight (5.6):** if either endpoint is unreachable at start, raise a connection error naming the unreachable server and return before any write.

### 5. CLI (`ge_pipeline/cli.py`) — Requirement 8

- `ingest`, `backfill`, `serve`, `export` are unchanged in code beyond the settings updates flowing through `influx.py` (8.1); the schema (and therefore field names) is preserved.
- `setup` now calls `setup_v3_database` and reports created/exists/connection-error (8.2, 4.x).
- New `migrate` command wraps `run_migration` and echoes read/written counts (8.3).
- Any command that needs v3 and can't reach it prints a message naming `influx3_host` and exits with code 1 (8.4). The existing `_require_settings` remediation is updated to name the `INFLUXDB3_*` variables.

### 6. Grafana (`grafana/provisioning/datasources` + dashboards) — Requirement 9

- The datasource type moves from the v2 InfluxDB/Flux datasource to the **InfluxDB v3 (FlightSQL/SQL) datasource**, referencing `host`, `token`, and `database` (Requirements 9.1, 9.2). The token stays injected from the environment (`${INFLUXDB3_AUTH_TOKEN}`), never hardcoded.
- Dashboard panels' Flux queries are rewritten to SQL selecting the four fields under the unchanged v2 schema names; the `$itemID` template variable query becomes `SELECT DISTINCT "itemID" FROM "itemPrice"` (Requirement 9.3). Candlestick/gauge field mappings keep the existing camelCase field names; SQL identifiers are double-quoted for case sensitivity.

## Data Models

### Current v2 storage schema

- Measurement: `itemPrice`
- Tag: `itemID` (string; ~thousands of distinct OSRS item IDs)
- Fields: `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume` (integers ≥ 0, nullable per item/window)
- Timestamp: unix seconds, aligned to 5-minute boundaries

### V3 schema — preserved unchanged from v2 (Requirement 3)

The V3_Schema is intentionally identical to the v2 layout. Every element carries over 1:1; nothing is renamed or retyped.

| Element | v2 value | **v3 value** | Status |
| --- | --- | --- | --- |
| Table / measurement | `itemPrice` | `itemPrice` | **unchanged** |
| Tag | `itemID` | `itemID` | **unchanged** |
| Field: high avg | `avgHighPrice` | `avgHighPrice` | **unchanged** |
| Field: low avg | `avgLowPrice` | `avgLowPrice` | **unchanged** |
| Field: high vol | `highPriceVolume` | `highPriceVolume` | **unchanged** |
| Field: low vol | `lowPriceVolume` | `lowPriceVolume` | **unchanged** |
| Timestamp | unix seconds, 5-min aligned | unix seconds, 5-min aligned | **unchanged** |
| Field types | int ≥ 0 | int ≥ 0 | **unchanged** |

**Rationale.** The user chose to preserve the schema unchanged. Keeping the exact v2 names keeps the migration a lossless, straight 1:1 copy (no rename step that could drift or lose data), and leaves downstream and domain naming untouched: `ingestion.build_price_records`, `data_access.py`, and `api.py` need no field-name changes, and dashboards keep their field references. The one accepted cost is that InfluxDB 3 SQL requires the camelCase measurement, tag, and field names to be double-quoted (`"itemPrice"`, `"itemID"`, `"avgHighPrice"`) so they are treated as case-sensitive identifiers rather than folded to lowercase. This tradeoff is accepted as the price of the simplest, safest migration. The four price/volume fields, the `itemID` tag, and the unix-second timestamp remain the required core (Requirement 3.1), preserved by type and unit so migrated values equal the source exactly (Requirement 3.2).

### Line protocol shape (write)

```
itemPrice,itemID=554 avgHighPrice=180i,avgLowPrice=176i,highPriceVolume=1200i,lowPriceVolume=980i 1615733100
```

Null fields are still filtered out before the point is built (a point with no non-null fields is skipped), preserving today's `build_price_records` behavior.

### Field-name mapping used by migration and ingestion

Because the schema is preserved, the v2 name equals the v3 name for every field — the mapping is the identity.

| Domain (`ItemPricePoint`) | v2 storage | **v3 storage** |
| --- | --- | --- |
| `avg_high_price` | `avgHighPrice` | `avgHighPrice` (== v2) |
| `avg_low_price` | `avgLowPrice` | `avgLowPrice` (== v2) |
| `high_price_volume` | `highPriceVolume` | `highPriceVolume` (== v2) |
| `low_price_volume` | `lowPriceVolume` | `lowPriceVolume` (== v2) |

`ingestion.build_price_records` and its `_FIELD_NAME_MAP` stay as-is; no change is needed because the storage names are unchanged.

## Error Handling

| Condition | Handling | Requirement |
| --- | --- | --- |
| Missing required v3 target config | `get_settings()` raises `ConfigError` naming the first missing `INFLUXDB3_*` var | 2.2 |
| Missing v2 source config (migration only) | `require_migration_source()` raises `ConfigError` naming the first missing `V2_*` var; non-migration commands never trigger it | 2.4 |
| v3 write failure | Wrapped as `TransientError`, retried with exponential backoff via `retry_with_backoff()` | 7.3 |
| v3 write retries exhausted (ingestion) | Logged; batch dropped; run continues so a later run backfills | 7.4 |
| v3 write retries exhausted (migration) | Counted as failure; surfaced in `MigrationResult` so `records_written` is accurate | 5.4, 5.5 |
| Database already exists at setup | Caught; reported as "already exists"; no duplicate | 4.2 |
| v3 server unreachable at setup | Connection error naming `influx3_host`; non-zero exit | 4.3, 8.4 |
| Source or target unreachable at migration start | Connection error naming the unreachable server; stop before any write | 5.6 |
| Malicious / odd `itemID` or time value in a query | Bound via `query_parameters`; treated as a literal value, matches nothing, never executed as SQL | 6.7 |
| Logging subsystem failure during ingestion | Swallowed via `_safe_log` (availability over audit trail) | unchanged |

## Testing Strategy

**Approach.** A live **InfluxDB 3 Core** instance via `testcontainers` backs the query/write and setup/migration integration tests (Requirements 10.1, 10.2). Pure transform, config, caching, and query-shaping logic are covered by Hypothesis property tests (Requirement 10.3) using in-memory fakes where a live server adds no value. A static import-scan test asserts the v2 client (`influxdb_client`) is imported **only** inside `ge_pipeline/migrate.py` (Requirement 10.4).

- **Unit / example tests:** empty-batch no-op, write-retry-then-drop, migration preflight-stops-before-write, setup idempotence and unreachable-host messaging, CLI exit codes and command registration, Grafana YAML/JSON config assertions.
- **Property tests:** config first-missing-variable, client caching identity, env-over-dotenv precedence, write→read round-trip preservation, latest-timestamp = max, series ascending + four fields, interval mean vs. reference, chunk uniqueness per `(item,time)`, distinct item ids, batch partition (coverage + no duplication), migration 1:1 copy preservation (names unchanged), parameter-binding injection safety.
- **Configuration:** Hypothesis profile `ge` already enforces ≥100 examples per property (`tests/conftest.py`); each property test is tagged **Feature: influxdb-v3-migration, Property N: <text>**.
- **Cutover check (10.4):** full suite passes with no v2 runtime-client references outside the migration tool.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Client caching identity

*For any* two `get_client` calls, they return the identical `InfluxDBClient3` instance if and only if their `(host, database, token)` connection tuples are equal.

**Validates: Requirements 1.3**

### Property 2: Config names the first missing target variable

*For any* subset of the required v3 target variables being unset or empty, `get_settings` raises a configuration error naming the first missing variable in declared order; when all are present it returns settings without error.

**Validates: Requirements 2.1, 2.2**

### Property 3: Migration source validated only on demand

*For any* environment where the v3 target variables are present but some `V2_*` source variables are missing, non-migration settings access succeeds, while requesting migration source settings raises a configuration error naming the first missing `V2_*` variable.

**Validates: Requirements 2.4**

### Property 4: Process environment overrides `.env`

*For any* variable set in both the process environment and the `.env` file, the loaded configuration value equals the process-environment value.

**Validates: Requirements 2.6**

### Property 5: Write/read round-trip preserves the snapshot

*For any* set of Price_Snapshots written through the v3 Write_Path, reading them back through the Query_Path yields the same `itemID`, the same four camelCase field values (`avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`), and the same unix-second timestamp for every non-null-bearing record.

**Validates: Requirements 3.1, 3.2, 3.3, 7.1**

### Property 6: Latest timestamp equals the maximum stored

*For any* stored series for an item, `get_latest_timestamp` returns the maximum stored timestamp (unix seconds) for that item, and returns a null result when the item has no stored data.

**Validates: Requirements 6.2**

### Property 7: Price series is time-ascending and complete

*For any* stored series and time range, `query_price_series` returns points ordered strictly ascending by time, each carrying the four price/volume fields.

**Validates: Requirements 6.3**

### Property 8: Interval downsampling is the per-window mean

*For any* stored series and aggregation interval, each returned window value equals the mean of the source values falling within that window, as computed by a reference `date_bin` mean.

**Validates: Requirements 6.4**

### Property 9: Chunk frame has one row per item-and-timestamp

*For any* set of items and time range, `query_chunk` returns a frame with exactly one row per `(itemID, timestamp)` present in the data and one column per price/volume field.

**Validates: Requirements 6.5**

### Property 10: Distinct item listing

*For any* stored dataset, `list_item_ids` returns exactly the set of distinct `itemID` values written.

**Validates: Requirements 6.6**

### Property 11: Query parameters are bound, not interpolated

*For any* item-id string — including ones containing SQL metacharacters such as quotes, semicolons, or comment markers — the Query_Path executes without error and returns results consistent with treating the string as a literal value (no injected SQL is executed).

**Validates: Requirements 6.7**

### Property 12: Migration copies records 1:1 preserving names

*For any* set of V2_Source records, the migration copies each record into V3_Target 1:1, preserving the `itemPrice` measurement, the `itemID` tag, the four camelCase field names and values (`avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`), and the record timestamp, without renaming or transforming any name.

**Validates: Requirements 5.1, 5.2, 10.2**

### Property 13: Migration batching partitions the records

*For any* record count and batch size, the migration writes the records as batches whose sizes never exceed the batch size and whose concatenation contains every source record exactly once (no loss, no duplication).

**Validates: Requirements 5.3**

### Property 14: Migration counts are accurate

*For any* source dataset whose records are all successfully written, the reported read count equals the number of records read from the source and the reported written count equals the number of records persisted to the target.

**Validates: Requirements 5.5**
