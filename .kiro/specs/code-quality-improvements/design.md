# Technical Design Document

## Overview

This design describes the refactoring of the OSRS Grand Exchange Price Collector to address twelve code quality improvements. The refactoring maintains the existing two-file architecture (InfluxAdmin.py as core module, RunTimestampFetch.py as ingestion script) while introducing proper configuration management, error handling, logging, and performance optimizations.

## Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     RunTimestampFetch.py                         │
│  ┌────────────┐  ┌────────────────┐  ┌──────────────────────┐  │
│  │  Constants  │  │  main()        │  │  build_price_records │  │
│  │  LOOKBACK   │  │  - setup log   │  │  - null filtering    │  │
│  │  INTERVAL   │  │  - create client│  │  - record assembly  │  │
│  │  REF_ITEM   │  │  - batch loop  │  └──────────────────────┘  │
│  └────────────┘  │  - flush final  │                            │
│                   └───────┬────────┘                            │
└───────────────────────────┼─────────────────────────────────────┘
                            │ imports
┌───────────────────────────▼─────────────────────────────────────┐
│                       InfluxAdmin.py                             │
│  ┌──────────────┐  ┌─────────────────┐  ┌───────────────────┐  │
│  │ Configuration│  │  DB Functions    │  │  API Functions     │  │
│  │ load_config()│  │  get_db_client() │  │  fetch_5m_data()   │  │
│  │ INFLUX_URL   │  │  write_to_db()   │  │  fetch_as_json()   │  │
│  │ INFLUX_TOKEN │  │  query_db()      │  │                    │  │
│  │ API_BASE_URL │  │  _create_flux()  │  │  @retry_on_error   │  │
│  │ USER_AGENT   │  │  _execute_query()│  │                    │  │
│  └──────────────┘  └─────────────────┘  └───────────────────────┘
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  retry_on_error(func, max_retries, initial_delay)            │ │
│  │  - exponential backoff decorator/helper                      │ │
│  └──────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘

External:
  .env / environment variables → loaded at module import
  .env.example → template for developers
  requirements.txt → pinned dependencies
  logging → configured at main() entry point
```

### Data Flow (Improved)

```
main() starts
  │
  ├─ Configure logging (level from LOG_LEVEL env var)
  ├─ Load config (env vars / .env via load_config())
  ├─ Create single InfluxDB client
  ├─ Query latest timestamp (with retry)
  ├─ Calculate time range
  │
  ├─ FOR each timestamp in range:
  │     ├─ Fetch 5m data from Wiki API (with retry, 10s timeout)
  │     ├─ Build price records (skip null items, omit null fields)
  │     ├─ Append to batch accumulator
  │     └─ IF batch_count >= BATCH_SIZE:
  │           └─ Flush batch to InfluxDB (with retry)
  │
  ├─ Flush remaining records
  └─ Close client (in finally block)
```

## Components and Interfaces

### InfluxAdmin.py (Core Module)

| Function | Signature | Description |
|----------|-----------|-------------|
| `load_config()` | `() -> dict[str, str]` | Loads env vars with .env fallback, raises EnvironmentError on missing |
| `get_db_client()` | `() -> InfluxDBClient` | Creates configured client from loaded config |
| `write_to_db()` | `(client: InfluxDBClient, records: list[dict]) -> None` | Writes records with retry; raises TransientError on failure |
| `query_db()` | `(client, start_time, item_id, bucket?) -> pd.DataFrame` | Queries price data with parameterized Flux |
| `fetch_5m_data()` | `(timestamp: int) -> dict` | Fetches Wiki API data with retry and timeout |
| `retry_on_transient()` | decorator | Exponential backoff decorator for transient errors |

### RunTimestampFetch.py (Ingestion Script)

| Function | Signature | Description |
|----------|-----------|-------------|
| `build_price_records()` | `(data: dict, timestamp: int) -> list[dict]` | Assembles records with null filtering |
| `main()` | `() -> None` | Orchestrates ingestion: query → fetch → batch → write |

### Exception Classes (InfluxAdmin.py)

| Class | Purpose |
|-------|---------|
| `TransientError` | Signals retryable failures (network, 5xx, timeouts) |
| `NonTransientError` | Signals non-retryable failures (4xx responses) |

## Data Models

### Price_Record (InfluxDB write format)

```python
{
    "measurement": "itemPrice",       # Always "itemPrice"
    "tags": {"itemID": str},          # Item identifier as string
    "fields": {                       # Only non-null fields included
        "avgHighPrice": int | None,   # Omitted if null from API
        "avgLowPrice": int | None,    # Omitted if null from API
        "highPriceVolume": int | None, # Omitted if null from API
        "lowPriceVolume": int | None,  # Omitted if null from API
    },
    "time": int                       # Unix timestamp (seconds)
}
```

### Configuration dict (from load_config)

```python
{
    "url": str,     # INFLUX_URL - e.g. "http://localhost:8086"
    "token": str,   # INFLUX_TOKEN - auth token
    "org": str,     # INFLUX_ORG - e.g. "Ge-data-project"
    "bucket": str,  # INFLUX_BUCKET - e.g. "GEItemPrices"
}
```

### Wiki API Response (from /5m endpoint)

```python
{
    "timestamp": int,           # Unix timestamp of this 5-min window
    "data": {
        "<item_id>": {          # String key
            "avgHighPrice": int | None,
            "highPriceVolume": int,
            "avgLowPrice": int | None,
            "lowPriceVolume": int,
        },
        ...
    }
}
```

## Detailed Design

### 1. Configuration Module (InfluxAdmin.py top-level)

```python
"""Configuration loading with .env fallback."""
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# --- Named Constants (Req 11) ---
API_BASE_URL: str = "https://prices.runescape.wiki/api/v1/osrs"
USER_AGENT: str = "GEoutlier-detection"

def load_config() -> dict[str, str]:
    """Load InfluxDB configuration from environment variables.

    Attempts to load from .env file first (if python-dotenv is available),
    then reads from environment. Raises EnvironmentError for missing values.

    Returns:
        dict with keys: url, token, org, bucket
    """
    # Try loading .env (Req 1, AC3)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    required_vars = {
        "INFLUX_URL": "url",
        "INFLUX_TOKEN": "token",
        "INFLUX_ORG": "org",
        "INFLUX_BUCKET": "bucket",
    }

    config = {}
    for env_var, key in required_vars.items():
        value = os.environ.get(env_var, "").strip()
        if not value:
            raise EnvironmentError(
                f"Required environment variable '{env_var}' is not set or empty"
            )
        config[key] = value

    return config


# Load at module level so all functions can access
_config = load_config()
```

### 2. Retry Logic (InfluxAdmin.py)

```python
"""Retry helper with exponential backoff."""
import time
from functools import wraps
from typing import TypeVar, Callable, Any

MAX_RETRIES: int = 3
INITIAL_DELAY: float = 1.0
BACKOFF_MULTIPLIER: float = 2.0
REQUEST_TIMEOUT: int = 10

class TransientError(Exception):
    """Wraps a transient failure that may succeed on retry."""
    pass

class NonTransientError(Exception):
    """Wraps a non-transient failure that should not be retried."""
    pass


def retry_on_transient(
    max_retries: int = MAX_RETRIES,
    initial_delay: float = INITIAL_DELAY,
    multiplier: float = BACKOFF_MULTIPLIER,
) -> Callable:
    """Decorator that retries a function on TransientError.

    Args:
        max_retries: Maximum number of retry attempts.
        initial_delay: Seconds to wait before first retry.
        multiplier: Exponential multiplier for subsequent delays.

    Returns:
        Decorated function with retry behavior.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = initial_delay
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except TransientError as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            "Transient error in %s (attempt %d/%d): %s. "
                            "Retrying in %.1fs...",
                            func.__name__, attempt + 1, max_retries, e, delay
                        )
                        time.sleep(delay)
                        delay *= multiplier
                    else:
                        logger.error(
                            "Failed after %d retries in %s: %s",
                            max_retries, func.__name__, e
                        )
            raise last_exception
        return wrapper
    return decorator
```

### 3. API Functions (InfluxAdmin.py)

```python
"""Wiki API interaction with retry and timeout."""
import requests
import json
from typing import Optional

@retry_on_transient()
def fetch_5m_data(timestamp: int) -> dict:
    """Fetch 5-minute price data from the RuneScape Wiki API.

    Args:
        timestamp: Unix timestamp for the 5-minute window.

    Returns:
        Parsed JSON response as a dict with 'data' and 'timestamp' keys.

    Raises:
        TransientError: On connection errors or HTTP 5xx responses.
        NonTransientError: On HTTP 4xx responses.
    """
    url = f"{API_BASE_URL}/5m?timestamp={timestamp}"
    headers = {"User-Agent": USER_AGENT}
    logger.debug("Fetching: %s", url)

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    except (requests.ConnectionError, requests.Timeout) as e:
        raise TransientError(f"Connection failed: {e}") from e

    if response.status_code >= 500:
        raise TransientError(
            f"Server error {response.status_code} from Wiki API"
        )
    if response.status_code >= 400:
        raise NonTransientError(
            f"Client error {response.status_code} from Wiki API"
        )

    return response.json()
```

### 4. Database Functions (InfluxAdmin.py)

```python
"""InfluxDB interaction with retry."""
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
import pandas as pd
from typing import Union
from datetime import datetime

def get_db_client() -> InfluxDBClient:
    """Create an InfluxDB client using loaded configuration.

    Returns:
        Configured InfluxDBClient instance.
    """
    return InfluxDBClient(
        url=_config["url"],
        token=_config["token"],
        org=_config["org"],
        timeout=REQUEST_TIMEOUT * 1000,  # client timeout in milliseconds
    )


@retry_on_transient()
def write_to_db(client: InfluxDBClient, records: list[dict]) -> None:
    """Write records to InfluxDB with retry on transient failures.

    Args:
        client: Active InfluxDBClient instance.
        records: List of InfluxDB record dicts to write.

    Raises:
        TransientError: On connection/timeout errors.
    """
    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(
            bucket=_config["bucket"],
            write_precision="s",
            record=records,
        )
        logger.debug("Wrote %d records to InfluxDB", len(records))
    except (ApiException, OSError) as e:
        raise TransientError(f"Database write failed: {e}") from e


def query_db(
    client: InfluxDBClient,
    start_time: Union[int, datetime],
    item_id: str,
    bucket: Optional[str] = None,
) -> pd.DataFrame:
    """Query InfluxDB for item price data.

    Args:
        client: Active InfluxDBClient instance.
        start_time: Start of the query time range (unix timestamp or datetime).
        item_id: The item ID to filter on.
        bucket: Optional bucket override; defaults to configured bucket.

    Returns:
        DataFrame with columns: avgHighPrice, avgLowPrice,
        highPriceVolume, lowPriceVolume, _time, itemID.
    """
    target_bucket = bucket or _config["bucket"]
    flux_query = '''
    from(bucket: _bucket)
    |> range(start: _timeStart)
    |> filter(fn: (r) => r.itemID == _itemID)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> keep(columns: ["avgHighPrice", "avgLowPrice","highPriceVolume","lowPriceVolume","_time","itemID"])
    '''
    params = {
        "_bucket": target_bucket,
        "_timeStart": start_time,
        "_itemID": item_id,
    }
    query_api = client.query_api()
    return query_api.query_data_frame(query=flux_query, params=params)
```

### 5. Ingestion Script (RunTimestampFetch.py)

```python
"""OSRS Grand Exchange price data ingestion script."""
import logging
import os
from datetime import datetime
from typing import Optional

import numpy as np
from tqdm import tqdm

from InfluxAdmin import (
    get_db_client,
    write_to_db,
    query_db,
    fetch_5m_data,
    TransientError,
    NonTransientError,
)

# --- Named Constants (Req 11) ---
LOOKBACK_SECONDS: int = 64800          # 18 hours
POLL_INTERVAL_SECONDS: int = 300       # 5 minutes
REFERENCE_ITEM_ID: str = "554"         # Item used to detect latest DB timestamp
BATCH_SIZE: int = 50                   # Timestamps accumulated before flush

# --- Logging Setup (Req 12) ---
logger = logging.getLogger(__name__)


def build_price_records(
    data: dict, timestamp: int
) -> list[dict]:
    """Build InfluxDB records from API response, filtering null values.

    Args:
        data: The 'data' dict from the Wiki API response keyed by item ID.
        timestamp: The Unix timestamp for these records.

    Returns:
        List of InfluxDB-compatible record dicts with null fields excluded.
    """
    price_fields = ["avgHighPrice", "avgLowPrice", "highPriceVolume", "lowPriceVolume"]
    records = []

    for item_id, measure in data.items():
        # Filter out null fields (Req 10)
        fields = {
            field: measure[field]
            for field in price_fields
            if measure.get(field) is not None
        }

        if not fields:
            logger.warning(
                "Skipping item %s at timestamp %d: all fields are null",
                item_id, timestamp,
            )
            continue

        record = {
            "measurement": "itemPrice",
            "tags": {"itemID": str(item_id)},
            "fields": fields,
            "time": timestamp,
        }
        records.append(record)

    return records


def main() -> None:
    """Run the price data ingestion pipeline.

    Queries InfluxDB for the latest stored timestamp, calculates the gap
    to the current time, fetches all missing 5-minute intervals from the
    Wiki API, and writes them in batches to InfluxDB.
    """
    # Configure logging (Req 12)
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    now = int(datetime.now().timestamp())
    client = get_db_client()

    try:
        # Determine latest data in DB
        response = query_db(
            client=client,
            start_time=(now - LOOKBACK_SECONDS),
            item_id=REFERENCE_ITEM_ID,
        )
        latest_time = int(response._time.max().timestamp())
        logger.info("Latest DB timestamp: %d", latest_time)
        logger.info("Gap: %d minutes behind", (now - latest_time) // 60)

        time_range = np.arange(latest_time, now, POLL_INTERVAL_SECONDS)
        logger.info("Processing %d timestamps", len(time_range))

        # Batch accumulator (Req 9)
        batch: list[dict] = []
        timestamps_in_batch: int = 0

        for ts in tqdm(time_range):
            try:
                json_data = fetch_5m_data(int(ts))
            except (TransientError, NonTransientError) as e:
                logger.error("Skipping timestamp %d: %s", ts, e)
                continue

            records = build_price_records(json_data["data"], json_data["timestamp"])
            batch.extend(records)
            timestamps_in_batch += 1

            # Flush when batch is full (Req 9)
            if timestamps_in_batch >= BATCH_SIZE:
                try:
                    write_to_db(client, batch)
                    logger.info(
                        "Flushed batch: %d records from %d timestamps",
                        len(batch), timestamps_in_batch,
                    )
                except TransientError as e:
                    logger.error("Batch write failed, discarding: %s", e)
                batch = []
                timestamps_in_batch = 0

        # Flush remaining records (Req 9, AC3)
        if batch:
            try:
                write_to_db(client, batch)
                logger.info(
                    "Final flush: %d records from %d timestamps",
                    len(batch), timestamps_in_batch,
                )
            except TransientError as e:
                logger.error("Final batch write failed: %s", e)

    finally:
        client.close()
        logger.info("Client closed, ingestion complete")


if __name__ == "__main__":
    main()
```

### 6. Project Configuration Files

#### .env.example
```
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=your-token-here
INFLUX_ORG=Ge-data-project
INFLUX_BUCKET=GEItemPrices
LOG_LEVEL=INFO
```

#### requirements.txt
```
influxdb-client==1.38.0
requests==2.31.0
numpy==1.24.4
pandas==2.0.3
tqdm==4.66.1
python-dotenv==1.0.0
```

#### .gitignore additions
```
.env
latestTime.txt
```

### 7. Files to Remove

| File | Reason |
|------|--------|
| `smallFcns.py` | Duplicate of InfluxAdmin.py functions (Req 4) |
| `latestTime.txt` | Legacy timestamp tracking, superseded by DB query (Req 4) |

## Error Handling

| Error Type | Source | Strategy | Outcome |
|---|---|---|---|
| Missing env var | `load_config()` | Raise `EnvironmentError` immediately | Script exits with clear message |
| Network timeout | `fetch_5m_data()` | Retry 3x with backoff (1s, 2s, 4s) | Skip timestamp on exhaustion |
| HTTP 5xx | `fetch_5m_data()` | Retry 3x with backoff | Skip timestamp on exhaustion |
| HTTP 4xx | `fetch_5m_data()` | No retry, raise `NonTransientError` | Skip timestamp immediately |
| DB write failure | `write_to_db()` | Retry 3x with backoff | Discard batch on exhaustion |
| All fields null | `build_price_records()` | Skip item, log WARNING | Item excluded from batch |
| Some fields null | `build_price_records()` | Omit null fields only | Partial record written |
| Unhandled exception | `main()` | try/finally closes client | Clean resource release |

## Correctness Properties

1. **No data loss on partial failure**: A single failed API call or write does not abort the entire run. The ingestion loop continues with remaining timestamps.
2. **Client resource safety**: The InfluxDB client is always closed, even on exceptions, via try/finally.
3. **Idempotent writes**: Writing the same timestamp twice to InfluxDB overwrites (InfluxDB deduplicates on measurement + tags + timestamp), so retries and re-runs are safe.
4. **Configuration fail-fast**: Missing credentials are detected at import time before any network calls.
5. **Batch integrity**: A batch is either fully written or fully discarded — no partial batch states.

## Testing Strategy

### Unit Tests (recommended future addition)

| Test | Validates |
|------|-----------|
| `test_load_config_missing_var` | Raises EnvironmentError with variable name |
| `test_load_config_success` | Returns correct dict from env |
| `test_build_price_records_all_null` | Returns empty list, logs warning |
| `test_build_price_records_partial_null` | Omits null fields, keeps non-null |
| `test_build_price_records_valid` | Returns complete records |
| `test_retry_on_transient_success` | Returns on first success |
| `test_retry_on_transient_eventual` | Succeeds after N failures |
| `test_retry_on_transient_exhausted` | Raises after max_retries |

### Integration Tests (manual verification)

1. Set valid .env, run `python RunTimestampFetch.py` — confirm data appears in InfluxDB
2. Remove INFLUX_TOKEN from env — confirm EnvironmentError with "INFLUX_TOKEN" in message
3. Stop InfluxDB — confirm retry logs and eventual skip (no crash)
4. Set LOG_LEVEL=DEBUG — confirm verbose API URL logging

## Traceability Matrix

| Requirement | Design Section | Implementation Location |
|---|---|---|
| Req 1: Secure Config | §1 Configuration Module | InfluxAdmin.py `load_config()` |
| Req 2: Error Handling | §2 Retry Logic, §3 API Functions | InfluxAdmin.py `retry_on_transient`, `fetch_5m_data`, `write_to_db` |
| Req 3: Client Lifecycle | §5 Ingestion Script | RunTimestampFetch.py `main()` try/finally |
| Req 4: Legacy Removal | §7 Files to Remove | Delete smallFcns.py, latestTime.txt |
| Req 5: Commented Code | §5 Ingestion Script | RunTimestampFetch.py rewrite (no dead comments) |
| Req 6: Unused Imports | §5 Ingestion Script | Explicit imports in both files |
| Req 7: Dependencies | §6 Project Config | requirements.txt |
| Req 8: Type Hints | §1-§5 all functions | All function signatures |
| Req 9: Batch Writes | §5 Ingestion Script | Batch accumulator in main() |
| Req 10: None Handling | §5 Ingestion Script | `build_price_records()` |
| Req 11: Named Constants | §1, §5 | Module-level UPPER_SNAKE_CASE constants |
| Req 12: Logging | §5 Ingestion Script, all modules | `logging` module throughout |
