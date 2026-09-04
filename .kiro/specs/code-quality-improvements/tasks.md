# Implementation Plan

## Overview

This plan implements twelve code quality improvements to the OSRS Grand Exchange Price Collector project. Tasks are ordered to handle foundational changes first (config files, legacy removal) before rewriting the two main modules, then validating everything works.

## Tasks

- [x] 1. Create project configuration files (.env.example, .gitignore updates, requirements.txt)
  - Create `.env.example` at the project root with placeholder entries for INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, and LOG_LEVEL
  - Update `.gitignore` to include entries for `.env` and `latestTime.txt`
  - Create `requirements.txt` at the project root with pinned versions: influxdb-client==1.38.0, requests==2.31.0, numpy==1.24.4, pandas==2.0.3, tqdm==4.66.1, python-dotenv==1.0.0
  - Requirements: 1, 4, 7

- [x] 2. Remove legacy files (smallFcns.py, latestTime.txt)
  - Delete `smallFcns.py` from the repository
  - Delete `latestTime.txt` from the repository
  - Verify no remaining import or reference to `smallFcns` exists in any Python file or notebook
  - Requirements: 4

- [x] 3. Rewrite InfluxAdmin.py with configuration, constants, retry logic, type hints, and logging
  - Remove hardcoded token, URL, org, and bucket variables from module top-level
  - Add `load_config()` function that loads from environment variables with python-dotenv fallback, raising EnvironmentError on missing/empty vars
  - Define module-level constants: `API_BASE_URL` and `USER_AGENT`
  - Define retry constants: `MAX_RETRIES = 3`, `INITIAL_DELAY = 1.0`, `BACKOFF_MULTIPLIER = 2.0`, `REQUEST_TIMEOUT = 10`
  - Add custom exception classes: `TransientError` and `NonTransientError`
  - Implement `retry_on_transient()` decorator with exponential backoff (delays: 1s, 2s, 4s)
  - Rewrite `get5mItemData` → `fetch_5m_data(timestamp: int) -> dict` with retry decorator, 10s timeout, transient/non-transient error classification
  - Rewrite `GetDatabaseClient` → `get_db_client() -> InfluxDBClient` using loaded config
  - Rewrite `WriteToDatabase` → `write_to_db(client: InfluxDBClient, records: list[dict]) -> None` with retry decorator
  - Rewrite `QueryDatabase` → `query_db(client, start_time, item_id, bucket=None) -> pd.DataFrame` with type hints
  - Remove legacy functions: `incrementTime`, `updateTimeFile`, `getLatestTimestamp`, `getDFAsJson`
  - Remove the bare `print` statement and any unused imports
  - Add Google-style docstrings to all public functions
  - Add `import logging` and create module-level `logger = logging.getLogger(__name__)`
  - Requirements: 1, 2, 6, 8, 11, 12

- [x] 4. Rewrite RunTimestampFetch.py with clean code, batching, null handling, and logging
  - Remove all commented-out lines of executable code
  - Remove unused imports (`from distutils.log import error`, `from tqdm.notebook import tqdm_notebook`)
  - Replace `from InfluxAdmin import *` with explicit named imports
  - Define module-level constants: `LOOKBACK_SECONDS = 64800`, `POLL_INTERVAL_SECONDS = 300`, `REFERENCE_ITEM_ID = "554"`, `BATCH_SIZE = 50`
  - Add logging configuration in `main()` with ISO 8601 format, level from `LOG_LEVEL` env var defaulting to INFO
  - Create `build_price_records(data: dict, timestamp: int) -> list[dict]` with null field filtering and WARNING log for all-null items
  - Rewrite `main()` to create a single client before the loop and close in a `finally` block
  - Implement batch accumulation: collect records across timestamps, flush when `timestamps_in_batch >= BATCH_SIZE`
  - Flush remaining records after the loop completes
  - Replace all `print()` calls with `logger.info()` / `logger.warning()` / `logger.error()`
  - Wrap API/DB calls in try/except, logging and skipping/discarding on failure
  - Add `if __name__ == "__main__": main()` guard
  - Add Google-style docstrings and type annotations to all public functions
  - Requirements: 2, 3, 5, 6, 8, 9, 10, 11, 12

- [x] 5. Validate the refactored code
  - Run `python -c "from InfluxAdmin import load_config, get_db_client, write_to_db, query_db, fetch_5m_data"` with a `.env` file to verify imports succeed
  - Run `python -c "import RunTimestampFetch"` to verify no syntax errors
  - Verify `.env.example` contains all 5 expected variable names with placeholder values
  - Verify `.gitignore` contains entries for `.env` and `latestTime.txt`
  - Verify `requirements.txt` is valid pip format
  - Verify `smallFcns.py` and `latestTime.txt` no longer exist in the project
  - Verify no `print()` statements remain in InfluxAdmin.py or RunTimestampFetch.py
  - Verify no commented-out executable code remains in RunTimestampFetch.py
  - Requirements: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

## Task Dependency Graph

```json
{
  "waves": [
    {"tasks": [1, 2]},
    {"tasks": [3]},
    {"tasks": [4]},
    {"tasks": [5]}
  ]
}
```

## Notes

- Tasks 1 and 2 are independent and can be done in parallel
- Task 3 must be completed before Task 4 because RunTimestampFetch.py imports from InfluxAdmin.py
- Task 5 validates all changes together and depends on tasks 1-4 being complete
- The design document at `design.md` contains full code examples for each rewritten module
