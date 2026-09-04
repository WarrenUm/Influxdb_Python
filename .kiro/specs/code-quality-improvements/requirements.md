# Requirements Document

## Introduction

This specification covers twelve code quality improvements to the OSRS Grand Exchange Price Collector project. The improvements span security hardening (removing hardcoded secrets), reliability (error handling, null guards), performance (client reuse, batch writes), code hygiene (removing dead code, unused imports, legacy files), developer experience (dependency management, type hints, docstrings, logging), and maintainability (extracting magic numbers to named constants).

## Glossary

- **Pipeline**: The OSRS Grand Exchange Price Collector data ingestion system comprising InfluxAdmin.py and RunTimestampFetch.py
- **Ingestion_Script**: The RunTimestampFetch.py module that orchestrates fetching price data and writing it to the database
- **Core_Module**: The InfluxAdmin.py module containing database connection, write, query, and API call functions
- **Legacy_File**: The smallFcns.py file containing older duplicate functions superseded by Core_Module
- **Wiki_API**: The RuneScape Wiki Prices API at https://prices.runescape.wiki/api/v1/osrs
- **Database_Client**: The InfluxDB v2 Python client instance used for reads and writes
- **Price_Record**: A dictionary containing measurement name, tags (itemID), fields (avgHighPrice, avgLowPrice, highPriceVolume, lowPriceVolume), and timestamp
- **Batch_Size**: The number of timestamps worth of Price_Records accumulated before flushing to InfluxDB
- **Retry_Backoff**: An exponential delay strategy where wait time doubles on each successive failure attempt (initial delay 1s, multiplier 2x)

## Requirements

### Requirement 1: Secure Configuration Management

**User Story:** As a developer, I want credentials and configuration stored in environment variables, so that secrets are not committed to version control.

#### Acceptance Criteria

1. THE Core_Module SHALL load the InfluxDB connection parameters from environment variables named INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, and INFLUX_BUCKET
2. IF a required environment variable (INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, or INFLUX_BUCKET) is missing or set to an empty string, THEN THE Core_Module SHALL raise an EnvironmentError whose message contains the name of the unset variable
3. WHERE python-dotenv is importable, THE Core_Module SHALL load variables from a .env file before reading environment variables, with actual environment variables taking precedence over .env file values
4. THE Pipeline SHALL include a .env.example file listing the entries INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, and INFLUX_BUCKET with placeholder values and no actual secrets
5. THE .gitignore SHALL include an entry that excludes .env files from version control

### Requirement 2: Error Handling with Retry Logic

**User Story:** As a developer, I want API and database operations to retry on transient failures, so that temporary network issues do not crash the entire ingestion run.

#### Acceptance Criteria

1. WHEN a Wiki_API request fails with a transient error (connection timeout, socket error, or HTTP 5xx response), THE Core_Module SHALL retry up to 3 times using Retry_Backoff with an initial delay of 1 second and exponential multiplier of 2 (delays: 1s, 2s, 4s)
2. WHEN a Wiki_API request fails after all 3 retry attempts, THE Core_Module SHALL log the error with the original error context, skip the current timestamp, and continue processing the next timestamp in the ingestion loop
3. IF a Wiki_API request fails with a non-transient error (HTTP 4xx response), THEN THE Core_Module SHALL not retry and SHALL log the error with the HTTP status code and skip the current timestamp
4. WHEN a database write operation fails with a transient error (connection timeout, socket error, or InfluxDB write timeout), THE Core_Module SHALL retry up to 3 times using Retry_Backoff with an initial delay of 1 second and exponential multiplier of 2 (delays: 1s, 2s, 4s)
5. WHEN a database write operation fails after all 3 retry attempts, THE Core_Module SHALL log the error with the original error context, skip the current batch, and continue processing the next timestamp in the ingestion loop
6. THE Core_Module SHALL set a 10-second timeout on all Wiki_API HTTP requests
7. THE Core_Module SHALL set a 10-second timeout on all database write operations

### Requirement 3: Database Client Lifecycle Management

**User Story:** As a developer, I want the InfluxDB client created once and reused across the ingestion loop, so that the system avoids creating hundreds of unnecessary TCP connections.

#### Acceptance Criteria

1. THE Ingestion_Script SHALL create a single Database_Client instance before entering the ingestion loop and reuse that same instance for both the initial database query and all subsequent write operations within a single run
2. WHILE the ingestion loop is executing, THE Ingestion_Script SHALL use the pre-created Database_Client instance for every write operation without creating or closing additional Database_Client instances
3. WHEN the ingestion loop completes successfully, THE Ingestion_Script SHALL close the Database_Client exactly once
4. IF an unhandled exception occurs during the ingestion loop, THEN THE Ingestion_Script SHALL close the Database_Client before exiting (e.g., via try/finally or context manager) ensuring no open connections remain
5. IF a single write operation fails within the ingestion loop, THEN THE Ingestion_Script SHALL continue to the next iteration using the same Database_Client instance without recreating the connection

### Requirement 4: Legacy File Removal

**User Story:** As a developer, I want the duplicate legacy file removed, so that the codebase has a single source of truth for utility functions.

#### Acceptance Criteria

1. THE Pipeline SHALL NOT include the Legacy_File (smallFcns.py) as a git-tracked file in the repository
2. THE Pipeline SHALL NOT include the latestTime.txt file as a git-tracked file in the repository
3. THE .gitignore SHALL include an entry for latestTime.txt to prevent the file from being tracked if accidentally re-created locally
4. THE codebase SHALL NOT contain any import statements or module references to smallFcns in any git-tracked Python file or Jupyter notebook
5. THE codebase SHALL NOT contain any file read or write operations referencing latestTime.txt in any git-tracked Python file or Jupyter notebook

### Requirement 5: Commented-Out Code Removal

**User Story:** As a developer, I want all commented-out code removed from the ingestion script, so that the codebase is clean and readable.

#### Acceptance Criteria

1. THE Ingestion_Script SHALL contain no commented-out lines of executable code, where a commented-out line of executable code is defined as any line beginning with `#` that contains valid Python statements, function calls, variable assignments, or control flow constructs
2. THE Ingestion_Script SHALL retain only comments that explain intent, describe behavior, or provide documentation (e.g., inline clarifications of logic, TODO notes, or docstrings)
3. WHEN a commented-out line is identified as executable code, THE Ingestion_Script SHALL have that line deleted entirely rather than replaced with blank lines or alternative text

### Requirement 6: Unused Import Removal

**User Story:** As a developer, I want unused imports removed, so that the codebase only references dependencies it actually uses.

#### Acceptance Criteria

1. THE Ingestion_Script SHALL NOT contain import statements for modules that are not referenced in the file's executable code
2. THE Core_Module SHALL NOT contain import statements for modules that are not referenced in the file's executable code
3. THE Ingestion_Script SHALL use explicit named imports (e.g., from module import name1, name2) instead of wildcard imports (from module import *)

### Requirement 7: Dependency Management

**User Story:** As a developer, I want a requirements file listing all dependencies with pinned versions, so that anyone cloning the project can reproduce the environment.

#### Acceptance Criteria

1. THE Pipeline SHALL include a requirements.txt file at the project root that is valid pip requirements format
2. THE requirements.txt SHALL list all runtime Python dependencies (influxdb_client, requests, numpy, pandas, tqdm, python-dotenv) with exact pinned versions using the == operator at full major.minor.patch precision
3. THE requirements.txt SHALL include python-dotenv as a dependency
4. WHEN a developer runs pip install -r requirements.txt in a clean virtual environment, THE Pipeline SHALL enable execution of RunTimestampFetch.py without ModuleNotFoundError

### Requirement 8: Type Hints and Docstrings

**User Story:** As a developer, I want all public functions annotated with type hints and docstrings, so that the code is self-documenting and easier to maintain.

#### Acceptance Criteria

1. THE Core_Module SHALL include type annotations on all public function parameters and return types, where a public function is any function whose name does not begin with an underscore
2. THE Core_Module SHALL include a Google-style docstring on every public function containing a one-line summary, an "Args" section listing each parameter with its type and description, and a "Returns" section describing the return value and its type
3. THE Ingestion_Script SHALL include type annotations on all public function parameters and return types, where a public function is any function whose name does not begin with an underscore
4. THE Ingestion_Script SHALL include a Google-style docstring on every public function containing a one-line summary, an "Args" section listing each parameter with its type and description, and a "Returns" section describing the return value and its type
5. IF a function parameter or return value may be None, THEN THE Core_Module and Ingestion_Script SHALL annotate that type using Optional from the typing module

### Requirement 9: Batch Write Optimization

**User Story:** As a developer, I want records accumulated across multiple timestamps and flushed in batches, so that InfluxDB write throughput is improved.

#### Acceptance Criteria

1. THE Ingestion_Script SHALL accumulate Price_Records across multiple timestamps before writing to the database
2. WHEN the number of accumulated timestamps reaches the configured Batch_Size, THE Ingestion_Script SHALL flush all accumulated records to the database in a single write call
3. WHEN the ingestion loop completes, THE Ingestion_Script SHALL flush any remaining accumulated records to the database regardless of count
4. THE Batch_Size SHALL default to 50 timestamps worth of records
5. IF a flush operation fails after retry exhaustion, THEN THE Ingestion_Script SHALL log the error, discard the failed batch, and continue accumulating new records

### Requirement 10: None Value Handling

**User Story:** As a developer, I want null price values from the API handled gracefully, so that invalid data does not corrupt the database or crash the pipeline.

#### Acceptance Criteria

1. WHEN the Wiki_API returns null for one or more of the fields avgHighPrice, avgLowPrice, highPriceVolume, or lowPriceVolume for an item, THE Ingestion_Script SHALL omit each null field from that item's Price_Record while retaining all non-null fields
2. WHEN all four fields (avgHighPrice, avgLowPrice, highPriceVolume, lowPriceVolume) are null for an item, THE Ingestion_Script SHALL not include that item in the batch written to InfluxDB
3. WHEN the Ingestion_Script skips an item because all four price and volume fields are null, THE Ingestion_Script SHALL log a warning message that includes the skipped item's itemID and the associated timestamp
4. WHEN a Price_Record contains at least one non-null field after null exclusion, THE Ingestion_Script SHALL write that record to InfluxDB with the measurement name, itemID tag, and timestamp preserved unchanged

### Requirement 11: Named Constants for Magic Numbers

**User Story:** As a developer, I want magic numbers replaced with named constants, so that their purpose is clear and values are easy to change.

#### Acceptance Criteria

1. THE Pipeline SHALL define a module-level constant in RunTimestampFetch.py with an UPPER_SNAKE_CASE name representing the lookback duration, assigned the value 64800 (18 hours in seconds)
2. THE Pipeline SHALL define a module-level constant in RunTimestampFetch.py with an UPPER_SNAKE_CASE name representing the polling interval, assigned the value 300 (5 minutes in seconds)
3. THE Pipeline SHALL define a module-level constant in RunTimestampFetch.py with an UPPER_SNAKE_CASE name representing the reference item ID used for timestamp detection, assigned the string value "554"
4. THE Pipeline SHALL define a module-level constant in InfluxAdmin.py with an UPPER_SNAKE_CASE name representing the API base URL, assigned the value "https://prices.runescape.wiki/api/v1/osrs"
5. THE Pipeline SHALL define a module-level constant in InfluxAdmin.py with an UPPER_SNAKE_CASE name representing the User-Agent header value, assigned the value "GEoutlier-detection"
6. WHEN a named constant is defined, THE Pipeline SHALL replace all corresponding inline literal usages in that module with a reference to the named constant

### Requirement 12: Structured Logging

**User Story:** As a developer, I want proper logging with severity levels, so that I can control output verbosity and diagnose issues in production.

#### Acceptance Criteria

1. THE Pipeline SHALL use Python's logging module for all diagnostic output
2. THE Pipeline SHALL NOT use print() statements for operational messages
3. THE Pipeline SHALL log at DEBUG level for detailed diagnostic information (API request URLs, raw response sizes, intermediate variable values)
4. THE Pipeline SHALL log at INFO level for normal operational progress (latest timestamp retrieved, number of timestamps to process, batch writes completed)
5. THE Pipeline SHALL log at WARNING level when encountering null price data fields or retryable failures (network timeouts, transient API errors)
6. THE Pipeline SHALL log at ERROR level when operations fail after all retry attempts are exhausted
7. THE Pipeline SHALL configure the log format to include ISO 8601 timestamp, severity level name, logger name, and message in each log record
8. THE Pipeline SHALL allow the minimum log level to be set via an environment variable (LOG_LEVEL) defaulting to INFO when not specified
