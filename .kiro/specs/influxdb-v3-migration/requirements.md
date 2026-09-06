# Requirements Document

## Introduction

The `python_InfluxDB` project is a Grand Exchange / Old School RuneScape (OSRS) price data pipeline that ingests 5-minute price snapshots from the OSRS Wiki API and stores them in InfluxDB v2, exposing them through a CLI, an HTTP/data-access layer, and Grafana dashboards. Today it depends on the InfluxDB v2 client (`influxdb-client==1.50.0`), Flux queries, and v2 connection settings (`INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_BUCKET`).

This feature performs a full cutover of the pipeline from InfluxDB v2 to InfluxDB 3 Core (open-source, self-hosted). It adopts the `influxdb3-python` client and query language (SQL/InfluxQL) while preserving the existing schema, creates the target v3 database, and provides a one-pass migration tool that reads from the existing v2 server and copies the data 1:1 into the new v3 database without renaming or transforming the schema. After cutover, v2 read logic survives only inside the migration tool; all other read/write paths, CLI commands, configuration, and the Grafana datasource use v3 equivalents.

## Glossary

- **Pipeline**: The `ge_pipeline` Python package and its modules (config, influx, ingestion, data_access, cli, api, scheduler, etc.) that ingest, store, and serve OSRS price data.
- **V2_Source**: The existing InfluxDB v2 server, organization (`Ge-data-project`), and bucket (`GEItemPrices`) that currently hold price data.
- **V3_Target**: The new InfluxDB 3 Core server and database that will hold migrated and future price data.
- **V3_Client**: The `influxdb3-python` client library used by the Pipeline to connect to V3_Target using host URL, token, and database name.
- **Price_Snapshot**: A single 5-minute observation for one item, carrying the fields `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, and `lowPriceVolume`, keyed by `itemID` and a unix-second timestamp.
- **V3_Schema**: The InfluxDB 3 table/measurement, tag, field, and timestamp layout adopted for Price_Snapshot data in V3_Target. The V3_Schema mirrors the current v2 layout exactly: the `itemPrice` measurement, the `itemID` tag, the four camelCase fields `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, and `lowPriceVolume`, integer types, and unix-second timestamps, all unchanged from v2.
- **Migration_Tool**: The command that reads Price_Snapshot data from V2_Source and copies the records 1:1 into V3_Target in a single pass, preserving all names.
- **Query_Path**: Any Pipeline function that reads price data (`get_latest_timestamp`, `query_price_series`, `query_chunk`, `list_item_ids`).
- **Write_Path**: The Pipeline function (`write_batch`) that persists Price_Snapshot records.
- **CLI**: The `ge_pipeline` command-line interface with `ingest`, `backfill`, `serve`, `export`, and `setup` subcommands.
- **Grafana_Datasource**: The provisioned Grafana datasource configuration under `grafana/provisioning/datasources` used by the price dashboards.
- **Config_Service**: The `ge_pipeline.config` module exposing `Settings` and `get_settings`.
- **Test_Suite**: The tests under `tests/`, including property-based (Hypothesis) and testcontainers-based tests.

## Requirements

### Requirement 1: V3 Client Integration

**User Story:** As a maintainer, I want the Pipeline to use the InfluxDB 3 client, so that the project connects to and communicates with an InfluxDB 3 Core server.

#### Acceptance Criteria

1. THE Pipeline SHALL declare `influxdb3-python` as a runtime dependency and SHALL remove the `influxdb-client` v2 dependency from the Pipeline runtime dependencies.
2. THE V3_Client SHALL connect to V3_Target using a host URL, an authentication token, and a database name.
3. WHEN the Pipeline requires a V3_Client with the same host URL, token, and database as a prior request, THE Pipeline SHALL reuse a single cached V3_Client instance rather than opening a new connection.
4. WHERE Python version compatibility is required, THE Pipeline SHALL run on Python 3.10 or later.
5. THE Migration_Tool SHALL retain the InfluxDB v2 client and Flux read logic for the sole purpose of reading from V2_Source.

### Requirement 2: Configuration and Environment Changes

**User Story:** As an operator, I want v3 connection settings, so that I can configure the Pipeline against an InfluxDB 3 Core server.

#### Acceptance Criteria

1. THE Config_Service SHALL expose settings for the V3_Target host URL, authentication token, and database name.
2. IF a required V3_Target configuration value is unset or empty at first access, THEN THE Config_Service SHALL raise a configuration error that names the first missing value.
3. THE Config_Service SHALL expose settings for the V2_Source host URL, token, organization, and bucket for use by the Migration_Tool.
4. WHERE Migration_Tool settings are not needed for non-migration commands, THE Config_Service SHALL treat V2_Source values as required only when the Migration_Tool is invoked.
5. THE Pipeline SHALL update `.env.example` to document the v3 and migration environment variables.
6. WHEN configuration is loaded, THE Config_Service SHALL read values from the process environment and an optional `.env` file without overriding values already set in the process environment.

### Requirement 3: V3 Schema Adoption

**User Story:** As a data engineer, I want the v3 storage to keep the same schema as the current v2 data, so that the migration changes only the storage engine and not the data layout.

#### Acceptance Criteria

1. THE V3_Schema SHALL represent each Price_Snapshot using the same names as the v2 schema: the `itemPrice` measurement, an `itemID` tag, the four fields `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, and `lowPriceVolume`, and a unix-second timestamp.
2. THE V3_Schema SHALL preserve the numeric type and unit of each Price_Snapshot field so that migrated values equal the corresponding V2_Source values.
3. THE Write_Path SHALL persist Price_Snapshot records into V3_Target using the V3_Schema.
4. THE design document SHALL record that the V3_Schema is intentionally preserved unchanged from the v2 layout.

### Requirement 4: Database Creation

**User Story:** As an operator, I want the Pipeline to create the v3 database, so that the target storage exists before ingestion or migration runs.

#### Acceptance Criteria

1. WHEN the operator runs the database setup command, THE Pipeline SHALL create the configured V3_Target database on the V3_Target server.
2. IF the configured V3_Target database already exists when setup runs, THEN THE Pipeline SHALL complete without creating a duplicate database and SHALL report that the database already exists.
3. IF the V3_Target server is unreachable when setup runs, THEN THE Pipeline SHALL report a connection error identifying the V3_Target host.

### Requirement 5: Data Migration

**User Story:** As a data engineer, I want a migration command, so that existing v2 price data moves into the v3 database unchanged, using the same schema.

#### Acceptance Criteria

1. WHEN the operator runs the Migration_Tool, THE Migration_Tool SHALL read Price_Snapshot data from V2_Source and write the records unchanged into V3_Target in a single pass.
2. THE Migration_Tool SHALL copy each V2_Source record into V3_Target as a 1:1 copy, preserving the `itemID`, the four field values, the field and measurement names, and the record timestamp without renaming or transforming any name.
3. WHEN the Migration_Tool writes records, THE Migration_Tool SHALL write in batches.
4. IF a batch write to V3_Target fails, THEN THE Migration_Tool SHALL retry the write with exponential backoff before reporting failure.
5. WHEN the Migration_Tool completes, THE Migration_Tool SHALL report the count of records read from V2_Source and the count of records written to V3_Target.
6. IF either V2_Source or V3_Target is unreachable when the Migration_Tool starts, THEN THE Migration_Tool SHALL report a connection error identifying the unreachable server and SHALL stop before writing.

### Requirement 6: Read Query Path Migration

**User Story:** As a developer, I want the read paths ported to v3 query languages, so that the Pipeline retrieves data from InfluxDB 3.

#### Acceptance Criteria

1. THE Query_Path SHALL retrieve price data from V3_Target using SQL or InfluxQL instead of Flux.
2. WHEN `get_latest_timestamp` is called for an item, THE Pipeline SHALL return the most recent stored timestamp for that item in unix seconds, or a null result when the item has no stored data.
3. WHEN `query_price_series` is called for an item and time range, THE Pipeline SHALL return a time-ascending series of points, each carrying the timestamp and the four price/volume fields.
4. WHERE an aggregation interval is supplied to `query_price_series` or `query_chunk`, THE Pipeline SHALL return values downsampled to that interval using a mean aggregation per window.
5. WHEN `query_chunk` is called for a set of items and a time range, THE Pipeline SHALL return a combined frame with one row per item-and-timestamp and a column per price/volume field.
6. WHEN `list_item_ids` is called, THE Pipeline SHALL return the distinct `itemID` values stored in V3_Target.
7. WHEN a Query_Path builds a query from item IDs or time ranges, THE Pipeline SHALL bind those values as query parameters rather than interpolating them into the query text.

### Requirement 7: Write Path Migration

**User Story:** As a developer, I want the write path ported to v3, so that ingestion persists snapshots into InfluxDB 3.

#### Acceptance Criteria

1. WHEN the Write_Path receives a non-empty batch of Price_Snapshot records, THE Pipeline SHALL write the batch to V3_Target using the V3_Schema.
2. WHEN the Write_Path receives an empty batch, THE Pipeline SHALL complete without writing to V3_Target.
3. IF a write to V3_Target fails, THEN THE Write_Path SHALL retry the write with exponential backoff.
4. IF all write retries are exhausted, THEN THE Write_Path SHALL log the failure and continue so that a later ingestion run can backfill the gap.

### Requirement 8: CLI Updates

**User Story:** As an operator, I want the CLI to work against v3, so that ingestion, serving, and export continue to function after cutover.

#### Acceptance Criteria

1. WHEN the operator runs the `ingest`, `backfill`, `serve`, or `export` command, THE CLI SHALL operate against V3_Target using the V3_Client and v3 Query_Path and Write_Path.
2. WHEN the operator runs the `setup` command, THE CLI SHALL create the V3_Target database as defined in Requirement 4.
3. THE CLI SHALL expose the Migration_Tool as an invocable command.
4. IF a CLI command requires V3_Target and the server is unreachable, THEN THE CLI SHALL report a connection error identifying the V3_Target host and SHALL exit with a non-zero status.

### Requirement 9: Grafana Datasource Update

**User Story:** As a dashboard user, I want the Grafana datasource updated for v3, so that the price dashboards continue to display data after cutover.

#### Acceptance Criteria

1. THE Grafana_Datasource SHALL be configured to query V3_Target using a v3-compatible query mode instead of the v2 Flux mode.
2. THE Grafana_Datasource SHALL reference the V3_Target host URL, token, and database.
3. WHERE dashboard queries reference the price data, THE dashboard queries SHALL retrieve the four price/volume fields under the V3_Schema.

### Requirement 10: Testing and Verification

**User Story:** As a maintainer, I want the test suite updated for v3, so that the migrated Pipeline is verified before and after cutover.

#### Acceptance Criteria

1. THE Test_Suite SHALL exercise the v3 Query_Path and Write_Path against an InfluxDB 3 Core instance.
2. THE Test_Suite SHALL verify that the Migration_Tool copies V2_Source records into V3_Target 1:1, preserving the `itemID` tag, the four field values, the field and measurement names `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`, and the record timestamp without renaming.
3. WHEN the Test_Suite runs the existing property-based checks, THE Test_Suite SHALL validate v3 Query_Path and Write_Path behavior over generated inputs.
4. WHEN the full Test_Suite is run, THE Test_Suite SHALL pass with no references to the removed v2 runtime client outside the Migration_Tool.
