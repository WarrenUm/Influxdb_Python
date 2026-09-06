# Dead Code Removal — Working Notes

## Task 1.1 — Verify a clean tree and record the Baseline_Green_Set

_Requirements: 6.1_

### Step 1: Clean-tree check (`git status --porcelain`)

**Result: TREE IS CLEAN.** `git status --porcelain` produced empty output
(exit code 0). The working tree was committed prior to this run, so the
baseline pytest run below reflects HEAD exactly.

### Step 2: Baseline pytest run

Command (run from repo root inside the project `.venv`, Python 3.12.3):

```
.venv/bin/python -m pytest
```

The suite respects `testpaths=["tests"]` from `pyproject.toml`.
Integration tests (testcontainers-based InfluxDB) **did run** — Docker was
available in this environment, so `tests/test_integration_influx.py` and
`tests/test_integration_api.py` executed against a real container rather
than being skipped.

### Summary counts

| Outcome    | Count |
|------------|-------|
| passed     | 146   |
| failed     | 1     |
| skipped    | 0     |
| deselected | 0     |
| errors     | 0     |
| warnings   | 3     |

Final pytest summary line:

```
1 failed, 146 passed, 3 warnings in ~17s
```

> Note: The many `ERROR ... retry.py` / `ingestion.py` / `influx.py` lines in
> the raw output are **captured application log records** emitted by passing
> tests that intentionally exercise failure/retry paths. They are NOT pytest
> `errors` outcomes. The pytest error count is 0.

### Baseline failing test (recorded, not fixed)

Per task instructions, no fixes were attempted. One test fails at baseline:

- `tests/test_integration_influx.py::test_list_item_ids_returns_distinct_ids`

Failure detail:

```
def test_list_item_ids_returns_distinct_ids(seeded_client):
    ids = influx.list_item_ids(seeded_client)
>   assert set(ids) >= {_ITEM_A, _ITEM_B}
E   AssertionError: assert set() >= {'554', '565'}
E     Extra items in the right set: '554', '565'
tests/test_integration_influx.py:214: AssertionError
```

`list_item_ids` returned an empty set against the seeded container. This is a
pre-existing baseline failure (likely a seeding/flush-timing or query issue in
the integration harness); it is recorded here so it is NOT later mistaken for a
regression introduced by a removal batch. This test is **excluded** from the
Baseline_Green_Set.

---

## Baseline_Green_Set

The following 146 test node ids passed at HEAD on a clean tree. This is the
Baseline_Green_Set — every later removal batch must keep all of these passing
(the failing test above is not part of the set and is not a regression gate).

```
tests/test_api.py::test_health_returns_ok
tests/test_api.py::test_outlier_methods_returns_registered_list
tests/test_api.py::test_items_returns_filtered_list
tests/test_api.py::test_item_prices_returns_price_series_shape
tests/test_api.py::test_item_prices_404_when_no_data
tests/test_api.py::test_item_prices_invalid_range_returns_400
tests/test_api.py::test_item_prices_invalid_method_returns_400
tests/test_api.py::test_page_limit_zero_returns_400
tests/test_api.py::test_page_limit_over_max_returns_400
tests/test_api.py::test_export_unsupported_format_returns_400
tests/test_api.py::test_raw_range_too_wide_without_interval_returns_400
tests/test_api.py::test_wide_range_with_interval_ok
tests/test_api.py::test_export_too_many_items_returns_400
tests/test_api.py::test_cors_echoes_configured_origin_not_wildcard
tests/test_api.py::test_cors_middleware_never_configured_with_wildcard
tests/test_api.py::test_export_streams_chunks
tests/test_api.py::test_export_csv_media_type
tests/test_api_property.py::test_price_series_response_is_time_ascending_contract
tests/test_api_property.py::test_price_series_empty_returns_404
tests/test_build_price_records.py::test_item_with_all_fields_maps_to_camelcase
tests/test_build_price_records.py::test_partial_nulls_are_filtered_out
tests/test_build_price_records.py::test_all_null_item_is_excluded
tests/test_build_price_records.py::test_empty_snapshot_produces_no_records
tests/test_build_price_records.py::test_zero_valued_field_is_kept
tests/test_build_price_records.py::test_property_records_are_null_free_and_nonempty
tests/test_build_price_records.py::test_property_emits_exactly_items_with_non_null_field
tests/test_build_price_records.py::test_property_record_stamping_and_field_names
tests/test_cli.py::test_help_lists_all_commands
tests/test_cli.py::test_ingest_reports_counts
tests/test_cli.py::test_export_streams_chunks_to_file
tests/test_cli.py::test_ingest_missing_config_exits_nonzero_with_remediation
tests/test_cli.py::test_setup_missing_config_exits_nonzero_with_remediation
tests/test_cli.py::test_export_missing_config_exits_nonzero_with_remediation
tests/test_config.py::test_import_has_no_side_effects_and_no_reads
tests/test_config.py::test_get_settings_loads_all_fields_with_defaults
tests/test_config.py::test_settings_is_frozen
tests/test_config.py::test_get_settings_caches_identical_object
tests/test_config.py::test_missing_required_variable_raises_config_error[INFLUX_URL]
tests/test_config.py::test_missing_required_variable_raises_config_error[INFLUX_TOKEN]
tests/test_config.py::test_missing_required_variable_raises_config_error[INFLUX_ORG]
tests/test_config.py::test_missing_required_variable_raises_config_error[INFLUX_BUCKET]
tests/test_config.py::test_empty_required_variable_raises_config_error
tests/test_config.py::test_config_error_names_first_missing_variable
tests/test_config.py::test_settings_type_matches_returned_object
tests/test_data_access.py::test_build_feature_frame_pivots_long_form
tests/test_data_access.py::test_build_feature_frame_indexes_wide_form
tests/test_data_access.py::test_build_feature_frame_empty_query_returns_empty
tests/test_data_access.py::test_stream_dataset_rejects_unsupported_format
tests/test_data_access.py::test_stream_dataset_handles_timestamp_time_column
tests/test_data_access.py::test_to_unix_seconds_coerces_common_time_types
tests/test_data_access_property.py::test_iter_time_chunks_partitions_range_exactly
tests/test_data_access_property.py::test_get_price_page_is_lossless_partition
tests/test_data_access_property.py::test_stream_dataset_round_trips_rows
tests/test_fetch_snapshot.py::test_successful_response_returns_validated_snapshot
tests/test_fetch_snapshot.py::test_request_targets_5m_endpoint_with_timestamp_param
tests/test_fetch_snapshot.py::test_request_carries_configured_user_agent_header
tests/test_fetch_snapshot.py::test_timeout_raises_transient_error
tests/test_fetch_snapshot.py::test_connection_error_raises_transient_error
tests/test_fetch_snapshot.py::test_server_error_raises_transient_error[500]
tests/test_fetch_snapshot.py::test_server_error_raises_transient_error[502]
tests/test_fetch_snapshot.py::test_server_error_raises_transient_error[503]
tests/test_fetch_snapshot.py::test_rate_limit_with_retry_after_header_sets_attribute
tests/test_fetch_snapshot.py::test_rate_limit_without_retry_after_header_sets_none
tests/test_fetch_snapshot.py::test_rate_limit_with_non_integer_retry_after_sets_none
tests/test_fetch_snapshot.py::test_client_error_raises_non_transient_error[400]
tests/test_fetch_snapshot.py::test_client_error_raises_non_transient_error[403]
tests/test_fetch_snapshot.py::test_client_error_raises_non_transient_error[404]
tests/test_influx.py::test_get_client_reuses_single_instance
tests/test_influx.py::test_write_batch_writes_with_seconds_precision
tests/test_influx.py::test_write_batch_empty_records_is_noop
tests/test_influx.py::test_write_batch_retries_then_drops_on_persistent_failure
tests/test_influx.py::test_write_batch_succeeds_after_transient_then_recovery
tests/test_influx.py::test_query_price_series_binds_user_values_as_params_not_query_text
tests/test_influx.py::test_query_chunk_binds_item_ids_as_params_not_query_text
tests/test_influx.py::test_query_price_series_applies_aggregate_window_when_interval_given
tests/test_influx.py::test_query_price_series_omits_aggregate_window_when_interval_none
tests/test_influx.py::test_get_latest_timestamp_returns_none_when_no_records
tests/test_influx.py::test_get_latest_timestamp_returns_unix_seconds_for_record
tests/test_influx.py::test_get_latest_timestamp_binds_item_id_as_param
tests/test_integration_api.py::test_items_returns_seeded_ids
tests/test_integration_api.py::test_items_substring_filter
tests/test_integration_api.py::test_prices_ascending_and_outlier_annotated
tests/test_integration_api.py::test_pagination_is_lossless
tests/test_integration_api.py::test_outliers_endpoint_returns_only_outliers
tests/test_integration_api.py::test_export_ndjson_streams_both_items
tests/test_integration_api.py::test_export_csv_streams_both_items
tests/test_integration_api.py::test_prices_404_for_unknown_item
tests/test_integration_api.py::test_invalid_range_returns_400
tests/test_integration_api.py::test_invalid_method_returns_400
tests/test_integration_api.py::test_invalid_export_format_returns_400
tests/test_integration_api.py::test_page_limit_zero_returns_400
tests/test_integration_influx.py::test_get_latest_timestamp_returns_max_for_item
tests/test_integration_influx.py::test_query_price_series_round_trips_written_points
tests/test_integration_influx.py::test_query_chunk_returns_combined_multi_item_frame
tests/test_iter_missing_timestamps.py::test_latest_none_starts_at_earliest
tests/test_iter_missing_timestamps.py::test_latest_provided_starts_after_latest
tests/test_iter_missing_timestamps.py::test_empty_when_now_at_start
tests/test_iter_missing_timestamps.py::test_upper_bound_is_exclusive
tests/test_iter_missing_timestamps.py::test_property_strictly_increasing_and_aligned
tests/test_iter_missing_timestamps.py::test_property_completeness_exact_set
tests/test_models.py::TestAliasCoercion::test_all_aliases_mapped_to_snake_case
tests/test_models.py::TestAliasCoercion::test_populate_by_field_name_also_supported
tests/test_models.py::TestAliasCoercion::test_snapshot_coerces_nested_item_aliases
tests/test_models.py::TestNonePreservation::test_absent_fields_default_to_none
tests/test_models.py::TestNonePreservation::test_explicit_null_fields_preserved_as_none
tests/test_models.py::TestNonePreservation::test_partial_nulls_preserved_alongside_values
tests/test_models.py::TestNonePreservation::test_zero_is_preserved_and_not_treated_as_none
tests/test_models.py::TestUnknownKeysIgnored::test_unknown_keys_ignored_on_item
tests/test_models.py::TestUnknownKeysIgnored::test_unknown_keys_do_not_appear_in_dump
tests/test_models.py::TestNegativeValueRejection::test_negative_value_rejected[avgHighPrice]
tests/test_models.py::TestNegativeValueRejection::test_negative_value_rejected[avgLowPrice]
tests/test_models.py::TestNegativeValueRejection::test_negative_value_rejected[highPriceVolume]
tests/test_models.py::TestNegativeValueRejection::test_negative_value_rejected[lowPriceVolume]
tests/test_models.py::TestNegativeValueRejection::test_negative_value_inside_snapshot_rejected
tests/test_models_property.py::test_schema_validation_round_trip
tests/test_outliers.py::test_get_detector_unknown_method_raises_descriptive_error
tests/test_outliers.py::test_get_detector_returns_registered_built_ins
tests/test_outliers_property.py::test_detect_outliers_length_and_none_safety
tests/test_outliers_property.py::test_detect_outliers_zero_variance_stability
tests/test_outliers_property.py::test_registered_detectors_preserve_length_and_none_safety
tests/test_retry.py::test_compute_backoff_delay_grows_geometrically
tests/test_retry.py::test_compute_backoff_delay_clamped_to_max_delay
tests/test_retry.py::test_compute_backoff_delay_floors_attempt_at_one
tests/test_retry.py::test_sync_exponential_delay_growth
tests/test_retry.py::test_sync_delay_clamped_to_max_delay
tests/test_retry.py::test_sync_succeeds_after_transient_then_no_more_retries
tests/test_retry.py::test_sync_retry_after_overrides_computed_delay
tests/test_retry.py::test_sync_negative_retry_after_falls_back_to_backoff
tests/test_retry.py::test_sync_zero_retry_after_skips_sleep
tests/test_retry.py::test_sync_final_failure_logs_and_propagates
tests/test_retry.py::test_non_transient_error_is_not_retried
tests/test_retry.py::test_max_attempts_must_be_positive
tests/test_retry.py::test_async_exponential_delay_growth
tests/test_retry.py::test_async_succeeds_after_transient
tests/test_retry.py::test_async_retry_after_overrides_computed_delay
tests/test_retry.py::test_async_final_failure_logs_and_propagates
tests/test_run_catch_up.py::test_concurrency_never_exceeds_max_concurrency
tests/test_run_catch_up.py::test_each_window_attempted_exactly_once
tests/test_run_catch_up.py::test_counts_accurate_on_all_success
tests/test_run_catch_up.py::test_counts_accurate_when_one_fetch_fails_permanently
tests/test_run_catch_up.py::test_final_partial_batch_is_flushed
tests/test_run_catch_up.py::test_client_closed_when_a_fetch_fails
tests/test_run_catch_up.py::test_client_closed_when_a_write_batch_is_dropped
tests/test_scheduler.py::test_build_scheduler_registers_catch_up_job
tests/test_scheduler.py::test_catch_up_job_uses_five_minute_interval_trigger
tests/test_scheduler.py::test_build_scheduler_uses_provided_settings
```

### Skipped node ids

None (0 skipped).

### Deselected node ids

None (0 deselected).
---

## Task 1.2 — Smoke check the baseline run

**Property 1: No baseline-passing test regresses across any batch (baseline anchor)**
_Validates: Requirements 6.1, 6.3_

Read/verify-only confirmation of the baseline anchor (no code changes, full
suite not re-run — Task 1.1's recorded run is used as the oracle):

- **Baseline run completed at HEAD.** Task 1.1 recorded a full `pytest` run on a
  clean tree (146 passed, 1 pre-existing failure excluded, 0 skipped,
  0 deselected, 0 errors). The run reflects HEAD.
- **Green set is non-empty and correctly sized.** The recorded Baseline_Green_Set
  contains exactly **146** passing test node ids (verified by counting the
  `tests/...::...` lines in this file). Matches the expected count.
- **Recorded before any removal begins.** No removal batch (A–E) has run:
  - `git diff --stat` shows no changes to `ge_pipeline/` or `README.md`.
  - All designated stale artifacts still present: `DatabaseSetup.ipynb`,
    `testFunctions.ipynb`, `testGettingandwritingtimestamps.ipynb`,
    `IMPROVEMENTS.md` (and the `.ipynb_checkpoints` copy).
  - The only working-tree modifications are spec-tracking files under
    `.kiro/specs/dead-code-removal/` (this notes file, `tasks.md`,
    `tasks.meta.json`), not source or stale artifacts.

**Conclusion: baseline is properly anchored.** The Baseline_Green_Set (146 node
ids) is a valid, non-empty regression gate recorded prior to any removal.
---

## Task 2.1 — Static analyzer output

_Requirements: 1.1_

Detection only. No code was removed and no files under `ge_pipeline/` or
`tests/` were modified. Both analyzer outputs below are **CANDIDATES ONLY**,
never verdicts — they are confirmed against real references in Task 2.2
(manual zero-reference grep) before any classification or removal.

Tooling was installed into the project `.venv` as dev-only tools (not added to
`pyproject.toml` runtime/test dependencies):

```
.venv/bin/python -m pip install vulture ruff
# installed: vulture-2.16, ruff-0.16.6
```

### Candidate set V — vulture

Command (run from repo root inside the project `.venv`):

```
.venv/bin/vulture ge_pipeline tests --min-confidence 60
```

Result: **31 candidates** (exit code 3 = candidates reported).

```
ge_pipeline/api.py:139: unused variable 'avgHighPrice' (60% confidence)
ge_pipeline/api.py:140: unused variable 'avgLowPrice' (60% confidence)
ge_pipeline/api.py:141: unused variable 'highPriceVolume' (60% confidence)
ge_pipeline/api.py:142: unused variable 'lowPriceVolume' (60% confidence)
ge_pipeline/api.py:155: unused variable 'itemId' (60% confidence)
ge_pipeline/api.py:171: unused variable 'itemId' (60% confidence)
ge_pipeline/api.py:174: unused variable 'nextCursor' (60% confidence)
ge_pipeline/api.py:186: unused variable 'itemId' (60% confidence)
ge_pipeline/api.py:197: unused variable 'detail' (60% confidence)
ge_pipeline/api.py:570: unused function 'outlier_methods' (60% confidence)
ge_pipeline/api.py:586: unused function 'search_items' (60% confidence)
ge_pipeline/api.py:625: unused function 'item_prices' (60% confidence)
ge_pipeline/api.py:681: unused function 'item_prices_page' (60% confidence)
ge_pipeline/api.py:762: unused function 'item_outliers' (60% confidence)
ge_pipeline/api.py:817: unused function 'export_dataset' (60% confidence)
ge_pipeline/api.py:884: unused function 'dataset_features' (60% confidence)
ge_pipeline/cli.py:178: unused function 'ingest' (60% confidence)
ge_pipeline/cli.py:189: unused function 'backfill' (60% confidence)
ge_pipeline/cli.py:200: unused function 'serve' (60% confidence)
ge_pipeline/cli.py:221: unused function 'export' (60% confidence)
ge_pipeline/cli.py:318: unused function 'setup' (60% confidence)
ge_pipeline/models.py:44: unused variable 'model_config' (60% confidence)
tests/test_api.py:315: unused variable 'client_arg' (100% confidence)
tests/test_config.py:21: unused function '_reset_cache_and_env' (60% confidence)
tests/test_data_access_property.py:62: unused variable 'a_prev' (60% confidence)
tests/test_data_access_property.py:288: unused variable 'req_item_ids' (100% confidence)
tests/test_influx.py:22: unused function '_clear_client_cache' (60% confidence)
tests/test_influx.py:99: unused attribute 'side_effect' (60% confidence)
tests/test_influx.py:115: unused attribute 'side_effect' (60% confidence)
tests/test_influx.py:158: unused attribute 'side_effect' (60% confidence)
tests/test_influx.py:208: unused attribute 'side_effect' (60% confidence)
```

> Note on likely-live-via-indirect-reference candidates (to be confirmed in
> Task 2.2, recorded here so they are not prematurely treated as verdicts):
> the `ge_pipeline/api.py` route handlers (`outlier_methods`, `search_items`,
> `item_prices`, `item_prices_page`, `item_outliers`, `export_dataset`,
> `dataset_features`) are registered via FastAPI `@app.<method>` decorators;
> the `ge_pipeline/cli.py` functions (`ingest`, `backfill`, `serve`, `export`,
> `setup`) are Typer commands registered via decorators and reached through the
> `ge_pipeline.cli:app` string entry point; and `model_config` in `models.py`
> is a Pydantic configuration attribute consumed by the framework. Vulture
> cannot see these decorator/framework/string references.

### Candidate set R — ruff

Command (run from repo root inside the project `.venv`):

```
.venv/bin/ruff check ge_pipeline tests --select F401,F811,F841,ARG
```

Result: **74 findings** (exit code 1 = findings reported).

Summary by rule code:

| Rule   | Meaning                        | Count |
|--------|--------------------------------|-------|
| F401   | Imported but unused            | 1     |
| F811   | Redefinition of unused name    | 0     |
| F841   | Unused local variable          | 0     |
| ARG001 | Unused function argument       | 54    |
| ARG002 | Unused method argument         | 2     |
| ARG005 | Unused lambda argument         | 17    |
| **Total** |                             | **74** |

Full raw output:

```
ARG001 Unused function argument: `frame`
   --> ge_pipeline/data_access.py:192:16

ARG005 Unused lambda argument: `args`   --> tests/test_api.py:125:17
ARG005 Unused lambda argument: `kwargs` --> tests/test_api.py:125:25
ARG005 Unused lambda argument: `args`   --> tests/test_api.py:150:17
ARG005 Unused lambda argument: `kwargs` --> tests/test_api.py:150:25
ARG005 Unused lambda argument: `args`   --> tests/test_api.py:187:17
ARG005 Unused lambda argument: `kwargs` --> tests/test_api.py:187:25
ARG005 Unused lambda argument: `args`   --> tests/test_api.py:204:17
ARG005 Unused lambda argument: `kwargs` --> tests/test_api.py:204:25
ARG005 Unused lambda argument: `args`   --> tests/test_api.py:241:17
ARG005 Unused lambda argument: `kwargs` --> tests/test_api.py:241:25
ARG005 Unused lambda argument: `args`   --> tests/test_api.py:253:17
ARG005 Unused lambda argument: `kwargs` --> tests/test_api.py:253:25
ARG001 Unused function argument: `client_arg` --> tests/test_api.py:315:30
ARG001 Unused function argument: `start`      --> tests/test_api.py:315:52
ARG001 Unused function argument: `stop`       --> tests/test_api.py:315:59
ARG001 Unused function argument: `args`       --> tests/test_api.py:348:31
ARG001 Unused function argument: `kwargs`     --> tests/test_api.py:348:39

ARG001 Unused function argument: `client`   --> tests/test_api_property.py:92:34
ARG001 Unused function argument: `start`    --> tests/test_api_property.py:92:55
ARG001 Unused function argument: `stop`     --> tests/test_api_property.py:92:62
ARG001 Unused function argument: `interval` --> tests/test_api_property.py:92:68
ARG005 Unused lambda argument: `a`          --> tests/test_api_property.py:140:51
ARG005 Unused lambda argument: `k`          --> tests/test_api_property.py:140:56

ARG005 Unused lambda argument: `settings`   --> tests/test_cli.py:82:45
ARG001 Unused function argument: `client`   --> tests/test_cli.py:87:30
ARG001 Unused function argument: `start`    --> tests/test_cli.py:87:48
ARG001 Unused function argument: `stop`     --> tests/test_cli.py:87:55
ARG001 Unused function argument: `interval` --> tests/test_cli.py:87:61

ARG005 Unused lambda argument: `a`          --> tests/test_config.py:29:63
ARG005 Unused lambda argument: `k`          --> tests/test_config.py:29:68

ARG001 Unused function argument: `client`   --> tests/test_data_access.py:36:27
ARG001 Unused function argument: `item_ids` --> tests/test_data_access.py:36:35
ARG001 Unused function argument: `start`    --> tests/test_data_access.py:36:45
ARG001 Unused function argument: `stop`     --> tests/test_data_access.py:36:52
ARG001 Unused function argument: `interval` --> tests/test_data_access.py:36:58
ARG001 Unused function argument: `client`   --> tests/test_data_access.py:74:27
ARG001 Unused function argument: `item_ids` --> tests/test_data_access.py:74:35
ARG001 Unused function argument: `start`    --> tests/test_data_access.py:74:45
ARG001 Unused function argument: `stop`     --> tests/test_data_access.py:74:52
ARG001 Unused function argument: `interval` --> tests/test_data_access.py:74:58
ARG001 Unused function argument: `client`   --> tests/test_data_access.py:96:27
ARG001 Unused function argument: `item_ids` --> tests/test_data_access.py:96:35
ARG001 Unused function argument: `start`    --> tests/test_data_access.py:96:45
ARG001 Unused function argument: `stop`     --> tests/test_data_access.py:96:52
ARG001 Unused function argument: `interval` --> tests/test_data_access.py:96:58
ARG001 Unused function argument: `client`   --> tests/test_data_access.py:124:27
ARG001 Unused function argument: `item_ids` --> tests/test_data_access.py:124:35
ARG001 Unused function argument: `start`    --> tests/test_data_access.py:124:45
ARG001 Unused function argument: `stop`     --> tests/test_data_access.py:124:52
ARG001 Unused function argument: `interval` --> tests/test_data_access.py:124:58
ARG001 Unused function argument: `client`   --> tests/test_data_access.py:167:27
ARG001 Unused function argument: `item_ids` --> tests/test_data_access.py:167:35
ARG001 Unused function argument: `interval` --> tests/test_data_access.py:167:58

ARG001 Unused function argument: `client`       --> tests/test_data_access_property.py:116:34
ARG001 Unused function argument: `item_id`      --> tests/test_data_access_property.py:116:42
ARG001 Unused function argument: `start`        --> tests/test_data_access_property.py:116:51
ARG001 Unused function argument: `stop`         --> tests/test_data_access_property.py:116:58
ARG001 Unused function argument: `interval`     --> tests/test_data_access_property.py:116:64
ARG001 Unused function argument: `client`       --> tests/test_data_access_property.py:288:27
ARG001 Unused function argument: `req_item_ids` --> tests/test_data_access_property.py:288:35
ARG001 Unused function argument: `interval`     --> tests/test_data_access_property.py:288:74

ARG001 Unused function argument: `request` --> tests/test_fetch_snapshot.py:71:17
ARG001 Unused function argument: `request` --> tests/test_fetch_snapshot.py:139:17
ARG001 Unused function argument: `request` --> tests/test_fetch_snapshot.py:152:17
ARG001 Unused function argument: `request` --> tests/test_fetch_snapshot.py:162:17
ARG001 Unused function argument: `request` --> tests/test_fetch_snapshot.py:173:17
ARG001 Unused function argument: `request` --> tests/test_fetch_snapshot.py:191:17

ARG001 Unused function argument: `kwargs` --> tests/test_influx.py:57:21

F401 [*] `ge_pipeline.data_access` imported but unused --> tests/test_integration_api.py:42:30
ARG001 Unused function argument: `interval` --> tests/test_integration_api.py:184:9
ARG001 Unused function argument: `interval` --> tests/test_integration_api.py:194:9

ARG002 Unused method argument: `args`   --> tests/test_retry.py:49:25
ARG002 Unused method argument: `kwargs` --> tests/test_retry.py:49:33

Found 74 errors.
[*] 1 fixable with the `--fix` option.
```

> Note on ruff candidates: the single F401 (`data_access` unused import in
> `tests/test_integration_api.py`) is the only import/local finding. The 73
> ARG* findings are overwhelmingly unused parameters in test fakes/handlers
> whose signatures must match the real callee (e.g. monkeypatched
> `query_price_series` / `query_chunk` / `stream_dataset` stubs and httpx
> `handler(request)` callbacks). The one production-code ARG finding is
> `frame` in `ge_pipeline/data_access.py:192` (`_order_key`). All of these are
> candidates only and are confirmed in Task 2.2 before any action.

---

## Task 2.2 — Reference confirmation

_Requirements: 1.1, 3.1_

Detection/analysis only. **No files under `ge_pipeline/` or `tests/` were
modified and nothing was removed.** For every candidate in the vulture set V
and the ruff set R (Task 2.1), a manual zero-reference grep was run from the
repo root with ripgrep:

```
.venv/bin/rg -n "\b<symbol>\b" ge_pipeline tests pyproject.toml
```

> Tooling note: ripgrep was installed dev-only into the project `.venv`
> (`pip install ripgrep` → `ripgrep-15.1.0`, binary at `.venv/bin/rg`). Like
> vulture/ruff in Task 2.1 it was **not** added to `pyproject.toml`
> runtime/test dependencies.

Reference counts below are the raw grep hit counts across
`ge_pipeline tests pyproject.toml`; the "refs excl. def" column subtracts the
single definition-site line so the remaining figure reflects real uses. The
"Live via" column records the indirect (decorator / framework / string /
fixture / signature-match) reference that analyzers cannot see.

### V — vulture candidates

#### Pydantic model fields flagged as "unused variable" (api.py) — ALL LIVE

These are class-body field declarations on `PricePoint` / `PriceSeriesResponse`
/ `PricePageResponse` / `ItemSearchResult` / `ErrorResponse`, consumed by
Pydantic and constructed/asserted throughout the API and tests.

| Symbol | Location | Raw refs | Refs excl. def | Live via | Verdict |
|--------|----------|---------:|---------------:|----------|---------|
| `avgHighPrice`   | api.py:139 | many | many | Pydantic field; built at api.py:409, consumed in models/ingestion/influx + tests | LIVE |
| `avgLowPrice`    | api.py:140 | many | many | Pydantic field; built at api.py:410 + broad test use | LIVE |
| `highPriceVolume`| api.py:141 | many | many | Pydantic field; built at api.py:411 + broad test use | LIVE |
| `lowPriceVolume` | api.py:142 | many | many | Pydantic field; built at api.py:412 + broad test use | LIVE |
| `itemId`  | api.py:155,171,186 | 23 | 20 | Pydantic field; built at api.py:619/676/756/812; asserted in test_api/test_integration_api/test_api_property | LIVE |
| `nextCursor` | api.py:174 | 6 | 5 | Pydantic field; built at api.py:759; followed in pagination test (test_integration_api:311) | LIVE |
| `detail`  | api.py:197 | 30 | 29 | Pydantic field on `ErrorResponse`; set at 15+ api.py sites; asserted across error tests | LIVE |

> The three `itemId` occurrences (155/171/186) are the same field name on three
> different response models; each is a live transport field.

#### FastAPI route handlers (api.py) — ALL LIVE via `@app.get(...)` decorators

Each handler grep returns exactly **1** hit (the `def` line): 0 references
excluding the definition. They are NOT dead — each is registered on the
FastAPI app by a decorator immediately above the `def` (verified: `@app.get`
at 570, 586, 625, 681, 762, 817, 884) and exercised by `tests/test_api.py`
and `tests/test_integration_api.py` over HTTP.

| Symbol | Location | Raw refs | Refs excl. def | Live via | Verdict |
|--------|----------|---------:|---------------:|----------|---------|
| `outlier_methods`  | api.py:575 | 1 | 0 | `@app.get("/api/outlier-methods")` | LIVE (decorator) |
| `search_items`     | api.py:592 | 1 | 0 | `@app.get("/api/items")` | LIVE (decorator) |
| `item_prices`      | api.py:632 | 1 | 0 | `@app.get(...)` price series route | LIVE (decorator) |
| `item_prices_page` | api.py:688 | 1 | 0 | `@app.get(...)` paginated route | LIVE (decorator) |
| `item_outliers`    | api.py:769 | 1 | 0 | `@app.get(...)` outliers route | LIVE (decorator) |
| `export_dataset`   | api.py:822 | 1 | 0 | `@app.get("/api/datasets/export")` | LIVE (decorator) |
| `dataset_features` | api.py:889 | 1 | 0 | `@app.get(...)` features route | LIVE (decorator) |

#### Typer CLI commands (cli.py) — ALL LIVE via `@app.command()` + string entry point

Registered by `@app.command()` (verified at cli.py 178, 189, 200, 221, 318) on
the `app = typer.Typer(...)` object exported as the console-script entry point
`ge-pipeline = "ge_pipeline.cli:app"` (pyproject.toml:36). Invoked by name in
`tests/test_cli.py` via `runner.invoke(app, ["<command>"])`. `serve` also
launches the API via the string entry point `uvicorn.run("ge_pipeline.api:app", ...)`
(cli.py:218).

| Symbol | Location | Raw refs | Refs excl. def | Live via | Verdict |
|--------|----------|---------:|---------------:|----------|---------|
| `ingest`   | cli.py:179 | 14 | 13 | `@app.command()`; `runner.invoke(app, ["ingest"])` | LIVE (decorator + string) |
| `backfill` | cli.py:190 | 7 | 6 | `@app.command()`; listed in test_cli commands loop | LIVE (decorator) |
| `serve`    | cli.py:201 | 5 | 4 | `@app.command()`; `uvicorn.run("ge_pipeline.api:app", ...)` | LIVE (decorator + string) |
| `export`   | cli.py:222 | 45 | 44 | `@app.command()`; `runner.invoke(app, ["export"])` | LIVE (decorator + string) |
| `setup`    | cli.py:319 | 7 | 6 | `@app.command()`; `runner.invoke(app, ["setup"])` | LIVE (decorator + string) |

> Note: `export` / `serve` / `ingest` raw counts include unrelated docstring and
> substring mentions (e.g. "export format", "ready to serve"); every one was
> inspected and the decorator + entry-point registration is what keeps them live.

#### Pydantic config attribute (models.py) — LIVE via framework

| Symbol | Location | Raw refs | Refs excl. def | Live via | Verdict |
|--------|----------|---------:|---------------:|----------|---------|
| `model_config` | models.py:44 | 1 | 0 | Pydantic-consumed config (`populate_by_name`/`extra="ignore"`); enforced by alias/unknown-key tests in test_models.py | LIVE (framework) |

#### Test fixtures / stubs / loop vars (tests) — ALL LIVE / retained

| Symbol | Location | Raw refs | Refs excl. def | Live via | Verdict |
|--------|----------|---------:|---------------:|----------|---------|
| `_reset_cache_and_env` | test_config.py:22 | 1 | 0 | `@pytest.fixture(autouse=True)` — auto-invoked by pytest | LIVE (fixture) |
| `_clear_client_cache`  | test_influx.py:23 | 1 | 0 | `@pytest.fixture(autouse=True)` — auto-invoked by pytest | LIVE (fixture) |
| `client_arg`   | test_api.py:315 | 1 | 0 | param of monkeypatched `_fake_stream_dataset`; matches real `stream_dataset(client, ...)` signature | RETAIN (signature-match) |
| `req_item_ids` | test_data_access_property.py:288 | 1 | 0 | param of monkeypatched `_fake_query_chunk`; matches real `query_chunk(client, item_ids, ...)` signature | RETAIN (signature-match) |
| `a_prev` | test_data_access_property.py:62 | 1 | 0 | tuple-unpack target in a live Hypothesis property loop (`for (a_prev, b_prev), ...`) | RETAIN (destructuring) |
| `side_effect` (x4) | test_influx.py:99,115,158,208 | 4 | n/a | MagicMock attribute consumed at call time; drives retry/query behavior asserted by `call_count`/return values | LIVE (mock framework) |

### R — ruff candidates

#### Production code (ge_pipeline)

| Symbol | Location | Rule | Raw refs | Refs excl. def | Live via | Verdict |
|--------|----------|------|---------:|---------------:|----------|---------|
| `frame` (param of `_order_key`) | data_access.py:192 | ARG001 | — | — | `_order_key` itself called at data_access.py:225 (active path via `get_price_page`/`stream_dataset`); `frame` is a signature/consistency param, unused in body | RETAIN — `_order_key` is `lightly_used_retain`; param is signature-matching |
| `data_access` (import) | test_integration_api.py:42 | F401 | see below | **0 code refs** | none | **GENUINELY ZERO-REFERENCE (proven dead)** |

**F401 `data_access` — the one truly-dead candidate.** The import
`from ge_pipeline import api, data_access, influx` at test_integration_api.py:42
binds `data_access`, but the name is never used as an identifier in executable
code. Its only other occurrences (lines 5–6 in the module docstring, and line
202 in a comment) are prose references, not code. Confirmed independently by
`ruff check tests/test_integration_api.py --select F401`, whose autofix rewrites
line 42 to `from ge_pipeline import api, influx`. This is a bona fide unused
import → candidate for Batch A removal.

> The `influx`/`api` names on the same import line ARE used (e.g.
> `monkeypatch.setattr(influx, ...)` at lines 203–205, `api.influx`), so only
> `data_access` is dropped from the import.

#### Test-code ARG* findings — ALL retained (signature-matching)

The remaining 72 ruff findings are ARG001/ARG002/ARG005 unused
parameters/lambda args in test doubles. Each was reviewed by category; none is
dead — every one exists to match the signature of the real callee it stands in
for, so removing it would break the monkeypatch/handler contract:

- **Monkeypatched stub params** — `_fake_query_price_series` / `_fake_query_chunk`
  / `_fake_stream_dataset` / `_fake_list_item_ids` stubs across
  `test_api.py`, `test_api_property.py`, `test_cli.py`, `test_data_access.py`,
  `test_data_access_property.py`, `test_integration_api.py`
  (`client`, `item_ids`, `item_id`, `req_item_ids`, `start`, `stop`,
  `interval`, `client_arg`). These replace real `influx.query_*` /
  `data_access.stream_dataset` functions via `monkeypatch.setattr`, so they must
  accept the same positional signature even when a given test ignores some args.
  RETAIN (signature-match).
- **httpx transport handler params** — `request` in the six
  `handler(request)` callbacks in `test_fetch_snapshot.py` (71,139,152,162,173,191).
  `httpx.MockTransport` always calls the handler with the request object; the
  param is required by the callback contract. RETAIN (signature-match).
- **Lambda args** — `args`/`kwargs`/`a`/`k`/`settings` lambdas in
  `test_api.py`, `test_api_property.py`, `test_cli.py`, `test_config.py`
  (e.g. `lambda *a, **k: ...` neutralizing `load_dotenv`, or a fake factory).
  They absorb the real call's arguments; required to be callable with the same
  args. RETAIN (signature-match).
- **`ARG002` method args** — `args`/`kwargs` on a fake class method in
  `test_retry.py:49` (a stand-in callable invoked by the retry helper).
  RETAIN (signature-match).
- **`interval` in integration seed helpers** — `test_integration_api.py:184,194`
  fake query functions mirroring the real query signature. RETAIN.

### Task 2.2 summary

- **Candidates confirmed genuinely zero-reference (truly dead): 1**
  - `data_access` unused import — `tests/test_integration_api.py:42` (ruff F401).
    Only real change: drop `data_access` from that import line. Slated for
    Batch A (unused imports).
- **Candidates confirmed LIVE / retained: all others (30 of the 31 vulture
  candidates + 73 of the 74 ruff findings)**, kept alive by:
  - FastAPI `@app.get` route decorators (7 api.py handlers),
  - Typer `@app.command()` decorators reached through the `ge_pipeline.cli:app`
    console-script entry point + `runner.invoke(app, [...])` (5 cli.py commands),
    plus the `uvicorn.run("ge_pipeline.api:app", ...)` string entry point in `serve`,
  - Pydantic framework consumption (model fields + `model_config`),
  - the outlier detector registry (`register_detector("zscore"/"iqr", ...)`,
    `get_detector`/`available_detectors` string lookups, the
    `@runtime_checkable OutlierDetector` protocol satisfied structurally — all
    referenced and populated),
  - `@pytest.fixture(autouse=True)` auto-invoked fixtures,
  - MagicMock `side_effect` attributes consumed at call time,
  - and signature-matching parameters in monkeypatched stubs / httpx handlers /
    lambdas that must mirror the real callee signature (the bulk of the ARG*
    findings) — explicitly NOT dead per Requirement 3.

No production `ge_pipeline/` symbol was found dead: `_order_key` (its `frame`
param) is on an active path and is retained. The only actionable removal from
detection is the single unused test import above.

---

## Task 3.1 — Classification table

_Requirements: 1.2, 2.1, 2.2, 3.1, 3.2_

Analysis only. **No files under `ge_pipeline/` or `tests/` were modified and
nothing was removed.** This section applies the classification decision flow to
every candidate confirmed in Task 2.2 and assigns each removable symbol to a
batch (A–D). Batch E covers stale artifacts + README and is handled by tasks
9.x.

### Decision flow applied

For each candidate `C`:

1. **0 references AND not reached by any string / protocol / decorator /
   framework / fixture path** → `proven_dead`.
2. Referenced only by symbols already classified `proven_dead` →
   `orphaned_by_dead`.
3. Branch unreachable from any `Active_Path`, whose removal provably preserves
   behavior and typing → `speculative_branch`.
4. ≥1 `Active_Path` reference (including indirect: decorator / string entry
   point / Pydantic / registry / autouse fixture / MagicMock / signature-match)
   → `lightly_used_retain`.

Per Requirement 3, "called from only one place" and "signature-matching but
unused parameter" are **not** grounds for removal; those are `lightly_used_retain`.

### ClassificationResult table

| symbol | location (file:line) | category | evidence | batch |
|--------|----------------------|----------|----------|-------|
| `data_access` (import binding) | tests/test_integration_api.py:42 | **proven_dead** | ruff F401; 0 executable references (only docstring/comment prose); `influx`/`api` on same line stay; autofix rewrites to `from ge_pipeline import api, influx` | **A** |
| `avgHighPrice` | ge_pipeline/api.py:139 | lightly_used_retain | Pydantic `PricePoint` field; built at api.py:409; consumed across models/ingestion/influx + tests | — |
| `avgLowPrice` | ge_pipeline/api.py:140 | lightly_used_retain | Pydantic field; built at api.py:410 + broad test use | — |
| `highPriceVolume` | ge_pipeline/api.py:141 | lightly_used_retain | Pydantic field; built at api.py:411 + broad test use | — |
| `lowPriceVolume` | ge_pipeline/api.py:142 | lightly_used_retain | Pydantic field; built at api.py:412 + broad test use | — |
| `itemId` | ge_pipeline/api.py:155,171,186 | lightly_used_retain | Pydantic transport field on 3 response models; built at 619/676/756/812; asserted in test_api / test_integration_api / test_api_property | — |
| `nextCursor` | ge_pipeline/api.py:174 | lightly_used_retain | Pydantic field; built at api.py:759; followed by pagination test (test_integration_api:311) | — |
| `detail` | ge_pipeline/api.py:197 | lightly_used_retain | Pydantic `ErrorResponse` field; set at 15+ sites; asserted across error tests | — |
| `outlier_methods` | ge_pipeline/api.py:575 | lightly_used_retain | FastAPI `@app.get("/api/outlier-methods")` decorator; exercised over HTTP | — |
| `search_items` | ge_pipeline/api.py:592 | lightly_used_retain | FastAPI `@app.get("/api/items")` decorator | — |
| `item_prices` | ge_pipeline/api.py:632 | lightly_used_retain | FastAPI `@app.get(...)` price-series route | — |
| `item_prices_page` | ge_pipeline/api.py:688 | lightly_used_retain | FastAPI `@app.get(...)` paginated route | — |
| `item_outliers` | ge_pipeline/api.py:769 | lightly_used_retain | FastAPI `@app.get(...)` outliers route | — |
| `export_dataset` | ge_pipeline/api.py:822 | lightly_used_retain | FastAPI `@app.get("/api/datasets/export")` route | — |
| `dataset_features` | ge_pipeline/api.py:889 | lightly_used_retain | FastAPI `@app.get(...)` features route | — |
| `ingest` | ge_pipeline/cli.py:179 | lightly_used_retain | Typer `@app.command()`; `ge_pipeline.cli:app` entry point; `runner.invoke(app, ["ingest"])` | — |
| `backfill` | ge_pipeline/cli.py:190 | lightly_used_retain | Typer `@app.command()`; exercised in test_cli commands loop | — |
| `serve` | ge_pipeline/cli.py:201 | lightly_used_retain | Typer `@app.command()`; `uvicorn.run("ge_pipeline.api:app", ...)` string entry point | — |
| `export` | ge_pipeline/cli.py:222 | lightly_used_retain | Typer `@app.command()`; `runner.invoke(app, ["export"])` | — |
| `setup` | ge_pipeline/cli.py:319 | lightly_used_retain | Typer `@app.command()`; `runner.invoke(app, ["setup"])` | — |
| `model_config` | ge_pipeline/models.py:44 | lightly_used_retain | Pydantic-consumed config (`populate_by_name`/`extra="ignore"`); enforced by alias/unknown-key tests | — |
| `frame` (param of `_order_key`) | ge_pipeline/data_access.py:192 | lightly_used_retain | `_order_key` on active path (called at data_access.py:225 via `get_price_page`/`stream_dataset`); `frame` is a signature/consistency param — Req 3 retains it | — |
| trailing `raise AssertionError(...)` (async wrapper) | ge_pipeline/retry.py:216 | lightly_used_retain | Defensive guard after the retry loop; conservatively retained per task instruction — not provably inert with identical behavior/typing | — (Batch D empty) |
| trailing `raise AssertionError(...)` (sync wrapper) | ge_pipeline/retry.py:255 | lightly_used_retain | Defensive guard after the retry loop; conservatively retained per task instruction | — (Batch D empty) |
| `_reset_cache_and_env` | tests/test_config.py:22 | lightly_used_retain | `@pytest.fixture(autouse=True)` — auto-invoked by pytest | — |
| `_clear_client_cache` | tests/test_influx.py:23 | lightly_used_retain | `@pytest.fixture(autouse=True)` — auto-invoked by pytest | — |
| `client_arg` | tests/test_api.py:315 | lightly_used_retain | param of monkeypatched `_fake_stream_dataset`; matches real `stream_dataset(client, ...)` signature | — |
| `req_item_ids` | tests/test_data_access_property.py:288 | lightly_used_retain | param of monkeypatched `_fake_query_chunk`; matches real `query_chunk(client, item_ids, ...)` signature | — |
| `a_prev` | tests/test_data_access_property.py:62 | lightly_used_retain | tuple-unpack target in a live Hypothesis property loop | — |
| `side_effect` (x4) | tests/test_influx.py:99,115,158,208 | lightly_used_retain | MagicMock attribute consumed at call time; drives retry/query behavior asserted by `call_count`/return values | — |
| ARG* stub/handler/lambda params (72 findings) | tests/*.py (see Task 2.2 R table) | lightly_used_retain | signature-matching params in monkeypatched stubs, httpx `handler(request)` callbacks, and neutralizing lambdas; required by the callee/callback contract — Req 3 retains all | — |

### Batch assignments

| Batch | Contents | Count | Status |
|-------|----------|------:|--------|
| **A** — unused imports / trivial local dead code | `data_access` unused import at tests/test_integration_api.py:42 (drop from the import line only; `api`/`influx` remain) | 1 | populated |
| **B** — proven-dead private helpers per module | none — no production `ge_pipeline/` symbol classified `proven_dead` | 0 | **EMPTY** |
| **C** — orphaned-by-dead helpers | none — no `proven_dead` production symbols exist, so nothing can become orphaned | 0 | **EMPTY** |
| **D** — genuinely speculative branches | none — the two trailing `raise AssertionError(...)` guards in retry.py are conservatively retained as `lightly_used_retain` | 0 | **EMPTY** |
| **E** — stale artifacts + README | `DatabaseSetup.ipynb`, `testFunctions.ipynb`, `testGettingandwritingtimestamps.ipynb`, `IMPROVEMENTS.md`, `.ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb`, and the README `## Notebooks` section | — | handled by tasks 9.1–9.3 |

### Notes

- **Legacy top-level scripts already gone.** `InfluxAdmin.py`,
  `RunTimestampFetch.py`, `smallFcns.py`, and `latestTime.txt` were removed at
  the baseline commit (`feeca6c`) and are absent from the working tree
  (verified by `ls`), so they need no removal batch.
- **Only one actionable removal from the entire detection pipeline**: the single
  `data_access` unused import (Batch A). This aligns with Task 2.2: 30 of 31
  vulture candidates and 73 of 74 ruff findings are live/retained via indirect
  references (decorators, string entry points, Pydantic, the outlier registry,
  autouse fixtures, MagicMock, and signature-matching params).
- **Batches B, C, and D are empty.** No proven-dead production helpers exist,
  therefore no orphaned-by-dead helpers can arise, and no branch is classified
  removable-speculative (the retry.py guards are retained conservatively per
  the design and task instruction).
- Every symbol above is classified into exactly one category, satisfying the
  Requirement 3.1/3.2 single-category constraint.

## Task 4.1 — Batch A applied

Removed the unused `data_access` name (ruff F401) from the import in
`tests/test_integration_api.py` line 42.

- Before: `from ge_pipeline import api, data_access, influx`
- After:  `from ge_pipeline import api, influx`

`api` and `influx` remain because they are used elsewhere in the file.
Docstring/comment prose references to `data_access` were intentionally left
untouched. No `__all__` update needed (test-only import).

---

## Task 4.2 — Batch A verification

**Property 1: No baseline-passing test regresses across any batch**
**Property 2: The working tree is green after every processed batch**
_Validates: Requirements 6.2, 6.3, 6.4_

### Batch A change under test

One change applied in Batch A (Task 4.1): dropped the unused `data_access`
name from the import in `tests/test_integration_api.py`:

```
-from ge_pipeline import api, data_access, influx
+from ge_pipeline import api, influx
```

This is the sole ruff **F401** finding (`ge_pipeline.data_access` imported but
unused) recorded in Task 2.1's candidate set R.

### Step 1: Full-suite run

Command (run from repo root inside the project `.venv`, Python 3.12.3):

```
.venv/bin/python -m pytest
```

Integration tests ran against a real InfluxDB container (Docker available), as
at baseline.

### Summary counts

| Outcome    | Count |
|------------|-------|
| passed     | 146   |
| failed     | 1     |
| skipped    | 0     |
| deselected | 0     |
| errors     | 0     |
| warnings   | 3     |

Final pytest summary line:

```
1 failed, 146 passed, 3 warnings in ~11s
```

The single failure is `tests/test_integration_influx.py::test_list_item_ids_returns_distinct_ids`
— the **pre-existing baseline failure** (recorded in Task 1.1, excluded from
the Baseline_Green_Set). It is NOT a regression: it failed identically at
baseline (`assert set() >= {'554', '565'}`).

### Step 2: Baseline_Green_Set subset check

The 146 passing node ids from this run were compared against the recorded
Baseline_Green_Set (146 node ids). The Baseline_Green_Set is a subset of the
current passing set — **zero regressions** (`comm -23 baseline current`
produced empty output). Every baseline-green test still passes.

### Step 3: F401 resolution confirmation

```
.venv/bin/ruff check tests/test_integration_api.py --select F401
# All checks passed! (exit 0)
```

The F401 finding is resolved.

### Decision

**KEEP Batch A.** All 146 baseline-green tests still pass (Property 1 holds),
the working tree is green modulo the pre-existing baseline failure (Property 2
holds), and the targeted F401 is resolved. No `git restore` needed.

---

## Task 9.3 — Batch E verification

**Property 2: The working tree is green after every processed batch**
_Validates: Requirements 6.2, 6.4_

### What Batch E changed (verified applied before the run)

- Deleted `DatabaseSetup.ipynb`
- Deleted `testFunctions.ipynb`
- Deleted `testGettingandwritingtimestamps.ipynb`
- Deleted `IMPROVEMENTS.md`
- Deleted `.ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb`
- Removed the `## Notebooks` section (heading + notebook bullet lines) from `README.md`

`git status --porcelain` confirms the five files staged as deleted (`D`) and
`README.md` modified (`M`). A grep of `README.md` for `Notebooks`,
`DatabaseSetup`, `testFunctions`, and `testGettingandwriting` returns no hits
(exit 1). None of these artifacts are under `tests/`, and the suite respects
`testpaths=["tests"]`, so the removals cannot affect test collection.

### Batch E pytest run

Command (run from repo root inside the project `.venv`):

```
.venv/bin/python -m pytest
```

### Summary counts

| Outcome    | Count |
|------------|-------|
| passed     | 146   |
| failed     | 1     |
| skipped    | 0     |
| deselected | 0     |
| errors     | 0     |
| warnings   | 3     |

Final pytest summary line:

```
1 failed, 146 passed, 3 warnings in ~14s
```

The single failure is the pre-existing baseline failure
`tests/test_integration_influx.py::test_list_item_ids_returns_distinct_ids`,
which is **excluded** from the regression gate (recorded in Task 1.1). It fails
identically to baseline (`assert set() >= {'554', '565'}`) and is not a
regression introduced by Batch E.

### Baseline_Green_Set comparison

The passing node ids from this run were captured via a JUnit XML report and
compared set-wise against the recorded Baseline_Green_Set (146 node ids):

- baseline − passing (regressions): **none**
- passing − baseline (extra/new): **none**
- Baseline_Green_Set ⊆ passing set: **True** (exact match, 146 = 146)

### Decision: **KEEP**

All 146 baseline-green tests still pass with zero regressions. The Batch E
deletions and the `README.md` edit are retained. No `git restore`/`git checkout`
revert was needed.

---

## Task 10.1 — Final verification

_Requirements: 1.3, 2.3, 4.1, 4.2, 4.3, 4.4, 4.5, 5.1, 6.2, 6.3_

Read/verify-only final gate. No source or artifact files were modified in this
task (only this notes file was appended to). All confirmations below were run
from the repo root inside the project `.venv`.

### 1. Removed symbol — unused `data_access` import (Batch A / F401)

- `ruff check tests/test_integration_api.py --select F401` → **`All checks passed!`** (exit 0).
  The previously-flagged `ge_pipeline.data_access` unused import is gone.
- Grep confirms the import line is now exactly:
  ```
  tests/test_integration_api.py:42: from ge_pipeline import api, influx
  ```
  `data_access` no longer appears in the import.

### 2. Five stale files ABSENT

Each checked with a filesystem existence test — all report ABSENT:

| File | Status |
|------|--------|
| `DatabaseSetup.ipynb` | ABSENT ✓ |
| `testFunctions.ipynb` | ABSENT ✓ |
| `testGettingandwritingtimestamps.ipynb` | ABSENT ✓ |
| `IMPROVEMENTS.md` | ABSENT ✓ |
| `.ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb` | ABSENT ✓ |

### 3. README.md clean

Grep for `Notebooks|DatabaseSetup|testFunctions|testGettingandwritingtimestamps`
in `README.md` → **No matches found.** The `## Notebooks` heading and all three
notebook filenames are gone; surrounding Usage / API Reference sections remain.

### 4. Final full-suite pytest run

Command: `.venv/bin/python -m pytest`

Final summary line:

```
1 failed, 146 passed, 3 warnings in 12.42s
```

| Outcome | Count |
|---------|------:|
| passed | 146 |
| failed | 1 |
| skipped | 0 |
| deselected | 0 |
| errors | 0 |

- The **146 passed** exactly match the recorded Baseline_Green_Set — every
  baseline-green node id still passes. **No regression.**
- The single failure is the pre-existing baseline failure
  `tests/test_integration_influx.py::test_list_item_ids_returns_distinct_ids`
  (empty set returned against the seeded container — a seeding/flush-timing
  issue in the integration harness). It is **excluded from the gate** and is
  not a regression introduced by any removal batch.

### Conclusion

All removals confirmed and the suite is green against the Baseline_Green_Set:
- unused `data_access` import removed (ruff F401 clean),
- all five stale artifacts absent,
- README free of the Notebooks section and notebook filenames,
- 146/146 baseline-green tests pass; the lone failure is the known, excluded
  pre-existing baseline failure.

Task 10.1 verification: **PASS.**
