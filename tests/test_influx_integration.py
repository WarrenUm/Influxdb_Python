"""Integration tests for the InfluxDB 3 storage seam against a live server.

These tests exercise :mod:`ge_pipeline.influx` against a real **InfluxDB 3
Core** instance started via ``testcontainers`` (design "Testing Strategy":
"A live InfluxDB 3 Core instance via ``testcontainers`` backs the query/write
... integration tests"). When Docker or ``testcontainers`` is not available the
whole module is skipped rather than failed, so the suite stays green in
environments without a container runtime.

Currently implemented:

* **Property 5: Write/read round-trip preserves the snapshot** (Task 3.4) —
  snapshots written through the v3 Write_Path (:func:`ge_pipeline.influx.write_batch`)
  and read back through the Query_Path (:func:`ge_pipeline.influx.query_price_series`)
  preserve the ``itemID``, the four camelCase field values, and the unix-second
  timestamp of every record.

The container fixture defined here (``influx3_client``) starts an ephemeral
in-memory InfluxDB 3 Core server with auth disabled. Task 11.1 may later
centralize this fixture in ``tests/conftest.py``; it is named descriptively so
the two can be reconciled without ambiguity.
"""

from __future__ import annotations

import itertools
import socket
import time

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ge_pipeline import influx

# --- Optional dependency / runtime guards ---------------------------------
#
# Import testcontainers lazily and skip the entire module when the container
# runtime (Docker) or the library is unavailable, so the suite runs cleanly on
# machines without Docker.
try:  # pragma: no cover - exercised only by presence/absence of the dep
    from testcontainers.core.container import DockerContainer  # noqa: F401
except Exception as exc:  # noqa: BLE001 - any import/runtime failure means skip # pragma: no cover
    pytest.skip(
        f"testcontainers is unavailable ({exc}); skipping InfluxDB 3 "
        "integration tests",
        allow_module_level=True,
    )

# The live InfluxDB 3 Core container fixtures (``influx3_container`` and
# ``influx3_client``) are centralized in ``tests/conftest.py`` (Task 11.1) so
# the query/write and setup/migration integration tests share one container
# definition. This module consumes them by name via pytest's fixture lookup;
# it keeps the module-level skip guard above so the whole file skips cleanly
# when ``testcontainers`` / Docker is unavailable.


# --- Strategies -----------------------------------------------------------

# Non-negative integer field values matching the v2/v3 schema (int >= 0). Kept
# in a modest range so generated batches are cheap to write and read back.
_FIELD_VALUE = st.integers(min_value=0, max_value=1_000_000)

# Distinct item ids drawn from a small pool so a batch commonly spans several
# items while keeping the round trip fast.
_ITEM_IDS = st.sampled_from(["554", "555", "2", "10", "4151"])


@st.composite
def _snapshot_batch(draw: st.DrawFn) -> list[dict]:
    """Draw a batch of unique-(itemID, time) price records to round-trip.

    Every record carries all four camelCase fields with non-null values and a
    unix-second timestamp. ``(itemID, time)`` pairs are unique within the batch
    so each written record maps to exactly one read-back point (the query path
    returns one row per stored point).
    """
    n = draw(st.integers(min_value=1, max_value=8))
    seen: set[tuple[str, int]] = set()
    records: list[dict] = []
    for _ in range(n):
        item_id = draw(_ITEM_IDS)
        # Timestamps in a bounded, well-separated range so they stay distinct
        # per item and comfortably inside the query window.
        ts = draw(st.integers(min_value=1_600_000_000, max_value=1_600_100_000))
        key = (item_id, ts)
        if key in seen:
            continue
        seen.add(key)
        records.append(
            {
                "measurement": influx.MEASUREMENT,
                "tags": {"itemID": item_id},
                "time": ts,
                "fields": {
                    "avgHighPrice": draw(_FIELD_VALUE),
                    "avgLowPrice": draw(_FIELD_VALUE),
                    "highPriceVolume": draw(_FIELD_VALUE),
                    "lowPriceVolume": draw(_FIELD_VALUE),
                },
            }
        )
    return records


# --- Property 5: write/read round-trip preserves the snapshot -------------


@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(records=_snapshot_batch())
def test_write_read_round_trip_preserves_snapshot(influx3_client, records):
    """Written snapshots read back identical in id, fields, and timestamp.

    **Feature: influxdb-v3-migration, Property 5: Write/read round-trip
    preserves the snapshot**

    **Validates: Requirements 3.1, 3.2, 3.3, 7.1**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    # Isolate each generated example by tagging its records with a unique run
    # marker appended to the item id, so previously-written points from earlier
    # examples cannot bleed into this example's read-back assertions.
    run_marker = f"-r{int(time.time() * 1_000_000)}-{socket.gethostname()[:4]}"
    tagged: list[dict] = []
    for record in records:
        tagged.append(
            {
                **record,
                "tags": {"itemID": record["tags"]["itemID"] + run_marker},
            }
        )

    influx.write_batch(client, database, tagged)

    # Expected round-trip view keyed by (itemID, time).
    expected: dict[tuple[str, int], dict] = {
        (rec["tags"]["itemID"], int(rec["time"])): rec["fields"] for rec in tagged
    }

    # Group by item so each item's series is queried across the full window.
    item_ids = {rec["tags"]["itemID"] for rec in tagged}
    start = 1_600_000_000
    stop = 1_600_100_001  # exclusive upper bound just past the max timestamp

    actual: dict[tuple[str, int], dict] = {}
    for item_id in item_ids:
        series = influx.query_price_series(
            client, item_id, start, stop, database=database
        )
        for point in series:
            key = (item_id, int(point["time"]))
            actual[key] = point

    # Every written record is read back exactly once, with matching id, four
    # field values, and timestamp.
    assert set(actual) == set(expected), (
        "read-back (itemID, time) set differs from what was written"
    )
    for key, fields in expected.items():
        point = actual[key]
        for name in influx.PRICE_FIELDS:
            assert point[name] == fields[name], (
                f"field {name} for {key} round-tripped as {point[name]!r}, "
                f"expected {fields[name]!r}"
            )


# --- Property 6: latest timestamp equals the maximum stored ---------------


import uuid


def _unique_item_id() -> str:
    """Return an item id unique per test so runs never collide in the table."""
    return f"item-{uuid.uuid4().hex}"


# Distinct unix-second timestamps aligned to 5-minute boundaries, matching the
# stored schema. Kept in a modest window so many-point series stay cheap.
_ALIGNED_TIMESTAMPS = st.integers(
    min_value=1_600_000_000, max_value=1_600_600_000
).map(lambda v: v - (v % 300))


def _write_series(client, database: str, item_id: str, timestamps: list[int]) -> None:
    """Write one snapshot per timestamp for ``item_id`` through the Write_Path."""
    records = [
        {
            "measurement": influx.MEASUREMENT,
            "tags": {"itemID": item_id},
            "time": ts,
            "fields": {"avgHighPrice": index + 1},
        }
        for index, ts in enumerate(timestamps)
    ]
    influx.write_batch(client, database, records)


@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(timestamps=st.lists(_ALIGNED_TIMESTAMPS, min_size=1, max_size=8, unique=True))
def test_get_latest_timestamp_equals_maximum_stored(influx3_client, timestamps):
    """``get_latest_timestamp`` returns the maximum stored timestamp for an item.

    **Feature: influxdb-v3-migration, Property 6: Latest timestamp equals the
    maximum stored** — for any stored series for an item,
    ``get_latest_timestamp`` returns the maximum stored unix-second timestamp
    for that item.

    **Validates: Requirements 6.2**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    item_id = _unique_item_id()
    _write_series(client, database, item_id, timestamps)

    latest = influx.get_latest_timestamp(client, item_id, database=database)

    assert latest == max(timestamps)


@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(data=st.data())
def test_get_latest_timestamp_none_for_unknown_item(influx3_client, data):
    """``get_latest_timestamp`` returns ``None`` for an item with no stored data.

    **Feature: influxdb-v3-migration, Property 6: Latest timestamp equals the
    maximum stored** — the null-result branch: an item that was never written
    has no maximum, so the result is ``None``.

    **Validates: Requirements 6.2**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    # A freshly generated id is guaranteed absent from the table.
    absent_item = _unique_item_id()

    result = influx.get_latest_timestamp(client, absent_item, database=database)

    assert result is None


# --- Property 10: distinct item listing -----------------------------------


@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(
    item_count=st.integers(min_value=1, max_value=5),
    timestamps=st.lists(_ALIGNED_TIMESTAMPS, min_size=1, max_size=4, unique=True),
)
def test_list_item_ids_returns_distinct_written_ids(
    influx3_client, item_count, timestamps
):
    """``list_item_ids`` returns exactly the distinct ``itemID`` values written.

    **Feature: influxdb-v3-migration, Property 10: Distinct item listing** —
    for the dataset written by this example, ``list_item_ids`` returns a
    distinct set of ids that contains exactly the ids just written (checked as
    membership + distinctness against the shared table, since other examples
    write into the same server).

    **Validates: Requirements 6.6**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    written_ids = {_unique_item_id() for _ in range(item_count)}
    for item_id in written_ids:
        _write_series(client, database, item_id, timestamps)

    listed = influx.list_item_ids(client, database=database)

    # The listing is distinct (a set of ids, no duplicates, no rename).
    assert len(listed) == len(set(listed))
    # Every id written in this example appears in the distinct listing.
    assert written_ids <= set(listed)

# --- Property 7: price series is time-ascending and complete --------------


@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(timestamps=st.lists(_ALIGNED_TIMESTAMPS, min_size=1, max_size=10, unique=True))
def test_query_price_series_is_time_ascending_and_complete(
    influx3_client, timestamps
):
    """``query_price_series`` returns strictly-ascending points with four fields.

    **Feature: influxdb-v3-migration, Property 7: Price series is time-ascending
    and complete** — for any stored series and time range,
    ``query_price_series`` returns points ordered strictly ascending by time,
    each carrying the four price/volume fields.

    **Validates: Requirements 6.3**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    # Unique id isolates this example's points in the shared table.
    item_id = _unique_item_id()
    records = [
        {
            "measurement": influx.MEASUREMENT,
            "tags": {"itemID": item_id},
            "time": ts,
            "fields": {
                "avgHighPrice": index + 1,
                "avgLowPrice": index + 2,
                "highPriceVolume": index + 3,
                "lowPriceVolume": index + 4,
            },
        }
        for index, ts in enumerate(sorted(timestamps))
    ]
    influx.write_batch(client, database, records)

    # Window covers every written timestamp; stop is exclusive so pad by one.
    start = min(timestamps)
    stop = max(timestamps) + 1

    series = influx.query_price_series(client, item_id, start, stop, database=database)

    # Completeness: one point per written timestamp.
    assert len(series) == len(timestamps)

    # Strictly ascending by time.
    times = [point["time"] for point in series]
    assert times == sorted(times)
    assert all(later > earlier for earlier, later in itertools.pairwise(times)), (
        f"series times are not strictly ascending: {times}"
    )

    # Every point carries the four price/volume fields.
    for point in series:
        for name in influx.PRICE_FIELDS:
            assert name in point, f"point {point!r} missing field {name!r}"


# --- Property 8: interval downsampling is the per-window mean --------------

# Interval choices (in seconds) that align with the 5-minute-aligned source
# timestamps so windows group several source points together.
_INTERVAL_SECONDS = st.sampled_from([300, 600, 900, 1800, 3600])


@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(
    timestamps=st.lists(_ALIGNED_TIMESTAMPS, min_size=1, max_size=12, unique=True),
    interval_seconds=_INTERVAL_SECONDS,
    data=st.data(),
)
def test_query_price_series_interval_is_per_window_mean(
    influx3_client, timestamps, interval_seconds, data
):
    """Each downsampled window equals the mean of its source values.

    **Feature: influxdb-v3-migration, Property 8: Interval downsampling is the
    per-window mean** — for any stored series and aggregation interval, each
    returned window value equals the mean of the source values falling within
    that window, as computed by a reference ``date_bin`` mean.

    **Validates: Requirements 6.4**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    item_id = _unique_item_id()

    # Draw a known field value for each timestamp so the reference mean is
    # computed from exactly the values stored.
    values_by_ts: dict[int, dict[str, int]] = {}
    records: list[dict] = []
    for ts in sorted(timestamps):
        fields = {
            name: data.draw(_FIELD_VALUE, label=f"{name}@{ts}")
            for name in influx.PRICE_FIELDS
        }
        values_by_ts[ts] = fields
        records.append(
            {
                "measurement": influx.MEASUREMENT,
                "tags": {"itemID": item_id},
                "time": ts,
                "fields": dict(fields),
            }
        )
    influx.write_batch(client, database, records)

    start = min(timestamps)
    stop = max(timestamps) + 1
    interval = f"{interval_seconds}s"

    series = influx.query_price_series(
        client, item_id, start, stop, interval=interval, database=database
    )

    # Reference per-window mean: group source values by their date_bin window.
    # InfluxDB ``date_bin`` with no explicit origin bins from the unix epoch,
    # so a point's window start is ``floor(time / interval) * interval``.
    reference: dict[int, dict[str, float]] = {}
    grouped: dict[int, list[dict[str, int]]] = {}
    for ts, fields in values_by_ts.items():
        window = (ts // interval_seconds) * interval_seconds
        grouped.setdefault(window, []).append(fields)
    for window, field_dicts in grouped.items():
        reference[window] = {
            name: sum(fd[name] for fd in field_dicts) / len(field_dicts)
            for name in influx.PRICE_FIELDS
        }

    # One returned point per non-empty window.
    assert len(series) == len(reference), (
        f"expected {len(reference)} windows, got {len(series)}: {series}"
    )

    actual_by_window = {int(point["time"]): point for point in series}
    assert set(actual_by_window) == set(reference), (
        f"window starts differ: got {sorted(actual_by_window)}, "
        f"expected {sorted(reference)}"
    )

    for window, expected_means in reference.items():
        point = actual_by_window[window]
        for name in influx.PRICE_FIELDS:
            assert point[name] == pytest.approx(expected_means[name], rel=1e-9, abs=1e-6), (
                f"window {window} field {name}: got {point[name]!r}, "
                f"expected mean {expected_means[name]!r}"
            )

# --- Property 9: chunk frame has one row per item-and-timestamp -----------


def _write_chunk_dataset(
    client, database: str, dataset: dict[str, list[int]]
) -> None:
    """Write one snapshot per (itemID, timestamp) described by ``dataset``.

    ``dataset`` maps each unique item id to its list of unix-second timestamps.
    Every point carries all four camelCase fields with deterministic,
    per-(item, ts) values so the read-back frame can be checked cell by cell.
    """
    records: list[dict] = []
    for item_id, timestamps in dataset.items():
        for offset, ts in enumerate(timestamps):
            records.append(
                {
                    "measurement": influx.MEASUREMENT,
                    "tags": {"itemID": item_id},
                    "time": ts,
                    "fields": {
                        "avgHighPrice": offset + 1,
                        "avgLowPrice": offset + 2,
                        "highPriceVolume": offset + 3,
                        "lowPriceVolume": offset + 4,
                    },
                }
            )
    influx.write_batch(client, database, records)


@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(
    item_count=st.integers(min_value=1, max_value=4),
    timestamps=st.lists(_ALIGNED_TIMESTAMPS, min_size=1, max_size=6, unique=True),
)
def test_query_chunk_has_one_row_per_item_and_timestamp(
    influx3_client, item_count, timestamps
):
    """``query_chunk`` returns one row per ``(itemID, timestamp)`` and a column per field.

    **Feature: influxdb-v3-migration, Property 9: Chunk frame has one row per
    item-and-timestamp** — for any set of items and time range, the combined
    frame has exactly one row per ``(itemID, timestamp)`` present in the data
    and one column per price/volume field.

    **Validates: Requirements 6.5**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    # Unique per-example item ids isolate this example's rows in the shared
    # table so the frame contains only the points written here.
    item_ids = [_unique_item_id() for _ in range(item_count)]
    dataset = {item_id: list(timestamps) for item_id in item_ids}
    _write_chunk_dataset(client, database, dataset)

    start = min(timestamps)
    stop = max(timestamps) + 1  # exclusive upper bound just past the max ts

    frame = influx.query_chunk(client, item_ids, start, stop, database=database)

    # Expected set of (itemID, timestamp) rows for exactly the data written.
    expected_keys = {
        (item_id, int(ts))
        for item_id, tss in dataset.items()
        for ts in tss
    }

    assert not frame.empty
    assert "itemID" in frame.columns
    assert "time" in frame.columns
    # One column per price/volume field (Requirement 6.5).
    for name in influx.PRICE_FIELDS:
        assert name in frame.columns, f"missing field column {name!r}"

    # Exactly one row per (itemID, timestamp): no duplicates, and the row set
    # equals the written (item, time) pairs.
    actual_keys = [
        (str(row["itemID"]), _unix(row["time"]))
        for row in frame.to_dict(orient="records")
    ]
    assert len(actual_keys) == len(expected_keys), (
        f"expected {len(expected_keys)} rows, got {len(actual_keys)}"
    )
    assert set(actual_keys) == expected_keys
    # Uniqueness: no (itemID, timestamp) appears more than once.
    assert len(actual_keys) == len(set(actual_keys))


def _unix(value) -> int:
    """Coerce a frame ``time`` cell to whole unix seconds for comparison."""
    return influx._time_to_unix_seconds(value)


# --- Property 11: query parameters are bound, not interpolated ------------

# Adversarial item-id strings containing SQL metacharacters (quotes,
# semicolons, comment markers, injection payloads). Bound as parameters, each
# must be treated purely as a literal value that matches no rows, never as
# executable SQL.
_MALICIOUS_ITEM_IDS = st.sampled_from(
    [
        "1'; DROP TABLE \"itemPrice\";--",
        "' OR '1'='1",
        "'; DELETE FROM \"itemPrice\"; --",
        "554' UNION SELECT * FROM \"itemPrice\" --",
        '") OR ("1"="1',
        "\\'; DROP TABLE itemPrice; --",
        "0); DROP DATABASE ge_test; --",
        "'--",
        "*/; SELECT 1 /*",
        "robert'); DROP TABLE students;--",
    ]
)


@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(malicious_id=_MALICIOUS_ITEM_IDS)
def test_query_parameters_are_bound_not_interpolated(influx3_client, malicious_id):
    """Adversarial item ids are bound as literals, never executed as SQL.

    **Feature: influxdb-v3-migration, Property 11: Query parameters are bound,
    not interpolated** — for any item-id string, including ones containing SQL
    metacharacters (quotes, semicolons, comment markers), the Query_Path runs
    without error and returns results consistent with treating the string as a
    literal value: a nonexistent malicious id simply matches no rows, and the
    table remains intact.

    **Validates: Requirements 6.7**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    # Seed a known-good row so we can prove the table survives the malicious
    # queries below (no injected DROP/DELETE ever executes).
    canary_id = _unique_item_id()
    canary_ts = 1_600_000_000
    influx.write_batch(
        client,
        database,
        [
            {
                "measurement": influx.MEASUREMENT,
                "tags": {"itemID": canary_id},
                "time": canary_ts,
                "fields": {"avgHighPrice": 42},
            }
        ],
    )

    start = 1_500_000_000
    stop = 1_700_000_000

    # Each Query_Path call must execute without error and, because the
    # malicious id is bound as a literal value that no row carries, return an
    # empty / null result.
    latest = influx.get_latest_timestamp(client, malicious_id, database=database)
    assert latest is None

    series = influx.query_price_series(
        client, malicious_id, start, stop, database=database
    )
    assert series == []

    frame = influx.query_chunk(client, [malicious_id], start, stop, database=database)
    assert frame.empty

    # A chunk mixing a real id with the malicious id returns only the real
    # id's rows (the malicious id contributes nothing) — the id was matched as
    # a value, not spliced into the SQL.
    mixed = influx.query_chunk(
        client, [canary_id, malicious_id], start, stop, database=database
    )
    mixed_ids = {str(v) for v in mixed["itemID"].tolist()} if not mixed.empty else set()
    assert mixed_ids == {canary_id}

    # The canary row (and therefore the table) is still intact: no injected
    # DROP/DELETE executed.
    assert (
        influx.get_latest_timestamp(client, canary_id, database=database)
        == canary_ts
    )
