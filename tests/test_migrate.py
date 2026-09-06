"""Property-based tests for the one-pass v2->v3 migration transform.

Exercises the pure transform at the heart of the migration reader,
:func:`ge_pipeline.migrate._flux_record_to_record`, which converts a single
pivoted v2 Flux row (a mapping of column name to value) into the v3 record dict
consumed by :func:`ge_pipeline.influx.write_batch`.

Implements the migration correctness property from the design's Correctness
Properties section:

* **Property 12: Migration copies records 1:1 preserving names** (Task 6.2)

Each property runs under the ``ge`` Hypothesis profile (``max_examples>=100``)
registered in ``tests/conftest.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ge_pipeline.influx import MEASUREMENT, PRICE_FIELDS
from ge_pipeline.migrate import _flux_record_to_record

# The v2 columns that carry the measurement, tag, and timestamp in a pivoted
# Flux row. These mirror the private constants in ``migrate.py`` and are
# duplicated here so the test asserts the wire-level column names directly.
_COL_MEASUREMENT = "_measurement"
_COL_TIME = "_time"
_TAG_ITEM_ID = "itemID"

# Integer field values kept in a wide but bounded range. Migration must copy
# the numeric values verbatim, so the exact range only needs to exercise a
# spread of magnitudes (including zero and negatives) without transformation.
_FIELD_VALUES = st.integers(min_value=-1_000_000, max_value=1_000_000)

# itemID tag values: numeric-string ids like the real OSRS item ids, plus a few
# awkward strings to confirm the tag value is copied as-is (only str-coerced).
_ITEM_IDS = st.sampled_from(["554", "2", "10", "13190", "0"])

# Timestamps as timezone-aware UTC datetimes (matching what v2 Flux returns).
# tz-aware datetimes make ``.timestamp()`` deterministic regardless of the host
# timezone, so the expected unix-second value is unambiguous. The ``min_value``/
# ``max_value`` bounds must be naive here -- ``st.datetimes`` attaches the UTC
# ``timezones`` strategy itself, so a tz-aware bound would raise.
_TIMES = st.datetimes(
    min_value=datetime(2019, 1, 1),  # noqa: DTZ001 - Hypothesis attaches UTC via `timezones`
    max_value=datetime(2035, 1, 1),  # noqa: DTZ001 - Hypothesis attaches UTC via `timezones`
    timezones=st.just(timezone.utc),
)


@st.composite
def _flux_row(draw: st.DrawFn) -> tuple[dict, dict, int]:
    """Draw a pivoted v2 Flux row plus the expected non-null fields and time.

    Returns ``(values, expected_fields, expected_unix_seconds)`` where:

    * ``values`` is the mapping a pivoted Flux row would expose
      (``_measurement``, ``itemID``, ``_time``, and a *subset* of the four
      camelCase field columns; omitted or explicitly-``None`` fields model the
      null columns Flux produces when an item has no value for that field).
    * ``expected_fields`` is exactly the non-null camelCase fields with their
      original values (what the transform must preserve).
    * ``expected_unix_seconds`` is the timestamp coerced to whole unix seconds.
    """
    item_id = draw(_ITEM_IDS)
    when = draw(_TIMES)

    # Independently decide, per field, whether it is present with a value,
    # explicitly null, or absent from the row entirely. All three model real
    # pivoted-Flux shapes; only present-with-value fields survive the transform.
    values: dict = {
        _COL_MEASUREMENT: MEASUREMENT,
        _TAG_ITEM_ID: item_id,
        _COL_TIME: when,
    }
    expected_fields: dict = {}
    for name in PRICE_FIELDS:
        presence = draw(st.sampled_from(["value", "null", "absent"]))
        if presence == "value":
            value = draw(_FIELD_VALUES)
            values[name] = value
            expected_fields[name] = value
        elif presence == "null":
            values[name] = None
        # "absent": leave the key out of the row entirely.

    return values, expected_fields, int(when.timestamp())


@given(row=_flux_row())
def test_migration_copies_records_1_to_1_preserving_names(
    row: tuple[dict, dict, int],
) -> None:
    """The transform copies each v2 row 1:1, preserving every name and value.

    **Feature: influxdb-v3-migration, Property 12: Migration copies records 1:1
    preserving names**

    **Validates: Requirements 5.1, 5.2, 10.2**
    """
    values, expected_fields, expected_time = row

    record = _flux_record_to_record(values)

    if not expected_fields:
        # A row with no non-null price field carries no snapshot and is dropped
        # (returns None), consistent with ingestion's null-field filtering.
        assert record is None
        return

    assert record is not None

    # Measurement name preserved verbatim (no rename).
    assert record["measurement"] == MEASUREMENT
    assert record["measurement"] == "itemPrice"

    # itemID tag preserved: same tag key, value copied as the original string.
    assert record["tags"] == {"itemID": str(values[_TAG_ITEM_ID])}

    # Timestamp preserved exactly as whole unix seconds (no shift/rounding).
    assert record["time"] == expected_time

    # Fields preserved 1:1: exactly the non-null camelCase columns, same names,
    # same values, nothing renamed, nothing added, nulls dropped.
    assert record["fields"] == expected_fields
    for name, value in expected_fields.items():
        # Field name is one of the four camelCase names, carried over verbatim.
        assert name in (
            "avgHighPrice",
            "avgLowPrice",
            "highPriceVolume",
            "lowPriceVolume",
        )
        assert record["fields"][name] == value

    # No null field leaked into the output.
    assert all(v is not None for v in record["fields"].values())

    # The record dict has exactly the four expected keys and nothing extra.
    assert set(record) == {"measurement", "tags", "time", "fields"}


@given(
    item_id=_ITEM_IDS,
    when=_TIMES,
    values=st.lists(_FIELD_VALUES, min_size=4, max_size=4),
)
def test_migration_preserves_all_four_field_names_and_values(
    item_id: str,
    when: datetime,
    values: list[int],
) -> None:
    """When all four fields are present, every name and value is preserved.

    This narrows Property 12 to the fully-populated snapshot: the transform must
    keep all four camelCase field names paired with their original values.

    **Feature: influxdb-v3-migration, Property 12: Migration copies records 1:1
    preserving names**

    **Validates: Requirements 5.1, 5.2, 10.2**
    """
    row = {
        _COL_MEASUREMENT: MEASUREMENT,
        _TAG_ITEM_ID: item_id,
        _COL_TIME: when,
    }
    for name, value in zip(PRICE_FIELDS, values):
        row[name] = value

    record = _flux_record_to_record(row)

    assert record is not None
    assert record["measurement"] == "itemPrice"
    assert record["tags"] == {"itemID": str(item_id)}
    assert record["time"] == int(when.timestamp())
    assert record["fields"] == dict(zip(PRICE_FIELDS, values))


# ---------------------------------------------------------------------------
# Unit tests: preflight stops before any write (Task 6.5, Requirement 5.6)
# ---------------------------------------------------------------------------
#
# These example-based tests exercise the full ``run_migration`` orchestration
# with lightweight fakes standing in for the v2 source client and the cached v3
# target client. They assert the preflight contract from Requirement 5.6: when
# either endpoint is unreachable at start, ``run_migration`` raises a
# ``ConnectionError`` naming the unreachable server and stops *before* writing
# anything -- the fake v3 client's ``write`` is never called and no records are
# read or written.

import pytest

from ge_pipeline import migrate
from ge_pipeline.config import Settings

# The v2 source values needed so ``require_migration_source`` succeeds without
# patching; the migration then proceeds to the preflight under test.
_V2_URL = "http://v2-source.example:8086"
_V3_HOST = "http://v3-target.example:8181"


def _migration_settings() -> Settings:
    """Build Settings with the v3 target and all four v2 source values set."""
    return Settings(
        influx3_host=_V3_HOST,
        influx3_token="v3-token",
        influx3_database="ge",
        v2_url=_V2_URL,
        v2_token="v2-token",
        v2_org="Ge-data-project",
        v2_bucket="GEItemPrices",
    )


class _PreflightFakeV2Client:
    """Stand-in for ``influxdb_client.InfluxDBClient`` used in preflight tests.

    Records whether ``ping``/``close`` were called and whether the Flux read
    stream was ever touched, so a test can assert no reading happened once
    preflight fails.
    """

    def __init__(
        self, *, ping_result: bool = True, ping_error: Exception | None = None
    ) -> None:
        self._ping_result = ping_result
        self._ping_error = ping_error
        self.ping_called = False
        self.close_called = False
        self.query_api_called = False

    def ping(self) -> bool:
        self.ping_called = True
        if self._ping_error is not None:
            raise self._ping_error
        return self._ping_result

    def query_api(self):  # pragma: no cover - must never run when preflight fails
        self.query_api_called = True
        raise AssertionError("query_api must not be called once preflight fails")

    def close(self) -> None:
        self.close_called = True


class _PreflightFakeV3Client:
    """Stand-in for the cached v3 client.

    ``query`` optionally raises to simulate an unreachable target. ``write``
    records every call so tests can assert it was never invoked.
    """

    def __init__(self, *, query_error: Exception | None = None) -> None:
        self._query_error = query_error
        self.query_calls: list[dict] = []
        self.write_calls: list[dict] = []

    def query(self, **kwargs) -> None:
        self.query_calls.append(kwargs)
        if self._query_error is not None:
            raise self._query_error

    def write(self, **kwargs) -> None:
        self.write_calls.append(kwargs)


def test_run_migration_unreachable_source_raises_naming_source_no_write(monkeypatch):
    """An unreachable v2 source aborts before any write (Requirement 5.6).

    When the v2 source ping returns falsy, ``run_migration`` raises a
    ``ConnectionError`` whose message names the source URL, never touches the v2
    read stream, and never calls the v3 client's ``write``.

    **Validates: Requirements 5.6**
    """
    settings = _migration_settings()
    fake_v2 = _PreflightFakeV2Client(ping_result=False)
    fake_v3 = _PreflightFakeV3Client()

    monkeypatch.setattr(migrate, "InfluxDBClient", lambda **kwargs: fake_v2)
    monkeypatch.setattr(migrate.influx, "get_client", lambda s: fake_v3)

    with pytest.raises(ConnectionError) as excinfo:
        migrate.run_migration(settings)

    # Error names the unreachable source.
    assert _V2_URL in str(excinfo.value)

    # Stopped before writing: no write attempted and the read stream untouched.
    assert fake_v3.write_calls == []
    assert fake_v2.query_api_called is False
    # v2 was pinged; source is checked first, so the v3 probe never ran.
    assert fake_v2.ping_called is True
    assert fake_v3.query_calls == []
    # The v2 client is always closed even on the early abort.
    assert fake_v2.close_called is True


def test_run_migration_source_ping_raises_raises_naming_source_no_write(monkeypatch):
    """A v2 source whose ping *raises* is treated as unreachable, no write.

    Confirms the transport-error branch of the source preflight also names the
    source and stops before writing.

    **Validates: Requirements 5.6**
    """
    settings = _migration_settings()
    fake_v2 = _PreflightFakeV2Client(ping_error=OSError("connection refused"))
    fake_v3 = _PreflightFakeV3Client()

    monkeypatch.setattr(migrate, "InfluxDBClient", lambda **kwargs: fake_v2)
    monkeypatch.setattr(migrate.influx, "get_client", lambda s: fake_v3)

    with pytest.raises(ConnectionError) as excinfo:
        migrate.run_migration(settings)

    assert _V2_URL in str(excinfo.value)
    assert fake_v3.write_calls == []
    assert fake_v2.query_api_called is False
    assert fake_v3.query_calls == []
    assert fake_v2.close_called is True


def test_run_migration_unreachable_target_raises_naming_target_no_write(monkeypatch):
    """An unreachable v3 target aborts before any write (Requirement 5.6).

    When the v2 source ping succeeds but the v3 target probe query raises,
    ``run_migration`` raises a ``ConnectionError`` naming the target host,
    never calls the v3 client's ``write``, and never reads from v2.

    **Validates: Requirements 5.6**
    """
    settings = _migration_settings()
    fake_v2 = _PreflightFakeV2Client(ping_result=True)
    fake_v3 = _PreflightFakeV3Client(query_error=OSError("connection refused"))

    monkeypatch.setattr(migrate, "InfluxDBClient", lambda **kwargs: fake_v2)
    monkeypatch.setattr(migrate.influx, "get_client", lambda s: fake_v3)

    with pytest.raises(ConnectionError) as excinfo:
        migrate.run_migration(settings)

    # Error names the unreachable target host.
    assert _V3_HOST in str(excinfo.value)

    # The source was reached (pinged) but the target probe failed; no write and
    # no read happened.
    assert fake_v2.ping_called is True
    assert len(fake_v3.query_calls) == 1
    assert fake_v3.write_calls == []
    assert fake_v2.query_api_called is False
    assert fake_v2.close_called is True


# ---------------------------------------------------------------------------
# Property 13 & 14: run_migration batching and count accuracy (Task 6.4)
#
# These drive the full ``run_migration`` orchestration with lightweight fakes
# and no live server: a fake v2 client whose ``query_stream`` yields generated
# pivoted Flux rows (so ``iter_v2_records`` produces N records), a fake v3
# client that records every ``write(record=...)`` call, a no-op preflight, and
# a ``Settings`` carrying all four ``V2_*`` source values. Varying the record
# count and ``batch_size`` exercises the batching partition and the reported
# read/written counts.
#
# ``mock.patch`` context managers (rather than the ``monkeypatch`` fixture) are
# used to apply the fakes so patching happens inside each generated example --
# Hypothesis flags a function-scoped fixture that is not reset per example.
# ---------------------------------------------------------------------------

from contextlib import ExitStack
from unittest import mock


class _StreamingFluxRecord:
    """Minimal stand-in for an influxdb_client FluxRecord.

    ``iter_v2_records`` only touches ``.values`` on each streamed row, so the
    fake exposes exactly that.
    """

    def __init__(self, values: dict) -> None:
        self.values = values


class _StreamingQueryApi:
    """Fake v2 query API whose ``query_stream`` replays canned Flux rows."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def query_stream(self, **_kwargs):  # mirrors client signature
        for values in self._rows:
            yield _StreamingFluxRecord(values)


class _StreamingV2Client:
    """Fake influxdb_client.InfluxDBClient that streams canned Flux rows."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.closed = False

    def query_api(self) -> _StreamingQueryApi:
        return _StreamingQueryApi(self._rows)

    def ping(self) -> bool:  # supports the real _preflight if it is not patched
        return True

    def close(self) -> None:
        self.closed = True


class _BatchRecordingV3Client:
    """Fake v3 client recording every ``write(record=...)`` batch it receives."""

    def __init__(self) -> None:
        self.batches: list[list] = []

    def query(self, **_kwargs):  # supports the real _preflight if not patched
        return None

    def write(self, *, record, write_precision=None):
        # ``record`` is a list of Point objects for one batch; capture a copy so
        # later batches can't mutate what we recorded.
        self.batches.append(list(record))


def _batching_settings(batch_size: int) -> Settings:
    """Build Settings with all V2_* source values set and the given batch size."""
    return Settings(
        influx3_host="http://localhost:8181",
        influx3_token="v3-token",
        influx3_database="ge",
        v2_url="http://localhost:8086",
        v2_token="v2-token",
        v2_org="ge-org",
        v2_bucket="ge-bucket",
        batch_size=batch_size,
    )


def _pivoted_flux_row(item_id: str, when: datetime, values: dict) -> dict:
    """Assemble a pivoted Flux row mapping for the given fields."""
    row: dict = {
        _COL_MEASUREMENT: MEASUREMENT,
        _TAG_ITEM_ID: item_id,
        _COL_TIME: when,
    }
    row.update(values)
    return row


@st.composite
def _source_dataset(draw: st.DrawFn) -> tuple[list[dict], list[dict]]:
    """Draw a v2 source dataset plus the records the migration should yield.

    Returns ``(rows, expected_records)`` where ``rows`` are the pivoted Flux
    rows the fake v2 client streams and ``expected_records`` are the v3 record
    dicts ``iter_v2_records`` will yield for them (rows with no non-null field
    are dropped, mirroring the transform).
    """
    n = draw(st.integers(min_value=0, max_value=25))
    rows: list[dict] = []
    expected: list[dict] = []
    for _ in range(n):
        item_id = draw(_ITEM_IDS)
        when = draw(_TIMES)
        field_values: dict = {}
        for name in PRICE_FIELDS:
            presence = draw(st.sampled_from(["value", "null", "absent"]))
            if presence == "value":
                field_values[name] = draw(_FIELD_VALUES)
            elif presence == "null":
                field_values[name] = None
            # "absent": omit the column entirely.
        rows.append(_pivoted_flux_row(item_id, when, field_values))

        non_null = {k: v for k, v in field_values.items() if v is not None}
        if non_null:
            expected.append(
                {
                    "measurement": MEASUREMENT,
                    "tags": {"itemID": str(item_id)},
                    "time": int(when.timestamp()),
                    "fields": non_null,
                }
            )
    return rows, expected


# Suppress the ``function_scoped_fixture`` health check for the two full
# ``run_migration`` property tests. They intentionally apply their fakes with
# ``mock.patch``/``ExitStack`` *inside* each example (not via a function-scoped
# fixture), so patching is reset per generated example. Depending on collection
# order Hypothesis can still raise ``FailedHealthCheck: function_scoped_fixture``
# for these ``@given`` tests; this guard keeps them deterministic without
# altering the properties. The other settings mirror the session ``ge`` profile
# (>=100 examples, no deadline) so the merge does not weaken coverage.
_MIGRATION_PROPERTY_SETTINGS = settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[
        HealthCheck.too_slow,
        HealthCheck.function_scoped_fixture,
    ],
)


@_MIGRATION_PROPERTY_SETTINGS
@given(dataset=_source_dataset(), batch_size=st.integers(min_value=1, max_value=10))
def test_migration_batching_partitions_records(
    dataset: tuple[list[dict], list[dict]],
    batch_size: int,
) -> None:
    """Batches never exceed batch_size and together cover every record once.

    **Feature: influxdb-v3-migration, Property 13: Migration batching partitions
    the records**

    **Validates: Requirements 5.3**
    """
    rows, expected_records = dataset
    settings = _batching_settings(batch_size)

    v2_client = _StreamingV2Client(rows)
    v3_client = _BatchRecordingV3Client()

    # Capture each batch's record dicts as run_migration hands them off, and
    # count them like the real counting write does. This keeps run_migration's
    # own batching/flush logic under test while giving us the exact records per
    # batch (Point objects would not round-trip cleanly back to record dicts).
    written_batches: list[list[dict]] = []

    def _fake_write_batch_counting(_client, _database, records):
        written_batches.append(list(records))
        return len(records)

    with ExitStack() as patches:
        patches.enter_context(
            mock.patch.object(migrate, "InfluxDBClient", lambda **_kw: v2_client)
        )
        patches.enter_context(
            mock.patch.object(migrate.influx, "get_client", lambda _s: v3_client)
        )
        patches.enter_context(
            mock.patch.object(migrate, "_preflight", lambda *a, **k: None)
        )
        patches.enter_context(
            mock.patch.object(
                migrate, "_write_batch_counting", _fake_write_batch_counting
            )
        )
        migrate.run_migration(settings)

    # No batch exceeds the configured size (an empty final flush is allowed).
    for batch in written_batches:
        assert len(batch) <= batch_size

    # Non-empty batches must be exactly full except possibly the last one, so
    # the partition is a genuine chunking rather than arbitrary splitting.
    non_empty = [b for b in written_batches if b]
    for batch in non_empty[:-1]:
        assert len(batch) == batch_size

    # Concatenation of all batches is the source records in order, each exactly
    # once: no loss, no duplication.
    flattened = [record for batch in written_batches for record in batch]
    assert flattened == expected_records


@_MIGRATION_PROPERTY_SETTINGS
@given(dataset=_source_dataset(), batch_size=st.integers(min_value=1, max_value=10))
def test_migration_counts_are_accurate(
    dataset: tuple[list[dict], list[dict]],
    batch_size: int,
) -> None:
    """Reported read count == records read; written count == records persisted.

    With every batch written successfully, ``records_read`` equals the number of
    non-null source snapshots streamed and ``records_written`` equals the number
    actually persisted through the v3 client.

    **Feature: influxdb-v3-migration, Property 14: Migration counts are
    accurate**

    **Validates: Requirements 5.5**
    """
    rows, expected_records = dataset
    settings = _batching_settings(batch_size)

    v2_client = _StreamingV2Client(rows)
    v3_client = _BatchRecordingV3Client()

    with ExitStack() as patches:
        patches.enter_context(
            mock.patch.object(migrate, "InfluxDBClient", lambda **_kw: v2_client)
        )
        patches.enter_context(
            mock.patch.object(migrate.influx, "get_client", lambda _s: v3_client)
        )
        patches.enter_context(
            mock.patch.object(migrate, "_preflight", lambda *a, **k: None)
        )
        result = migrate.run_migration(settings)

    # The number of records persisted, tallied straight from the v3 client's
    # recorded write calls (independent of run_migration's own counter).
    persisted = sum(len(batch) for batch in v3_client.batches)

    assert result.records_read == len(expected_records)
    assert result.records_written == persisted
    assert result.records_written == len(expected_records)

    # The v2 source client is always closed when the migration finishes.
    assert v2_client.closed is True
