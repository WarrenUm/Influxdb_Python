"""Migration transform-preservation exercised end-to-end into a live server.

The query/write paths (Properties 5-11) are already exercised against the live
InfluxDB 3 Core container in ``tests/test_influx_integration.py``. This module
fills the remaining gap for spec task 11.2: the **migration transform** copied
end-to-end into the *live* v3 container (Requirement 10.2 -- "verify that the
Migration_Tool copies V2_Source records into V3_Target 1:1, preserving the
``itemID`` tag, the four field values, the field and measurement names, and the
record timestamp without renaming").

Spinning up a real InfluxDB v2 server just to migrate from it is heavy and
brittle, so the *read* side of the migration is faked while the *write* side is
the real v3 write path landing in the shared live container:

* :func:`test_migration_transform_preserves_into_live_container` takes a set of
  representative v2-shaped *pivoted Flux rows* (measurement ``itemPrice``,
  ``itemID`` tag, a timezone-aware ``_time`` datetime, the four camelCase
  fields, plus some ``None`` fields), runs them through the real migration
  transform :func:`ge_pipeline.migrate._flux_record_to_record`, writes the
  resulting v3 records into the live container via
  :func:`ge_pipeline.influx.write_batch`, and reads them back through every
  Query_Path helper (:func:`~ge_pipeline.influx.query_price_series`,
  :func:`~ge_pipeline.influx.query_chunk`,
  :func:`~ge_pipeline.influx.get_latest_timestamp`,
  :func:`~ge_pipeline.influx.list_item_ids`), asserting the migrated data
  preserved the ``itemID``, the four field values, and the timestamps 1:1.

* :func:`test_run_migration_end_to_end_into_live_container` drives the full
  :func:`ge_pipeline.migrate.run_migration` path with **only** the v2 read side
  faked (a fake v2 client whose ``query_api().query_stream()`` yields the Flux
  rows and whose ``ping()`` is truthy), while the real cached v3 client bound to
  the container performs the writes. It then reads the data back out of the live
  container and asserts the reported ``records_read``/``records_written`` counts
  and that the migrated snapshots survived 1:1.

Both consume the shared session-scoped ``influx3_container`` /
``influx3_client`` fixtures from ``tests/conftest.py`` and skip cleanly (via the
module-level guard) when ``testcontainers`` / Docker is unavailable.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from ge_pipeline import influx, migrate

# --- Optional dependency / runtime guards ---------------------------------
#
# Skip the whole module when the container runtime (Docker) or testcontainers
# is unavailable, mirroring ``tests/test_influx_integration.py`` so the suite
# stays green on machines without a container runtime. The live container
# itself is provided by the shared fixtures in ``tests/conftest.py``.
try:  # pragma: no cover - exercised only by presence/absence of the dep
    from testcontainers.core.container import DockerContainer  # noqa: F401
except Exception as exc:  # noqa: BLE001 - any import/runtime failure means skip # pragma: no cover
    pytest.skip(
        f"testcontainers is unavailable ({exc}); skipping InfluxDB 3 "
        "migration integration tests",
        allow_module_level=True,
    )


def _unique_item_id() -> str:
    """Return an item id unique per test so runs never collide in the table."""
    return f"mig-{uuid.uuid4().hex}"


def _flux_row(
    item_id: str,
    ts: int,
    *,
    avg_high=None,
    avg_low=None,
    high_vol=None,
    low_vol=None,
) -> dict:
    """Build a representative pivoted v2 Flux row (column name -> value).

    Mirrors what ``FluxRecord.values`` carries for a pivoted v2 query: the
    ``_measurement``/``itemID``/``_time`` columns plus the four camelCase field
    columns. ``_time`` is a timezone-aware ``datetime`` exactly as the v2 client
    returns it, so the migration transform's datetime -> unix-seconds coercion
    is exercised on realistic input. Unset field arguments stay ``None`` so the
    row carries null fields the transform must drop.
    """
    return {
        "_measurement": influx.MEASUREMENT,
        "itemID": item_id,
        "_time": datetime.fromtimestamp(ts, tz=timezone.utc),
        "avgHighPrice": avg_high,
        "avgLowPrice": avg_low,
        "highPriceVolume": high_vol,
        "lowPriceVolume": low_vol,
    }


def _expected_fields(row: dict) -> dict:
    """Return the non-null price fields the transform should preserve for a row."""
    return {
        name: row[name]
        for name in influx.PRICE_FIELDS
        if row.get(name) is not None
    }


# --- Migration transform preservation into the live container -------------


def test_migration_transform_preserves_into_live_container(influx3_client):
    """Migrated v2 rows land in the live v3 container preserving names/values 1:1.

    **Feature: influxdb-v3-migration, Task 11.2 (migration transform
    preservation)** -- representative v2 pivoted Flux rows are converted by the
    real migration transform (:func:`ge_pipeline.migrate._flux_record_to_record`),
    written into the live InfluxDB 3 Core container via the v3 Write_Path, and
    read back through every Query_Path helper, verifying the ``itemID`` tag, the
    four camelCase field values, the measurement name, and the record timestamps
    survive the migration 1:1 with no rename or transform.

    **Validates: Requirements 10.1, 10.2, 10.3**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    # Two distinct items, isolated per test run so the shared table's other
    # rows cannot bleed into these assertions.
    item_a = _unique_item_id()
    item_b = _unique_item_id()

    base = 1_600_000_000  # already 5-minute aligned

    # Representative v2 rows: all-fields, partial-fields (some null), and a
    # fully-null row that the transform must skip entirely.
    flux_rows = [
        _flux_row(item_a, base, avg_high=180, avg_low=176, high_vol=1200, low_vol=980),
        _flux_row(item_a, base + 300, avg_high=181, avg_low=0, high_vol=0, low_vol=5),
        # Partial: only two of the four fields are present; the other two are
        # null and must not appear in the migrated record.
        _flux_row(item_b, base, avg_high=42, low_vol=7),
        _flux_row(item_b, base + 600, avg_low=99, high_vol=3),
        # Fully-null row: the transform returns None and it is never written.
        _flux_row(item_b, base + 900),
    ]

    # Run every row through the REAL migration transform, exactly as
    # iter_v2_records does, dropping the fully-null row.
    records = [
        rec
        for rec in (migrate._flux_record_to_record(row) for row in flux_rows)
        if rec is not None
    ]

    # The fully-null row was dropped; every surviving row kept its measurement.
    assert len(records) == 4
    assert all(rec["measurement"] == influx.MEASUREMENT for rec in records)

    # Write the migrated records into the live container via the v3 Write_Path.
    influx.write_batch(client, database, records)

    # Expected round-trip view keyed by (itemID, unix-second time), holding only
    # the non-null fields the transform preserved.
    expected: dict[tuple[str, int], dict] = {}
    for row in flux_rows:
        fields = _expected_fields(row)
        if not fields:
            continue  # fully-null row was skipped by the transform
        ts = int(row["_time"].timestamp())
        expected[(row["itemID"], ts)] = fields

    start = base
    stop = base + 1_000  # exclusive upper bound past the max timestamp

    # 1) query_price_series: per-item series preserves timestamps + field values.
    actual: dict[tuple[str, int], dict] = {}
    for item_id in (item_a, item_b):
        series = influx.query_price_series(
            client, item_id, start, stop, database=database
        )
        for point in series:
            actual[(item_id, int(point["time"]))] = point

    assert set(actual) == set(expected), (
        "migrated (itemID, time) set read back differs from what was migrated"
    )
    for key, fields in expected.items():
        point = actual[key]
        for name in influx.PRICE_FIELDS:
            if name in fields:
                assert point[name] == fields[name], (
                    f"migrated field {name} for {key} read back as "
                    f"{point[name]!r}, expected {fields[name]!r}"
                )
            else:
                # A field the source row lacked round-trips as null (the
                # migration never invented a value for it).
                assert point[name] is None, (
                    f"field {name} for {key} was null at source but read back "
                    f"as {point[name]!r}"
                )

    # 2) get_latest_timestamp: max stored timestamp per migrated item.
    assert (
        influx.get_latest_timestamp(client, item_a, database=database)
        == base + 300
    )
    assert (
        influx.get_latest_timestamp(client, item_b, database=database)
        == base + 600
    )

    # 3) list_item_ids: both migrated items appear in the distinct listing.
    listed = set(influx.list_item_ids(client, database=database))
    assert {item_a, item_b} <= listed

    # 4) query_chunk: one row per migrated (itemID, time) with a column per field.
    frame = influx.query_chunk(
        client, [item_a, item_b], start, stop, database=database
    )
    assert not frame.empty
    for name in influx.PRICE_FIELDS:
        assert name in frame.columns
    chunk_keys = [
        (str(rec["itemID"]), influx._time_to_unix_seconds(rec["time"]))
        for rec in frame.to_dict(orient="records")
    ]
    assert set(chunk_keys) == set(expected)
    assert len(chunk_keys) == len(set(chunk_keys))  # one row per (item, time)


# --- Fakes for the v2 read side of run_migration --------------------------


class _FakeFluxRecord:
    """Minimal stand-in for ``influxdb_client`` ``FluxRecord``.

    Only ``.values`` is consumed by :func:`ge_pipeline.migrate.iter_v2_records`.
    """

    def __init__(self, values: dict) -> None:
        self.values = values


class _FakeQueryApi:
    """Fake v2 query API yielding pre-built pivoted Flux records."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.query_stream_calls: list[dict] = []

    def query_stream(self, **kwargs):
        # Record the call so the test can assert the reader was actually driven.
        self.query_stream_calls.append(kwargs)
        for row in self._rows:
            yield _FakeFluxRecord(row)


class _FakeV2Client:
    """Fake v2 client: reachable (``ping`` truthy) and streams the given rows.

    Substitutes for :class:`influxdb_client.InfluxDBClient` so ``run_migration``
    can exercise its full read->write pass without a live v2 server, while the
    v3 write side lands in the real container.
    """

    def __init__(self, rows: list[dict]) -> None:
        self._api = _FakeQueryApi(rows)
        self.closed = False

    def ping(self) -> bool:
        return True

    def query_api(self) -> _FakeQueryApi:
        return self._api

    def close(self) -> None:
        self.closed = True


def test_run_migration_end_to_end_into_live_container(influx3_client, monkeypatch):
    """``run_migration`` streams faked v2 rows into the live container 1:1.

    **Feature: influxdb-v3-migration, Task 11.2 (migration end-to-end)** --
    drives the full :func:`ge_pipeline.migrate.run_migration` path with only the
    v2 read side faked (a fake v2 client whose ``query_api().query_stream()``
    yields pivoted Flux rows and whose ``ping()`` is truthy) while the real
    cached v3 client bound to the container performs the writes. Reads the data
    back out of the live container and asserts the reported
    ``records_read``/``records_written`` counts and that every migrated snapshot
    survived 1:1 (``itemID``, four field values, timestamps).

    **Validates: Requirements 10.1, 10.2, 10.3**
    """
    client, settings_obj = influx3_client
    database = settings_obj.influx3_database

    item_a = _unique_item_id()
    item_b = _unique_item_id()
    base = 1_600_500_000  # aligned, distinct window from the transform test

    flux_rows = [
        _flux_row(item_a, base, avg_high=10, avg_low=8, high_vol=100, low_vol=90),
        _flux_row(item_a, base + 300, avg_high=11, avg_low=9, high_vol=101, low_vol=91),
        _flux_row(item_a, base + 600, avg_high=12, avg_low=10, high_vol=102, low_vol=92),
        # Partial fields on item_b (two null) exercise null-dropping mid-stream.
        _flux_row(item_b, base, avg_high=500, high_vol=7),
        _flux_row(item_b, base + 300, avg_low=499, low_vol=6),
        # Fully-null row: skipped by the transform, so it is neither read-counted
        # (it is filtered inside iter_v2_records) nor written.
        _flux_row(item_b, base + 900),
    ]

    # Records the migration should actually read+write: the four non-null rows
    # (fully-null row is filtered by iter_v2_records before it is counted).
    expected: dict[tuple[str, int], dict] = {}
    for row in flux_rows:
        fields = _expected_fields(row)
        if not fields:
            continue
        expected[(row["itemID"], int(row["_time"].timestamp()))] = fields
    expected_count = len(expected)
    assert expected_count == 5

    fake_v2 = _FakeV2Client(flux_rows)

    # Provide validated v2 source settings without requiring real env vars, and
    # substitute the fake v2 client for the real InfluxDBClient constructor.
    from ge_pipeline.config import MigrationSource

    monkeypatch.setattr(
        migrate,
        "require_migration_source",
        lambda settings: MigrationSource(
            url="http://fake-v2:8086",
            token="v2-token",
            org="v2-org",
            bucket="v2-bucket",
        ),
    )
    # run_migration constructs the v2 client via ``migrate.InfluxDBClient(...)``.
    monkeypatch.setattr(migrate, "InfluxDBClient", lambda **kwargs: fake_v2)
    # Bind run_migration's v3 client to the live container's cached client.
    monkeypatch.setattr(influx, "get_client", lambda settings: client)

    # Use a small batch size so batching + final-flush are both exercised.
    from dataclasses import replace

    migration_settings = replace(settings_obj, batch_size=2)

    result = migrate.run_migration(migration_settings)

    # Counts: every non-null source row was read and written 1:1 (Req 5.5),
    # and the v2 client was driven and then closed.
    assert result.records_read == expected_count
    assert result.records_written == expected_count
    assert fake_v2.closed is True
    assert fake_v2.query_api().query_stream_calls, "v2 reader was never driven"

    # Read the migrated data back out of the live container and confirm 1:1
    # preservation of itemID, the four field values, and the timestamps.
    start = base
    stop = base + 1_000
    actual: dict[tuple[str, int], dict] = {}
    for item_id in (item_a, item_b):
        for point in influx.query_price_series(
            client, item_id, start, stop, database=database
        ):
            actual[(item_id, int(point["time"]))] = point

    assert set(actual) == set(expected), (
        "migrated (itemID, time) set read back differs from the source rows"
    )
    for key, fields in expected.items():
        point = actual[key]
        for name in influx.PRICE_FIELDS:
            if name in fields:
                assert point[name] == fields[name], (
                    f"migrated field {name} for {key} read back as "
                    f"{point[name]!r}, expected {fields[name]!r}"
                )
            else:
                assert point[name] is None

    # Latest-timestamp per migrated item matches the max source timestamp.
    assert (
        influx.get_latest_timestamp(client, item_a, database=database)
        == base + 600
    )
    assert (
        influx.get_latest_timestamp(client, item_b, database=database)
        == base + 300
    )
