"""Unit tests for the InfluxDB 3 storage seam (write path + query shaping).

These tests exercise :mod:`ge_pipeline.influx` against a **mocked**
:class:`~influxdb_client_3.InfluxDBClient3`, so no real database or container is
required. They complement the live-container property tests in
``tests/test_influx_integration.py`` (round-trip, latest-timestamp, listing) and
the caching property test in ``tests/test_influx_property.py`` (Property 1),
neither of which they duplicate. Here the focus is the container-free behavior:

* :func:`ge_pipeline.influx.write_batch` — seconds-precision write, empty-batch
  no-op, and retry-then-drop / recover-after-transient behavior (Requirement 7).
* The query helpers' SQL/parameter construction — user item ids and time bounds
  are bound via ``query_parameters`` rather than interpolated into the query
  text, identifiers are double-quoted camelCase, time bounds use
  ``to_timestamp_seconds(CAST($start AS BIGINT))``, and ``date_bin`` downsampling
  is applied only when an interval is given (Requirement 6).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest
from influxdb_client_3 import Point

from ge_pipeline import influx
from ge_pipeline.config import Settings


@pytest.fixture(autouse=True)
def _clear_client_cache():
    """Ensure the module-level client cache is empty around each test."""
    influx._client_cache.clear()
    yield
    influx._client_cache.clear()


@pytest.fixture
def settings() -> Settings:
    """Return Settings with dummy v3 connection values."""
    return Settings(
        influx3_host="http://localhost:8181",
        influx3_token="test-token",
        influx3_database="ge_test",
    )


def _sample_records() -> list[dict]:
    """Return a minimal batch of well-formed price records."""
    return [
        {
            "measurement": "itemPrice",
            "tags": {"itemID": "554"},
            "time": 1615733100,
            "fields": {"avgHighPrice": 5, "lowPriceVolume": 100},
        }
    ]


# --- get_client ------------------------------------------------------------
#
# Client caching identity is covered as a property in
# ``tests/test_influx_property.py`` (Property 1), so it is not re-tested here.


# --- write_batch (Requirement 7) ------------------------------------------


def test_write_batch_writes_with_seconds_precision(settings):
    """write_batch writes Point records through client.write with 's' precision."""
    client = MagicMock()

    influx.write_batch(client, settings.influx3_database, _sample_records())

    client.write.assert_called_once()
    _, kwargs = client.write.call_args
    assert kwargs["write_precision"] == "s"
    points = kwargs["record"]
    assert isinstance(points, list)
    assert len(points) == 1
    assert all(isinstance(point, Point) for point in points)


def test_write_batch_empty_records_is_noop(settings):
    """An empty batch performs no write."""
    client = MagicMock()
    influx.write_batch(client, settings.influx3_database, [])
    client.write.assert_not_called()


def test_write_batch_retries_then_drops_on_persistent_failure(monkeypatch, settings):
    """Persistent write failures are retried then dropped, not raised."""
    client = MagicMock()
    client.write.side_effect = OSError("connection refused")

    # Patch time.sleep used inside the retry helper to keep the test fast.
    monkeypatch.setattr("ge_pipeline.retry.time.sleep", lambda _seconds: None)

    # Should NOT raise: the batch is dropped after retries are exhausted.
    influx.write_batch(client, settings.influx3_database, _sample_records())

    # Default max_attempts is 5, so write is attempted 5 times.
    assert client.write.call_count == 5


def test_write_batch_succeeds_after_transient_then_recovery(monkeypatch, settings):
    """A transient failure followed by success writes without dropping."""
    client = MagicMock()
    client.write.side_effect = [OSError("boom"), None]

    monkeypatch.setattr("ge_pipeline.retry.time.sleep", lambda _seconds: None)

    influx.write_batch(client, settings.influx3_database, _sample_records())

    assert client.write.call_count == 2


# --- Query helpers: mocked client -----------------------------------------


def _client_with_query(frame: pd.DataFrame | None = None):
    """Return a mocked client whose ``query`` returns ``frame`` and records args.

    The captured ``query``/``language``/``query_parameters`` kwargs are stored
    on ``client._captured`` so tests can assert the SQL text and bound params.
    """
    client = MagicMock(name="InfluxDBClient3")
    captured: dict = {}

    def _query(*, query, language, query_parameters):
        captured["query"] = query
        captured["language"] = language
        captured["params"] = query_parameters
        return pd.DataFrame() if frame is None else frame

    client.query.side_effect = _query
    client._captured = captured
    return client


def _assert_not_in_query(query_text: str, forbidden) -> None:
    """Assert none of the forbidden user values appear in the query text."""
    for value in forbidden:
        assert str(value) not in query_text, (
            f"user value {value!r} was interpolated into the query string"
        )


# --- Parameter binding (Requirement 6.7) ----------------------------------


def test_query_price_series_binds_user_values_as_params_not_query_text():
    """User item_id/start/stop are bound in params, never in the query text."""
    client = _client_with_query()
    item_id = "9999999"
    start = 1_600_000_111
    stop = 1_600_009_222

    influx.query_price_series(client, item_id, start, stop, database="ge_test")

    captured = client._captured
    query_text = captured["query"]
    params = captured["params"]

    # User values must be carried in params, not interpolated into the query.
    _assert_not_in_query(query_text, [item_id, start, stop])
    assert captured["language"] == "sql"
    assert params["item_id"] == item_id
    assert params["start"] == start
    assert params["stop"] == stop
    # Time bounds cast the bound value before conversion (design "Storage seam").
    assert "to_timestamp_seconds(CAST($start AS BIGINT))" in query_text
    assert "to_timestamp_seconds(CAST($stop AS BIGINT))" in query_text
    # camelCase identifiers stay double-quoted for case sensitivity.
    assert '"itemPrice"' in query_text
    assert '"itemID"' in query_text


def test_query_chunk_binds_item_ids_as_params_not_query_text():
    """Multi-item IDs are each bound as $idN params, never substringed in SQL."""
    client = _client_with_query()

    item_ids = ["1234567", "7654321"]
    start = 1_611_111_000
    stop = 1_611_222_000

    influx.query_chunk(client, item_ids, start, stop, database="ge_test")

    captured = client._captured
    query_text = captured["query"]
    params = captured["params"]

    _assert_not_in_query(query_text, item_ids + [start, stop])
    # Each id is bound under its own placeholder ($id0, $id1, ...).
    assert params["id0"] == item_ids[0]
    assert params["id1"] == item_ids[1]
    assert "$id0" in query_text
    assert "$id1" in query_text
    assert params["start"] == start
    assert params["stop"] == stop


def test_query_chunk_empty_item_ids_returns_empty_frame_without_querying():
    """An empty item_ids list short-circuits to an empty frame, no query run."""
    client = _client_with_query()

    frame = influx.query_chunk(client, [], 1, 2, database="ge_test")

    assert isinstance(frame, pd.DataFrame)
    assert frame.empty
    client.query.assert_not_called()


def test_get_latest_timestamp_binds_item_id_as_param():
    """The item_id is bound as a param, not interpolated into the query."""
    client = _client_with_query()
    item_id = "8675309"

    influx.get_latest_timestamp(client, item_id, database="ge_test")

    captured = client._captured
    _assert_not_in_query(captured["query"], [item_id])
    assert captured["params"]["item_id"] == item_id
    assert 'max(time)' in captured["query"]


# --- date_bin downsampling (Requirement 6.4) ------------------------------


def test_query_price_series_applies_date_bin_when_interval_given():
    """date_bin/avg appear when an interval is supplied."""
    client = _client_with_query()

    influx.query_price_series(
        client, "554", 1_600_000_000, 1_600_100_000, interval="5m", database="ge_test"
    )

    query_text = client._captured["query"]
    assert "date_bin" in query_text
    assert "avg(" in query_text
    # 5m -> 300 seconds interval literal.
    assert "INTERVAL '300 seconds'" in query_text


def test_query_price_series_omits_date_bin_when_interval_none():
    """date_bin/avg are absent when no interval is supplied."""
    client = _client_with_query()

    influx.query_price_series(
        client, "554", 1_600_000_000, 1_600_100_000, database="ge_test"
    )

    query_text = client._captured["query"]
    assert "date_bin" not in query_text
    assert "avg(" not in query_text


# --- get_latest_timestamp result shaping (Requirement 6.2) ----------------


def test_get_latest_timestamp_returns_none_when_no_rows():
    """An empty result frame yields None."""
    client = _client_with_query(pd.DataFrame())

    assert influx.get_latest_timestamp(client, "554", database="ge_test") is None


def test_get_latest_timestamp_returns_unix_seconds_for_row():
    """A populated result yields the integer unix-seconds timestamp."""
    latest = pd.Timestamp("2021-03-14T15:09:26Z")
    frame = pd.DataFrame({"latest": [latest]})
    client = _client_with_query(frame)

    result = influx.get_latest_timestamp(client, "554", database="ge_test")

    assert result == int(latest.timestamp())


def test_list_item_ids_returns_distinct_values_from_frame():
    """list_item_ids returns the itemID column values from the result frame."""
    frame = pd.DataFrame({"itemID": ["2", "554", "4151"]})
    client = _client_with_query(frame)

    ids = influx.list_item_ids(client, database="ge_test")

    assert ids == ["2", "554", "4151"]
    query_text = client._captured["query"]
    assert "DISTINCT" in query_text
    assert '"itemID"' in query_text
