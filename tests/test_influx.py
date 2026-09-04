"""Unit tests for the influx service client lifecycle and batched writes.

Covers :func:`ge_pipeline.influx.get_client` client reuse and
:func:`ge_pipeline.influx.write_batch` seconds-precision writes plus the
retry-then-drop behavior on transient write failures. A mocked InfluxDB client
is used so no real database is required.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest
from influxdb_client.rest import ApiException

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
    """Return Settings with dummy connection values for client construction."""
    return Settings(
        influx_url="http://localhost:8086",
        influx_token="test-token",
        influx_org="test-org",
        influx_bucket="GEItemPrices",
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


def test_get_client_reuses_single_instance(monkeypatch, settings):
    """get_client returns the identical cached client for the same settings."""
    made: list[MagicMock] = []

    def fake_ctor(**kwargs):
        client = MagicMock(name="InfluxDBClient")
        made.append(client)
        return client

    monkeypatch.setattr(influx, "InfluxDBClient", fake_ctor)

    first = influx.get_client(settings)
    second = influx.get_client(settings)

    assert first is second
    assert len(made) == 1


def test_write_batch_writes_with_seconds_precision(settings):
    """write_batch writes through the client with seconds precision."""
    client = MagicMock()
    write_api = client.write_api.return_value

    influx.write_batch(client, settings.influx_bucket, _sample_records())

    client.write_api.assert_called_once()
    write_api.write.assert_called_once()
    _, kwargs = write_api.write.call_args
    assert kwargs["bucket"] == settings.influx_bucket
    assert kwargs["write_precision"] == "s"
    assert kwargs["record"] == _sample_records()


def test_write_batch_empty_records_is_noop(settings):
    """An empty batch performs no write."""
    client = MagicMock()
    influx.write_batch(client, settings.influx_bucket, [])
    client.write_api.assert_not_called()


def test_write_batch_retries_then_drops_on_persistent_failure(monkeypatch, settings):
    """Persistent transient write failures are retried then dropped, not raised."""
    # Make retries instantaneous by patching the retry helper's sleep is not
    # exposed here; instead configure the write_api to always raise ApiException.
    client = MagicMock()
    write_api = client.write_api.return_value
    write_api.write.side_effect = ApiException(status=503, reason="unavailable")

    # Patch time.sleep used inside retry to keep the test fast.
    monkeypatch.setattr("ge_pipeline.retry.time.sleep", lambda _seconds: None)

    # Should NOT raise: batch is dropped after retries are exhausted.
    influx.write_batch(client, settings.influx_bucket, _sample_records())

    # Default max_attempts is 5, so write is attempted 5 times.
    assert write_api.write.call_count == 5


def test_write_batch_succeeds_after_transient_then_recovery(monkeypatch, settings):
    """A transient failure followed by success writes without dropping."""
    client = MagicMock()
    write_api = client.write_api.return_value
    write_api.write.side_effect = [OSError("boom"), None]

    monkeypatch.setattr("ge_pipeline.retry.time.sleep", lambda _seconds: None)

    influx.write_batch(client, settings.influx_bucket, _sample_records())

    assert write_api.write.call_count == 2


# --- Query helpers: fakes for a mocked query API --------------------------


def _make_record(*, time=None, values=None, value=None):
    """Build a fake InfluxDB record exposing get_time/values/get_value."""
    record = MagicMock(name="FluxRecord")
    record.get_time.return_value = time
    record.values = values if values is not None else {}
    record.get_value.return_value = value
    return record


def _make_table(records):
    """Build a fake InfluxDB table wrapping the given records."""
    table = MagicMock(name="FluxTable")
    table.records = list(records)
    return table


def _client_with_query(tables):
    """Return a mocked client whose query_api().query(...) returns tables.

    The returned client records the ``query`` and ``params`` kwargs it was
    called with on ``client._captured`` so tests can assert on them.
    """
    client = MagicMock(name="InfluxDBClient")
    query_api = client.query_api.return_value
    captured: dict = {}

    def _query(*, query, params):
        captured["query"] = query
        captured["params"] = params
        return tables

    query_api.query.side_effect = _query
    client._captured = captured
    return client


def _assert_not_in_query(query_text: str, forbidden) -> None:
    """Assert none of the forbidden user values appear in the query text."""
    for value in forbidden:
        assert str(value) not in query_text, (
            f"user value {value!r} was interpolated into the query string"
        )


# --- Parameterization (Req 9.1) -------------------------------------------


def test_query_price_series_binds_user_values_as_params_not_query_text():
    """User item_id/start/stop appear only in params, never in query text."""
    client = _client_with_query([])
    item_id = "9999999"  # distinctive value unlikely to collide with Flux text
    start = 1_600_000_111
    stop = 1_600_009_222

    influx.query_price_series(
        client, item_id, start, stop, bucket="my-secret-bucket-42"
    )

    captured = client._captured
    query_text = captured["query"]
    params = captured["params"]

    # User values must be carried in params, not interpolated into the query.
    _assert_not_in_query(query_text, [item_id, start, stop, "my-secret-bucket-42"])
    assert params["_itemID"] == item_id
    assert params["_bucket"] == "my-secret-bucket-42"
    assert params["_start"] == datetime.fromtimestamp(start, tz=timezone.utc)
    assert params["_stop"] == datetime.fromtimestamp(stop, tz=timezone.utc)


def test_query_chunk_binds_item_ids_as_params_not_query_text():
    """Multi-item IDs are bound in params, never substringed into the query."""
    client = MagicMock(name="InfluxDBClient")
    query_api = client.query_api.return_value
    captured: dict = {}

    def _query_df(*, query, params):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    query_api.query_data_frame.side_effect = _query_df

    item_ids = ["1234567", "7654321"]
    start = 1_611_111_000
    stop = 1_611_222_000

    influx.query_chunk(client, item_ids, start, stop, bucket="chunk-bucket-77")

    query_text = captured["query"]
    params = captured["params"]

    _assert_not_in_query(
        query_text, item_ids + [start, stop, "chunk-bucket-77"]
    )
    assert params["_itemIDs"] == item_ids
    assert params["_bucket"] == "chunk-bucket-77"
    assert params["_start"] == datetime.fromtimestamp(start, tz=timezone.utc)
    assert params["_stop"] == datetime.fromtimestamp(stop, tz=timezone.utc)


# --- aggregateWindow application (Req 9.4) --------------------------------


def test_query_price_series_applies_aggregate_window_when_interval_given():
    """aggregateWindow appears and _interval is bound when interval is set."""
    client = _client_with_query([])

    influx.query_price_series(
        client, "554", 1_600_000_000, 1_600_100_000, interval="5m"
    )

    captured = client._captured
    assert "aggregateWindow" in captured["query"]
    assert "_interval" in captured["query"]
    assert "_interval" in captured["params"]


def test_query_price_series_omits_aggregate_window_when_interval_none():
    """aggregateWindow is absent and _interval unbound when interval is None."""
    client = _client_with_query([])

    influx.query_price_series(client, "554", 1_600_000_000, 1_600_100_000)

    captured = client._captured
    assert "aggregateWindow" not in captured["query"]
    assert "_interval" not in captured["params"]


# --- get_latest_timestamp None-on-empty (Req 9.5) -------------------------


def test_get_latest_timestamp_returns_none_when_no_records():
    """No tables/records yields None."""
    client = _client_with_query([])

    result = influx.get_latest_timestamp(client, "554")

    assert result is None


def test_get_latest_timestamp_returns_unix_seconds_for_record():
    """A record with a time yields its integer unix-seconds timestamp."""
    moment = datetime(2021, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
    table = _make_table([_make_record(time=moment)])
    client = _client_with_query([table])

    result = influx.get_latest_timestamp(client, "554")

    assert result == int(moment.timestamp())


def test_get_latest_timestamp_binds_item_id_as_param():
    """The item_id is bound as a param, not interpolated into the query."""
    client = _client_with_query([])
    item_id = "8675309"

    influx.get_latest_timestamp(client, item_id, bucket="ts-bucket-13")

    captured = client._captured
    _assert_not_in_query(captured["query"], [item_id, "ts-bucket-13"])
    assert captured["params"]["_itemID"] == item_id
    assert captured["params"]["_bucket"] == "ts-bucket-13"
