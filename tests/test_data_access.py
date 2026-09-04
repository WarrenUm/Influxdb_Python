"""Unit tests for the data access layer's feature frame and export formats.

Covers:

* ``build_feature_frame`` shaping (time-indexed, per-item columns) for both
  long-form and wide inputs from ``influx.query_chunk`` (Task 9.8).
* ``stream_dataset`` rejecting unsupported export formats with a descriptive
  ``ValueError`` (Task 9.8; the encoder itself lands in Task 9.3).

**Validates: Requirements 11.4, 11.5, 21.1**
"""

from __future__ import annotations

import pandas as pd
import pytest

from ge_pipeline import data_access
from ge_pipeline.data_access import build_feature_frame

# The three export formats the streaming encoder supports.
_SUPPORTED_FORMATS = {"ndjson", "csv", "parquet"}


def test_build_feature_frame_pivots_long_form(monkeypatch) -> None:
    """A long-form query result is pivoted to a time-indexed, per-item frame."""
    long_frame = pd.DataFrame(
        {
            "_time": [100, 100, 200, 200],
            "itemID": ["554", "555", "554", "555"],
            "avgHighPrice": [10, 20, 11, 21],
            "avgLowPrice": [5, 15, 6, 16],
        }
    )

    def _fake_query_chunk(client, item_ids, start, stop, interval):
        return long_frame

    monkeypatch.setattr(data_access.influx, "query_chunk", _fake_query_chunk)

    frame = build_feature_frame(
        client=None,
        item_ids=["554", "555"],
        start=0,
        stop=1_000,
        interval="1h",
    )

    # Time-indexed and sorted ascending by time.
    assert frame.index.name == "time"
    assert list(frame.index) == [100, 200]

    # Two items x two fields => four per-item columns.
    assert frame.shape == (2, 4)
    for item in ("554", "555"):
        for field in ("avgHighPrice", "avgLowPrice"):
            assert f"{item}_{field}" in frame.columns

    # Values land in the right per-item column.
    assert frame.loc[100, "554_avgHighPrice"] == 10
    assert frame.loc[200, "555_avgLowPrice"] == 16


def test_build_feature_frame_indexes_wide_form(monkeypatch) -> None:
    """A wide query result (no item column) is simply time-indexed."""
    wide_frame = pd.DataFrame(
        {
            "_time": [300, 100, 200],
            "avgHighPrice": [3, 1, 2],
            "avgLowPrice": [30, 10, 20],
        }
    )

    def _fake_query_chunk(client, item_ids, start, stop, interval):
        return wide_frame

    monkeypatch.setattr(data_access.influx, "query_chunk", _fake_query_chunk)

    frame = build_feature_frame(
        client=None,
        item_ids=["554"],
        start=0,
        stop=1_000,
        interval="1h",
    )

    assert frame.index.name == "time"
    # Sorted ascending by time regardless of input order.
    assert list(frame.index) == [100, 200, 300]
    assert list(frame["avgHighPrice"]) == [1, 2, 3]


def test_build_feature_frame_empty_query_returns_empty(monkeypatch) -> None:
    """An empty query result yields an empty frame without raising."""

    def _fake_query_chunk(client, item_ids, start, stop, interval):
        return pd.DataFrame()

    monkeypatch.setattr(data_access.influx, "query_chunk", _fake_query_chunk)

    frame = build_feature_frame(
        client=None,
        item_ids=["554"],
        start=0,
        stop=1_000,
        interval="1h",
    )

    assert isinstance(frame, pd.DataFrame)
    assert frame.empty


def test_stream_dataset_rejects_unsupported_format(monkeypatch) -> None:
    """An unsupported export format raises a descriptive ``ValueError``.

    ``stream_dataset`` is a generator, so the rejection is asserted by draining
    it. ``query_chunk`` is patched to a no-op so any pre-yield read succeeds and
    the failure is specifically the format check.
    """
    stream_dataset = getattr(data_access, "stream_dataset", None)
    if stream_dataset is None:
        pytest.skip("stream_dataset not implemented yet (spec task 9.3)")

    def _fake_query_chunk(client, item_ids, start, stop, interval):
        return pd.DataFrame()

    monkeypatch.setattr(data_access.influx, "query_chunk", _fake_query_chunk)

    with pytest.raises(ValueError) as exc_info:
        # Draining the generator triggers the format validation.
        list(
            stream_dataset(
                client=None,
                item_ids=["554"],
                start=0,
                stop=1_000,
                interval="1h",
                fmt="xml",
            )
        )

    message = str(exc_info.value)
    # Descriptive: names the offending format and/or the supported set.
    assert "xml" in message or any(f in message for f in _SUPPORTED_FORMATS)


def test_stream_dataset_handles_timestamp_time_column(monkeypatch) -> None:
    """``stream_dataset`` streams when ``query_chunk`` returns pandas Timestamps.

    Real InfluxDB reads populate the ``_time`` column with
    :class:`pandas.Timestamp` values rather than plain ``int`` unix seconds
    (as the in-memory fakes use). The per-item ordering check must coerce those
    Timestamps instead of raising ``TypeError`` on ``int(timestamp)``.
    """
    stream_dataset = data_access.stream_dataset

    frame = pd.DataFrame(
        {
            "_time": pd.to_datetime(
                [1_600_000_000, 1_600_000_300, 1_600_000_600], unit="s", utc=True
            ),
            "itemID": ["554", "554", "554"],
            "avgHighPrice": [10, 11, 12],
        }
    )

    def _fake_query_chunk(client, item_ids, start, stop, interval):
        # Return the whole frame for the first window, empty afterwards, so the
        # ascending-per-item check runs over Timestamp values.
        if start <= 1_600_000_000 < stop:
            return frame
        return pd.DataFrame()

    monkeypatch.setattr(data_access.influx, "query_chunk", _fake_query_chunk)

    payload = b"".join(
        stream_dataset(
            client=None,
            item_ids=["554"],
            start=1_600_000_000,
            stop=1_600_000_000 + data_access.STREAM_CHUNK_SECONDS,
            interval="5m",
            fmt="ndjson",
        )
    )

    # Every row survives the stream without a Timestamp coercion error.
    lines = [line for line in payload.decode("utf-8").splitlines() if line]
    assert len(lines) == 3


def test_to_unix_seconds_coerces_common_time_types() -> None:
    """The timestamp coercion helper accepts ints, floats, datetimes, Timestamps."""
    import datetime as dt

    assert data_access._to_unix_seconds(1_600_000_000) == 1_600_000_000
    assert data_access._to_unix_seconds(1_600_000_000.9) == 1_600_000_000
    assert (
        data_access._to_unix_seconds(pd.Timestamp(1_600_000_000, unit="s", tz="UTC"))
        == 1_600_000_000
    )
    assert (
        data_access._to_unix_seconds(dt.datetime(2020, 9, 13, 12, 26, 40, tzinfo=dt.timezone.utc))
        == 1_600_000_000
    )
