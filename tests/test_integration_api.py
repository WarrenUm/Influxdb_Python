"""End-to-end FastAPI integration tests against a seeded in-memory store.

This module exercises the *whole* read/export stack at the HTTP layer -- API
routing, query-parameter validation, guardrails, outlier annotation, cursor
pagination (``data_access.get_price_page``), and chunked dataset streaming
(``data_access.stream_dataset``) -- against realistic, deterministic seeded
data, **without** requiring a real InfluxDB (and therefore without Docker).

Approach
--------
A :class:`FakeStore` is seeded with two items (``"554"`` and ``"565"``), each
carrying 60 five-minute price points anchored just before "now", with a couple
of injected price spikes so the z-score detector flags something. The InfluxDB
query functions the API and data-access layer call
(:func:`ge_pipeline.influx.query_price_series`,
:func:`ge_pipeline.influx.query_chunk`, and
:func:`ge_pipeline.influx.list_item_ids`) are monkeypatched to read from the
store. Because :mod:`ge_pipeline.api` and :mod:`ge_pipeline.data_access` both
reference the *same* ``ge_pipeline.influx`` module object, patching the module
attributes covers every call site. The ``get_influx_client`` dependency is
overridden with a dummy so no real client is ever constructed.

The result is a true end-to-end contract test: requests flow through the real
FastAPI routes and the real ``data_access``/outlier logic, only the storage
read is faked.

**Validates: Requirements 21.4**
"""

from __future__ import annotations

import csv
import io
import json
import time
from typing import Any, Iterator

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from ge_pipeline import api, influx
from ge_pipeline.api import create_app, get_influx_client

# Five minutes, in seconds -- the native cadence of the price store.
_FIVE_MINUTES = 300

# How many points to seed per item.
_POINTS_PER_ITEM = 60

# Item ids to seed.
_ITEM_IDS = ("554", "565")

# Indices at which to inject a dramatic price spike so the z-score detector
# (default threshold=3, window=20) flags the point as an outlier.
_SPIKE_INDICES = (30, 50)

# The value used for an injected spike -- far above the ~100 baseline so it is
# unambiguously flagged regardless of small baseline variation.
_SPIKE_VALUE = 100_000.0

# Fields on the four-field price schema.
_PRICE_FIELDS = (
    "avgHighPrice",
    "avgLowPrice",
    "highPriceVolume",
    "lowPriceVolume",
)


class FakeStore:
    """A deterministic in-memory stand-in for the InfluxDB price store.

    Seeds each item with :data:`_POINTS_PER_ITEM` five-minute points ending just
    before ``now`` so every point falls inside a ``range=7d`` window. A couple of
    injected spikes (see :data:`_SPIKE_INDICES`) guarantee the outlier detector
    has something to flag.

    Attributes:
        now: The anchor "now" (unix seconds) the series is built relative to.
        series: Mapping of item id -> time-ascending list of point dicts.
    """

    def __init__(self, now: int) -> None:
        """Seed the store relative to ``now``.

        Args:
            now: The anchor time (unix seconds). Points are placed strictly
                before this instant at five-minute spacing.
        """
        self.now = now
        self.series: dict[str, list[dict[str, Any]]] = {}
        for item_id in _ITEM_IDS:
            self.series[item_id] = self._seed_item(item_id)

    def _seed_item(self, item_id: str) -> list[dict[str, Any]]:
        """Build the time-ascending seeded point list for one item."""
        points: list[dict[str, Any]] = []
        # Oldest point is (POINTS_PER_ITEM) intervals before now; newest is one
        # interval before now (strictly < now so it is inside a range ending at
        # "now").
        for i in range(_POINTS_PER_ITEM):
            offset = (_POINTS_PER_ITEM - i) * _FIVE_MINUTES
            moment = self.now - offset
            # Small deterministic baseline variation keeps sigma non-zero only
            # around spikes; a per-item bias distinguishes the two series.
            bias = 0 if item_id == "554" else 50
            base_high = 100.0 + bias + float(i % 5) * 2.0
            if i in _SPIKE_INDICES:
                base_high = _SPIKE_VALUE + bias
            points.append(
                {
                    "time": int(moment),
                    "avgHighPrice": base_high,
                    "avgLowPrice": base_high - 5.0,
                    "highPriceVolume": 10 + (i % 7),
                    "lowPriceVolume": 5 + (i % 3),
                }
            )
        return points

    def item_ids(self) -> list[str]:
        """Return the sorted seeded item ids."""
        return sorted(self.series)

    def price_series(
        self, item_id: str, start: int, stop: int
    ) -> list[dict[str, Any]]:
        """Return the seeded points for ``item_id`` within ``[start, stop)``."""
        points = self.series.get(item_id, [])
        return [
            dict(point)
            for point in points
            if start <= point["time"] < stop
        ]

    def chunk_frame(
        self, item_ids: list[str], start: int, stop: int
    ) -> pd.DataFrame:
        """Return a combined multi-item frame for ``[start, stop)``.

        Rows are sorted by item id then time, mirroring the shape
        :func:`ge_pipeline.influx.query_chunk` produces (a long-form frame with
        an ``itemID`` column and a ``_time`` column).
        """
        rows: list[dict[str, Any]] = []
        for item_id in item_ids:
            for point in self.price_series(item_id, start, stop):
                row = {"itemID": item_id, "_time": point["time"]}
                for field in _PRICE_FIELDS:
                    row[field] = point[field]
                rows.append(row)
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        return frame.sort_values(["itemID", "_time"]).reset_index(drop=True)


@pytest.fixture
def store() -> FakeStore:
    """Provide a deterministic seeded store anchored at the current time."""
    return FakeStore(now=int(time.time()))


@pytest.fixture
def client(monkeypatch, store: FakeStore) -> Iterator[TestClient]:
    """Yield a ``TestClient`` wired to read from the seeded :class:`FakeStore`.

    Patches the three InfluxDB query entry points to read from ``store`` and
    overrides the ``get_influx_client`` dependency with a dummy. A stable
    localhost/no-auth posture and a fixed SPA origin keep the environment
    deterministic.
    """
    monkeypatch.setenv(api.ENV_SPA_ORIGIN, api.DEFAULT_SPA_ORIGIN)
    monkeypatch.delenv(api.ENV_API_KEY, raising=False)
    monkeypatch.delenv(api.ENV_REQUIRE_AUTH, raising=False)
    monkeypatch.delenv(api.ENV_HOST, raising=False)

    def _fake_query_price_series(
        _client: Any,
        item_id: str,
        start: Any,
        stop: Any,
        interval: Any = None,
        **_kwargs: Any,
    ) -> list[dict[str, Any]]:
        return store.price_series(item_id, int(start), int(stop))

    def _fake_query_chunk(
        _client: Any,
        item_ids: list[str],
        start: Any,
        stop: Any,
        interval: Any = None,
        **_kwargs: Any,
    ) -> pd.DataFrame:
        return store.chunk_frame(list(item_ids), int(start), int(stop))

    def _fake_list_item_ids(_client: Any, **_kwargs: Any) -> list[str]:
        return store.item_ids()

    # api.influx and data_access.influx are the same module object, so patching
    # the module attributes here covers every call site in both layers.
    monkeypatch.setattr(influx, "query_price_series", _fake_query_price_series)
    monkeypatch.setattr(influx, "query_chunk", _fake_query_chunk)
    monkeypatch.setattr(influx, "list_item_ids", _fake_list_item_ids)

    app = create_app()
    app.dependency_overrides[get_influx_client] = lambda: object()

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


# --------------------------------------------------------------------------- #
# Item search
# --------------------------------------------------------------------------- #


def test_items_returns_seeded_ids(client: TestClient, store: FakeStore) -> None:
    """``GET /api/items`` (no filter) returns exactly the seeded ids."""
    response = client.get("/api/items", params={"query": ""})
    assert response.status_code == 200
    returned = {item["itemId"] for item in response.json()}
    assert returned == set(store.item_ids())


def test_items_substring_filter(client: TestClient) -> None:
    """``GET /api/items?query=`` filters seeded ids by substring."""
    response = client.get("/api/items", params={"query": "56"})
    assert response.status_code == 200
    returned = {item["itemId"] for item in response.json()}
    assert returned == {"565"}


# --------------------------------------------------------------------------- #
# Price series contract: ordering + outlier annotation
# --------------------------------------------------------------------------- #


def test_prices_ascending_and_outlier_annotated(client: TestClient) -> None:
    """Prices come back strictly time-ascending, each with an ``isOutlier`` bool.

    Given the injected spikes, at least one point must be flagged.
    """
    response = client.get(
        "/api/items/554/prices",
        params={"range": "7d", "interval": "1h"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["itemId"] == "554"
    assert body["interval"] == "1h"

    points = body["points"]
    assert points, "expected a non-empty seeded series"

    times = [p["time"] for p in points]
    assert times == sorted(times)
    assert len(times) == len(set(times)), "timestamps must be unique/ascending"
    assert all(t2 > t1 for t1, t2 in zip(times, times[1:])), "strictly ascending"

    assert all(isinstance(p["isOutlier"], bool) for p in points)
    assert any(p["isOutlier"] for p in points), "injected spike should flag one"


# --------------------------------------------------------------------------- #
# Cursor pagination: lossless traversal matching the full series
# --------------------------------------------------------------------------- #


def test_pagination_is_lossless(client: TestClient) -> None:
    """Following ``nextCursor`` visits every point exactly once (lossless).

    The concatenation of all pages must equal the full price series with no
    gaps, duplicates, or reordering.
    """
    # Ground truth: the full series from the un-paginated endpoint.
    full = client.get(
        "/api/items/554/prices",
        params={"range": "7d", "interval": "1h"},
    )
    assert full.status_code == 200
    full_times = [p["time"] for p in full.json()["points"]]
    assert len(full_times) == _POINTS_PER_ITEM

    limit = 10
    collected: list[int] = []
    cursor: int | None = None
    pages = 0
    while True:
        params: dict[str, Any] = {
            "range": "7d",
            "interval": "1h",
            "limit": limit,
        }
        if cursor is not None:
            params["cursor"] = cursor
        response = client.get("/api/items/554/prices/page", params=params)
        assert response.status_code == 200
        body = response.json()
        page_times = [p["time"] for p in body["points"]]

        assert len(page_times) <= limit
        collected.extend(page_times)
        pages += 1
        assert pages <= _POINTS_PER_ITEM, "pagination failed to terminate"

        cursor = body["nextCursor"]
        if cursor is None:
            break

    # Lossless partition: same points, in the same ascending order, once each.
    assert collected == full_times
    assert len(collected) == len(set(collected))


# --------------------------------------------------------------------------- #
# Outliers endpoint: only flagged points
# --------------------------------------------------------------------------- #


def test_outliers_endpoint_returns_only_outliers(client: TestClient) -> None:
    """``GET /api/items/{id}/outliers`` returns only outlier points."""
    response = client.get(
        "/api/items/554/outliers",
        params={"range": "7d", "interval": "1h"},
    )
    assert response.status_code == 200
    points = response.json()["points"]
    assert points, "injected spikes should surface at least one outlier"
    assert all(p["isOutlier"] for p in points)

    times = [p["time"] for p in points]
    assert times == sorted(times)


# --------------------------------------------------------------------------- #
# Dataset export streaming: ndjson + csv, both items present
# --------------------------------------------------------------------------- #


def test_export_ndjson_streams_both_items(client: TestClient) -> None:
    """NDJSON export streams rows for both seeded items via ``stream_dataset``."""
    response = client.get(
        "/api/datasets/export",
        params={
            "items": "554,565",
            "range": "7d",
            "interval": "1h",
            "format": "ndjson",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")

    lines = [line for line in response.text.splitlines() if line.strip()]
    assert lines, "expected streamed NDJSON rows"
    records = [json.loads(line) for line in lines]
    item_ids = {str(record["itemID"]) for record in records}
    assert item_ids == {"554", "565"}


def test_export_csv_streams_both_items(client: TestClient) -> None:
    """CSV export streams a well-formed document containing both items."""
    response = client.get(
        "/api/datasets/export",
        params={
            "items": "554,565",
            "range": "7d",
            "interval": "1h",
            "format": "csv",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")

    reader = csv.DictReader(io.StringIO(response.text))
    rows = list(reader)
    assert rows, "expected streamed CSV rows"
    item_ids = {row["itemID"] for row in rows}
    assert item_ids == {"554", "565"}


# --------------------------------------------------------------------------- #
# 404 on missing item
# --------------------------------------------------------------------------- #


def test_prices_404_for_unknown_item(client: TestClient) -> None:
    """An item absent from the store yields a 404 with a typed body."""
    response = client.get(
        "/api/items/999/prices",
        params={"range": "7d", "interval": "1h"},
    )
    assert response.status_code == 404
    body = response.json()
    assert "detail" in body and isinstance(body["detail"], str)


# --------------------------------------------------------------------------- #
# Invalid params -> 4xx
# --------------------------------------------------------------------------- #


def test_invalid_range_returns_400(client: TestClient) -> None:
    """A malformed ``range`` is rejected with 400."""
    response = client.get(
        "/api/items/554/prices", params={"range": "not-a-range"}
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_invalid_method_returns_400(client: TestClient) -> None:
    """An unregistered outlier ``method`` is rejected with 400."""
    response = client.get(
        "/api/items/554/prices",
        params={"range": "7d", "interval": "1h", "method": "bogus"},
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_invalid_export_format_returns_400(client: TestClient) -> None:
    """An unsupported export ``format`` is rejected with 400."""
    response = client.get(
        "/api/datasets/export",
        params={"items": "554,565", "interval": "1h", "format": "xml"},
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_page_limit_zero_returns_400(client: TestClient) -> None:
    """A page ``limit`` of 0 is rejected with 400 (0 < limit <= MAX)."""
    response = client.get(
        "/api/items/554/prices/page",
        params={"range": "7d", "interval": "1h", "limit": 0},
    )
    assert response.status_code == 400
    assert "detail" in response.json()
