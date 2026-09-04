"""Property-based tests for the Query API HTTP contract.

Implements the API-contract correctness property from the design's Correctness
Properties section:

* **Property 9: PriceSeriesResponse time ordering (API contract)** (Task 12.3)

The ``GET /api/items/{item_id}/prices`` endpoint must return a
``PriceSeriesResponse`` whose ``points`` are in strictly ascending time order
and where every point carries an ``isOutlier`` boolean, regardless of the order
or duplication present in the underlying store's raw rows (Req 16.2/16.3).

Each property runs under the ``ge`` Hypothesis profile (``max_examples>=100``)
registered in ``tests/conftest.py``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from fastapi.testclient import TestClient
from hypothesis import given
from hypothesis import strategies as st

from ge_pipeline import api
from ge_pipeline.api import create_app, get_influx_client

# Item ids are numeric strings, matching the store's ``itemID`` tag convention.
_ITEM_IDS = st.from_regex(r"[0-9]{1,6}", fullmatch=True)

# Nullable integer price/volume fields (the API tolerates None for inactive
# items). Times are kept in a modest window so many-point series stay cheap.
_NULLABLE_INT = st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000))
_TIMES = st.integers(min_value=0, max_value=100_000)


@st.composite
def _series_and_item(draw: st.DrawFn) -> tuple[str, list[dict]]:
    """Draw ``(item_id, raw_series)`` for a non-empty, possibly-disordered series.

    The generated ``time`` values are intentionally left unordered so the test
    exercises the endpoint's contractual guarantee to return points in strictly
    ascending time order even when the underlying store yields them out of
    order. Times are unique because InfluxDB identifies a point uniquely by
    ``(measurement, tag set, timestamp)`` — two rows for the same item can never
    share a timestamp — so duplicate times are outside the real input space.
    ``avgHighPrice`` and the other OHLC/volume fields may be ``None`` (inactive
    items).
    """
    item_id = draw(_ITEM_IDS)
    times = draw(
        st.lists(_TIMES, min_size=1, max_size=40, unique=True)
    )
    series = [
        {
            "time": t,
            "avgHighPrice": draw(_NULLABLE_INT),
            "avgLowPrice": draw(_NULLABLE_INT),
            "highPriceVolume": draw(_NULLABLE_INT),
            "lowPriceVolume": draw(_NULLABLE_INT),
        }
        for t in times
    ]
    return item_id, series


@given(data=_series_and_item())
def test_price_series_response_is_time_ascending_contract(
    data: tuple[str, list[dict]],
) -> None:
    """The price-series endpoint honours its ordering + ``isOutlier`` contract.

    **Property 9: PriceSeriesResponse time ordering (API contract)**

    **Validates: Requirements 16.2, 16.3**
    """
    item_id, series = data

    app = create_app()

    # Override the InfluxDB client dependency with a lightweight fake so no real
    # database is touched; the fake's identity is irrelevant because the query
    # layer itself is patched below.
    fake_client: Any = object()
    app.dependency_overrides[get_influx_client] = lambda: fake_client

    # ``unittest.mock.patch`` (not the monkeypatch fixture) because Hypothesis
    # re-runs this body many times and function-scoped fixtures are not reset
    # between generated examples. The endpoint calls
    # ``influx.query_price_series(client, item_id, start, stop, interval)``.
    def _fake_query_price_series(client, req_item_id, start, stop, interval):
        assert req_item_id == item_id
        return list(series)

    with patch.object(api.influx, "query_price_series", _fake_query_price_series):
        client = TestClient(app)
        # interval="1h" keeps the request within the raw-range guardrail.
        response = client.get(
            f"/api/items/{item_id}/prices",
            params={"range": "7d", "interval": "1h"},
        )

    # Non-empty series yields HTTP 200 (404 is reserved for the empty case).
    assert response.status_code == 200, response.text

    body = response.json()

    # itemId echoes the requested id.
    assert body["itemId"] == item_id

    points = body["points"]
    # The series was non-empty, so at least one point is returned.
    assert points, "a non-empty series must yield at least one point"

    times = [point["time"] for point in points]

    # Strictly ascending time order: time[i] < time[i+1] for all i.
    assert all(a < b for a, b in zip(times, times[1:])), times

    # Every point carries an ``isOutlier`` flag that is exactly a bool.
    for point in points:
        assert "isOutlier" in point
        assert type(point["isOutlier"]) is bool


@given(item_id=_ITEM_IDS)
def test_price_series_empty_returns_404(item_id: str) -> None:
    """An empty underlying series produces a typed 404 (Req 16.5).

    This complements Property 9's non-empty focus: the ordering/contract
    guarantee only applies to a returned series, and an absent series must be
    reported as ``404`` rather than an empty ``200`` body.
    """
    app = create_app()
    fake_client: Any = object()
    app.dependency_overrides[get_influx_client] = lambda: fake_client

    with patch.object(
        api.influx, "query_price_series", lambda *a, **k: []
    ):
        client = TestClient(app)
        response = client.get(
            f"/api/items/{item_id}/prices",
            params={"range": "7d", "interval": "1h"},
        )

    assert response.status_code == 404, response.text
    assert "detail" in response.json()
