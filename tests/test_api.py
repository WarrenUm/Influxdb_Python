"""Unit tests for the FastAPI query service (``ge_pipeline.api``).

Exercises the HTTP surface with Starlette's ``TestClient`` and a fake InfluxDB
client injected via ``app.dependency_overrides[get_influx_client]``. The
influx/data_access functions the routes call are replaced with
``unittest.mock``/``monkeypatch`` fakes so no real InfluxDB is touched.

Covers:

* Route contracts for health, outlier methods, item search, and price series
  (Req 16.1).
* Chunked/streamed dataset export (Req 16.4).
* 404 typed body when an item has no data (Req 16.5).
* Guardrail enforcement: raw range too wide, >50 items (Req 16.6).
* 4xx on invalid params: bad range, unknown method, bad page limit, bad export
  format (Req 16.7).
* CORS restricted to the configured SPA origin, never ``"*"`` (Req 19.1).

**Validates: Requirements 16.1, 16.4, 16.5, 16.6, 16.7, 19.1, 21.1**
"""

from __future__ import annotations

from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from ge_pipeline import api, data_access
from ge_pipeline.api import create_app, get_influx_client


class _FakeInfluxClient:
    """Stand-in for the reusable InfluxDB client injected into routes."""


def _make_series(times: list[int]) -> list[dict]:
    """Build a plain-dict price series matching ``query_price_series`` output."""
    return [
        {
            "time": t,
            "avgHighPrice": 100 + i,
            "avgLowPrice": 90 + i,
            "highPriceVolume": 10 + i,
            "lowPriceVolume": 5 + i,
        }
        for i, t in enumerate(times)
    ]


@pytest.fixture
def client(monkeypatch) -> Iterator[TestClient]:
    """Yield a ``TestClient`` with the influx client dependency overridden.

    The SPA origin is pinned to the default so CORS assertions are stable, and
    no auth-related environment variables are set (localhost single-user => the
    ``require_auth`` dependency is a no-op).
    """
    # Ensure a deterministic, localhost no-auth posture with a known origin.
    monkeypatch.setenv(api.ENV_SPA_ORIGIN, api.DEFAULT_SPA_ORIGIN)
    monkeypatch.delenv(api.ENV_API_KEY, raising=False)
    monkeypatch.delenv(api.ENV_REQUIRE_AUTH, raising=False)
    monkeypatch.delenv(api.ENV_HOST, raising=False)

    app = create_app()
    fake_client = _FakeInfluxClient()
    app.dependency_overrides[get_influx_client] = lambda: fake_client

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


# --------------------------------------------------------------------------- #
# Route contracts (Req 16.1)
# --------------------------------------------------------------------------- #


def test_health_returns_ok(client: TestClient) -> None:
    """``GET /api/health`` returns 200 with the liveness body."""
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_outlier_methods_returns_registered_list(client: TestClient) -> None:
    """``GET /api/outlier-methods`` returns 200 and the registered detectors."""
    response = client.get("/api/outlier-methods")
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    # Built-in detectors registered by the outliers module.
    assert set(body) >= {"iqr", "zscore"}


def test_items_returns_filtered_list(client: TestClient, monkeypatch) -> None:
    """``GET /api/items`` returns only ids matching the query substring."""
    monkeypatch.setattr(
        api.influx,
        "list_item_ids",
        lambda _client: ["554", "565", "4151"],
    )

    response = client.get("/api/items", params={"query": "55"})
    assert response.status_code == 200
    body = response.json()
    returned_ids = {item["itemId"] for item in body}
    # Only "554" contains the "55" substring; the others are filtered out.
    assert returned_ids == {"554"}
    # ``name`` mirrors ``itemId`` since there is no name store.
    for item in body:
        assert item["name"] == item["itemId"]


def test_item_prices_returns_price_series_shape(
    client: TestClient, monkeypatch
) -> None:
    """``GET /api/items/{id}/prices`` returns a valid PriceSeriesResponse."""
    # Supply out-of-order times to assert the response is sorted ascending.
    series = _make_series([300, 100, 200])
    monkeypatch.setattr(
        api.influx,
        "query_price_series",
        lambda *args, **kwargs: series,
    )

    response = client.get("/api/items/554/prices", params={"range": "7d"})
    assert response.status_code == 200
    body = response.json()

    assert body["itemId"] == "554"
    assert body["interval"] == "raw"
    times = [point["time"] for point in body["points"]]
    assert times == [100, 200, 300]  # strictly ascending
    for point in body["points"]:
        assert isinstance(point["isOutlier"], bool)


# --------------------------------------------------------------------------- #
# 404 on missing data (Req 16.5)
# --------------------------------------------------------------------------- #


def test_item_prices_404_when_no_data(client: TestClient, monkeypatch) -> None:
    """An empty series yields a 404 with a typed ``{"detail": ...}`` body."""
    monkeypatch.setattr(
        api.influx,
        "query_price_series",
        lambda *args, **kwargs: [],
    )

    response = client.get("/api/items/999/prices", params={"range": "7d"})
    assert response.status_code == 404
    body = response.json()
    assert "detail" in body
    assert isinstance(body["detail"], str)


# --------------------------------------------------------------------------- #
# 4xx on invalid params (Req 16.7)
# --------------------------------------------------------------------------- #


def test_item_prices_invalid_range_returns_400(client: TestClient) -> None:
    """A malformed ``range`` value is rejected with 400."""
    response = client.get("/api/items/554/prices", params={"range": "bogus"})
    assert response.status_code == 400
    assert "detail" in response.json()


def test_item_prices_invalid_method_returns_400(client: TestClient) -> None:
    """An unregistered outlier ``method`` is rejected with 400."""
    response = client.get(
        "/api/items/554/prices",
        params={"range": "7d", "method": "not-a-method"},
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_page_limit_zero_returns_400(client: TestClient, monkeypatch) -> None:
    """A page ``limit`` of 0 is rejected with 400 (0 < limit <= MAX)."""
    monkeypatch.setattr(
        api.influx,
        "query_price_series",
        lambda *args, **kwargs: _make_series([100, 200]),
    )
    response = client.get(
        "/api/items/554/prices/page",
        params={"range": "7d", "interval": "1h", "limit": 0},
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_page_limit_over_max_returns_400(
    client: TestClient, monkeypatch
) -> None:
    """A page ``limit`` above ``MAX_PAGE_LIMIT`` is rejected with 400."""
    monkeypatch.setattr(
        api.influx,
        "query_price_series",
        lambda *args, **kwargs: _make_series([100, 200]),
    )
    response = client.get(
        "/api/items/554/prices/page",
        params={
            "range": "7d",
            "interval": "1h",
            "limit": data_access.MAX_PAGE_LIMIT + 1,
        },
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_export_unsupported_format_returns_400(client: TestClient) -> None:
    """An unsupported export ``format`` (xml) is rejected with 400."""
    response = client.get(
        "/api/datasets/export",
        params={"items": "554", "format": "xml"},
    )
    assert response.status_code == 400
    assert "detail" in response.json()


# --------------------------------------------------------------------------- #
# Guardrail enforcement (Req 16.6)
# --------------------------------------------------------------------------- #


def test_raw_range_too_wide_without_interval_returns_400(
    client: TestClient, monkeypatch
) -> None:
    """``range=all`` without an ``interval`` violates the raw-range guardrail."""
    # Should never reach the query layer; fail loudly if it does.
    monkeypatch.setattr(
        api.influx,
        "query_price_series",
        lambda *args, **kwargs: pytest.fail("query layer must not be reached"),
    )
    response = client.get("/api/items/554/prices", params={"range": "all"})
    assert response.status_code == 400
    assert "detail" in response.json()


def test_wide_range_with_interval_ok(client: TestClient, monkeypatch) -> None:
    """Supplying an ``interval`` allows a wide range through (200)."""
    monkeypatch.setattr(
        api.influx,
        "query_price_series",
        lambda *args, **kwargs: _make_series([100, 200, 300]),
    )
    response = client.get(
        "/api/items/554/prices",
        params={"range": "all", "interval": "1h"},
    )
    assert response.status_code == 200
    assert response.json()["interval"] == "1h"


def test_export_too_many_items_returns_400(client: TestClient) -> None:
    """Exporting more than ``MAX_ITEMS_PER_REQUEST`` items is rejected (400)."""
    too_many = ",".join(str(i) for i in range(api.MAX_ITEMS_PER_REQUEST + 1))
    response = client.get(
        "/api/datasets/export",
        params={"items": too_many, "interval": "1h", "format": "ndjson"},
    )
    assert response.status_code == 400
    assert "detail" in response.json()


# --------------------------------------------------------------------------- #
# CORS restriction (Req 19.1)
# --------------------------------------------------------------------------- #


def test_cors_echoes_configured_origin_not_wildcard(
    client: TestClient,
) -> None:
    """CORS echoes the configured SPA origin and never ``"*"``."""
    response = client.get(
        "/api/health",
        headers={"Origin": api.DEFAULT_SPA_ORIGIN},
    )
    assert response.status_code == 200
    allow_origin = response.headers.get("access-control-allow-origin")
    assert allow_origin == api.DEFAULT_SPA_ORIGIN
    assert allow_origin != "*"


def test_cors_middleware_never_configured_with_wildcard() -> None:
    """The allowed-origins helper never returns a wildcard entry."""
    assert "*" not in api._get_allowed_origins()


# --------------------------------------------------------------------------- #
# Export chunked streaming (Req 16.4)
# --------------------------------------------------------------------------- #


def test_export_streams_chunks(client: TestClient, monkeypatch) -> None:
    """Export streams the encoder's byte chunks unbuffered via StreamingResponse.

    ``stream_dataset`` is patched to a generator yielding multiple chunks. We
    assert the response body equals the concatenation of the chunks, the
    generator was actually consumed (lazily, by the streaming response), and the
    content type matches the requested format.
    """
    chunks = [b'{"itemId":"554"}\n', b'{"itemId":"565"}\n', b'{"itemId":"4151"}\n']
    consumed: list[bytes] = []
    calls: list[tuple] = []

    def _fake_stream_dataset(client_arg, item_ids, start, stop, interval, fmt):
        calls.append((tuple(item_ids), interval, fmt))
        for chunk in chunks:
            consumed.append(chunk)
            yield chunk

    monkeypatch.setattr(data_access, "stream_dataset", _fake_stream_dataset)

    response = client.get(
        "/api/datasets/export",
        params={
            "items": "554,565,4151",
            "range": "all",
            "interval": "1h",
            "format": "ndjson",
        },
    )

    assert response.status_code == 200
    # StreamingResponse for ndjson carries the ndjson media type.
    assert response.headers["content-type"].startswith("application/x-ndjson")
    # Body is exactly the concatenation of the yielded chunks.
    assert response.content == b"".join(chunks)
    # The patched generator was consumed and stream_dataset was called once.
    assert consumed == chunks
    assert len(calls) == 1
    assert calls[0] == (("554", "565", "4151"), "1h", "ndjson")


def test_export_csv_media_type(client: TestClient, monkeypatch) -> None:
    """CSV exports advertise the ``text/csv`` media type while streaming."""
    chunks = [b"time,price\n", b"100,10\n", b"200,11\n"]

    def _fake_stream_dataset(*args, **kwargs):
        yield from chunks

    monkeypatch.setattr(data_access, "stream_dataset", _fake_stream_dataset)

    response = client.get(
        "/api/datasets/export",
        params={"items": "554", "interval": "1h", "format": "csv"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert response.content == b"".join(chunks)
