"""Unit tests for :func:`ge_pipeline.ingestion.fetch_snapshot`.

Exercises the async fetch-and-validate path with a mocked httpx transport
(:class:`httpx.MockTransport`), covering the successful validation path plus the
error classification rules: request timeouts and connection errors map to
:class:`TransientError`; HTTP 5xx maps to :class:`TransientError`; HTTP 429 maps
to :class:`TransientError` carrying a ``retry_after`` parsed from the
``Retry-After`` header; non-429 HTTP 4xx maps to :class:`NonTransientError`. Also
asserts the outgoing request targets ``/5m`` with the ``timestamp`` param and
carries the configured ``User-Agent`` header.

Requirements: 5.2, 5.3, 5.4, 21.1
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from ge_pipeline.errors import NonTransientError, TransientError
from ge_pipeline.ingestion import fetch_snapshot
from ge_pipeline.models import FiveMinuteSnapshot
from ge_pipeline.retry import RETRY_AFTER_ATTR

BASE_URL = "https://prices.runescape.wiki/api/v1/osrs"
USER_AGENT = "GEoutlier-detection"


def _run(coro):
    """Run a coroutine to completion without requiring pytest-asyncio."""
    return asyncio.run(coro)


def _make_client(handler) -> httpx.AsyncClient:
    """Build an AsyncClient wired to a MockTransport with the given handler."""
    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=BASE_URL,
        headers={"User-Agent": USER_AGENT},
    )


async def _fetch_with(handler, ts: int) -> FiveMinuteSnapshot:
    """Run ``fetch_snapshot`` against a client backed by ``handler``."""
    async with _make_client(handler) as client:
        return await fetch_snapshot(client, ts)


# ---------------------------------------------------------------------------
# Successful 2xx validation path
# ---------------------------------------------------------------------------


def test_successful_response_returns_validated_snapshot():
    ts = 1700000000
    body = {
        "timestamp": ts,
        "data": {
            "554": {
                "avgHighPrice": 5,
                "avgLowPrice": 4,
                "highPriceVolume": 100,
                "lowPriceVolume": 90,
            },
            "2": {"avgHighPrice": None, "avgLowPrice": None},
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    snapshot = _run(_fetch_with(handler, ts))

    assert isinstance(snapshot, FiveMinuteSnapshot)
    assert snapshot.timestamp == ts
    assert snapshot.data["554"].avg_high_price == 5
    assert snapshot.data["554"].low_price_volume == 90
    assert snapshot.data["2"].avg_high_price is None


def test_request_targets_5m_endpoint_with_timestamp_param():
    ts = 1615733100
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["timestamp"] = request.url.params.get("timestamp")
        return httpx.Response(200, json={"timestamp": ts, "data": {}})

    _run(_fetch_with(handler, ts))

    assert captured["path"] == "/api/v1/osrs/5m"
    assert captured["timestamp"] == str(ts)


def test_request_carries_configured_user_agent_header():
    ts = 1700000300
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["user_agent"] = request.headers.get("User-Agent")
        return httpx.Response(200, json={"timestamp": ts, "data": {}})

    _run(_fetch_with(handler, ts))

    assert captured["user_agent"] == USER_AGENT


# ---------------------------------------------------------------------------
# Transient errors: timeout and connection
# ---------------------------------------------------------------------------


def test_timeout_raises_transient_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out", request=request)

    with pytest.raises(TransientError):
        _run(_fetch_with(handler, 1700000000))


def test_connection_error_raises_transient_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(TransientError):
        _run(_fetch_with(handler, 1700000000))


# ---------------------------------------------------------------------------
# Transient errors: HTTP 5xx
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [500, 502, 503])
def test_server_error_raises_transient_error(status: int):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    with pytest.raises(TransientError):
        _run(_fetch_with(handler, 1700000000))


# ---------------------------------------------------------------------------
# HTTP 429 with Retry-After handling
# ---------------------------------------------------------------------------


def test_rate_limit_with_retry_after_header_sets_attribute():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "12"})

    with pytest.raises(TransientError) as exc_info:
        _run(_fetch_with(handler, 1700000000))

    assert getattr(exc_info.value, RETRY_AFTER_ATTR) == 12


def test_rate_limit_without_retry_after_header_sets_none():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    with pytest.raises(TransientError) as exc_info:
        _run(_fetch_with(handler, 1700000000))

    assert getattr(exc_info.value, RETRY_AFTER_ATTR) is None


def test_rate_limit_with_non_integer_retry_after_sets_none():
    # HTTP-date form is not honored; falls back to None per _parse_retry_after.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429, headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}
        )

    with pytest.raises(TransientError) as exc_info:
        _run(_fetch_with(handler, 1700000000))

    assert getattr(exc_info.value, RETRY_AFTER_ATTR) is None


# ---------------------------------------------------------------------------
# Non-transient errors: non-429 HTTP 4xx
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 403, 404])
def test_client_error_raises_non_transient_error(status: int):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    with pytest.raises(NonTransientError):
        _run(_fetch_with(handler, 1700000000))
