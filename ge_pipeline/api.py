"""FastAPI query service exposing read/export endpoints.

Exposes typed REST routes for health, item search, price series, cursor
pagination, outliers, bulk dataset export (streamed), ML feature frames, and the
list of registered outlier-detector names. CORS is restricted to the React SPA
origin and all query parameters are validated.

This module provides the foundational application wiring:

* A :func:`create_app` factory that builds and returns a configured
  :class:`fastapi.FastAPI` instance, plus a module-level ``app`` instance so the
  service can be launched with ``uvicorn ge_pipeline.api:app``.
* CORS restricted to the React SPA origin(s) (never ``"*"``), configurable via
  the ``GE_SPA_ORIGIN`` environment variable.
* A conditional authentication dependency: a no-op for a local single-user
  deployment on localhost, but an ``X-API-Key`` check once the service is
  exposed beyond localhost.
* ``GET /api/health`` and ``GET /api/outlier-methods`` endpoints.

Additional read/export endpoints (item search, price series, pagination,
outliers, dataset export, feature frames) are added by later tasks (12.2 / 12.4)
on top of this scaffolding.

Security note:
    Requirement 19 is satisfied by three complementary measures:

    1. **CORS restriction** to the React SPA origin (this module) prevents
       arbitrary browser origins from reading responses.
    2. **Parameterized Flux queries** in :mod:`ge_pipeline.influx` prevent
       injection through item IDs or ranges.
    3. **Conditional authentication** (this module) leaves a localhost
       single-user deployment unauthenticated (Req 19.3) while enforcing an API
       key when the service is exposed beyond localhost (Req 19.4).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import pandas as pd
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from . import data_access, influx
from .config import get_settings
from .outliers import available_detectors, detect_outliers

__all__ = [
    "create_app",
    "app",
    "require_auth",
    "get_influx_client",
    "PricePoint",
    "PriceSeriesResponse",
    "ItemSearchResult",
    "PricePageResponse",
]

logger = logging.getLogger(__name__)

#: Environment variable holding a comma-separated list of allowed CORS origins
#: (the React SPA origin(s)). Defaults to the Vite dev server on localhost.
ENV_SPA_ORIGIN = "GE_SPA_ORIGIN"

#: Default React SPA origin used for local development (Vite's default port).
DEFAULT_SPA_ORIGIN = "http://localhost:5173"

#: Environment variable holding the API key required when authentication is
#: enforced. When set (non-empty), authentication is enabled automatically.
ENV_API_KEY = "GE_API_KEY"

#: Optional environment flag ("1"/"true"/"yes"/"on") that forces authentication
#: on even without inferring exposure from the host binding.
ENV_REQUIRE_AUTH = "GE_REQUIRE_AUTH"

#: Environment variable naming the host/interface the service binds to. A
#: non-localhost bind address implies the API is exposed beyond localhost and
#: therefore requires authentication (Req 19.4).
ENV_HOST = "GE_API_HOST"

#: HTTP header carrying the API key when authentication is enforced.
API_KEY_HEADER = "X-API-Key"

#: Host values considered "localhost" for the purposes of the single-user
#: no-auth policy (Req 19.3).
_LOCALHOST_HOSTS = frozenset({"", "localhost", "127.0.0.1", "::1"})

#: Values interpreted as boolean-true for flag-style environment variables.
_TRUTHY = frozenset({"1", "true", "yes", "on"})

#: Maximum number of items a single request may target (Req 16.6). Applies to
#: the item-search result cap and the dataset export/features item count.
MAX_ITEMS_PER_REQUEST = 50

#: Maximum un-downsampled (raw) range, in seconds, allowed when no explicit
#: ``interval`` is supplied (Req 16.6). Ranges wider than this must provide an
#: ``interval`` so the query is downsampled server-side. Defaults to 7 days.
MAX_RAW_RANGE_SECONDS = 7 * 86_400

#: The reference start used to anchor an ``"all"`` range at the store's start.
#: 2021-03-14 is when the RuneScape Wiki 5-minute price API history begins.
_ALL_RANGE_START = 1_615_733_100

#: Supported duration suffixes for range/interval strings, mapped to seconds.
_DURATION_UNITS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}

#: Media types emitted for each supported export format.
_EXPORT_MEDIA_TYPES: dict[str, str] = {
    "ndjson": "application/x-ndjson",
    "csv": "text/csv",
    "parquet": "application/octet-stream",
}


class PricePoint(BaseModel):
    """A single price point in a series, matching the SPA transport model.

    Attributes:
        time: Unix seconds timestamp of the point.
        avgHighPrice: Average high price for the window, or ``None``.
        avgLowPrice: Average low price for the window, or ``None``.
        highPriceVolume: High-price traded volume, or ``None``.
        lowPriceVolume: Low-price traded volume, or ``None``.
        isOutlier: Whether this point was flagged as an outlier. Always present.
    """

    time: int
    avgHighPrice: float | None = None
    avgLowPrice: float | None = None
    highPriceVolume: float | None = None
    lowPriceVolume: float | None = None
    isOutlier: bool = False


class PriceSeriesResponse(BaseModel):
    """A full price series for one item (``PriceSeriesResponse`` transport model).

    Attributes:
        itemId: The item identifier the points belong to.
        interval: The downsample interval used (for example ``"5m"``).
        points: Time-ascending price points, each carrying an ``isOutlier`` flag.
    """

    itemId: str
    interval: str
    points: list[PricePoint] = Field(default_factory=list)


class PricePageResponse(BaseModel):
    """One cursor-paginated page of price points (``PricePage`` transport model).

    Attributes:
        itemId: The item identifier the points belong to.
        interval: The downsample interval used (for example ``"5m"``).
        points: The time-ascending price points in this page.
        nextCursor: The ``time`` of the last point when more data exists, else
            ``None``.
    """

    itemId: str
    interval: str
    points: list[PricePoint] = Field(default_factory=list)
    nextCursor: int | None = None


class ItemSearchResult(BaseModel):
    """A single item-search match (``ItemSearchResult`` transport model).

    Attributes:
        itemId: The matching item identifier.
        name: A human-readable name. There is no item-name store in InfluxDB,
            so this is a best-effort value that currently mirrors ``itemId``.
    """

    itemId: str
    name: str


class ErrorResponse(BaseModel):
    """Typed error body returned for 4xx/404 responses.

    Attributes:
        detail: A human-readable description of what went wrong.
    """

    detail: str


def get_influx_client() -> Any:
    """FastAPI dependency yielding the reusable InfluxDB client.

    Resolves :func:`ge_pipeline.config.get_settings` and returns the process-wide
    cached client from :func:`ge_pipeline.influx.get_client`. Tests override this
    dependency (via ``app.dependency_overrides``) to inject a fake client, so the
    routes never touch a real InfluxDB during unit testing.

    Returns:
        A reusable InfluxDB client instance.
    """
    return influx.get_client(get_settings())


def _parse_duration(text: str) -> int:
    """Return the number of seconds represented by a duration string.

    Args:
        text: A duration such as ``"30s"``, ``"5m"``, ``"1h"``, ``"7d"``, or
            ``"1w"``. A bare integer is treated as seconds.

    Returns:
        The duration in seconds (always ``> 0``).

    Raises:
        ValueError: If ``text`` is empty, malformed, or non-positive.
    """
    value = text.strip().lower()
    if not value:
        raise ValueError("duration must be a non-empty string")

    unit = value[-1]
    if unit.isdigit():
        seconds = int(value)
    else:
        if unit not in _DURATION_UNITS:
            raise ValueError(
                f"unsupported duration unit {unit!r}; expected one of "
                f"{sorted(_DURATION_UNITS)}"
            )
        try:
            magnitude = int(value[:-1])
        except ValueError as exc:
            raise ValueError(f"invalid duration {text!r}") from exc
        seconds = magnitude * _DURATION_UNITS[unit]

    if seconds <= 0:
        raise ValueError("duration must be a positive number of seconds")
    return seconds


def _resolve_range(range_param: str, now: int | None = None) -> tuple[int, int]:
    """Resolve a ``range`` query parameter into ``(start, stop)`` unix seconds.

    Args:
        range_param: Either ``"all"`` (anchored at the store's start) or a
            duration string ending at "now" (for example ``"7d"``, ``"1h"``).
        now: Optional override for the current time (unix seconds); defaults to
            the wall clock. Useful for deterministic tests.

    Returns:
        A ``(start, stop)`` half-open range in unix seconds with ``start < stop``.

    Raises:
        ValueError: If ``range_param`` is empty or not a recognized duration.
    """
    stop = int(time.time()) if now is None else now
    text = range_param.strip().lower()
    if not text:
        raise ValueError("range must be a non-empty string")
    if text == "all":
        return _ALL_RANGE_START, stop

    span = _parse_duration(text)
    return stop - span, stop


def _enforce_range_guardrail(
    start: int, stop: int, interval: str | None
) -> None:
    """Reject an un-downsampled range wider than the raw-range guardrail.

    A request spanning more than :data:`MAX_RAW_RANGE_SECONDS` must supply an
    explicit ``interval`` so the query is downsampled server-side (Req 16.6).

    Args:
        start: Inclusive range start (unix seconds).
        stop: Exclusive range stop (unix seconds).
        interval: The requested downsample interval, or ``None`` for raw data.

    Raises:
        HTTPException: 400 when the raw range exceeds the guardrail and no
            ``interval`` was provided.
    """
    if interval is None and (stop - start) > MAX_RAW_RANGE_SECONDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Requested range of {stop - start}s exceeds the maximum "
                f"un-downsampled range of {MAX_RAW_RANGE_SECONDS}s; supply an "
                "explicit 'interval' to downsample the data server-side."
            ),
        )


def _parse_items_param(items: str) -> list[str]:
    """Parse and validate a comma-separated ``items`` query parameter.

    Args:
        items: A comma-separated list of item identifiers, for example
            ``"554,565"``.

    Returns:
        The de-duplicated, order-preserving list of item identifiers.

    Raises:
        HTTPException: 400 when the list is empty or exceeds
            :data:`MAX_ITEMS_PER_REQUEST`.
    """
    parsed: list[str] = []
    for raw in items.split(","):
        candidate = raw.strip()
        if candidate and candidate not in parsed:
            parsed.append(candidate)

    if not parsed:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one item id must be supplied in 'items'.",
        )
    if len(parsed) > MAX_ITEMS_PER_REQUEST:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Requested {len(parsed)} items exceeds the maximum of "
                f"{MAX_ITEMS_PER_REQUEST} items per request."
            ),
        )
    return parsed


def _resolve_range_or_400(range_param: str) -> tuple[int, int]:
    """Resolve a ``range`` parameter, translating parse errors to HTTP 400.

    Args:
        range_param: The raw ``range`` query value.

    Returns:
        A ``(start, stop)`` half-open range in unix seconds.

    Raises:
        HTTPException: 400 with a descriptive body when the range is invalid.
    """
    try:
        return _resolve_range(range_param)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid 'range' parameter: {exc}",
        ) from exc


def _validate_method(method: str) -> None:
    """Validate an outlier-detection method name against the registry.

    Args:
        method: The requested detector name.

    Raises:
        HTTPException: 400 with a descriptive body when ``method`` is not a
            registered detector (Req 16.7).
    """
    known = available_detectors()
    if method not in known:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unknown outlier method {method!r}; expected one of {known}."
            ),
        )


def _to_price_points(
    series: list[dict], method: str = "zscore"
) -> list[PricePoint]:
    """Annotate a raw price series with per-point ``isOutlier`` flags.

    Outliers are computed on the ``avgHighPrice`` channel using the named
    detector; points with a ``None`` high price contribute ``False`` (the
    detector is None-safe). Points are returned in strictly ascending time
    order.

    Args:
        series: Raw point dicts from
            :func:`ge_pipeline.influx.query_price_series`.
        method: Registered outlier-detector name to apply (default ``"zscore"``).

    Returns:
        A list of :class:`PricePoint` sorted ascending by ``time``.
    """
    ordered = sorted(series, key=lambda point: point["time"])
    high_values = [point.get("avgHighPrice") for point in ordered]
    flags = detect_outliers(high_values, method=method)

    points: list[PricePoint] = []
    for point, is_outlier in zip(ordered, flags):
        points.append(
            PricePoint(
                time=int(point["time"]),
                avgHighPrice=point.get("avgHighPrice"),
                avgLowPrice=point.get("avgLowPrice"),
                highPriceVolume=point.get("highPriceVolume"),
                lowPriceVolume=point.get("lowPriceVolume"),
                isOutlier=bool(is_outlier),
            )
        )
    return points


def _get_allowed_origins() -> list[str]:
    """Return the list of CORS origins allowed to call the API.

    Reads a comma-separated list from :data:`ENV_SPA_ORIGIN`, falling back to
    :data:`DEFAULT_SPA_ORIGIN` for local development. The wildcard ``"*"`` is
    deliberately never used so that credentials-bearing requests stay scoped to
    the React SPA origin (Req 19.1).

    Returns:
        A non-empty list of allowed origin strings.
    """
    raw = os.environ.get(ENV_SPA_ORIGIN, "").strip()
    if not raw:
        return [DEFAULT_SPA_ORIGIN]
    origins = [origin.strip() for origin in raw.split(",") if origin.strip()]
    return origins or [DEFAULT_SPA_ORIGIN]


def _is_localhost(host: str | None) -> bool:
    """Return whether ``host`` refers to the local machine.

    Args:
        host: The configured bind host, or ``None`` when unset.

    Returns:
        ``True`` when the host is empty or a recognized loopback address.
    """
    if host is None:
        return True
    return host.strip().lower() in _LOCALHOST_HOSTS


def _auth_required() -> bool:
    """Determine whether the API must enforce authentication.

    Authentication is required (Req 19.4) when any of the following holds:

    * :data:`ENV_API_KEY` is set to a non-empty value, or
    * :data:`ENV_REQUIRE_AUTH` is set to a truthy value, or
    * :data:`ENV_HOST` is set to a non-localhost bind address.

    Otherwise the deployment is treated as a local single-user setup on
    localhost and may operate without authentication (Req 19.3).

    Returns:
        ``True`` when authentication must be enforced, ``False`` otherwise.
    """
    if os.environ.get(ENV_API_KEY, "").strip():
        return True
    if os.environ.get(ENV_REQUIRE_AUTH, "").strip().lower() in _TRUTHY:
        return True
    if not _is_localhost(os.environ.get(ENV_HOST)):
        return True
    return False


def require_auth(x_api_key: str | None = Header(default=None)) -> None:
    """Conditional authentication dependency for protected endpoints.

    Implements the security policy from Requirement 19:

    * **Localhost single-user (Req 19.3):** when authentication is not required
      (see :func:`_auth_required`), this dependency is a no-op and requests are
      allowed through without credentials.
    * **Exposed beyond localhost (Req 19.4):** when authentication is required,
      the request must carry an ``X-API-Key`` header whose value matches the
      configured :data:`ENV_API_KEY`; otherwise the request is rejected with
      HTTP 401.

    Args:
        x_api_key: Value of the ``X-API-Key`` request header, if present.

    Raises:
        HTTPException: With status 401 when authentication is required and the
            supplied key is missing or does not match. With status 500 when
            authentication is required but no server-side key is configured
            (misconfiguration guard).
    """
    if not _auth_required():
        # Local single-user deployment: no authentication layer (Req 19.3).
        return

    expected_key = os.environ.get(ENV_API_KEY, "").strip()
    if not expected_key:
        # Auth is required (e.g. non-localhost host) but no key is configured.
        logger.error(
            "Authentication is required but %s is not set; refusing request.",
            ENV_API_KEY,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server authentication is misconfigured.",
        )

    if not x_api_key or x_api_key != expected_key:
        logger.warning("Rejected request with missing or invalid API key.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )


def create_app() -> FastAPI:
    """Build and return the configured FastAPI application.

    Wires up CORS (restricted to the React SPA origin), logs the effective
    security posture, and registers the foundational routes. Later tasks extend
    the returned app with additional read/export endpoints.

    Returns:
        A configured :class:`fastapi.FastAPI` instance.
    """
    app = FastAPI(
        title="GE Pipeline Query API",
        description=(
            "Read/export API over the OSRS Grand Exchange price store. "
            "CORS is restricted to the React SPA origin and Flux queries are "
            "parameterized; authentication is conditional on deployment."
        ),
        version="0.1.0",
    )

    allowed_origins = _get_allowed_origins()
    # Restrict CORS to the React SPA origin(s); never use "*" (Req 19.1).
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    auth_enabled = _auth_required()
    logger.info(
        "Query API initialized (allowed_origins=%s, auth_enabled=%s).",
        allowed_origins,
        auth_enabled,
    )

    @app.get("/api/health", tags=["system"])
    def health() -> dict[str, str]:
        """Liveness probe.

        Returns:
            ``{"status": "ok"}`` with HTTP 200. This endpoint is intentionally
            unauthenticated so infrastructure health checks work regardless of
            the auth policy.
        """
        return {"status": "ok"}

    @app.get(
        "/api/outlier-methods",
        tags=["outliers"],
        dependencies=[Depends(require_auth)],
    )
    def outlier_methods() -> list[str]:
        """Return the names of all registered outlier detectors (Req 13.8).

        Returns:
            The sorted list of registered detector names, e.g.
            ``["iqr", "zscore"]``.
        """
        methods = available_detectors()
        logger.debug("Reporting %d registered outlier methods.", len(methods))
        return methods

    @app.get(
        "/api/items",
        tags=["items"],
        response_model=list[ItemSearchResult],
        dependencies=[Depends(require_auth)],
    )
    def search_items(
        client: Any = Depends(get_influx_client),
        query: str = Query(default="", description="Substring to match item ids."),
        limit: int = Query(
            default=MAX_ITEMS_PER_REQUEST,
            ge=1,
            le=MAX_ITEMS_PER_REQUEST,
            description="Maximum number of matches to return.",
        ),
    ) -> list[ItemSearchResult]:
        """Search stored item ids by case-insensitive substring (Req 16.6).

        Args:
            client: The injected InfluxDB client.
            query: Substring to match against item ids; empty matches all.
            limit: Maximum number of matches (capped at
                :data:`MAX_ITEMS_PER_REQUEST`).

        Returns:
            Up to ``limit`` :class:`ItemSearchResult` matches. ``name`` mirrors
            ``itemId`` because InfluxDB stores no human-readable item names.
        """
        needle = query.strip().lower()
        matches: list[ItemSearchResult] = []
        for item_id in influx.list_item_ids(client):
            if needle and needle not in item_id.lower():
                continue
            matches.append(ItemSearchResult(itemId=item_id, name=item_id))
            if len(matches) >= limit:
                break
        logger.debug("Item search %r returned %d matches.", query, len(matches))
        return matches

    @app.get(
        "/api/items/{item_id}/prices",
        tags=["prices"],
        response_model=PriceSeriesResponse,
        responses={404: {"model": ErrorResponse}},
        dependencies=[Depends(require_auth)],
    )
    def item_prices(
        item_id: str,
        client: Any = Depends(get_influx_client),
        range: str = Query(default="7d", description="Range, e.g. '7d' or 'all'."),
        interval: str | None = Query(
            default=None, description="Optional downsample interval, e.g. '1h'."
        ),
        method: str = Query(
            default="zscore", description="Outlier-detection method to apply."
        ),
    ) -> PriceSeriesResponse:
        """Return an outlier-annotated, time-ascending price series (Req 16.2/16.3).

        Args:
            item_id: The item identifier to query.
            client: The injected InfluxDB client.
            range: Time range (a duration such as ``"7d"`` or ``"all"``).
            interval: Optional downsample interval; required for ranges wider
                than :data:`MAX_RAW_RANGE_SECONDS` (Req 16.6).
            method: Registered outlier-detector name (validated).

        Returns:
            A :class:`PriceSeriesResponse` with points in strictly ascending
            time order, each carrying an ``isOutlier`` boolean.

        Raises:
            HTTPException: 400 on invalid params or guardrail violations; 404
                with a typed error body when the item has no data (Req 16.5).
        """
        _validate_method(method)
        start, stop = _resolve_range_or_400(range)
        _enforce_range_guardrail(start, stop, interval)

        series = influx.query_price_series(
            client, item_id, start, stop, interval
        )
        if not series:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No price data found for item {item_id!r}.",
            )

        points = _to_price_points(series, method=method)
        return PriceSeriesResponse(
            itemId=item_id,
            interval=interval or "raw",
            points=points,
        )

    @app.get(
        "/api/items/{item_id}/prices/page",
        tags=["prices"],
        response_model=PricePageResponse,
        responses={404: {"model": ErrorResponse}},
        dependencies=[Depends(require_auth)],
    )
    def item_prices_page(
        item_id: str,
        client: Any = Depends(get_influx_client),
        range: str = Query(default="7d", description="Range, e.g. '7d' or 'all'."),
        interval: str | None = Query(
            default=None, description="Optional downsample interval, e.g. '1h'."
        ),
        cursor: int | None = Query(
            default=None, description="Time of the last point from the prior page."
        ),
        limit: int = Query(
            default=data_access.MAX_PAGE_LIMIT,
            description="Maximum points to return (0 < limit <= MAX_PAGE_LIMIT).",
        ),
        method: str = Query(
            default="zscore", description="Outlier-detection method to apply."
        ),
    ) -> PricePageResponse:
        """Return one cursor-paginated page of price points (Req 12.1).

        Args:
            item_id: The item identifier to page over.
            client: The injected InfluxDB client.
            range: Time range (a duration such as ``"7d"`` or ``"all"``).
            interval: Optional downsample interval (Req 16.6 guardrail applies).
            cursor: The ``time`` of the last point from the previous page.
            limit: Maximum points per page; validated against
                :data:`ge_pipeline.data_access.MAX_PAGE_LIMIT`.
            method: Registered outlier-detector name (validated).

        Returns:
            A :class:`PricePageResponse` with the page's points and ``nextCursor``.

        Raises:
            HTTPException: 400 on invalid params/guardrail; 404 when the item has
                no data in the requested range.
        """
        _validate_method(method)
        start, stop = _resolve_range_or_400(range)
        _enforce_range_guardrail(start, stop, interval)

        # ``get_price_page`` requires a concrete interval; use a sentinel when
        # the caller asked for raw data so the paging math still runs.
        effective_interval = interval if interval is not None else "raw"
        try:
            page = data_access.get_price_page(
                client,
                item_id,
                start,
                stop,
                effective_interval,
                cursor,
                limit,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid pagination request: {exc}",
            ) from exc

        if not page.points and cursor is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No price data found for item {item_id!r}.",
            )

        points = _to_price_points(list(page.points), method=method)
        return PricePageResponse(
            itemId=page.item_id,
            interval=interval or "raw",
            points=points,
            nextCursor=page.next_cursor,
        )

    @app.get(
        "/api/items/{item_id}/outliers",
        tags=["outliers"],
        response_model=PriceSeriesResponse,
        responses={404: {"model": ErrorResponse}},
        dependencies=[Depends(require_auth)],
    )
    def item_outliers(
        item_id: str,
        client: Any = Depends(get_influx_client),
        range: str = Query(default="7d", description="Range, e.g. '7d' or 'all'."),
        interval: str | None = Query(
            default=None, description="Optional downsample interval, e.g. '1h'."
        ),
        method: str = Query(
            default="zscore", description="Outlier-detection method to apply."
        ),
    ) -> PriceSeriesResponse:
        """Return only the outlier points for an item (Req 16.2).

        Args:
            item_id: The item identifier to query.
            client: The injected InfluxDB client.
            range: Time range (a duration such as ``"7d"`` or ``"all"``).
            interval: Optional downsample interval (Req 16.6 guardrail applies).
            method: Registered outlier-detector name to select (validated).

        Returns:
            A :class:`PriceSeriesResponse` whose ``points`` contain only the
            points flagged as outliers, in strictly ascending time order.

        Raises:
            HTTPException: 400 on invalid params/guardrail; 404 when the item has
                no data.
        """
        _validate_method(method)
        start, stop = _resolve_range_or_400(range)
        _enforce_range_guardrail(start, stop, interval)

        series = influx.query_price_series(
            client, item_id, start, stop, interval
        )
        if not series:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No price data found for item {item_id!r}.",
            )

        points = [p for p in _to_price_points(series, method=method) if p.isOutlier]
        return PriceSeriesResponse(
            itemId=item_id,
            interval=interval or "raw",
            points=points,
        )

    @app.get(
        "/api/datasets/export",
        tags=["datasets"],
        dependencies=[Depends(require_auth)],
    )
    def export_dataset(
        client: Any = Depends(get_influx_client),
        items: str = Query(..., description="Comma-separated item ids."),
        range: str = Query(default="all", description="Range, e.g. '7d' or 'all'."),
        interval: str = Query(
            default="1h", description="Downsample interval, e.g. '1h'."
        ),
        format: str = Query(
            default="ndjson", description="Export format: ndjson, csv, or parquet."
        ),
    ) -> StreamingResponse:
        """Stream a bulk multi-item dataset with chunked transfer (Req 16.4).

        The response is always a :class:`StreamingResponse` wrapping
        :func:`ge_pipeline.data_access.stream_dataset`, so the full result set is
        never buffered even for small exports.

        Args:
            client: The injected InfluxDB client.
            items: Comma-separated item ids (capped at
                :data:`MAX_ITEMS_PER_REQUEST`).
            range: Time range (a duration such as ``"7d"`` or ``"all"``).
            interval: Downsample interval applied server-side.
            format: One of :data:`ge_pipeline.data_access.SUPPORTED_EXPORT_FORMATS`.

        Returns:
            A chunked :class:`StreamingResponse` of the encoded dataset.

        Raises:
            HTTPException: 400 on invalid items, range, or unsupported format.
        """
        item_ids = _parse_items_param(items)
        start, stop = _resolve_range_or_400(range)

        if format not in data_access.SUPPORTED_EXPORT_FORMATS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Unsupported export format {format!r}; expected one of "
                    f"{list(data_access.SUPPORTED_EXPORT_FORMATS)}."
                ),
            )

        media_type = _EXPORT_MEDIA_TYPES.get(format, "application/octet-stream")
        stream = data_access.stream_dataset(
            client, item_ids, start, stop, interval, format
        )
        logger.debug(
            "Streaming export items=%d format=%s range=%r interval=%s",
            len(item_ids),
            format,
            range,
            interval,
        )
        return StreamingResponse(
            stream,
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="dataset.{format}"'
            },
        )

    @app.get(
        "/api/datasets/features",
        tags=["datasets"],
        dependencies=[Depends(require_auth)],
    )
    def dataset_features(
        client: Any = Depends(get_influx_client),
        items: str = Query(..., description="Comma-separated item ids."),
        range: str = Query(default="all", description="Range, e.g. '7d' or 'all'."),
        interval: str = Query(
            default="1h", description="Downsample interval, e.g. '1h'."
        ),
    ) -> list[dict[str, Any]]:
        """Return an ML-ready feature frame as JSON records (Req 11.5).

        Args:
            client: The injected InfluxDB client.
            items: Comma-separated item ids (capped at
                :data:`MAX_ITEMS_PER_REQUEST`).
            range: Time range (a duration such as ``"7d"`` or ``"all"``).
            interval: Downsample interval applied server-side.

        Returns:
            A list of records (one per time index) with a ``time`` key and
            per-item feature columns. An empty list when no data exists.

        Raises:
            HTTPException: 400 on invalid items or range.
        """
        item_ids = _parse_items_param(items)
        start, stop = _resolve_range_or_400(range)

        frame = data_access.build_feature_frame(
            client, item_ids, start, stop, interval
        )
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            return []

        # Surface the time index as a column so each record is self-describing.
        records = frame.reset_index().to_dict(orient="records")
        logger.debug("Feature frame returned %d records.", len(records))
        return records

    return app


#: Module-level application instance for ASGI servers, e.g.
#: ``uvicorn ge_pipeline.api:app``.
app = create_app()
