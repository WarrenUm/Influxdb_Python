"""InfluxDB client lifecycle, batched writes, and parameterized queries.

Owns a single reusable InfluxDB v2 client, performs batched line-protocol
writes, and builds parameterized Flux queries (single-item, multi-item, latest
timestamp, item listing) with server-side ``aggregateWindow`` downsampling.

All read helpers construct Flux with client-side *parameter binding* (the
``params`` argument of the InfluxDB query API) rather than string
interpolation, so user-supplied item IDs and time ranges can never be injected
into a query string (spec Requirement 9.1 / 19.2).

Query return shapes:

* :func:`get_latest_timestamp` -> ``int | None`` (unix seconds).
* :func:`query_price_series` -> ``list[dict]`` of time-ascending point dicts
  (the ``PriceSeries`` structure); each point carries ``time`` (unix seconds)
  plus ``avgHighPrice``/``avgLowPrice``/``highPriceVolume``/``lowPriceVolume``
  (``int``/``float``/``None``). Values may be floats when an ``interval`` is
  supplied because ``aggregateWindow`` computes the mean per window.
* :func:`query_chunk` -> :class:`pandas.DataFrame` (combined multi-item frame).
* :func:`list_item_ids` -> ``list[str]`` of distinct ``itemID`` tag values.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from .config import Settings, get_settings
from .errors import TransientError
from .retry import retry_with_backoff

__all__ = [
    "get_client",
    "write_batch",
    "get_latest_timestamp",
    "query_price_series",
    "query_chunk",
    "list_item_ids",
]

logger = logging.getLogger(__name__)

#: Measurement name for all stored price data.
MEASUREMENT = "itemPrice"

#: The four price/volume fields stored per item snapshot.
PRICE_FIELDS = (
    "avgHighPrice",
    "avgLowPrice",
    "highPriceVolume",
    "lowPriceVolume",
)

#: Write precision for all InfluxDB writes. The storage schema records
#: 5-minute snapshots stamped with unix-second timestamps, so writes use
#: seconds precision.
WRITE_PRECISION = "s"

#: Client timeout in milliseconds passed to :class:`InfluxDBClient`.
_CLIENT_TIMEOUT_MS = 10_000

# Process-wide cache of InfluxDB clients keyed by the connection values that
# uniquely identify a client. This lets callers reuse a single client across
# operations (per steering rules) instead of opening one per iteration.
_client_cache: dict[tuple[str, str, str], InfluxDBClient] = {}


def get_client(settings: Settings) -> InfluxDBClient:
    """Return a reusable :class:`InfluxDBClient` for the given settings.

    The client is created on first use and cached, keyed by the InfluxDB URL,
    organization, and token, so repeated calls with the same connection values
    return the identical client rather than opening a new connection per
    operation.

    Args:
        settings: The application settings holding the InfluxDB connection
            values (``influx_url``, ``influx_token``, ``influx_org``).

    Returns:
        A configured, reusable :class:`InfluxDBClient` instance.
    """
    key = (settings.influx_url, settings.influx_org, settings.influx_token)
    client = _client_cache.get(key)
    if client is None:
        logger.debug(
            "Creating InfluxDB client for url=%s org=%s",
            settings.influx_url,
            settings.influx_org,
        )
        client = InfluxDBClient(
            url=settings.influx_url,
            token=settings.influx_token,
            org=settings.influx_org,
            timeout=_CLIENT_TIMEOUT_MS,
        )
        _client_cache[key] = client
    return client


def write_batch(
    client: InfluxDBClient, bucket: str, records: list[dict]
) -> None:
    """Write a batch of price records to InfluxDB, retrying on transient failure.

    Records are written synchronously with seconds precision through the reused
    ``client``. Write failures are wrapped as
    :class:`~ge_pipeline.errors.TransientError` and retried with exponential
    backoff via the shared retry helper. If every attempt fails, the error is
    logged and the batch is dropped (this function does not re-raise) so that
    ingestion can continue and the next scheduled run can backfill the gap.

    An empty ``records`` list is a no-op.

    Args:
        client: The reusable :class:`InfluxDBClient` to write through.
        bucket: The target InfluxDB bucket name.
        records: The line-protocol record dicts to write. Each record is
            expected to carry ``measurement``, ``tags``, ``time``, and a
            non-empty ``fields`` mapping.

    Returns:
        None. On final write failure the batch is dropped rather than raised.
    """
    if not records:
        logger.debug("write_batch called with no records; nothing to write")
        return

    @retry_with_backoff()
    def _do_write() -> None:
        """Perform a single synchronous write, wrapping failures as transient."""
        try:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            write_api.write(
                bucket=bucket,
                write_precision=WRITE_PRECISION,
                record=records,
            )
            logger.debug("Wrote %d records to bucket %r", len(records), bucket)
        except (ApiException, OSError) as exc:
            raise TransientError(f"InfluxDB write failed: {exc}") from exc

    try:
        _do_write()
    except TransientError as exc:
        # Retries exhausted: log and drop the batch so ingestion continues.
        logger.error(
            "Dropping batch of %d record(s) for bucket %r after write retries "
            "were exhausted: %s",
            len(records),
            bucket,
            exc,
        )


# --- Query helpers ---------------------------------------------------------

# Flux filter matching the four stored price/volume fields. These are fixed
# schema constants (never user input), so embedding them in the query text is
# safe and keeps the bound parameters limited to user-supplied values.
_FIELD_FILTER = " or ".join(f'r._field == "{name}"' for name in PRICE_FIELDS)

# Columns InfluxDB adds to pivoted results that are not part of the price data.
_DROP_COLUMNS = ("result", "table", "_start", "_stop", "_measurement")

# Supported duration suffixes for interval strings, mapped to seconds.
_DURATION_UNITS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}


def _resolve_bucket(bucket: str | None) -> str:
    """Return an explicit bucket or fall back to the configured bucket.

    Args:
        bucket: An explicit bucket name, or ``None`` to read
            ``Settings.influx_bucket`` from :func:`ge_pipeline.config.get_settings`.

    Returns:
        The bucket name to query.
    """
    if bucket is not None:
        return bucket
    return get_settings().influx_bucket


def _to_datetime(value: int | float | datetime) -> datetime:
    """Coerce a unix-seconds timestamp or datetime into an aware UTC datetime.

    Bound as a Flux ``DateTimeLiteral`` by the query API, keeping raw ranges out
    of the query string.

    Args:
        value: Unix seconds (``int``/``float``) or a :class:`datetime.datetime`.
            Naive datetimes are assumed to be UTC.

    Returns:
        A timezone-aware UTC :class:`datetime.datetime`.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.fromtimestamp(value, tz=timezone.utc)


def _parse_interval(interval: str) -> timedelta:
    """Parse a Flux-style duration string (e.g. ``"5m"``, ``"1h"``) to timedelta.

    The parsed value is bound as a Flux ``DurationLiteral`` parameter for
    ``aggregateWindow`` rather than interpolated into the query text.

    Args:
        interval: A duration such as ``"30s"``, ``"5m"``, ``"1h"``, ``"1d"``, or
            ``"1w"``. A bare integer is treated as seconds.

    Returns:
        The equivalent :class:`datetime.timedelta`.

    Raises:
        ValueError: If ``interval`` is empty or not a recognized duration.
    """
    text = interval.strip()
    if not text:
        raise ValueError("interval must be a non-empty duration string")

    unit = text[-1]
    if unit.isdigit():
        seconds = int(text)
    else:
        if unit not in _DURATION_UNITS:
            raise ValueError(
                f"Unsupported interval unit {unit!r}; expected one of "
                f"{sorted(_DURATION_UNITS)}"
            )
        try:
            magnitude = int(text[:-1])
        except ValueError as exc:
            raise ValueError(f"Invalid interval {interval!r}") from exc
        seconds = magnitude * _DURATION_UNITS[unit]

    if seconds <= 0:
        raise ValueError("interval must be a positive duration")
    return timedelta(seconds=seconds)


def get_latest_timestamp(
    client: InfluxDBClient,
    item_id: str,
    *,
    bucket: str | None = None,
) -> int | None:
    """Return the most recent stored timestamp for an item, or ``None``.

    Runs a parameterized Flux query (the ``itemID`` value is bound, not
    interpolated) that scans all stored data for the item and returns the
    newest record's ``_time`` as unix seconds.

    Args:
        client: The reusable :class:`InfluxDBClient` to query through.
        item_id: The item's ``itemID`` tag value.
        bucket: Optional bucket override; defaults to the configured bucket.

    Returns:
        The most recent timestamp for ``item_id`` in unix seconds, or ``None``
        when the item has no stored data.
    """
    flux = (
        "from(bucket: _bucket)\n"
        "  |> range(start: 0)\n"
        f'  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")\n'
        "  |> filter(fn: (r) => r.itemID == _itemID)\n"
        f"  |> filter(fn: (r) => {_FIELD_FILTER})\n"
        "  |> group()\n"
        '  |> sort(columns: ["_time"], desc: true)\n'
        "  |> limit(n: 1)\n"
        '  |> keep(columns: ["_time"])'
    )
    params = {
        "_bucket": _resolve_bucket(bucket),
        "_itemID": item_id,
    }

    tables = client.query_api().query(query=flux, params=params)
    for table in tables:
        for record in table.records:
            moment = record.get_time()
            if moment is not None:
                return int(moment.timestamp())
    return None


def query_price_series(
    client: InfluxDBClient,
    item_id: str,
    start: int | float | datetime,
    stop: int | float | datetime,
    interval: str | None = None,
    *,
    bucket: str | None = None,
) -> list[dict]:
    """Return a time-ascending price series for a single item.

    Uses a fully parameterized Flux query: the item ID and the ``start``/``stop``
    bounds are bound as query parameters, and when ``interval`` is supplied a
    server-side ``aggregateWindow(every: interval, fn: mean)`` downsamples the
    data at that resolution. Rows are pivoted so each point carries all four
    price/volume fields.

    Args:
        client: The reusable :class:`InfluxDBClient` to query through.
        item_id: The item's ``itemID`` tag value.
        start: Range start as unix seconds or a :class:`datetime.datetime`.
        stop: Range stop as unix seconds or a :class:`datetime.datetime`.
        interval: Optional downsample duration (e.g. ``"5m"``, ``"1h"``). When
            provided, server-side ``aggregateWindow`` downsampling is applied.
        bucket: Optional bucket override; defaults to the configured bucket.

    Returns:
        A list of point dicts sorted by ascending ``time`` (unix seconds). Each
        dict contains ``time`` plus the four price/volume fields (values may be
        ``None`` when absent, or floats when ``interval`` averaging is applied).
    """
    aggregate = ""
    params: dict = {
        "_bucket": _resolve_bucket(bucket),
        "_itemID": item_id,
        "_start": _to_datetime(start),
        "_stop": _to_datetime(stop),
    }
    if interval is not None:
        aggregate = (
            "  |> aggregateWindow(every: _interval, fn: mean, "
            "createEmpty: false)\n"
        )
        params["_interval"] = _parse_interval(interval)

    flux = (
        "from(bucket: _bucket)\n"
        "  |> range(start: _start, stop: _stop)\n"
        f'  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")\n'
        "  |> filter(fn: (r) => r.itemID == _itemID)\n"
        f"  |> filter(fn: (r) => {_FIELD_FILTER})\n"
        f"{aggregate}"
        '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], '
        'valueColumn: "_value")\n'
        '  |> sort(columns: ["_time"])'
    )

    tables = client.query_api().query(query=flux, params=params)
    series: list[dict] = []
    for table in tables:
        for record in table.records:
            values = record.values
            moment = record.get_time()
            point: dict = {
                "time": int(moment.timestamp()) if moment is not None else None,
            }
            for field in PRICE_FIELDS:
                point[field] = values.get(field)
            series.append(point)
    return series


def query_chunk(
    client: InfluxDBClient,
    item_ids: list[str],
    start: int | float | datetime,
    stop: int | float | datetime,
    interval: str | None = None,
    *,
    bucket: str | None = None,
) -> pd.DataFrame:
    """Return a combined multi-item price frame for the given items and range.

    Builds a parameterized Flux query that matches any of ``item_ids`` via a
    bound array parameter (``contains``), optionally applies server-side
    ``aggregateWindow`` downsampling when ``interval`` is provided, and pivots
    per ``(_time, itemID)`` so each row carries all four price/volume fields.

    Args:
        client: The reusable :class:`InfluxDBClient` to query through.
        item_ids: The ``itemID`` tag values to include.
        start: Range start as unix seconds or a :class:`datetime.datetime`.
        stop: Range stop as unix seconds or a :class:`datetime.datetime`.
        interval: Optional downsample duration (e.g. ``"5m"``, ``"1h"``). When
            provided, server-side ``aggregateWindow`` downsampling is applied.
        bucket: Optional bucket override; defaults to the configured bucket.

    Returns:
        A :class:`pandas.DataFrame` with one row per ``(itemID, _time)`` and a
        column per price/volume field, sorted by ``itemID`` then ``_time``.
        Empty (``item_ids`` empty or no data) returns an empty DataFrame.
    """
    if not item_ids:
        return pd.DataFrame()

    aggregate = ""
    params: dict = {
        "_bucket": _resolve_bucket(bucket),
        "_itemIDs": list(item_ids),
        "_start": _to_datetime(start),
        "_stop": _to_datetime(stop),
    }
    if interval is not None:
        aggregate = (
            "  |> aggregateWindow(every: _interval, fn: mean, "
            "createEmpty: false)\n"
        )
        params["_interval"] = _parse_interval(interval)

    flux = (
        "from(bucket: _bucket)\n"
        "  |> range(start: _start, stop: _stop)\n"
        f'  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")\n'
        "  |> filter(fn: (r) => contains(value: r.itemID, set: _itemIDs))\n"
        f"  |> filter(fn: (r) => {_FIELD_FILTER})\n"
        f"{aggregate}"
        '  |> pivot(rowKey: ["_time", "itemID"], columnKey: ["_field"], '
        'valueColumn: "_value")\n'
        '  |> sort(columns: ["itemID", "_time"])'
    )

    frame = client.query_api().query_data_frame(query=flux, params=params)
    if isinstance(frame, list):
        frame = pd.concat(frame, ignore_index=True) if frame else pd.DataFrame()

    if frame.empty:
        return frame

    drop = [column for column in _DROP_COLUMNS if column in frame.columns]
    if drop:
        frame = frame.drop(columns=drop)
    return frame.reset_index(drop=True)


def list_item_ids(
    client: InfluxDBClient,
    *,
    bucket: str | None = None,
) -> list[str]:
    """Return the distinct ``itemID`` tag values stored in the bucket.

    Uses the InfluxDB ``schema.tagValues`` helper with the bucket bound as a
    query parameter.

    Args:
        client: The reusable :class:`InfluxDBClient` to query through.
        bucket: Optional bucket override; defaults to the configured bucket.

    Returns:
        A sorted list of distinct ``itemID`` tag values.
    """
    flux = (
        'import "influxdata/influxdb/schema"\n'
        "schema.tagValues(bucket: _bucket, tag: _tag)"
    )
    params = {
        "_bucket": _resolve_bucket(bucket),
        "_tag": "itemID",
    }

    tables = client.query_api().query(query=flux, params=params)
    item_ids: list[str] = []
    for table in tables:
        for record in table.records:
            value = record.get_value()
            if value is not None:
                item_ids.append(str(value))
    return sorted(item_ids)
