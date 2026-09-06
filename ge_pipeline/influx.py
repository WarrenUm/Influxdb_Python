"""InfluxDB 3 client lifecycle, batched writes, and parameterized queries.

Owns a single reusable :class:`InfluxDBClient3` (cached per
``(host, database, token)``), performs batched line-protocol writes, and builds
parameterized SQL queries (single-item, multi-item, latest timestamp, item
listing) with ``date_bin`` downsampling.

All read helpers bind user-supplied item IDs and time ranges as
``query_parameters`` rather than interpolating them into the query text, so
they can never be injected into a query string (Requirement 6.7).

The v3 storage schema is intentionally identical to the v2 layout: measurement
``itemPrice``, tag ``itemID``, and the four camelCase fields. Because those
identifiers are camelCase, every SQL identifier for the measurement, tag, and
fields is double-quoted so InfluxDB 3's SQL engine treats it as a case-sensitive
identifier rather than folding it to lowercase.

Query return shapes:

* :func:`get_latest_timestamp` -> ``int | None`` (unix seconds).
* :func:`query_price_series` -> ``list[dict]`` of time-ascending point dicts
  (the ``PriceSeries`` structure); each point carries ``time`` (unix seconds)
  plus ``avgHighPrice``/``avgLowPrice``/``highPriceVolume``/``lowPriceVolume``
  (``int``/``float``/``None``). Values may be floats when an ``interval`` is
  supplied because ``date_bin`` averaging computes the mean per window.
* :func:`query_chunk` -> :class:`pandas.DataFrame` (combined multi-item frame).
* :func:`list_item_ids` -> ``list[str]`` of distinct ``itemID`` tag values.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from influxdb_client_3 import InfluxDBClient3, Point, WritePrecision

from .config import Settings, get_settings
from .errors import TransientError
from .retry import retry_with_backoff

__all__ = [
    "get_client",
    "get_latest_timestamp",
    "list_item_ids",
    "query_chunk",
    "query_price_series",
    "write_batch",
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

# Process-wide cache of InfluxDB 3 clients keyed by the connection values that
# uniquely identify a client: ``(host, database, token)``. This lets callers
# reuse a single client across operations (per steering rules) instead of
# opening one per iteration (Requirement 1.3).
_client_cache: dict[tuple[str, str, str], InfluxDBClient3] = {}


def get_client(settings: Settings) -> InfluxDBClient3:
    """Return a reusable :class:`InfluxDBClient3` for the given settings.

    The client is created on first use and cached, keyed by the InfluxDB 3
    host URL, database, and token, so repeated calls with the same connection
    values return the identical client rather than opening a new connection per
    operation (Requirement 1.3).

    Args:
        settings: The application settings holding the InfluxDB 3 connection
            values (``influx3_host``, ``influx3_token``, ``influx3_database``).

    Returns:
        A configured, reusable :class:`InfluxDBClient3` instance.
    """
    key = (settings.influx3_host, settings.influx3_database, settings.influx3_token)
    client = _client_cache.get(key)
    if client is None:
        logger.debug(
            "Creating InfluxDB 3 client for host=%s database=%s",
            settings.influx3_host,
            settings.influx3_database,
        )
        client = InfluxDBClient3(
            host=settings.influx3_host,
            token=settings.influx3_token,
            database=settings.influx3_database,
        )
        _client_cache[key] = client
    return client


def _record_to_point(record: dict) -> Point:
    """Map a price record dict to an InfluxDB 3 :class:`Point`.

    The record uses the unchanged v2 schema shape produced by
    :func:`ge_pipeline.ingestion.build_price_records`: a ``measurement`` name
    (``itemPrice``), a ``tags`` mapping (``{"itemID": ...}``), a ``fields``
    mapping keyed by the four camelCase field names, and a ``time`` value in
    unix seconds. Field/tag/measurement names are carried over verbatim so the
    v3 schema stays identical to v2.

    Args:
        record: A record dict carrying ``measurement``, ``tags``, ``fields``,
            and ``time`` (unix seconds).

    Returns:
        A :class:`influxdb_client_3.Point` ready to be written with seconds
        precision.
    """
    point = Point(record["measurement"])
    for tag, value in record["tags"].items():
        point = point.tag(tag, value)
    for field, value in record["fields"].items():
        point = point.field(field, value)
    # ``Point.time`` defaults to nanosecond precision, so a unix-second value
    # like ``1600000000`` would otherwise be stored as nanoseconds regardless
    # of the ``write_precision`` passed to ``client.write``. Declare the
    # precision on the point itself so whole unix seconds are stamped
    # correctly (Requirement 3.3, 7.1).
    return point.time(record["time"], write_precision=WritePrecision.S)


def write_batch(
    client: InfluxDBClient3, database: str, records: list[dict]
) -> None:
    """Write a batch of price records to InfluxDB 3, retrying on failure.

    Each record is converted to a :class:`~influxdb_client_3.Point` via
    :func:`_record_to_point` (preserving the unchanged v2 schema) and the batch
    is written synchronously with seconds precision through the reused
    ``client``. Write failures are wrapped as
    :class:`~ge_pipeline.errors.TransientError` and retried with exponential
    backoff via the shared retry helper. If every attempt fails, the error is
    logged and the batch is dropped (this function does not re-raise) so that
    ingestion can continue and the next scheduled run can backfill the gap.

    An empty ``records`` list is a no-op (Requirement 7.2).

    Args:
        client: The reusable :class:`~influxdb_client_3.InfluxDBClient3` to
            write through. The client is already bound to the target database,
            so ``database`` is accepted for signature compatibility and logging.
        database: The target InfluxDB 3 database name (for logging/traceability).
        records: The record dicts to write. Each record is expected to carry
            ``measurement``, ``tags``, ``time``, and a non-empty ``fields``
            mapping.

    Returns:
        None. On final write failure the batch is dropped rather than raised.
    """
    if not records:
        logger.debug("write_batch called with no records; nothing to write")
        return

    @retry_with_backoff()
    def _do_write() -> None:
        """Perform a single write, wrapping failures as transient."""
        try:
            points = [_record_to_point(record) for record in records]
            client.write(record=points, write_precision=WRITE_PRECISION)
            logger.debug(
                "Wrote %d records to database %r", len(records), database
            )
        except Exception as exc:
            raise TransientError(f"InfluxDB 3 write failed: {exc}") from exc

    try:
        _do_write()
    except TransientError as exc:
        # Retries exhausted: log and drop the batch so ingestion continues.
        logger.error(
            "Dropping batch of %d record(s) for database %r after write retries "
            "were exhausted: %s",
            len(records),
            database,
            exc,
        )


# --- Query helpers ---------------------------------------------------------

# SQL language name passed to ``client.query`` for every read (Requirement 6.1).
_SQL = "sql"

# Double-quoted, case-sensitive SQL identifiers for the preserved v2 schema.
# The camelCase names would fold to lowercase (and fail to resolve) if left
# unquoted, so every measurement/tag/field identifier is quoted (design:
# "Storage seam").
_QUOTED_MEASUREMENT = f'"{MEASUREMENT}"'
_TAG_ITEM_ID = '"itemID"'


def _quote_measurement(measurement: str | None) -> str:
    """Return a safe, double-quoted SQL identifier for a measurement name.

    Read helpers accept an optional ``measurement`` so callers can query a
    rollup/downsampled measurement (e.g. ``itemPrice_1h``) with the same
    parameterized query builders. Measurement names are internal, code-supplied
    identifiers (never request text), but they are still validated to a
    conservative ``[A-Za-z0-9_]`` charset before being embedded in the query so
    a stray value can never break out of the quoted identifier.

    Args:
        measurement: The measurement name, or ``None`` to use the default
            :data:`MEASUREMENT` (``itemPrice``).

    Returns:
        The measurement as a double-quoted, case-sensitive SQL identifier.

    Raises:
        ValueError: If ``measurement`` contains characters outside
            ``[A-Za-z0-9_]`` or is empty.
    """
    name = MEASUREMENT if measurement is None else measurement
    if not name or not all(c.isalnum() or c == "_" for c in name):
        raise ValueError(
            f"invalid measurement name {name!r}; expected non-empty "
            "[A-Za-z0-9_] identifier"
        )
    return f'"{name}"'

# Comma-separated ``"avgHighPrice", "avgLowPrice", ...`` for raw SELECT lists.
_QUOTED_FIELD_LIST = ", ".join(f'"{name}"' for name in PRICE_FIELDS)

# Time-bound comparison expressions. The ``start``/``stop`` bounds are bound as
# unix-second integer parameters (JSON-serializable, unlike ``datetime``); each
# is cast to ``BIGINT`` (signed Int64) before ``to_timestamp_seconds`` because
# the driver infers unsigned ``UInt64`` for the bound literal, which
# ``to_timestamp_seconds`` rejects. The cast makes the conversion to a
# ``time``-comparable timestamp unambiguous while keeping the value bound (never
# interpolated, Requirement 6.7).
_START_TS = "to_timestamp_seconds(CAST($start AS BIGINT))"
_STOP_TS = "to_timestamp_seconds(CAST($stop AS BIGINT))"

# Supported duration suffixes for interval strings, mapped to seconds.
_DURATION_UNITS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}


def _resolve_database(database: str | None) -> str:
    """Return an explicit database or fall back to the configured v3 database.

    The database name is only used for signature/back-compat parity: the cached
    :class:`~influxdb_client_3.InfluxDBClient3` is already bound to its database,
    so reads run against the client's database regardless. This helper simply
    resolves a sensible non-``None`` value for callers that still pass one.

    Args:
        database: An explicit database name, or ``None`` to read
            ``Settings.influx3_database`` from
            :func:`ge_pipeline.config.get_settings`.

    Returns:
        The database name to associate with the query.
    """
    if database is not None:
        return database
    return get_settings().influx3_database


def _to_unix_seconds(value: float | datetime) -> int:
    """Coerce a unix-seconds timestamp or datetime into whole unix seconds.

    Time bounds are bound as JSON-serializable unix-second integers via
    ``query_parameters`` (never interpolated). The InfluxDB 3 client
    JSON-serializes query parameters over Flight, and :class:`datetime.datetime`
    is not JSON-serializable, so a bare ``datetime`` bound would raise
    ``TypeError: Object of type datetime is not JSON serializable``. Binding an
    ``int`` and converting it in SQL via ``to_timestamp_seconds($bound)`` keeps
    the comparison against the ``time`` column correct and unambiguous.

    Args:
        value: Unix seconds (``int``/``float``) or a :class:`datetime.datetime`.
            Naive datetimes are assumed to be UTC.

    Returns:
        The value as whole unix seconds.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp())
    return int(value)


def _interval_seconds(interval: str) -> int:
    """Parse a duration string (e.g. ``"5m"``, ``"1h"``) to whole seconds.

    The parsed value is a fixed, validated positive integer used to build a
    ``date_bin`` interval literal. It is never derived from untrusted request
    text: only the well-known suffix forms below (or a bare integer number of
    seconds) are accepted, and anything else raises, so the value can be safely
    embedded in the query text as ``INTERVAL '<n> seconds'``.

    Args:
        interval: A duration such as ``"30s"``, ``"5m"``, ``"1h"``, ``"1d"``, or
            ``"1w"``. A bare integer is treated as seconds.

    Returns:
        The equivalent number of whole seconds.

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
    return seconds


def _date_bin_expr(interval: str) -> str:
    """Return a ``date_bin`` SQL expression binning ``time`` to ``interval``.

    The interval is a validated positive integer of seconds (see
    :func:`_interval_seconds`), so embedding it as ``INTERVAL '<n> seconds'`` is
    safe; user-supplied values (item ids, time bounds) are always bound as
    parameters, never interpolated.
    """
    return f"date_bin(INTERVAL '{_interval_seconds(interval)} seconds', time)"


def _query_frame(
    client: InfluxDBClient3,
    query: str,
    query_parameters: dict,
) -> pd.DataFrame:
    """Run an SQL query and return the result as a :class:`pandas.DataFrame`.

    The InfluxDB 3 client returns a PyArrow object (``Table``/``RecordBatch``
    reader); this normalizes it to a DataFrame regardless of the concrete type.
    """
    result = client.query(
        query=query, language=_SQL, query_parameters=query_parameters
    )
    if result is None:
        return pd.DataFrame()
    if isinstance(result, pd.DataFrame):
        return result
    # PyArrow RecordBatchReader (FlightStreamReader) -> Table -> DataFrame.
    if hasattr(result, "read_all"):
        result = result.read_all()
    if hasattr(result, "to_pandas"):
        return result.to_pandas()
    return pd.DataFrame(result)


def _time_to_unix_seconds(value) -> int | None:
    """Coerce a result ``time`` value to whole unix seconds, or ``None``.

    Results may carry ``time`` as a :class:`pandas.Timestamp`, ``datetime``, or
    numeric unix seconds/nanoseconds depending on the driver; this normalizes
    them all to whole unix seconds.
    """
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if value is pd.NaT:
            return None
        return int(value.timestamp())
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, (int, float)):
        return int(value)
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if ts is pd.NaT:
        return None
    return int(ts.timestamp())


def get_latest_timestamp(
    client: InfluxDBClient3,
    item_id: str,
    *,
    database: str | None = None,
    measurement: str | None = None,
) -> int | None:
    """Return the most recent stored timestamp for an item, or ``None``.

    Runs a parameterized SQL query (the ``itemID`` value is bound via
    ``query_parameters``, not interpolated) that returns ``max(time)`` for the
    item as unix seconds.

    Args:
        client: The reusable :class:`~influxdb_client_3.InfluxDBClient3`.
        item_id: The item's ``itemID`` tag value.
        database: Optional database override; defaults to the configured v3
            database. The client is already bound to its database, so this is
            accepted for signature parity.
        measurement: Optional measurement override; defaults to the raw
            ``itemPrice`` measurement. Pass a rollup measurement name to read
            the latest rolled-up timestamp.

    Returns:
        The most recent timestamp for ``item_id`` in unix seconds, or ``None``
        when the item has no stored data.
    """
    _resolve_database(database)
    query = (
        f'SELECT max(time) AS latest FROM {_quote_measurement(measurement)} '
        f"WHERE {_TAG_ITEM_ID} = $item_id"
    )
    frame = _query_frame(client, query, {"item_id": item_id})
    if frame.empty or "latest" not in frame.columns:
        return None
    latest = frame["latest"].iloc[0]
    if pd.isna(latest):
        return None
    return _time_to_unix_seconds(latest)


def query_price_series(
    client: InfluxDBClient3,
    item_id: str,
    start: float | datetime,
    stop: float | datetime,
    interval: str | None = None,
    *,
    database: str | None = None,
    measurement: str | None = None,
) -> list[dict]:
    """Return a time-ascending price series for a single item.

    Uses a fully parameterized SQL query: the item ID and the ``start``/``stop``
    bounds are bound as ``query_parameters``. Without ``interval`` the raw
    points are selected and ordered by time; with ``interval`` the rows are
    binned with ``date_bin`` and each field is averaged per window
    (Requirement 6.4). Every camelCase identifier is double-quoted so it stays
    case-sensitive.

    Args:
        client: The reusable :class:`~influxdb_client_3.InfluxDBClient3`.
        item_id: The item's ``itemID`` tag value.
        start: Range start as unix seconds or a :class:`datetime.datetime`.
        stop: Range stop as unix seconds or a :class:`datetime.datetime`.
        interval: Optional downsample duration (e.g. ``"5m"``, ``"1h"``). When
            provided, ``date_bin`` mean downsampling is applied.
        database: Optional database override; defaults to the configured v3
            database.
        measurement: Optional measurement override; defaults to the raw
            ``itemPrice`` measurement. Pass a rollup measurement name (e.g.
            ``"itemPrice_1h"``) to read downsampled data.

    Returns:
        A list of point dicts sorted by ascending ``time`` (unix seconds). Each
        dict contains ``time`` plus the four price/volume fields (values may be
        ``None`` when absent, or floats when ``interval`` averaging is applied).
    """
    _resolve_database(database)
    quoted_measurement = _quote_measurement(measurement)
    params: dict = {
        "item_id": item_id,
        "start": _to_unix_seconds(start),
        "stop": _to_unix_seconds(stop),
    }
    where = (
        f"WHERE {_TAG_ITEM_ID} = $item_id "
        f"AND time >= {_START_TS} AND time < {_STOP_TS}"
    )

    if interval is None:
        query = (
            f'SELECT time, {_QUOTED_FIELD_LIST} '
            f"FROM {quoted_measurement} {where} ORDER BY time"
        )
    else:
        bin_expr = _date_bin_expr(interval)
        averaged = ", ".join(
            f'avg("{name}") AS "{name}"' for name in PRICE_FIELDS
        )
        query = (
            f"SELECT {bin_expr} AS time, {averaged} "
            f"FROM {quoted_measurement} {where} "
            f"GROUP BY {bin_expr} ORDER BY time"
        )

    frame = _query_frame(client, query, params)
    return _frame_to_series(frame)


def _frame_to_series(frame: pd.DataFrame) -> list[dict]:
    """Convert a single-item result frame to the ``list[dict]`` series shape.

    Each returned dict carries ``time`` (unix seconds) plus the four camelCase
    fields; missing fields are filled with ``None`` to preserve the existing
    return shape.
    """
    if frame.empty:
        return []
    series: list[dict] = []
    records = frame.to_dict(orient="records")
    for row in records:
        point: dict = {"time": _time_to_unix_seconds(row.get("time"))}
        for name in PRICE_FIELDS:
            value = row.get(name)
            point[name] = None if value is not None and pd.isna(value) else value
        series.append(point)
    return series


def query_chunk(
    client: InfluxDBClient3,
    item_ids: list[str],
    start: float | datetime,
    stop: float | datetime,
    interval: str | None = None,
    *,
    database: str | None = None,
    measurement: str | None = None,
) -> pd.DataFrame:
    """Return a combined multi-item price frame for the given items and range.

    Builds a parameterized SQL query whose ``WHERE "itemID" IN (...)`` list
    binds each id as its own ``$id0, $id1, ...`` parameter (never concatenated
    into the query text, Requirement 6.7), optionally applies ``date_bin`` mean
    downsampling when ``interval`` is provided, and returns one row per
    ``(itemID, time)`` with a column per price/volume field. Every camelCase
    identifier is double-quoted so it stays case-sensitive.

    Args:
        client: The reusable :class:`~influxdb_client_3.InfluxDBClient3`.
        item_ids: The ``itemID`` tag values to include.
        start: Range start as unix seconds or a :class:`datetime.datetime`.
        stop: Range stop as unix seconds or a :class:`datetime.datetime`.
        interval: Optional downsample duration (e.g. ``"5m"``, ``"1h"``). When
            provided, ``date_bin`` mean downsampling is applied.
        database: Optional database override; defaults to the configured v3
            database.
        measurement: Optional measurement override; defaults to the raw
            ``itemPrice`` measurement. Pass a rollup measurement name (e.g.
            ``"itemPrice_1h"``) to read downsampled data.

    Returns:
        A :class:`pandas.DataFrame` with one row per ``(itemID, time)`` and a
        column per price/volume field, sorted by ``itemID`` then ``time``.
        Empty ``item_ids`` returns an empty DataFrame.
    """
    if not item_ids:
        return pd.DataFrame()

    _resolve_database(database)
    quoted_measurement = _quote_measurement(measurement)
    placeholders = [f"id{index}" for index in range(len(item_ids))]
    params: dict = {name: value for name, value in zip(placeholders, item_ids)}
    params["start"] = _to_unix_seconds(start)
    params["stop"] = _to_unix_seconds(stop)

    in_list = ", ".join(f"${name}" for name in placeholders)
    where = (
        f"WHERE {_TAG_ITEM_ID} IN ({in_list}) "
        f"AND time >= {_START_TS} AND time < {_STOP_TS}"
    )

    if interval is None:
        query = (
            f'SELECT {_TAG_ITEM_ID}, time, {_QUOTED_FIELD_LIST} '
            f"FROM {quoted_measurement} {where} "
            f"ORDER BY {_TAG_ITEM_ID}, time"
        )
    else:
        bin_expr = _date_bin_expr(interval)
        averaged = ", ".join(
            f'avg("{name}") AS "{name}"' for name in PRICE_FIELDS
        )
        query = (
            f"SELECT {_TAG_ITEM_ID}, {bin_expr} AS time, {averaged} "
            f"FROM {quoted_measurement} {where} "
            f"GROUP BY {_TAG_ITEM_ID}, {bin_expr} "
            f"ORDER BY {_TAG_ITEM_ID}, time"
        )

    frame = _query_frame(client, query, params)
    if frame.empty:
        return frame
    return frame.reset_index(drop=True)


def list_item_ids(
    client: InfluxDBClient3,
    *,
    database: str | None = None,
    measurement: str | None = None,
    start: float | datetime | None = None,
    stop: float | datetime | None = None,
) -> list[str]:
    """Return the distinct ``itemID`` tag values stored in the database.

    Runs ``SELECT DISTINCT "itemID" FROM "itemPrice" ORDER BY "itemID"``.

    When ``start``/``stop`` bounds are supplied the distinct scan is restricted
    to that half-open time window. On InfluxDB 3 Core an unbounded
    ``SELECT DISTINCT`` scans the whole store and can exceed the Parquet file
    limit; a bounded window keeps the scan under the cap, which is why the
    rollup driver enumerates items window-by-window.

    Args:
        client: The reusable :class:`~influxdb_client_3.InfluxDBClient3`.
        database: Optional database override; defaults to the configured v3
            database.
        measurement: Optional measurement override; defaults to the raw
            ``itemPrice`` measurement.
        start: Optional inclusive start bound (unix seconds or datetime). When
            provided together with ``stop`` the distinct scan is time-bounded.
        stop: Optional exclusive stop bound (unix seconds or datetime).

    Returns:
        A sorted list of distinct ``itemID`` tag values.
    """
    _resolve_database(database)
    quoted_measurement = _quote_measurement(measurement)
    params: dict = {}
    where = ""
    if start is not None and stop is not None:
        params["start"] = _to_unix_seconds(start)
        params["stop"] = _to_unix_seconds(stop)
        where = f" WHERE time >= {_START_TS} AND time < {_STOP_TS}"
    query = (
        f'SELECT DISTINCT {_TAG_ITEM_ID} FROM {quoted_measurement}{where} '
        f"ORDER BY {_TAG_ITEM_ID}"
    )
    frame = _query_frame(client, query, params)
    if frame.empty or "itemID" not in frame.columns:
        return []
    return [str(value) for value in frame["itemID"].tolist() if value is not None]
