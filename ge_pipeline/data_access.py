"""Scale-oriented data access layer for bulk, streaming, and export reads.

Exposes ``iter_time_chunks`` (lazy time partitioning), ``stream_dataset``
(NDJSON/CSV/Parquet streaming encoder), ``get_price_page`` (cursor-based
pagination), and ``build_feature_frame`` (tidy ML-ready DataFrame), all designed
to bound memory regardless of range length.

``stream_dataset`` walks the range one time-chunk at a time via
``iter_time_chunks`` and encodes each chunk's rows as it goes, so at most one
chunk's worth of rows is held in memory. NDJSON and CSV are emitted as true
incremental byte streams. Parquet is a single columnar container whose footer
must reference every row group, so its encoded bytes necessarily accumulate in
an in-memory buffer as row groups are appended; even so, only one chunk's *rows*
(one Arrow table) are materialized at a time, honoring the bounded-memory
postcondition on rows (see :func:`stream_dataset`).

These reads build on the parameterized query builders in
:mod:`ge_pipeline.influx` (``query_price_series`` / ``query_chunk``). The
``influx`` module is referenced at call time rather than imported by name so
this layer stays decoupled from the exact order in which those query builders
land.
"""

from __future__ import annotations

import datetime as _dt
import io
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from . import influx

__all__ = [
    "MAX_PAGE_LIMIT",
    "STREAM_CHUNK_SECONDS",
    "SUPPORTED_EXPORT_FORMATS",
    "PricePage",
    "iter_time_chunks",
    "stream_dataset",
    "get_price_page",
    "build_feature_frame",
]

logger = logging.getLogger(__name__)

#: Upper bound on the number of points a single :func:`get_price_page` call may
#: return. Requests outside ``0 < limit <= MAX_PAGE_LIMIT`` are rejected with a
#: descriptive :class:`ValueError`.
MAX_PAGE_LIMIT = 1000

#: Default width, in seconds, of each time window walked by
#: :func:`stream_dataset`. One day bounds how many rows a single
#: :func:`ge_pipeline.influx.query_chunk` call can materialize while keeping the
#: number of chunks manageable for multi-year exports.
STREAM_CHUNK_SECONDS = 86_400

#: The export formats :func:`stream_dataset` knows how to encode. Any other
#: value is rejected with a descriptive :class:`ValueError` (Requirement 11.4).
SUPPORTED_EXPORT_FORMATS: tuple[str, ...] = ("ndjson", "csv", "parquet")

#: Candidate column names, in priority order, that may hold the per-row unix
#: timestamp in a DataFrame returned by :func:`ge_pipeline.influx.query_chunk`.
_TIME_COLUMN_CANDIDATES: tuple[str, ...] = ("_time", "time", "_start")

#: Candidate column names, in priority order, that may hold the item identifier
#: in a long-form DataFrame returned by :func:`ge_pipeline.influx.query_chunk`.
_ITEM_COLUMN_CANDIDATES: tuple[str, ...] = ("itemID", "item_id", "item")


def _to_unix_seconds(value: Any) -> int:
    """Coerce a timestamp-like value to an integer unix-seconds moment.

    Real InfluxDB reads return the ``_time`` column as a
    :class:`pandas.Timestamp` (or ``datetime``), while the in-memory fakes used
    by unit/property tests use plain ``int``/``float`` unix seconds. This helper
    accepts all of those uniformly so downstream ordering checks and point
    extraction work regardless of the source.

    Args:
        value: An ``int``, ``float``, :class:`datetime.datetime`,
            :class:`pandas.Timestamp`, or numeric string carrying a moment. A
            :class:`pandas.Timestamp`/``datetime`` is interpreted as unix
            seconds since the epoch.

    Returns:
        The moment as whole unix seconds.

    Raises:
        TypeError: If ``value`` cannot be interpreted as a moment.
    """
    # Plain integers (and bools, which we treat numerically) pass straight
    # through. Guard bool explicitly since ``isinstance(True, int)`` is True.
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    # pandas.Timestamp is a subclass-friendly datetime; NaT and other pandas
    # NA sentinels raise cleanly below rather than silently coercing.
    if isinstance(value, pd.Timestamp):
        if value is pd.NaT:
            raise TypeError("cannot convert NaT to unix seconds")
        return int(value.timestamp())
    if isinstance(value, _dt.datetime):
        return int(value.timestamp())
    # Fall back to pandas' broad parser for numeric strings, numpy datetimes,
    # and numpy integers/floats.
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"cannot interpret {value!r} as a unix-seconds moment"
            ) from exc
    if parsed is pd.NaT:
        raise TypeError(f"cannot interpret {value!r} as a unix-seconds moment")
    return int(parsed.timestamp())


@dataclass(frozen=True)
class PricePage:
    """A single cursor-paginated page of price points.

    Mirrors the ``PricePage`` transport model consumed by the API/SPA
    (``{itemId, interval, points, nextCursor}``) using PEP 8 snake_case field
    names on the Python side.

    Attributes:
        item_id: The item identifier the points belong to.
        interval: The downsample interval used to build the points
            (for example ``"5m"`` or ``"1h"``).
        points: The time-ascending price points in this page (at most
            ``limit`` of them). Each point is whatever
            :func:`ge_pipeline.influx.query_price_series` yields (a mapping or
            object carrying at least a ``time`` value).
        next_cursor: The ``time`` of the last returned point when more data
            exists beyond this page, otherwise ``None``.
    """

    item_id: str
    interval: str
    points: list[Any] = field(default_factory=list)
    next_cursor: int | None = None


def iter_time_chunks(
    start: int, stop: int, chunk_seconds: int
) -> Iterator[tuple[int, int]]:
    """Yield a contiguous, non-overlapping partition of ``[start, stop)``.

    Each yielded window ``(chunk_start, chunk_stop)`` is half-open and satisfies
    ``0 < chunk_stop - chunk_start <= chunk_seconds``. Consecutive windows are
    contiguous (chunk ``k``'s ``stop`` equals chunk ``k+1``'s ``start``) and
    their union is exactly ``[start, stop)``. No windows are produced when
    ``start == stop``.

    The generator holds at most one window at a time, so memory stays O(1)
    regardless of how long the range is.

    Args:
        start: Inclusive start of the range (unix seconds). Must be ``<= stop``.
        stop: Exclusive end of the range (unix seconds).
        chunk_seconds: Maximum width of each window in seconds. Must be ``> 0``.

    Yields:
        ``(chunk_start, chunk_stop)`` half-open windows covering ``[start, stop)``.

    Raises:
        ValueError: If ``start > stop`` or ``chunk_seconds <= 0``.
    """
    if start > stop:
        raise ValueError(f"start ({start}) must be <= stop ({stop})")
    if chunk_seconds <= 0:
        raise ValueError(f"chunk_seconds ({chunk_seconds}) must be > 0")

    # Loop invariant: before yielding a window, `chunk_start` equals
    # start + k*chunk_seconds for the current index k and chunk_start < stop.
    chunk_start = start
    while chunk_start < stop:
        chunk_stop = min(chunk_start + chunk_seconds, stop)
        yield (chunk_start, chunk_stop)
        chunk_start = chunk_stop


def _order_key(frame: pd.DataFrame, item_col: str | None) -> list[str]:
    """Return the column order used to keep a chunk time-ascending per item."""
    return [item_col] if item_col is not None else []


def _check_chunk_ordering(
    frame: pd.DataFrame,
    time_col: str,
    item_col: str | None,
    last_times: dict[Any, int],
) -> None:
    """Verify a chunk's rows are strictly time-ascending per item and update state.

    ``last_times`` maps an item key (or a single sentinel when no item column is
    present) to the greatest ``time`` emitted so far for that item. Because
    :func:`iter_time_chunks` yields contiguous, non-overlapping windows in
    ascending order, a later chunk must never contain a timestamp less than or
    equal to one already emitted for the same item; such a row is either a
    time-ordering violation or a duplicated boundary row and is rejected.

    Args:
        frame: The chunk DataFrame returned by
            :func:`ge_pipeline.influx.query_chunk`.
        time_col: The resolved timestamp column name.
        item_col: The resolved item-id column name, or ``None`` when the frame is
            single-item / wide.
        last_times: Mutable per-item high-water marks; updated in place.

    Raises:
        ValueError: If any row's ``time`` is not strictly greater than the last
            ``time`` seen for its item (a time-ordering violation).
    """
    _SINGLE = object()  # sentinel key when there is no item column
    for _, row in frame[[time_col] + _order_key(frame, item_col)].iterrows():
        moment = _to_unix_seconds(row[time_col])
        key = row[item_col] if item_col is not None else _SINGLE
        previous = last_times.get(key)
        if previous is not None and moment <= previous:
            item_label = key if item_col is not None else "series"
            raise ValueError(
                "time-ordering violation while streaming dataset: item "
                f"{item_label!r} produced time {moment} which is not strictly "
                f"after the previously emitted time {previous}"
            )
        last_times[key] = moment


def stream_dataset(
    client: Any,
    item_ids: list[str],
    start: int,
    stop: int,
    interval: str,
    fmt: str,
    *,
    chunk_seconds: int = STREAM_CHUNK_SECONDS,
) -> Iterator[bytes]:
    """Stream a multi-item price dataset as encoded byte chunks.

    Walks ``[start, stop)`` one time-window at a time via
    :func:`iter_time_chunks`, reads each window through
    :func:`ge_pipeline.influx.query_chunk`, and encodes that window's rows in the
    requested ``fmt``. At most one window's worth of rows is materialized at a
    time, so memory stays bounded regardless of how long the range is.

    Decoding and concatenating every yielded chunk reproduces exactly the rows
    the underlying chunked queries returned, with no loss and no duplication
    across window boundaries (windows are half-open and non-overlapping). Rows
    are emitted in strictly time-ascending order per item; any timestamp that is
    not strictly after the last one seen for its item is rejected as a
    time-ordering violation.

    Format notes:

    * ``ndjson`` and ``csv`` are emitted as true incremental byte streams. For
      CSV the header is written only with the first non-empty window so the
      concatenated stream is a single well-formed CSV document.
    * ``parquet`` is one columnar container whose footer must index every row
      group, so its encoded bytes are accumulated in an in-memory buffer as each
      window is appended as a row group. Only one window's *rows* (one Arrow
      table) are held at a time, honoring the bounded-rows contract; the encoded
      byte buffer itself necessarily spans the whole file.

    Args:
        client: The reusable InfluxDB client passed through to the query layer.
        item_ids: The item identifiers to include. Must be non-empty.
        start: Inclusive range start (unix seconds).
        stop: Exclusive range end (unix seconds).
        interval: Downsample interval (for example ``"5m"`` or ``"1h"``).
        fmt: Export format, one of :data:`SUPPORTED_EXPORT_FORMATS`.
        chunk_seconds: Width of each time window in seconds. Must be ``> 0``.

    Yields:
        Encoded ``bytes`` for the dataset in the requested format.

    Raises:
        ValueError: If ``fmt`` is unsupported, ``item_ids`` is empty, or a
            time-ordering violation is detected while streaming.
    """
    if fmt not in SUPPORTED_EXPORT_FORMATS:
        raise ValueError(
            f"unsupported export format {fmt!r}; expected one of "
            f"{list(SUPPORTED_EXPORT_FORMATS)}"
        )
    if not item_ids:
        raise ValueError("item_ids must be a non-empty list")

    logger.debug(
        "Streaming dataset fmt=%s items=%d range=[%s, %s) interval=%s "
        "chunk_seconds=%s",
        fmt,
        len(item_ids),
        start,
        stop,
        interval,
        chunk_seconds,
    )

    if fmt == "parquet":
        yield from _stream_parquet(
            client, item_ids, start, stop, interval, chunk_seconds
        )
    else:
        yield from _stream_text(
            client, item_ids, start, stop, interval, fmt, chunk_seconds
        )


def _iter_chunk_frames(
    client: Any,
    item_ids: list[str],
    start: int,
    stop: int,
    interval: str,
    chunk_seconds: int,
) -> Iterator[pd.DataFrame]:
    """Yield each non-empty, ordering-checked chunk frame in ascending time order.

    Shared helper for the text and parquet encoders: it queries one window at a
    time, skips empty windows, and enforces the strictly-ascending-per-item
    contract across window boundaries.
    """
    last_times: dict[Any, int] = {}
    for chunk_start, chunk_stop in iter_time_chunks(start, stop, chunk_seconds):
        frame = influx.query_chunk(
            client, item_ids, chunk_start, chunk_stop, interval
        )
        if not isinstance(frame, pd.DataFrame):
            frame = pd.DataFrame(frame)
        if frame.empty:
            continue

        time_col = _resolve_column(frame, _TIME_COLUMN_CANDIDATES)
        if time_col is not None:
            item_col = _resolve_column(frame, _ITEM_COLUMN_CANDIDATES)
            _check_chunk_ordering(frame, time_col, item_col, last_times)
        yield frame


def _stream_text(
    client: Any,
    item_ids: list[str],
    start: int,
    stop: int,
    interval: str,
    fmt: str,
    chunk_seconds: int,
) -> Iterator[bytes]:
    """Encode the dataset as an incremental NDJSON or CSV byte stream."""
    header_written = False
    for frame in _iter_chunk_frames(
        client, item_ids, start, stop, interval, chunk_seconds
    ):
        if fmt == "ndjson":
            payload = frame.to_json(orient="records", lines=True)
            if payload:
                # ``lines=True`` omits the trailing newline; add one so chunks
                # concatenate into valid NDJSON with one object per line.
                if not payload.endswith("\n"):
                    payload += "\n"
                yield payload.encode("utf-8")
        else:  # csv
            payload = frame.to_csv(index=False, header=not header_written)
            header_written = True
            if payload:
                yield payload.encode("utf-8")


def _stream_parquet(
    client: Any,
    item_ids: list[str],
    start: int,
    stop: int,
    interval: str,
    chunk_seconds: int,
) -> Iterator[bytes]:
    """Encode the dataset as a single Parquet container, yielding bytes lazily.

    Each chunk is appended as a row group to a shared in-memory buffer via a
    :class:`pyarrow.parquet.ParquetWriter`. Newly appended bytes are yielded as
    they accumulate, and the footer is flushed on close. Only one chunk's rows
    (one Arrow table) exist at a time.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    buffer = io.BytesIO()
    writer: pq.ParquetWriter | None = None
    yielded = 0

    def _drain() -> bytes:
        """Return buffer bytes appended since the last drain (without resetting)."""
        nonlocal yielded
        data = buffer.getvalue()
        new = data[yielded:]
        yielded = len(data)
        return new

    try:
        for frame in _iter_chunk_frames(
            client, item_ids, start, stop, interval, chunk_seconds
        ):
            table = pa.Table.from_pandas(frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(buffer, table.schema)
            else:
                # Align to the established schema so every row group matches.
                table = table.cast(writer.schema)
            writer.write_table(table)
            new = _drain()
            if new:
                yield new
    finally:
        if writer is not None:
            writer.close()
            tail = _drain()
            if tail:
                yield tail


def _point_time(point: Any) -> int:
    """Return the unix ``time`` of a price point regardless of its container.

    Supports both mapping-style points (``point["time"]``) and object/attribute
    style points (``point.time``).

    Args:
        point: A single price point produced by the query layer.

    Returns:
        The point's ``time`` as an ``int``.

    Raises:
        TypeError: If no ``time`` value can be extracted from ``point``.
    """
    if isinstance(point, dict):
        if "time" in point:
            return _to_unix_seconds(point["time"])
    else:
        value = getattr(point, "time", None)
        if value is not None:
            return _to_unix_seconds(value)
    raise TypeError(f"price point has no 'time' value: {point!r}")


def _extract_points(series: Any) -> list[Any]:
    """Return the list of points from a ``query_price_series`` result.

    Accepts either a mapping with a ``"points"`` key or an object exposing a
    ``points`` attribute; falls back to treating ``series`` as an already-plain
    iterable of points.

    Args:
        series: The value returned by
            :func:`ge_pipeline.influx.query_price_series`.

    Returns:
        A list of price points.
    """
    if isinstance(series, dict):
        return list(series.get("points", []))
    points = getattr(series, "points", None)
    if points is not None:
        return list(points)
    return list(series)


def get_price_page(
    client: Any,
    item_id: str,
    start: int,
    stop: int,
    interval: str,
    cursor: int | None,
    limit: int,
) -> PricePage:
    """Return one cursor-paginated page of time-ascending price points.

    Points begin strictly after ``cursor`` (or at ``start`` when ``cursor`` is
    ``None``) and at most ``limit`` are returned. When more data exists beyond
    the returned page, ``next_cursor`` is set to the ``time`` of the last
    returned point; otherwise it is ``None`` (including the case where no points
    are returned). Following the returned cursors from ``cursor=None`` until
    ``next_cursor`` is ``None`` visits every point exactly once with no gaps or
    overlaps.

    Args:
        client: The reusable InfluxDB client passed through to the query layer.
        item_id: The item identifier to page over.
        start: Inclusive range start (unix seconds).
        stop: Exclusive range end (unix seconds).
        interval: Downsample interval (for example ``"5m"``).
        cursor: The ``time`` of the last point from the previous page, or
            ``None`` to start at ``start``.
        limit: Maximum number of points to return. Must satisfy
            ``0 < limit <= MAX_PAGE_LIMIT``.

    Returns:
        A :class:`PricePage` with the page's points and the next cursor.

    Raises:
        ValueError: If ``limit`` is not within ``0 < limit <= MAX_PAGE_LIMIT``.
    """
    if not 0 < limit <= MAX_PAGE_LIMIT:
        raise ValueError(
            f"limit ({limit}) must satisfy 0 < limit <= {MAX_PAGE_LIMIT}"
        )

    series = influx.query_price_series(client, item_id, start, stop, interval)
    points = _extract_points(series)

    # Ensure ascending time order so pagination is deterministic even if the
    # query layer does not guarantee it.
    points.sort(key=_point_time)

    # Keep only points strictly after the cursor (or all points when starting).
    if cursor is not None:
        remaining = [p for p in points if _point_time(p) > cursor]
    else:
        remaining = points

    page_points = remaining[:limit]

    # More data exists beyond this page only when we truncated the remainder.
    if page_points and len(remaining) > limit:
        next_cursor: int | None = _point_time(page_points[-1])
    else:
        next_cursor = None

    return PricePage(
        item_id=item_id,
        interval=interval,
        points=page_points,
        next_cursor=next_cursor,
    )


def _resolve_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    """Return the first column in ``frame`` matching ``candidates``, else ``None``."""
    for name in candidates:
        if name in frame.columns:
            return name
    return None


def build_feature_frame(
    client: Any,
    item_ids: list[str],
    start: int,
    stop: int,
    interval: str,
) -> pd.DataFrame:
    """Build a tidy, time-indexed, ML-ready feature frame for the items.

    Delegates the bulk read to :func:`ge_pipeline.influx.query_chunk` and shapes
    the result into a wide, time-indexed frame with per-item columns so it can
    be fed directly to model training or analysis. When the underlying query
    returns a long-form frame (with an item-id column), it is pivoted so each
    ``(item, field)`` pair becomes its own ``"<itemID>_<field>"`` column.

    Args:
        client: The reusable InfluxDB client passed through to the query layer.
        item_ids: The item identifiers to include.
        start: Inclusive range start (unix seconds).
        stop: Exclusive range end (unix seconds).
        interval: Downsample interval (for example ``"1h"``).

    Returns:
        A time-indexed :class:`pandas.DataFrame` (index named ``"time"``) with
        per-item feature columns, sorted ascending by time. An empty frame is
        returned when the query yields no rows.
    """
    frame = influx.query_chunk(client, item_ids, start, stop, interval)

    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)

    if frame.empty:
        return frame

    time_col = _resolve_column(frame, _TIME_COLUMN_CANDIDATES)
    item_col = _resolve_column(frame, _ITEM_COLUMN_CANDIDATES)

    if time_col is None:
        # Nothing to index on; return the frame unchanged so callers still get
        # whatever the query produced rather than raising.
        return frame

    if item_col is not None:
        # Long form -> pivot to wide, per-item columns.
        value_cols = [
            col for col in frame.columns if col not in (time_col, item_col)
        ]
        wide = frame.pivot_table(
            index=time_col,
            columns=item_col,
            values=value_cols,
        )
        # Flatten the (field, item) MultiIndex columns to "<item>_<field>".
        wide.columns = [
            f"{item}_{field}" for field, item in wide.columns.to_flat_index()
        ]
        wide.index.name = "time"
        return wide.sort_index()

    # Already wide: just index on time.
    indexed = frame.set_index(time_col).sort_index()
    indexed.index.name = "time"
    return indexed
