"""Downsample raw price data into a coarser rollup measurement.

InfluxDB 3 **Core** does not compact its gen1 Parquet files (compaction is an
Enterprise-only service), so a long history backfill leaves thousands of small
files and wide/unbounded queries eventually exceed the ``--query-file-limit``
cap. This module provides the supported do-it-yourself alternative: a *rollup*
that re-reads the raw ``itemPrice`` measurement in bounded time windows,
aggregates each window with a ``date_bin`` interval (e.g. 1 hour), and writes the
far smaller aggregated result into a separate rollup measurement (e.g.
``itemPrice_1h``). Dashboards and exports can then read the rollup for long
historical ranges while keeping the raw measurement for recent, fine-grained
data.

This is a rewrite-through-the-database, not a file-level merge: the engine's
catalog stays authoritative and no Parquet files are touched directly, which is
the only safe way to reduce effective file pressure on Core.

Design notes:

* **Bounded memory.** The range is walked one window at a time via
  :func:`ge_pipeline.data_access.iter_time_chunks`, and each window is read with
  the already-parameterized :func:`ge_pipeline.influx.query_chunk` (which applies
  the ``date_bin`` mean downsampling server-side). At most one window's rows are
  materialized at once, matching the project's scale rules.
* **Schema preserved.** Rolled-up records reuse the exact camelCase field names
  and the ``itemID`` tag, so the rollup measurement is schema-identical to the
  raw one and the existing query builders can read it via their ``measurement``
  override. Averaged price fields stay floats; the two volume fields are rounded
  to whole integers (a rolled-up volume is still a count).
* **Write path reuse.** Aggregated rows are turned into the same record dict
  shape produced by :func:`ge_pipeline.ingestion.build_price_records`, only with
  ``measurement`` set to the rollup name, and written through
  :func:`ge_pipeline.influx.write_batch` (retry/backoff, seconds precision,
  never an empty ``fields`` dict).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import pandas as pd

from . import influx
from .config import Settings
from .data_access import iter_time_chunks

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_ROLLUP_INTERVAL",
    "DEFAULT_ROLLUP_MEASUREMENT",
    "DEFAULT_WINDOW_SECONDS",
    "RollupResult",
    "build_rollup_records",
    "rollup_window",
    "run_rollup",
]

#: Default downsample interval for the rollup (hourly means).
DEFAULT_ROLLUP_INTERVAL = "1h"

#: Default measurement name the rollup writes into. Kept schema-identical to the
#: raw ``itemPrice`` measurement so the existing query builders can read it.
DEFAULT_ROLLUP_MEASUREMENT = "itemPrice_1h"

#: Default width, in seconds, of each read/aggregate/write window. One week
#: bounds how many rows a single ``query_chunk`` scan materializes while keeping
#: the per-query Parquet-file scan under Core's ``--query-file-limit`` cap.
DEFAULT_WINDOW_SECONDS = 7 * 86_400

#: The two fields whose averaged value is rounded back to a whole integer. The
#: two price fields keep their floating-point mean.
_VOLUME_FIELDS = ("highPriceVolume", "lowPriceVolume")


@dataclass
class RollupResult:
    """Counts describing the outcome of a rollup run.

    Attributes:
        windows: Number of time windows processed (including empty ones).
        rows_read: Number of aggregated ``(itemID, bin)`` rows read from the raw
            measurement across all windows.
        records_written: Number of rollup records written to the rollup
            measurement (rows with at least one non-null field).
        failures: Number of windows whose write was dropped after the write
            retries were exhausted.
    """

    windows: int = 0
    rows_read: int = 0
    records_written: int = 0
    failures: int = 0


def _coerce_field_value(name: str, value: object) -> float | int | None:
    """Coerce one averaged field value to its stored type, or ``None``.

    Null/NaN values are dropped so no record ever carries a ``None`` field.
    Volume fields are rounded to whole integers (a count stays a count); price
    fields keep their floating-point mean.

    Args:
        name: The camelCase field name.
        value: The averaged value from the ``date_bin`` query result.

    Returns:
        The coerced value, or ``None`` when the value is missing/NaN.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return None
    number = float(value)
    if math.isnan(number) or math.isinf(number):
        return None
    if name in _VOLUME_FIELDS:
        return round(number)
    return number


def build_rollup_records(
    frame: pd.DataFrame, measurement: str
) -> list[dict]:
    """Convert an aggregated multi-item frame into rollup record dicts.

    Takes the long-form frame produced by
    :func:`ge_pipeline.influx.query_chunk` with an interval (columns ``itemID``,
    ``time``, and the four camelCase price/volume fields, one row per
    ``(itemID, bin)``) and emits one record per row whose ``fields`` mapping
    holds only the non-null coerced values. Rows whose every field is null are
    skipped so no record with an empty ``fields`` dict is ever produced.

    Args:
        frame: The aggregated result frame from ``query_chunk``.
        measurement: The rollup measurement name to stamp on each record.

    Returns:
        A list of record dicts in the ``{measurement, tags, time, fields}``
        shape expected by :func:`ge_pipeline.influx.write_batch`.
    """
    if frame is None or frame.empty:
        return []

    if "itemID" not in frame.columns or "time" not in frame.columns:
        raise ValueError(
            "rollup frame must have 'itemID' and 'time' columns; got "
            f"{list(frame.columns)}"
        )

    records: list[dict] = []
    for row in frame.to_dict(orient="records"):
        moment = influx._time_to_unix_seconds(row.get("time"))
        if moment is None:
            continue

        fields: dict[str, float | int] = {}
        for name in influx.PRICE_FIELDS:
            coerced = _coerce_field_value(name, row.get(name))
            if coerced is not None:
                fields[name] = coerced

        if not fields:
            continue

        item_id = row.get("itemID")
        if item_id is None:
            continue

        records.append(
            {
                "measurement": measurement,
                "tags": {"itemID": str(item_id)},
                "time": moment,
                "fields": fields,
            }
        )
    return records


def rollup_window(
    client,
    database: str,
    item_ids: list[str],
    start: int,
    stop: int,
    interval: str,
    measurement: str,
) -> tuple[int, int]:
    """Read, aggregate, and write one time window's rollup.

    Reads ``[start, stop)`` for ``item_ids`` with ``date_bin`` averaging via
    :func:`ge_pipeline.influx.query_chunk`, converts the result to rollup
    records, and writes them to ``measurement`` through
    :func:`ge_pipeline.influx.write_batch`.

    Args:
        client: The reusable InfluxDB 3 client.
        database: Target database name (for logging/traceability).
        item_ids: The items to include in this window.
        start: Inclusive window start (unix seconds).
        stop: Exclusive window stop (unix seconds).
        interval: The ``date_bin`` downsample interval (e.g. ``"1h"``).
        measurement: The rollup measurement to write into.

    Returns:
        A ``(rows_read, records_written)`` tuple for this window.
    """
    frame = influx.query_chunk(client, item_ids, start, stop, interval)
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    rows_read = 0 if frame.empty else len(frame)

    records = build_rollup_records(frame, measurement)
    if records:
        influx.write_batch(client, database, records)
    return rows_read, len(records)


def run_rollup(
    settings: Settings,
    start: int,
    stop: int,
    *,
    interval: str = DEFAULT_ROLLUP_INTERVAL,
    measurement: str = DEFAULT_ROLLUP_MEASUREMENT,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
    item_ids: list[str] | None = None,
) -> RollupResult:
    """Roll up raw price data over ``[start, stop)`` into a rollup measurement.

    Walks the range one bounded window at a time (lazy, O(1) windows in memory),
    and for each window downsamples the raw ``itemPrice`` data to ``interval``
    means and writes the result into ``measurement``. When ``item_ids`` is not
    supplied, the distinct items are discovered per window with a time-bounded
    :func:`ge_pipeline.influx.list_item_ids` scan so the enumeration itself stays
    under Core's Parquet-file cap.

    Args:
        settings: Application settings providing the InfluxDB connection and
            target database.
        start: Inclusive range start (unix seconds).
        stop: Exclusive range end (unix seconds).
        interval: The ``date_bin`` downsample interval (e.g. ``"1h"``, ``"1d"``).
        measurement: Rollup measurement name to write into (must be a
            ``[A-Za-z0-9_]`` identifier).
        window_seconds: Width of each read/aggregate/write window in seconds.
        item_ids: Optional explicit item list. When ``None`` items are
            discovered per window.

    Returns:
        A :class:`RollupResult` with window/row/record/failure counts.

    Raises:
        ValueError: If ``start > stop`` or ``window_seconds <= 0`` (surfaced by
            :func:`ge_pipeline.data_access.iter_time_chunks`).
    """
    client = influx.get_client(settings)
    database = settings.influx3_database
    result = RollupResult()

    logger.info(
        "rollup start: range=[%s, %s) interval=%s measurement=%r window=%ss",
        start,
        stop,
        interval,
        measurement,
        window_seconds,
    )

    for win_start, win_stop in iter_time_chunks(start, stop, window_seconds):
        result.windows += 1

        window_items = item_ids
        if window_items is None:
            window_items = influx.list_item_ids(
                client, start=win_start, stop=win_stop
            )
        if not window_items:
            logger.debug(
                "window [%s, %s) has no items; skipping", win_start, win_stop
            )
            continue

        rows_read, written = rollup_window(
            client,
            database,
            window_items,
            win_start,
            win_stop,
            interval,
            measurement,
        )
        result.rows_read += rows_read
        result.records_written += written

        logger.info(
            "window [%s, %s): items=%d rows_read=%d written=%d",
            win_start,
            win_stop,
            len(window_items),
            rows_read,
            written,
        )

    logger.info(
        "rollup complete: windows=%d rows_read=%d records_written=%d failures=%d",
        result.windows,
        result.rows_read,
        result.records_written,
        result.failures,
    )
    return result
