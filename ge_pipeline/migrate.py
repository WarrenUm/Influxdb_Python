"""One-pass migration tool: read InfluxDB v2, copy 1:1 into InfluxDB 3.

This module is the **sole** place the InfluxDB v2 client (``influxdb-client``)
and its Flux read logic survive after the v3 cutover (Requirements 1.5, 10.4).
Every other read/write path in the pipeline uses the v3 client via
:mod:`ge_pipeline.influx`.

The migration is a straight **1:1 copy with no name transformation**: because
the v3 schema is intentionally identical to the v2 layout, each v2 point
(measurement ``itemPrice``, tag ``itemID``, the four camelCase fields, and a
unix-second timestamp) is carried over verbatim. The ``itemID`` string, the
four numeric field values, and the timestamp are preserved exactly, with no
rename or transform (Requirements 5.1, 5.2).

The reader is split into two pieces so the transform is independently
unit/property testable (see spec task 6.2):

* :func:`_flux_record_to_record` -- a pure function that converts a single
  pivoted v2 Flux row (a mapping of column name to value) into the v3 record
  dict, preserving names 1:1 and dropping null fields.
* :func:`iter_v2_records` -- the live streaming reader that opens a Flux query
  stream against the v2 source and yields records via the pure transform,
  bounding memory by streaming one row at a time.

:func:`run_migration` ties these together for the CLI ``migrate`` command: it
resolves the validated v2 source, opens the v2 client alongside the cached v3
client, preflights **both** endpoints before writing anything (Requirement
5.6), then streams the source in a single pass, writing batches of
``settings.batch_size`` through a counting variant of the v3 write path so the
reported ``records_written`` reflects only records actually persisted
(Requirements 5.1, 5.3, 5.4, 5.5).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

# Retained ONLY in this module: the v2 client and Flux read logic (Req 1.5).
from influxdb_client import InfluxDBClient

from . import influx
from .config import Settings, require_migration_source
from .errors import TransientError
from .influx import MEASUREMENT, PRICE_FIELDS
from .retry import retry_with_backoff

__all__ = [
    "MigrationResult",
    "iter_v2_records",
    "run_migration",
]

logger = logging.getLogger(__name__)

#: The v2 tag that keys each price series. Preserved unchanged in v3.
_TAG_ITEM_ID = "itemID"

# Flux column names carrying the measurement, tag, and timestamp in a pivoted
# result. The four price/volume field columns use the same camelCase names as
# the v3 schema (``PRICE_FIELDS``), so they are copied over verbatim.
_COL_MEASUREMENT = "_measurement"
_COL_TIME = "_time"


@dataclass
class MigrationResult:
    """Counts reported when the migration completes (Requirement 5.5).

    Attributes:
        records_read: The number of Price_Snapshot records read from the v2
            source.
        records_written: The number of records successfully persisted to the
            v3 target.
    """

    records_read: int
    records_written: int


def _to_unix_seconds(value: Any) -> int:
    """Coerce a Flux ``_time`` value to whole unix seconds.

    v2 Flux returns ``_time`` as a timezone-aware :class:`datetime.datetime`;
    numeric unix-second values are also accepted for testability. The value is
    preserved exactly (truncated to whole seconds, matching the v2 write
    precision of ``s``), never rounded or shifted (Requirement 5.2).

    Args:
        value: A :class:`datetime.datetime` or a numeric unix-seconds value.

    Returns:
        The timestamp as whole unix seconds.

    Raises:
        TypeError: If ``value`` is neither a datetime nor a number.
    """
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, bool):  # guard: bool is an int subclass
        raise TypeError(f"unsupported timestamp value: {value!r}")
    if isinstance(value, (int, float)):
        return int(value)
    raise TypeError(f"unsupported timestamp value: {value!r}")


def _flux_record_to_record(values: Mapping[str, Any]) -> dict | None:
    """Convert one pivoted v2 Flux row into a v3 record dict (1:1, no rename).

    This is a pure function -- it takes a plain mapping of Flux column names to
    values (as produced by a ``pivot``-ed Flux query, i.e.
    :attr:`influxdb_client.client.flux_table.FluxRecord.values`) and returns the
    record dict consumed by :func:`ge_pipeline.influx.write_batch`. It performs
    no I/O so it can be unit/property tested in isolation (spec task 6.2).

    The v3 schema is identical to v2, so nothing is renamed: the ``itemPrice``
    measurement, the ``itemID`` tag value, the four camelCase field values, and
    the record timestamp are copied over exactly (Requirements 5.1, 5.2). Null
    fields are dropped, consistent with
    :func:`ge_pipeline.ingestion.build_price_records`; a row that carries no
    non-null price field is skipped entirely (returns ``None``).

    Args:
        values: A mapping of Flux column names to values for a single pivoted
            row. Recognized keys are ``_measurement``, ``itemID``, ``_time``,
            and the four camelCase field columns.

    Returns:
        A record dict of the form::

            {
                "measurement": "itemPrice",
                "tags": {"itemID": <id string>},
                "time": <unix seconds>,
                "fields": {<camelCase field>: <non-null value>, ...},
            }

        or ``None`` when the row has no non-null price field.
    """
    fields = {
        name: values[name]
        for name in PRICE_FIELDS
        if values.get(name) is not None
    }
    if not fields:
        return None

    measurement = values.get(_COL_MEASUREMENT) or MEASUREMENT

    return {
        "measurement": measurement,
        "tags": {_TAG_ITEM_ID: str(values[_TAG_ITEM_ID])},
        "time": _to_unix_seconds(values[_COL_TIME]),
        "fields": fields,
    }


def _build_flux_query(bucket: str) -> str:
    """Build the Flux query that reads every price point from the v2 source.

    Pivots the four fields into columns per ``(itemID, _time)`` so each streamed
    row carries the whole snapshot, matching the shape
    :func:`_flux_record_to_record` expects. The bucket name is the only
    interpolated value and comes from validated configuration
    (:func:`ge_pipeline.config.require_migration_source`), never from untrusted
    request input.

    Args:
        bucket: The v2 source bucket holding the ``itemPrice`` measurement.

    Returns:
        A Flux query string streaming the full ``itemPrice`` history.
    """
    return (
        f'from(bucket: "{bucket}")\n'
        "  |> range(start: 0)\n"
        f'  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")\n'
        '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], '
        'valueColumn: "_value")\n'
    )


def iter_v2_records(
    v2_client: InfluxDBClient,
    bucket: str,
    *,
    org: str | None = None,
) -> Iterator[dict]:
    """Stream v2 price records as v3 record dicts, preserving names 1:1.

    Reads the full ``itemPrice`` history from the v2 source via a Flux query and
    yields one v3 record dict per snapshot. Rows are streamed one at a time
    (``query_stream``) so memory stays bounded regardless of how much history
    the source holds (Requirement 5.1). Each row is converted by the pure
    :func:`_flux_record_to_record`, which preserves the measurement, ``itemID``
    tag, the four camelCase field values, and the timestamp exactly and drops
    null fields (Requirements 5.1, 5.2).

    Args:
        v2_client: An open :class:`influxdb_client.InfluxDBClient` connected to
            the v2 source.
        bucket: The v2 source bucket holding the ``itemPrice`` measurement.
        org: Optional organization to scope the query to; when ``None`` the
            client's configured organization is used.

    Yields:
        Record dicts ready for :func:`ge_pipeline.influx.write_batch`, one per
        v2 snapshot that has at least one non-null price field.
    """
    query = _build_flux_query(bucket)
    query_api = v2_client.query_api()
    kwargs: dict[str, Any] = {"query": query}
    if org is not None:
        kwargs["org"] = org
    for flux_record in query_api.query_stream(**kwargs):
        record = _flux_record_to_record(flux_record.values)
        if record is not None:
            yield record


def _preflight(
    v2_client: InfluxDBClient,
    v3_client: Any,
    *,
    source_url: str,
    target_host: str,
) -> None:
    """Confirm both migration endpoints are reachable before any write.

    Checks the v2 source and the v3 target in turn *before* the migration
    streams or writes a single record, so a dead source or target fails fast
    with a naming error and zero partial writes (Requirement 5.6):

    * **v2 source:** a lightweight :meth:`influxdb_client.InfluxDBClient.ping`
      call. Ping returning falsy or raising is treated as unreachable.
    * **v3 target:** a trivial parameter-free ``SELECT 1`` query through the
      cached v3 client (:meth:`influxdb_client_3.InfluxDBClient3.query`), which
      round-trips the connection without touching any data.

    This helper performs only reachability checks (no writes), so it is
    unit-testable with lightweight fakes for either client.

    Args:
        v2_client: The open v2 source client to ping.
        v3_client: The cached v3 target client to probe with a trivial query.
        source_url: The v2 source URL, used only in the error message.
        target_host: The v3 target host URL, used only in the error message.

    Raises:
        ConnectionError: If the v2 source is unreachable (message names
            ``source_url``) or the v3 target is unreachable (message names
            ``target_host``). The source is checked first.
    """
    try:
        reachable = v2_client.ping()
    except Exception as exc:  # any transport/client error means unreachable
        raise ConnectionError(
            f"Cannot reach InfluxDB v2 source at {source_url!r}: {exc}"
        ) from exc
    if not reachable:
        raise ConnectionError(
            f"Cannot reach InfluxDB v2 source at {source_url!r}: ping failed"
        )
    logger.debug("InfluxDB v2 source at %s is reachable", source_url)

    try:
        v3_client.query(query="SELECT 1", language="sql")
    except Exception as exc:  # any transport/client error means unreachable
        raise ConnectionError(
            f"Cannot reach InfluxDB 3 target at {target_host!r}: {exc}"
        ) from exc
    logger.debug("InfluxDB 3 target at %s is reachable", target_host)


def _write_batch_counting(v3_client: Any, database: str, records: list[dict]) -> int:
    """Write a batch to v3 and return the count persisted, raising on failure.

    This is the migration-specific counterpart to
    :func:`ge_pipeline.influx.write_batch`. The transform and schema are
    **identical** -- each record becomes a :class:`~influxdb_client_3.Point`
    via :func:`ge_pipeline.influx._record_to_point` and is written with the
    same seconds precision -- but the failure semantics differ:
    ``influx.write_batch`` drops-and-logs an exhausted batch so ingestion can
    continue, whereas migration must surface a failed batch as a failure so the
    reported ``records_written`` count stays accurate (Requirements 5.4, 5.5).

    The write is retried with exponential backoff via the shared
    :func:`ge_pipeline.retry.retry_with_backoff` helper (Requirement 5.4); once
    retries are exhausted the underlying :class:`~ge_pipeline.errors.TransientError`
    propagates rather than being swallowed.

    An empty ``records`` list is a no-op that returns ``0``.

    Args:
        v3_client: The cached v3 client to write through.
        database: The target v3 database name (for logging/traceability; the
            client is already bound to its database).
        records: The record dicts to write, in the unchanged v2 schema shape.

    Returns:
        The number of records persisted (``len(records)`` on success, ``0`` for
        an empty batch).

    Raises:
        TransientError: If the batch write fails after all retries are
            exhausted.
    """
    if not records:
        return 0

    @retry_with_backoff()
    def _do_write() -> None:
        try:
            points = [influx._record_to_point(record) for record in records]
            v3_client.write(record=points, write_precision=influx.WRITE_PRECISION)
        except Exception as exc:
            raise TransientError(f"InfluxDB 3 migration write failed: {exc}") from exc

    _do_write()
    logger.debug("Migrated batch of %d record(s) into database %r", len(records), database)
    return len(records)


def run_migration(settings: Settings) -> MigrationResult:
    """Copy all v2 price data into the v3 target in a single streaming pass.

    Resolves the validated v2 source via
    :func:`ge_pipeline.config.require_migration_source` (Requirement 2.4), opens
    the v2 source client, and reuses the cached v3 client from
    :func:`ge_pipeline.influx.get_client`. Both endpoints are preflighted before
    any write; if either is unreachable the run raises a connection error naming
    the unreachable server and stops before writing anything (Requirement 5.6).

    It then streams the full source history via :func:`iter_v2_records` in a
    single pass, accumulating records into batches of ``settings.batch_size``
    and persisting each batch through :func:`_write_batch_counting`, flushing
    the final partial batch at the end (Requirements 5.1, 5.3). Each record is a
    straight 1:1 copy with no name transformation, since the v3 schema is
    identical to v2 (Requirement 5.2). Because the counting write surfaces an
    exhausted batch as a failure rather than silently dropping it, the returned
    ``records_written`` reflects only records actually persisted (Requirements
    5.4, 5.5).

    The v2 source client is always closed before returning, even on error.

    Args:
        settings: The application settings, carrying the v3 target connection
            values and the optional v2 source values.

    Returns:
        A :class:`MigrationResult` reporting ``records_read`` (records streamed
        from the source) and ``records_written`` (records persisted to the
        target).

    Raises:
        ConfigError: If any required ``V2_*`` source variable is unset or empty.
        ConnectionError: If the v2 source or v3 target is unreachable at start.
        TransientError: If a batch write fails after all retries are exhausted.
    """
    source = require_migration_source(settings)
    v3_client = influx.get_client(settings)
    v2_client = InfluxDBClient(
        url=source.url, token=source.token, org=source.org
    )

    try:
        _preflight(
            v2_client,
            v3_client,
            source_url=source.url,
            target_host=settings.influx3_host,
        )

        batch: list[dict] = []
        records_read = 0
        records_written = 0

        for record in iter_v2_records(v2_client, source.bucket, org=source.org):
            records_read += 1
            batch.append(record)
            if len(batch) >= settings.batch_size:
                records_written += _write_batch_counting(
                    v3_client, settings.influx3_database, batch
                )
                batch = []

        # Flush the final partial batch.
        records_written += _write_batch_counting(
            v3_client, settings.influx3_database, batch
        )

        logger.info(
            "Migration complete: read %d record(s) from %s, wrote %d into %s",
            records_read,
            source.url,
            records_written,
            settings.influx3_database,
        )
        return MigrationResult(
            records_read=records_read, records_written=records_written
        )
    finally:
        v2_client.close()
