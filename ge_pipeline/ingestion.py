"""Async catch-up ingestion with bounded concurrency and rate-limit respect.

Exposes ``iter_missing_timestamps`` (lazy timestamp generator),
``build_price_records`` (null-filtered record construction), ``fetch_snapshot``
(async httpx fetch + validation), and ``run_catch_up`` (orchestration with a
concurrency semaphore, retry/backoff, batched writes, and an accurate
``IngestionResult``).

Implementation lands in later tasks (see spec tasks 6 and 8).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass

import httpx
from pydantic import ValidationError

from ge_pipeline import influx
from ge_pipeline.config import Settings
from ge_pipeline.errors import NonTransientError, TransientError
from ge_pipeline.retry import RETRY_AFTER_ATTR, retry_with_backoff

logger = logging.getLogger(__name__)


def iter_missing_timestamps(
    latest: int | None,
    now: int,
    interval: int = 300,
    earliest: int = 1615733100,
) -> Iterator[int]:
    """Lazily yield the missing 5-minute timestamps up to ``now``.

    Yields strictly increasing, ``interval``-aligned unix timestamps in the
    half-open range ``[start, now)``, where ``start`` is ``earliest`` when
    ``latest`` is ``None`` and ``latest + interval`` otherwise. The generator
    holds at most one timestamp in memory at a time, so memory stays flat
    regardless of how large the backfill range is.

    Args:
        latest: The most recent stored timestamp, or ``None`` when the store is
            empty. When provided it must be ``>= earliest``.
        now: The exclusive upper bound (current time). Must be ``>= earliest``.
        interval: The spacing between consecutive timestamps, in seconds. Must
            be ``> 0``. Defaults to 300 (5 minutes).
        earliest: The earliest timestamp to consider when the store is empty.
            Defaults to the project's first available snapshot.

    Yields:
        Consecutive timestamps ``start + k * interval`` (for ``k >= 0``) that
        are strictly less than ``now``.

    Raises:
        ValueError: If ``interval <= 0`` or ``now < earliest``, or if ``latest``
            is provided but is ``< earliest``.
    """
    if interval <= 0:
        raise ValueError(f"interval must be positive, got {interval}")
    if now < earliest:
        raise ValueError(f"now ({now}) must be >= earliest ({earliest})")
    if latest is not None and latest < earliest:
        raise ValueError(f"latest ({latest}) must be >= earliest ({earliest})")

    start = earliest if latest is None else latest + interval
    current = start
    while current < now:
        yield current
        current += interval
from ge_pipeline.models import FiveMinuteSnapshot

__all__ = [
    "IngestionResult",
    "build_price_records",
    "fetch_snapshot",
    "iter_missing_timestamps",
    "run_catch_up",
]

# Maps snake_case domain fields (from :class:`ItemPricePoint`) back to the
# camelCase InfluxDB storage/API field names (see design storage model).
_FIELD_NAME_MAP: dict[str, str] = {
    "avg_high_price": "avgHighPrice",
    "avg_low_price": "avgLowPrice",
    "high_price_volume": "highPriceVolume",
    "low_price_volume": "lowPriceVolume",
}

MEASUREMENT_NAME: str = "itemPrice"

#: Path (relative to the client's ``base_url``) of the Wiki API 5-minute
#: snapshot endpoint.
FIVE_MINUTE_ENDPOINT: str = "/5m"


def build_price_records(snapshot: FiveMinuteSnapshot) -> list[dict]:
    """Build InfluxDB records from a validated snapshot, filtering nulls.

    Iterates over each item in the snapshot and emits one InfluxDB-compatible
    record per item that has at least one non-null price field. Items whose
    every price field is ``None`` are excluded, so no empty or null-valued
    records are produced. Each record's ``fields`` dict contains only non-null
    values keyed by the camelCase storage field names
    (``avgHighPrice``, ``avgLowPrice``, ``highPriceVolume``, ``lowPriceVolume``).

    Args:
        snapshot: A validated :class:`FiveMinuteSnapshot` for one 5-minute
            window.

    Returns:
        A list of record dicts. Each record has the form::

            {
                "measurement": "itemPrice",
                "tags": {"itemID": <item id string>},
                "time": <snapshot timestamp>,
                "fields": {<camelCase field>: <non-null value>, ...},
            }

        The ``fields`` dict of every returned record is non-empty and contains
        no ``None`` values.
    """
    records: list[dict] = []

    for item_id, point in snapshot.data.items():
        fields = {
            storage_name: value
            for domain_name, storage_name in _FIELD_NAME_MAP.items()
            if (value := getattr(point, domain_name)) is not None
        }

        if not fields:
            continue

        records.append(
            {
                "measurement": MEASUREMENT_NAME,
                "tags": {"itemID": str(item_id)},
                "time": snapshot.timestamp,
                "fields": fields,
            }
        )

    return records


def _parse_retry_after(response: httpx.Response) -> int | None:
    """Parse the ``Retry-After`` header of a response into integer seconds.

    Only the delta-seconds form of the header is honored; an HTTP-date value or
    a missing/unparseable header yields ``None`` so the retry helper falls back
    to its computed exponential-backoff delay.

    Args:
        response: The HTTP response whose ``Retry-After`` header should be read.

    Returns:
        The number of seconds to wait as a non-negative ``int``, or ``None``
        when the header is absent or not an integer number of seconds.
    """
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        seconds = int(raw.strip())
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None


async def fetch_snapshot(client: httpx.AsyncClient, ts: int) -> FiveMinuteSnapshot:
    """Fetch and validate a single 5-minute snapshot from the Wiki API.

    Issues an asynchronous GET to the ``/5m`` endpoint with ``timestamp=ts`` over
    the supplied :class:`httpx.AsyncClient` (which the caller configures with the
    required ``User-Agent`` header and ``base_url``). A successful ``2xx``
    response body is validated into a :class:`FiveMinuteSnapshot`. Failures are
    classified so the retry helper can distinguish retryable from permanent
    errors.

    Args:
        client: An open :class:`httpx.AsyncClient` configured with the required
            ``User-Agent`` header and the Wiki API ``base_url``.
        ts: A non-negative unix timestamp aligned to a 5-minute boundary.

    Returns:
        The validated :class:`FiveMinuteSnapshot` for the requested window.

    Raises:
        TransientError: On a request timeout, connection error, HTTP ``5xx``, or
            HTTP ``429``. For a ``429`` the exception carries a ``retry_after``
            attribute parsed from the ``Retry-After`` header (or ``None`` when
            the header is absent or not an integer number of seconds), which the
            retry helper honors.
        NonTransientError: On a non-``429`` HTTP ``4xx`` response.
        pydantic.ValidationError: If a ``2xx`` body does not conform to the
            :class:`FiveMinuteSnapshot` schema.
    """
    try:
        response = await client.get(FIVE_MINUTE_ENDPOINT, params={"timestamp": ts})
    except httpx.TimeoutException as exc:
        logger.warning("Timeout fetching snapshot for ts=%s: %s", ts, exc)
        raise TransientError(f"Timeout fetching snapshot for ts={ts}") from exc
    except httpx.TransportError as exc:
        # Covers ConnectError and other connection-level transport failures.
        logger.warning("Connection error fetching snapshot for ts=%s: %s", ts, exc)
        raise TransientError(
            f"Connection error fetching snapshot for ts={ts}"
        ) from exc

    status = response.status_code

    if status == 429:
        retry_after = _parse_retry_after(response)
        logger.warning(
            "Rate limited (HTTP 429) fetching snapshot for ts=%s; retry_after=%s",
            ts,
            retry_after,
        )
        error = TransientError(f"Rate limited (HTTP 429) fetching snapshot for ts={ts}")
        setattr(error, RETRY_AFTER_ATTR, retry_after)
        raise error

    if 500 <= status < 600:
        logger.warning(
            "Server error (HTTP %d) fetching snapshot for ts=%s", status, ts
        )
        raise TransientError(
            f"Server error (HTTP {status}) fetching snapshot for ts={ts}"
        )

    if 400 <= status < 500:
        logger.error(
            "Client error (HTTP %d) fetching snapshot for ts=%s", status, ts
        )
        raise NonTransientError(
            f"Client error (HTTP {status}) fetching snapshot for ts={ts}"
        )

    return FiveMinuteSnapshot.model_validate(response.json())


#: Item whose stored history is queried to determine the latest ingested
#: timestamp (per the project data-flow: query item ``554`` for the latest
#: timestamp before generating the missing 5-minute windows).
REFERENCE_ITEM_ID: str = "554"

#: HTTP client timeout, in seconds, for Wiki API snapshot fetches.
_HTTP_TIMEOUT_SECONDS: float = 30.0


@dataclass
class IngestionResult:
    """Accurate counts describing the outcome of a catch-up ingestion run.

    Attributes:
        timestamps_processed: Number of 5-minute windows whose snapshot was
            successfully fetched, validated, and turned into records.
        records_written: Total number of individual price records written to
            InfluxDB across all (partial and full) batches.
        timestamps_skipped: Number of windows skipped without being counted as a
            hard failure (reserved for future use; currently ``0``).
        failures: Number of windows that could not be fetched/validated after
            retries, plus any batches dropped after write retries were exhausted.
    """

    timestamps_processed: int
    records_written: int
    timestamps_skipped: int
    failures: int


def _safe_log(level: int, message: str, *args: object) -> None:
    """Log a message without ever letting a logging failure abort the run.

    Availability of ingestion takes priority over the audit trail: if the
    logging subsystem itself raises (for example a misconfigured handler), the
    exception is swallowed so the catch-up loop keeps making progress
    (Requirement 8.2).

    Args:
        level: The :mod:`logging` level (e.g. :data:`logging.WARNING`).
        message: A printf-style log message.
        *args: Arguments interpolated into ``message`` by the logging framework.
    """
    try:
        logger.log(level, message, *args)
    except Exception:  # noqa: BLE001 - logging must never crash ingestion.
        pass


async def run_catch_up(settings: Settings) -> IngestionResult:
    """Catch the database up to the current time with bounded concurrency.

    Determines the latest stored timestamp (by querying
    :data:`REFERENCE_ITEM_ID`), lazily iterates every missing 5-minute window up
    to now, and fetches each window's snapshot concurrently -- but never more
    than ``settings.max_concurrency`` fetches in flight at once. Each fetch is
    retried with exponential backoff on transient failures (HTTP 429/5xx,
    timeouts, connection errors), honoring any ``Retry-After``; a window that
    still fails, or fails permanently (non-429 4xx) or fails schema validation,
    is logged and skipped while incrementing ``failures``. Successful snapshots
    are turned into null-filtered records that accumulate into a batch, which is
    written to InfluxDB every ``settings.batch_size`` processed windows, with a
    final flush of any partial batch. The InfluxDB client is always closed, even
    on partial failure.

    Args:
        settings: The application settings providing the InfluxDB connection,
            the Wiki API base URL and ``User-Agent``, and the
            ``max_concurrency``/``batch_size`` tuning values.

    Returns:
        An :class:`IngestionResult` with accurate processed / written / skipped /
        failure counts for the run.
    """
    client_db = influx.get_client(settings)
    result = IngestionResult(
        timestamps_processed=0,
        records_written=0,
        timestamps_skipped=0,
        failures=0,
    )
    batch: list[dict] = []
    ts_in_batch = 0

    # Retry-wrapped fetch: transient failures back off and retry, honoring
    # Retry-After; non-transient/validation errors propagate immediately.
    retried_fetch = retry_with_backoff()(fetch_snapshot)

    def _flush_batch() -> None:
        """Write the accumulated batch and update counts, dropping on failure."""
        nonlocal batch, ts_in_batch
        if not batch:
            return
        try:
            influx.write_batch(client_db, settings.influx_bucket, batch)
            result.records_written += len(batch)
        except TransientError as exc:
            _safe_log(
                logging.ERROR,
                "Batch of %d record(s) dropped after write retries exhausted: %s",
                len(batch),
                exc,
            )
            result.failures += 1
        finally:
            batch = []
            ts_in_batch = 0

    try:
        latest = influx.get_latest_timestamp(client_db, REFERENCE_ITEM_ID)
        now = int(time.time())

        pending = iter_missing_timestamps(latest, now)
        semaphore = asyncio.Semaphore(settings.max_concurrency)

        async with httpx.AsyncClient(
            base_url=settings.api_base_url,
            headers={"User-Agent": settings.user_agent},
            timeout=_HTTP_TIMEOUT_SECONDS,
        ) as http_client:

            async def _fetch(ts: int) -> FiveMinuteSnapshot:
                """Fetch one window's snapshot under the concurrency semaphore."""
                async with semaphore:
                    return await retried_fetch(http_client, ts)

            in_flight: dict[asyncio.Task[FiveMinuteSnapshot], int] = {}

            def _schedule_next() -> bool:
                """Schedule the next pending window, if any, as an in-flight task."""
                try:
                    ts = next(pending)
                except StopIteration:
                    return False
                task = asyncio.ensure_future(_fetch(ts))
                in_flight[task] = ts
                return True

            # Prime the pool up to the concurrency bound; the pool then refills
            # by one each time a task completes, so at most max_concurrency
            # fetches are ever in flight and each window is attempted once.
            for _ in range(max(1, settings.max_concurrency)):
                if not _schedule_next():
                    break

            while in_flight:
                done, _pending = await asyncio.wait(
                    in_flight.keys(), return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    ts = in_flight.pop(task)
                    try:
                        snapshot = task.result()
                    except (
                        TransientError,
                        NonTransientError,
                        ValidationError,
                    ) as exc:
                        _safe_log(
                            logging.WARNING,
                            "Skipping timestamp %s after fetch failure: %s",
                            ts,
                            exc,
                        )
                        result.failures += 1
                        _schedule_next()
                        continue

                    batch.extend(build_price_records(snapshot))
                    ts_in_batch += 1
                    result.timestamps_processed += 1

                    if ts_in_batch >= settings.batch_size:
                        _flush_batch()

                    _schedule_next()

        # Flush the final partial batch outside the HTTP client context.
        _flush_batch()
    finally:
        try:
            client_db.close()
        except Exception as exc:  # noqa: BLE001 - close must not mask results.
            _safe_log(
                logging.WARNING, "Error closing InfluxDB client: %s", exc
            )

    return result
