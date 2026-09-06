"""Chunked full-history backfill driver for the OSRS GE price pipeline.

Operational tool (NOT part of the ``ge_pipeline`` library). Walks history
oldest -> newest in fixed-size chunks of 5-minute windows, reusing the
pipeline's own fetch/validate/build/write helpers so behavior (bounded
concurrency, retry/backoff, null filtering, seconds precision) matches
``run_catch_up``. Writes a JSON checkpoint after each *fully committed* chunk so
the backfill is resumable and safe to interrupt.

Key correctness detail: a single 5-minute snapshot expands to ~3k item records,
so a whole weekly chunk (~6M records) blows past InfluxDB 3's 10 MB request
limit. We therefore flush in bounded sub-batches sized by a RECORD cap (not a
whole chunk), mirroring how ``run_catch_up`` flushes every ``batch_size``
timestamps. If any sub-batch fails to persist, the chunk is treated as FAILED
and the checkpoint is NOT advanced, so a resume re-fetches that chunk rather
than silently leaving a gap.

Usage:
    python scripts/backfill_history.py --chunk-windows 2016 --max-chunks 4
      [--start-ts ...] [--stop-ts ...] [--concurrency 4] [--pause 2.0]
      [--max-records-per-write 250000] [--checkpoint scripts/backfill_checkpoint.json]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path

import httpx
from pydantic import ValidationError

from ge_pipeline import influx
from ge_pipeline.config import get_settings
from ge_pipeline.errors import NonTransientError, TransientError
from ge_pipeline.ingestion import build_price_records, fetch_snapshot
from ge_pipeline.models import FiveMinuteSnapshot
from ge_pipeline.retry import retry_with_backoff

INTERVAL = 300
EARLIEST = 1615733100
HTTP_TIMEOUT = 30.0

logger = logging.getLogger("backfill")


class ChunkWriteError(RuntimeError):
    """Raised when a sub-batch write fails so the chunk is not checkpointed."""


def _aligned(ts: int) -> int:
    return ts - (ts % INTERVAL)


def _load_checkpoint(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _save_checkpoint(path: Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_records_bounded(
    client, database: str, records: list[dict], max_records: int
) -> int:
    """Write ``records`` to v3 in sub-batches of at most ``max_records`` each.

    Each sub-batch goes through the pipeline's ``write_batch`` (retry/backoff,
    seconds precision). ``write_batch`` drops-and-logs on exhausted retries, so
    to detect that we compare the store's row count via a targeted check is too
    costly; instead we wrap the client write directly here and RAISE on failure
    so the caller can avoid checkpointing a partially-written chunk.

    Returns the number of records successfully persisted.
    """
    written = 0
    for i in range(0, len(records), max_records):
        sub = records[i : i + max_records]
        if not sub:
            continue

        @retry_with_backoff()
        def _do_write(batch=sub) -> None:
            try:
                points = [influx._record_to_point(r) for r in batch]
                client.write(record=points, write_precision=influx.WRITE_PRECISION)
            except Exception as exc:  # noqa: BLE001 - classify as transient for retry
                raise TransientError(f"v3 write failed: {exc}") from exc

        try:
            _do_write()
        except TransientError as exc:
            raise ChunkWriteError(
                f"sub-batch of {len(sub)} record(s) failed after retries: {exc}"
            ) from exc
        written += len(sub)
    return written


async def _fetch_chunk(http_client, timestamps, concurrency):
    """Fetch a chunk concurrently; return (records, processed, failures)."""
    semaphore = asyncio.Semaphore(max(1, concurrency))
    retried_fetch = retry_with_backoff()(fetch_snapshot)

    async def _one(ts: int) -> FiveMinuteSnapshot:
        async with semaphore:
            return await retried_fetch(http_client, ts)

    records: list[dict] = []
    processed = 0
    failures = 0
    tasks = [asyncio.ensure_future(_one(ts)) for ts in timestamps]
    for task in asyncio.as_completed(tasks):
        try:
            snapshot = await task
        except (TransientError, NonTransientError, ValidationError) as exc:
            failures += 1
            logger.warning("skip window after fetch failure: %s", exc)
            continue
        records.extend(build_price_records(snapshot))
        processed += 1
    return records, processed, failures


async def main() -> int:
    parser = argparse.ArgumentParser(description="Chunked OSRS history backfill.")
    parser.add_argument("--chunk-windows", type=int, default=2016)
    parser.add_argument("--max-chunks", type=int, default=0)
    parser.add_argument("--start-ts", type=int, default=EARLIEST)
    parser.add_argument("--stop-ts", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--pause", type=float, default=2.0)
    parser.add_argument("--max-records-per-write", type=int, default=50_000,
                        help="Max records per v3 write request (keeps <10MB).")
    parser.add_argument("--checkpoint", type=str,
                        default="scripts/backfill_checkpoint.json")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Quiet httpx per-request logging so progress stays readable.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    settings = get_settings()
    client_db = influx.get_client(settings)
    database = settings.influx3_database

    stop_ts = _aligned(args.stop_ts) if args.stop_ts else _aligned(int(time.time()))
    checkpoint_path = Path(args.checkpoint)

    ckpt = _load_checkpoint(checkpoint_path)
    if ckpt and ckpt.get("next_start_ts"):
        cursor = int(ckpt["next_start_ts"])
        logger.info("resuming from checkpoint: next_start_ts=%s", cursor)
    else:
        cursor = _aligned(args.start_ts)
        logger.info("starting fresh at start_ts=%s", cursor)

    totals = dict(chunks=0, processed=0, written=0, failures=0,
                  first_ts=None, last_ts=None)
    step = args.chunk_windows * INTERVAL

    async with httpx.AsyncClient(
        base_url=settings.api_base_url,
        headers={"User-Agent": settings.user_agent},
        timeout=HTTP_TIMEOUT,
    ) as http_client:
        while cursor < stop_ts:
            if args.max_chunks and totals["chunks"] >= args.max_chunks:
                logger.info("reached --max-chunks=%d; stopping", args.max_chunks)
                break

            chunk_stop = min(cursor + step, stop_ts)
            timestamps = list(range(cursor, chunk_stop, INTERVAL))
            if not timestamps:
                cursor = chunk_stop
                continue

            logger.info("chunk %d: windows [%s, %s) count=%d",
                        totals["chunks"] + 1, cursor, chunk_stop, len(timestamps))

            records, processed, failures = await _fetch_chunk(
                http_client, timestamps, args.concurrency
            )

            try:
                written = _write_records_bounded(
                    client_db, database, records, args.max_records_per_write
                )
            except ChunkWriteError as exc:
                logger.error(
                    "chunk starting %s NOT checkpointed (write failed): %s",
                    cursor, exc,
                )
                print(json.dumps({
                    "status": "aborted_on_write_failure",
                    "failed_chunk_start_ts": cursor,
                    "totals_before_failure": totals,
                }, indent=2))
                return 1

            totals["chunks"] += 1
            totals["processed"] += processed
            totals["written"] += written
            totals["failures"] += failures
            if totals["first_ts"] is None:
                totals["first_ts"] = timestamps[0]
            totals["last_ts"] = timestamps[-1]

            _save_checkpoint(checkpoint_path, {
                "next_start_ts": chunk_stop,
                "stop_ts": stop_ts,
                "chunk_windows": args.chunk_windows,
                "updated": int(time.time()),
                "totals": totals,
            })
            logger.info("chunk %d done: processed=%d written=%d failures=%d "
                        "(next_start_ts=%s)",
                        totals["chunks"], processed, written, failures, chunk_stop)

            cursor = chunk_stop
            if cursor < stop_ts and args.pause > 0:
                await asyncio.sleep(args.pause)

    logger.info("run complete: chunks=%d processed=%d written=%d failures=%d "
                "range=[%s, %s]",
                totals["chunks"], totals["processed"], totals["written"],
                totals["failures"], totals["first_ts"], totals["last_ts"])
    print(json.dumps({"summary": totals, "next_start_ts_after_run": cursor}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
