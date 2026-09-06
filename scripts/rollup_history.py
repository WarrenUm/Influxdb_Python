"""Chunked, resumable rollup/downsample driver for the OSRS GE price pipeline.

Operational tool (NOT part of the ``ge_pipeline`` library). Walks the raw
``itemPrice`` history in fixed-size time windows, downsamples each window to a
``date_bin`` interval (default hourly), and writes the far smaller aggregated
result into a separate rollup measurement (default ``itemPrice_1h``). Its purpose
is to reduce effective Parquet-file pressure on InfluxDB 3 **Core** (which cannot
auto-compact) by producing a compact, query-cheap historical measurement that
dashboards and exports can read for long ranges.

Like ``backfill_history.py`` it is checkpointed and safe to interrupt: a JSON
checkpoint is written after each *fully committed* window, so a resume continues
from the last committed window rather than redoing work or leaving gaps. A window
whose write fails after retries does NOT advance the checkpoint.

Correctness detail: the aggregation and write reuse the library's
``build_rollup_records`` and query builders, but the actual write is wrapped here
with retry/backoff so a persistent write failure RAISES (rather than being
silently dropped as ``influx.write_batch`` would), letting us avoid checkpointing
a partially-written window.

Usage:
    PYTHONPATH=. python scripts/rollup_history.py \
        [--interval 1h] [--measurement itemPrice_1h] \
        [--window-seconds 604800] [--start-ts ...] [--stop-ts ...] \
        [--max-windows 0] [--pause 0.5] [--max-records-per-write 50000] \
        [--checkpoint scripts/rollup_checkpoint.json]
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from ge_pipeline import influx, rollup
from ge_pipeline.config import get_settings
from ge_pipeline.errors import TransientError
from ge_pipeline.retry import retry_with_backoff

EARLIEST = 1615733100

logger = logging.getLogger("rollup")


class WindowWriteError(RuntimeError):
    """Raised when a sub-batch write fails so the window is not checkpointed."""


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
    client, records: list[dict], max_records: int
) -> int:
    """Write ``records`` in sub-batches of at most ``max_records``, raising on failure.

    Mirrors the raw-backfill driver: each sub-batch is written directly through
    the client with retry/backoff and RAISES :class:`WindowWriteError` if it
    still fails, so the caller can avoid checkpointing a partially-written
    window. Keeping each request bounded stays under InfluxDB 3's ~10 MB request
    limit.

    Returns:
        The number of records successfully persisted.
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
            except Exception as exc:  # classify all failures as transient for retry
                raise TransientError(f"v3 rollup write failed: {exc}") from exc

        try:
            _do_write()
        except TransientError as exc:
            raise WindowWriteError(
                f"sub-batch of {len(sub)} rollup record(s) failed after retries: {exc}"
            ) from exc
        written += len(sub)
    return written


def _aligned(ts: int, interval_seconds: int) -> int:
    """Align a timestamp down to the rollup interval boundary."""
    return ts - (ts % interval_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Chunked, resumable OSRS price rollup/downsample."
    )
    parser.add_argument("--interval", type=str, default=rollup.DEFAULT_ROLLUP_INTERVAL,
                        help="date_bin downsample interval (e.g. 1h, 1d).")
    parser.add_argument("--measurement", type=str,
                        default=rollup.DEFAULT_ROLLUP_MEASUREMENT,
                        help="Rollup measurement to write into.")
    parser.add_argument("--window-seconds", type=int,
                        default=rollup.DEFAULT_WINDOW_SECONDS,
                        help="Width of each read/aggregate/write window.")
    parser.add_argument("--start-ts", type=int, default=EARLIEST)
    parser.add_argument("--stop-ts", type=int, default=0)
    parser.add_argument("--max-windows", type=int, default=0,
                        help="0 = process all windows up to stop.")
    parser.add_argument("--pause", type=float, default=0.5)
    parser.add_argument("--max-records-per-write", type=int, default=50_000,
                        help="Max records per v3 write request (keeps <10MB).")
    parser.add_argument("--checkpoint", type=str,
                        default="scripts/rollup_checkpoint.json")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    settings = get_settings()
    client = influx.get_client(settings)

    # Align the interval so window boundaries land on clean bin edges.
    interval_seconds = influx._interval_seconds(args.interval)
    if args.window_seconds <= 0:
        logger.error("--window-seconds must be > 0")
        return 2

    stop_ts = args.stop_ts if args.stop_ts else int(time.time())
    stop_ts = _aligned(stop_ts, interval_seconds)
    checkpoint_path = Path(args.checkpoint)

    ckpt = _load_checkpoint(checkpoint_path)
    if ckpt and ckpt.get("next_start_ts"):
        cursor = int(ckpt["next_start_ts"])
        logger.info("resuming rollup from checkpoint: next_start_ts=%s", cursor)
    else:
        cursor = _aligned(args.start_ts, interval_seconds)
        logger.info("starting fresh rollup at start_ts=%s", cursor)

    totals = {
        "windows": 0, "rows_read": 0, "written": 0,
        "first_ts": None, "last_ts": None,
    }

    while cursor < stop_ts:
        if args.max_windows and totals["windows"] >= args.max_windows:
            logger.info("reached --max-windows=%d; stopping", args.max_windows)
            break

        win_stop = min(cursor + args.window_seconds, stop_ts)

        # Discover the items present in this window with a time-bounded DISTINCT
        # scan so the enumeration stays under Core's Parquet-file cap.
        item_ids = influx.list_item_ids(client, start=cursor, stop=win_stop)
        if not item_ids:
            logger.info("window [%s, %s): no items; skipping", cursor, win_stop)
            totals["windows"] += 1
            _save_checkpoint(checkpoint_path, {
                "next_start_ts": win_stop,
                "stop_ts": stop_ts,
                "interval": args.interval,
                "measurement": args.measurement,
                "window_seconds": args.window_seconds,
                "updated": int(time.time()),
                "totals": totals,
            })
            cursor = win_stop
            continue

        frame = influx.query_chunk(client, item_ids, cursor, win_stop, args.interval)
        records = rollup.build_rollup_records(frame, args.measurement)
        rows_read = 0 if frame is None or frame.empty else len(frame)

        try:
            written = _write_records_bounded(
                client, records, args.max_records_per_write
            )
        except WindowWriteError as exc:
            logger.error(
                "window starting %s NOT checkpointed (write failed): %s",
                cursor, exc,
            )
            print(json.dumps({
                "status": "aborted_on_write_failure",
                "failed_window_start_ts": cursor,
                "totals_before_failure": totals,
            }, indent=2))
            return 1

        totals["windows"] += 1
        totals["rows_read"] += rows_read
        totals["written"] += written
        if totals["first_ts"] is None:
            totals["first_ts"] = cursor
        totals["last_ts"] = win_stop

        _save_checkpoint(checkpoint_path, {
            "next_start_ts": win_stop,
            "stop_ts": stop_ts,
            "interval": args.interval,
            "measurement": args.measurement,
            "window_seconds": args.window_seconds,
            "updated": int(time.time()),
            "totals": totals,
        })
        logger.info(
            "window [%s, %s): items=%d rows_read=%d written=%d (next_start_ts=%s)",
            cursor, win_stop, len(item_ids), rows_read, written, win_stop,
        )

        cursor = win_stop
        if cursor < stop_ts and args.pause > 0:
            time.sleep(args.pause)

    logger.info(
        "rollup run complete: windows=%d rows_read=%d written=%d range=[%s, %s]",
        totals["windows"], totals["rows_read"], totals["written"],
        totals["first_ts"], totals["last_ts"],
    )
    print(json.dumps({"summary": totals, "next_start_ts_after_run": cursor}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
