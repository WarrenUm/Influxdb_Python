"""Report progress of the history backfill / data-load background process.

Operational helper (NOT part of the ``ge_pipeline`` library). It answers "how
far along is the data load?" from two independent angles and degrades
gracefully if either is unavailable:

1. **Checkpoint file** (always fast, always local): reads
   ``scripts/backfill_checkpoint.json`` written by ``backfill_history.py`` after
   each committed chunk. Shows the cursor position, percent of the full history
   window covered, running totals, and how long ago the checkpoint last
   advanced (a stale checkpoint hints the backfill has stalled or stopped).

2. **Live database probe** (optional, best-effort): queries the target InfluxDB
   for the newest stored snapshot of a reference item. This is a *bounded*
   query (single item, small LIMIT) to avoid the InfluxDB 3 Core
   ``--query-file-limit`` trap that unbounded whole-store scans hit while a
   backfill is producing many small Parquet files. If the DB is unreachable or
   the query errors/times out, the script still prints the checkpoint summary.

Usage:
    python scripts/check_backfill_progress.py
    python scripts/check_backfill_progress.py --checkpoint scripts/backfill_checkpoint.json
    python scripts/check_backfill_progress.py --host http://192.168.1.85:8181 \
        --database GEItemPrices --item 554 --no-db   # skip the DB probe

Connection defaults come from the environment (same vars the pipeline uses):
``INFLUXDB3_HOST_URL``, ``INFLUXDB3_DATABASE_NAME``, ``INFLUXDB3_AUTH_TOKEN``.
Exit code is 0 on success, 2 if the checkpoint file is missing/unreadable.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

# The project's earliest snapshot (2021-03-14 14:45 UTC); matches EARLIEST in
# backfill_history.py. Used to compute "percent of full history covered".
EARLIEST_TS = 1615733100
INTERVAL = 300  # seconds between 5-minute snapshots

DEFAULT_CHECKPOINT = "scripts/backfill_checkpoint.json"
DEFAULT_REFERENCE_ITEM = "554"


def _fmt_ts(ts: float | None) -> str:
    """Format a unix-seconds timestamp as an ISO-ish UTC string, or '-'."""
    if ts is None:
        return "-"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


def _fmt_duration(seconds: float) -> str:
    """Render a duration in seconds as a compact 'Nd Nh Nm Ns' string."""
    seconds = int(seconds)
    if seconds < 0:
        return "?"
    parts: list[str] = []
    for label, size in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        if seconds >= size or (label == "s" and not parts):
            value, seconds = divmod(seconds, size)
            parts.append(f"{value}{label}")
    return " ".join(parts)


def _load_checkpoint(path: Path) -> dict | None:
    """Load and parse the checkpoint JSON, or return None if unavailable."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _report_checkpoint(ckpt: dict) -> None:
    """Print the checkpoint-derived progress summary."""
    now = int(time.time())
    cursor = int(ckpt.get("next_start_ts", EARLIEST_TS))
    stop = int(ckpt.get("stop_ts", now))
    updated = int(ckpt.get("updated", 0))
    totals = ckpt.get("totals", {}) or {}

    span_total = max(1, stop - EARLIEST_TS)
    span_done = max(0, cursor - EARLIEST_TS)
    pct = min(100.0, span_done / span_total * 100)
    remaining_days = max(0, stop - cursor) / 86400

    print("== Backfill checkpoint ==")
    print(f"  cursor (next window) : {_fmt_ts(cursor)}")
    print(f"  target stop          : {_fmt_ts(stop)}")
    print(f"  earliest history     : {_fmt_ts(EARLIEST_TS)}")
    print(f"  progress             : {pct:.1f}% of full history")
    print(f"  remaining history    : {remaining_days:.1f} days")
    print("  totals:")
    print(f"    chunks committed   : {totals.get('chunks', '?')}")
    print(f"    snapshots processed: {totals.get('processed', '?')}")
    print(f"    records written    : {totals.get('written', '?')}")
    print(f"    fetch failures     : {totals.get('failures', '?')}")
    print(
        f"    loaded range       : {_fmt_ts(totals.get('first_ts'))} "
        f"-> {_fmt_ts(totals.get('last_ts'))}"
    )

    if updated:
        age = now - updated
        freshness = "advancing" if age < 300 else "STALE?"
        print(
            f"  checkpoint updated   : {_fmt_ts(updated)} "
            f"({_fmt_duration(age)} ago) [{freshness}]"
        )
    else:
        print("  checkpoint updated   : (unknown)")


def _probe_database(
    host: str, database: str, token: str, item_id: str, timeout: float
) -> None:
    """Best-effort: print the newest stored snapshot for a reference item.

    Uses a bounded, single-item SQL query so it stays under InfluxDB 3 Core's
    query-file-limit. Any failure (import, network, HTTP, parse) is caught and
    reported without aborting the script.
    """
    print("\n== Live database probe ==")
    print(f"  target   : {host}  db={database}  item={item_id}")
    try:
        import httpx  # local import: script still works for checkpoint-only use
    except ImportError:
        print("  (skipped: httpx not installed)")
        return

    # Bound the scan to the single reference item; ORDER BY DESC + LIMIT keeps
    # it cheap and avoids a whole-store scan.
    sql = (
        'SELECT time FROM "itemPrice" '
        "WHERE \"itemID\" = '" + item_id.replace("'", "''") + "' "
        "ORDER BY time DESC LIMIT 1"
    )
    url = host.rstrip("/") + "/api/v3/query_sql"
    try:
        resp = httpx.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"db": database, "q": sql, "format": "json"},
            timeout=timeout,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort probe
        print(f"  (unreachable: {exc})")
        return

    if resp.status_code != 200:
        body = resp.text.strip().replace("\n", " ")
        print(f"  (query failed HTTP {resp.status_code}: {body[:200]})")
        return

    try:
        rows = resp.json()
    except ValueError:
        print("  (could not parse response)")
        return

    if not rows:
        print(f"  no stored data for item {item_id} yet")
        return

    latest_raw = rows[0].get("time")
    # v3 returns time as an ISO string; parse to compute staleness.
    latest_ts: int | None = None
    if isinstance(latest_raw, (int, float)):
        latest_ts = int(latest_raw)
    elif isinstance(latest_raw, str):
        try:
            latest_ts = int(
                datetime.fromisoformat(
                    latest_raw.replace("Z", "+00:00")
                ).replace(tzinfo=timezone.utc if latest_raw.endswith("Z") else None)
                .timestamp()
            )
        except ValueError:
            latest_ts = None

    print(f"  newest stored snapshot (item {item_id}): {latest_raw}")
    if latest_ts is not None:
        age_days = (time.time() - latest_ts) / 86400
        print(f"  that is ~{age_days:.1f} days behind now")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Report data-load / backfill progress."
    )
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    parser.add_argument(
        "--host",
        default=os.environ.get("INFLUXDB3_HOST_URL", "http://localhost:8181"),
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("INFLUXDB3_DATABASE_NAME", "GEItemPrices"),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("INFLUXDB3_AUTH_TOKEN", "local-dev-token"),
    )
    parser.add_argument("--item", default=DEFAULT_REFERENCE_ITEM)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip the live database probe; report the checkpoint only.",
    )
    args = parser.parse_args()

    ckpt = _load_checkpoint(Path(args.checkpoint))
    if ckpt is None:
        print(f"No readable checkpoint at {args.checkpoint}.")
        print("The backfill may not have committed a chunk yet.")
    else:
        _report_checkpoint(ckpt)

    if not args.no_db:
        _probe_database(
            args.host, args.database, args.token, args.item, args.timeout
        )

    return 0 if ckpt is not None else 2


if __name__ == "__main__":
    raise SystemExit(main())
