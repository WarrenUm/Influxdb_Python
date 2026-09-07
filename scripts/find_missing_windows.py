"""Scan the stored history for missing 5-minute snapshot windows.

Operational helper (NOT part of the ``ge_pipeline`` library). The pipeline stores
one snapshot every 5 minutes (``INTERVAL`` = 300s); each snapshot fans out to
~3k per-item rows, so the number of **distinct** ``time`` values in a period is
the number of snapshots actually captured there. A fully covered day has
``288`` snapshots (12/hour x 24h); an hour has ``12``.

This script counts distinct snapshot timestamps per day using ``date_bin`` and
compares each day to the expected count, so it can report which days are short
and by how many windows -- without ever running an unbounded whole-store scan
(which trips InfluxDB's ``--query-file-limit`` / times out). It walks the
history one bounded window at a time (default 30 days per query).

For any day found short, pass ``--drill`` to additionally list the exact missing
5-minute timestamps within the flagged days (a second, day-bounded pass).

Usage:
    PYTHONPATH=. .venv/bin/python scripts/find_missing_windows.py
    PYTHONPATH=. .venv/bin/python scripts/find_missing_windows.py \
        --host http://192.168.1.85:8181 --database GEItemPrices
    PYTHONPATH=. .venv/bin/python scripts/find_missing_windows.py --drill
    PYTHONPATH=. .venv/bin/python scripts/find_missing_windows.py \
        --start 2021-03-14 --stop 2021-12-31 --drill

Connection defaults come from the environment (same vars the pipeline uses):
``INFLUXDB3_HOST_URL``, ``INFLUXDB3_DATABASE_NAME``, ``INFLUXDB3_AUTH_TOKEN``.
Exit code: 0 if no gaps found, 1 if any missing windows were detected, 2 on a
connection/query error.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import httpx

try:
    from dotenv import load_dotenv

    # Load the repo-root .env (does not override already-set process env vars),
    # so INFLUXDB3_* target the same host the pipeline uses.
    load_dotenv(override=False)
except ImportError:
    pass

INTERVAL = 300  # seconds between 5-minute snapshots
SNAPSHOTS_PER_DAY = 86400 // INTERVAL  # 288
# Project's earliest snapshot (2021-03-14 14:45 UTC); matches EARLIEST elsewhere.
EARLIEST_TS = 1615733100


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _parse_day(s: str) -> int:
    """Parse YYYY-MM-DD (or full ISO) to a UTC unix-seconds day start."""
    s = s.strip()
    if len(s) == 10:
        dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _query(client: httpx.Client, url: str, database: str, token: str, sql: str) -> list[dict]:
    """Run one SQL query against the v3 HTTP endpoint; return list-of-row-dicts."""
    resp = client.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"db": database, "q": sql, "format": "json"},
    )
    resp.raise_for_status()
    return resp.json()


def _daily_counts(
    client: httpx.Client, url: str, database: str, token: str, start: int, stop: int
) -> dict[str, int]:
    """Distinct-snapshot count per UTC day in [start, stop), keyed by 'YYYY-MM-DD'.

    Uses a nested aggregate so the DISTINCT is evaluated inside the DB: first the
    distinct snapshot timestamps, then bucketed to days.
    """
    sql = (
        "SELECT date_bin(INTERVAL '1 day', t) AS day, COUNT(*) AS snapshots FROM ("
        '  SELECT DISTINCT time AS t FROM "itemPrice" '
        f"  WHERE time >= timestamp '{_iso(start)}' AND time < timestamp '{_iso(stop)}'"
        ") GROUP BY day ORDER BY day"
    )
    rows = _query(client, url, database, token, sql)
    out: dict[str, int] = {}
    for row in rows:
        day_raw = row.get("day")
        n = int(row.get("snapshots", 0))
        if isinstance(day_raw, str):
            key = day_raw[:10]
        else:  # numeric (ns or s) fallback
            ts = int(day_raw)
            if ts > 10_000_000_000:  # nanoseconds
                ts //= 1_000_000_000
            key = _day(ts)
        out[key] = n
    return out


def _missing_timestamps_in_day(
    client: httpx.Client, url: str, database: str, token: str, day_start: int
) -> list[int]:
    """Return the exact missing 5-min timestamps within a single UTC day."""
    day_stop = day_start + 86400
    sql = (
        "SELECT DISTINCT time AS t FROM \"itemPrice\" "
        f"WHERE time >= timestamp '{_iso(day_start)}' AND time < timestamp '{_iso(day_stop)}' "
        "ORDER BY t"
    )
    rows = _query(client, url, database, token, sql)
    present: set[int] = set()
    for row in rows:
        t = row.get("t")
        if isinstance(t, str):
            ts = int(datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp())
        else:
            ts = int(t)
            if ts > 10_000_000_000:
                ts //= 1_000_000_000
        present.add(ts - (ts % INTERVAL))
    expected = range(day_start, day_stop, INTERVAL)
    return [ts for ts in expected if ts not in present]


def main() -> int:
    parser = argparse.ArgumentParser(description="Find missing 5-minute snapshot windows.")
    parser.add_argument("--host", default=os.environ.get("INFLUXDB3_HOST_URL", "http://localhost:8181"))
    parser.add_argument("--database", default=os.environ.get("INFLUXDB3_DATABASE_NAME", "GEItemPrices"))
    parser.add_argument("--token", default=os.environ.get("INFLUXDB3_AUTH_TOKEN", "local-dev-token"))
    parser.add_argument("--start", default=None, help="UTC day (YYYY-MM-DD) to start; default = project earliest.")
    parser.add_argument("--stop", default=None, help="UTC day (YYYY-MM-DD) exclusive stop; default = now.")
    parser.add_argument("--window-days", type=int, default=30, help="Days scanned per query (bounded to dodge the file limit).")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--drill", action="store_true", help="List exact missing timestamps for each short day.")
    parser.add_argument("--max-drill-days", type=int, default=50, help="Cap how many short days to drill into.")
    args = parser.parse_args()

    start = _parse_day(args.start) if args.start else (EARLIEST_TS - (EARLIEST_TS % INTERVAL))
    stop = _parse_day(args.stop) if args.stop else int(datetime.now(tz=timezone.utc).timestamp())
    stop -= stop % INTERVAL

    url = args.host.rstrip("/") + "/api/v3/query_sql"
    print(f"Scanning {_iso(start)} .. {_iso(stop)} against {args.host} db={args.database}")
    print(f"(expected {SNAPSHOTS_PER_DAY} snapshots/day; window {args.window_days}d/query)\n")

    short_days: list[tuple[str, int, int]] = []  # (day, got, missing)
    total_present = 0
    total_days_seen = 0

    with httpx.Client(timeout=args.timeout) as client:
        cursor = start
        try:
            while cursor < stop:
                win_stop = min(cursor + args.window_days * 86400, stop)
                counts = _daily_counts(client, url, args.database, args.token, cursor, win_stop)
                # Walk every expected day in this window, including days entirely absent.
                d = cursor - (cursor % 86400)
                while d < win_stop:
                    key = _day(d)
                    # A day partially outside [start, stop) can't hit 288; only
                    # judge full days that lie within the scan range.
                    day_start_in = max(d, start)
                    day_stop_in = min(d + 86400, stop)
                    expected = (day_stop_in - day_start_in) // INTERVAL
                    got = counts.get(key, 0)
                    total_present += got
                    total_days_seen += 1
                    if got < expected:
                        short_days.append((key, got, expected - got))
                    d += 86400
                cursor = win_stop
        except httpx.HTTPStatusError as exc:
            body = exc.response.text.strip().replace("\n", " ")
            print(f"ERROR: query failed HTTP {exc.response.status_code}: {body[:300]}", file=sys.stderr)
            return 2
        except (httpx.HTTPError, OSError) as exc:
            print(f"ERROR: cannot reach {args.host}: {exc}", file=sys.stderr)
            return 2

        total_missing = sum(m for _, _, m in short_days)
        print("== Summary ==")
        print(f"  days scanned         : {total_days_seen}")
        print(f"  snapshots present    : {total_present}")
        print(f"  days with gaps       : {len(short_days)}")
        print(f"  total missing windows: {total_missing}")

        if not short_days:
            print("\nNo missing windows found. History is complete.")
            return 0

        print("\n== Days with missing windows ==")
        for key, got, missing in short_days:
            print(f"  {key}: {got}/{SNAPSHOTS_PER_DAY} present, {missing} missing")

        if args.drill:
            print("\n== Exact missing timestamps (drill) ==")
            for key, _got, _missing in short_days[: args.max_drill_days]:
                day_start = _parse_day(key)
                missing_ts = _missing_timestamps_in_day(client, url, args.database, args.token, day_start)
                pretty = ", ".join(_iso(t)[11:16] for t in missing_ts)
                print(f"  {key} ({len(missing_ts)}): {pretty}")
            if len(short_days) > args.max_drill_days:
                print(f"  ... {len(short_days) - args.max_drill_days} more short day(s) not drilled "
                      f"(raise --max-drill-days).")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
