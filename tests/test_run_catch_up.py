"""Unit tests for :func:`ge_pipeline.ingestion.run_catch_up` orchestration.

Exercises the async catch-up orchestrator with mocked fetch/write/client so the
behavior can be asserted deterministically without touching the network or a
real InfluxDB:

* the concurrency ceiling is never exceeded (Requirement 5.1),
* every missing 5-minute window is attempted exactly once (Requirement 5.5),
* the returned :class:`IngestionResult` counts are accurate on the all-success
  path and when a fetch fails permanently (Requirement 5.6),
* the final partial batch is flushed when the window count is not a multiple of
  ``batch_size`` (Requirement 7.2), and
* the InfluxDB client is always closed, both when a fetch fails and when a write
  batch is dropped (Requirement 5.7).

Requirements: 5.1, 5.5, 5.6, 5.7, 7.1, 7.2, 21.1
"""

from __future__ import annotations

import asyncio
from unittest import mock

from ge_pipeline import ingestion
from ge_pipeline.config import Settings
from ge_pipeline.errors import NonTransientError, TransientError
from ge_pipeline.models import FiveMinuteSnapshot

INTERVAL = 300
LATEST = 1_700_000_000


def _make_settings(*, max_concurrency: int = 2, batch_size: int = 3) -> Settings:
    """Build a Settings instance with small concurrency/batch tuning values."""
    return Settings(
        influx_url="http://localhost:8086",
        influx_token="token",
        influx_org="org",
        influx_bucket="GEItemPrices",
        max_concurrency=max_concurrency,
        batch_size=batch_size,
    )


def _expected_windows(n: int) -> list[int]:
    """Return the ``n`` missing windows produced for ``LATEST`` with ``_now(n)``."""
    return [LATEST + INTERVAL * (k + 1) for k in range(n)]


def _now(n: int) -> int:
    """Return a ``now`` value that yields exactly ``n`` missing windows."""
    return LATEST + INTERVAL * (n + 1)


def _make_snapshot(ts: int, num_items: int = 2) -> FiveMinuteSnapshot:
    """Build a snapshot for ``ts`` with ``num_items`` non-null price records."""
    data = {
        str(1000 + i): {
            "avgHighPrice": 10 + i,
            "avgLowPrice": 9 + i,
            "highPriceVolume": 100 + i,
            "lowPriceVolume": 90 + i,
        }
        for i in range(num_items)
    }
    return FiveMinuteSnapshot.model_validate({"timestamp": ts, "data": data})


class _Harness:
    """Bundles patched module-level names and the records/batches they capture."""

    def __init__(self, num_windows: int, *, records_per_snapshot: int = 2):
        self.num_windows = num_windows
        self.records_per_snapshot = records_per_snapshot
        self.client = mock.MagicMock(name="influx_client")
        self.fetched: list[int] = []
        self.written_batches: list[list[dict]] = []
        # Concurrency tracking.
        self.in_flight = 0
        self.max_in_flight = 0
        # Optional per-ts fetch behavior override: ts -> Exception to raise.
        self.fetch_errors: dict[int, Exception] = {}
        # Optional flag to make write_batch raise TransientError (dropped batch).
        self.write_raises = False

    async def fake_fetch(self, _client, ts: int) -> FiveMinuteSnapshot:
        """Async stand-in for ``fetch_snapshot`` that tracks concurrency."""
        self.fetched.append(ts)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            # Yield control so overlapping fetches can accumulate; this lets the
            # test observe the true concurrency ceiling.
            await asyncio.sleep(0.01)
            if ts in self.fetch_errors:
                raise self.fetch_errors[ts]
            return _make_snapshot(ts, self.records_per_snapshot)
        finally:
            self.in_flight -= 1

    def fake_write_batch(self, _client, _bucket, records: list[dict]) -> None:
        """Sync stand-in for ``influx.write_batch`` that records each batch."""
        if self.write_raises:
            raise TransientError("simulated write failure")
        self.written_batches.append(list(records))

    def run(self, settings: Settings) -> ingestion.IngestionResult:
        """Run ``run_catch_up`` under all patches and return its result."""
        now = _now(self.num_windows)
        with mock.patch.object(
            ingestion.influx, "get_client", return_value=self.client
        ), mock.patch.object(
            ingestion.influx, "get_latest_timestamp", return_value=LATEST
        ), mock.patch.object(
            ingestion.influx, "write_batch", side_effect=self.fake_write_batch
        ), mock.patch.object(
            ingestion, "fetch_snapshot", new=self.fake_fetch
        ), mock.patch.object(
            ingestion.time, "time", return_value=now
        ):
            return asyncio.run(ingestion.run_catch_up(settings))


# ---------------------------------------------------------------------------
# Concurrency ceiling (Requirement 5.1)
# ---------------------------------------------------------------------------


def test_concurrency_never_exceeds_max_concurrency():
    settings = _make_settings(max_concurrency=2, batch_size=50)
    harness = _Harness(num_windows=8)

    harness.run(settings)

    assert harness.max_in_flight <= settings.max_concurrency
    # With more windows than the ceiling and overlapping fetches, we expect the
    # ceiling to actually be reached (not merely respected trivially).
    assert harness.max_in_flight == settings.max_concurrency


# ---------------------------------------------------------------------------
# Exactly-once attempt per window (Requirement 5.5)
# ---------------------------------------------------------------------------


def test_each_window_attempted_exactly_once():
    settings = _make_settings(max_concurrency=2, batch_size=3)
    n = 7
    harness = _Harness(num_windows=n)

    harness.run(settings)

    expected = _expected_windows(n)
    assert sorted(harness.fetched) == expected
    assert len(harness.fetched) == n  # no window fetched more than once


# ---------------------------------------------------------------------------
# Accurate result counts (Requirement 5.6)
# ---------------------------------------------------------------------------


def test_counts_accurate_on_all_success():
    settings = _make_settings(max_concurrency=2, batch_size=3)
    n = 7
    records_per = 2
    harness = _Harness(num_windows=n, records_per_snapshot=records_per)

    result = harness.run(settings)

    assert result.timestamps_processed == n
    assert result.records_written == n * records_per
    assert result.timestamps_skipped == 0
    assert result.failures == 0


def test_counts_accurate_when_one_fetch_fails_permanently():
    settings = _make_settings(max_concurrency=2, batch_size=3)
    n = 7
    records_per = 2
    harness = _Harness(num_windows=n, records_per_snapshot=records_per)
    failing_ts = _expected_windows(n)[3]
    harness.fetch_errors[failing_ts] = NonTransientError("HTTP 404")

    result = harness.run(settings)

    # The failing window is still attempted exactly once...
    assert failing_ts in harness.fetched
    assert sorted(harness.fetched) == _expected_windows(n)
    # ...but is counted as a failure and not processed.
    assert result.failures == 1
    assert result.timestamps_processed == n - 1
    assert result.records_written == (n - 1) * records_per


# ---------------------------------------------------------------------------
# Final-batch flush (Requirements 7.1, 7.2)
# ---------------------------------------------------------------------------


def test_final_partial_batch_is_flushed():
    settings = _make_settings(max_concurrency=2, batch_size=3)
    n = 7  # 7 is not a multiple of batch_size (3): last batch is partial.
    records_per = 2
    harness = _Harness(num_windows=n, records_per_snapshot=records_per)

    result = harness.run(settings)

    total_written = sum(len(b) for b in harness.written_batches)
    assert total_written == n * records_per
    assert result.records_written == total_written
    # 7 windows at batch_size 3 -> full batches for 3 + 3 windows, then a final
    # partial batch covering the remaining 1 window.
    assert len(harness.written_batches) == 3
    assert len(harness.written_batches[-1]) == 1 * records_per


# ---------------------------------------------------------------------------
# Client close on partial failure (Requirement 5.7)
# ---------------------------------------------------------------------------


def test_client_closed_when_a_fetch_fails():
    settings = _make_settings(max_concurrency=2, batch_size=3)
    n = 5
    harness = _Harness(num_windows=n)
    harness.fetch_errors[_expected_windows(n)[2]] = NonTransientError("HTTP 400")

    result = harness.run(settings)

    assert result.failures == 1
    harness.client.close.assert_called_once()


def test_client_closed_when_a_write_batch_is_dropped():
    settings = _make_settings(max_concurrency=2, batch_size=3)
    n = 6
    harness = _Harness(num_windows=n)
    harness.write_raises = True  # every flush raises -> batches dropped

    result = harness.run(settings)

    # No records land, and each dropped batch increments failures.
    assert result.records_written == 0
    assert result.failures >= 1
    harness.client.close.assert_called_once()
