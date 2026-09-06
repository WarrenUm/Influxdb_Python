"""Unit tests for :mod:`ge_pipeline.scheduler`.

Verifies that :func:`ge_pipeline.scheduler.build_scheduler` registers the
catch-up ingestion job on a 5-minute interval trigger. The scheduler is built
but never started, so no event loop is required.

Requirements: 15.1 (trigger catch-up every 5 minutes), 21.1 (pytest unit
tests).
"""

from __future__ import annotations

from datetime import timedelta

from apscheduler.triggers.interval import IntervalTrigger

from ge_pipeline.config import Settings
from ge_pipeline.scheduler import (
    CATCH_UP_JOB_ID,
    INTERVAL_MINUTES,
    build_scheduler,
)


def _dummy_settings() -> Settings:
    """Build a Settings instance without touching the environment."""
    return Settings(
        influx3_host="http://localhost:8181",
        influx3_token="dummy-token",
        influx3_database="GEItemPrices",
    )


def test_build_scheduler_registers_catch_up_job():
    """The catch-up job is registered under the expected id."""
    scheduler = build_scheduler(settings=_dummy_settings())

    job = scheduler.get_job(CATCH_UP_JOB_ID)
    assert job is not None, f"expected a job with id {CATCH_UP_JOB_ID!r}"


def test_catch_up_job_uses_five_minute_interval_trigger():
    """The catch-up job fires on a 5-minute IntervalTrigger."""
    scheduler = build_scheduler(settings=_dummy_settings())

    job = scheduler.get_job(CATCH_UP_JOB_ID)
    assert job is not None

    assert isinstance(job.trigger, IntervalTrigger)
    assert job.trigger.interval == timedelta(minutes=5)
    assert job.trigger.interval.total_seconds() == 300
    # The module constant driving the interval is 5 minutes.
    assert INTERVAL_MINUTES == 5


def test_build_scheduler_uses_provided_settings(monkeypatch):
    """When settings are supplied, get_settings must not be consulted."""
    import ge_pipeline.scheduler as scheduler_module

    def _fail():  # pragma: no cover - should never be called
        raise AssertionError("get_settings should not be called when settings given")

    monkeypatch.setattr(scheduler_module, "get_settings", _fail)

    scheduler = build_scheduler(settings=_dummy_settings())
    assert scheduler.get_job(CATCH_UP_JOB_ID) is not None
