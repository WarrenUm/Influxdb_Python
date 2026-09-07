"""APScheduler daemon that triggers catch-up ingestion every 5 minutes.

Configures APScheduler to run :func:`ge_pipeline.ingestion.run_catch_up` on a
5-minute interval (not continuously) as a long-running, systemd-friendly daemon.

Because ``run_catch_up`` itself queries the latest stored timestamp and ingests
every missing 5-minute window up to ``now``, a run that follows a prior *failed*
run naturally backfills the gap the failure left behind: the missing windows are
still absent from InfluxDB, so the next scheduled run re-detects and fills them.
The scheduler therefore does not need any gap-tracking logic of its own
(Requirement 15.2).

The module exposes :func:`build_scheduler` (constructs and configures an
:class:`~apscheduler.schedulers.asyncio.AsyncIOScheduler` with the catch-up job
registered on a 5-minute :class:`~apscheduler.triggers.interval.IntervalTrigger`)
and :func:`main` (a blocking, signal-aware entrypoint suitable for a systemd
service unit).
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from . import ingestion
from .config import Settings, get_settings

__all__ = ["CATCH_UP_JOB_ID", "INTERVAL_MINUTES", "build_scheduler", "main", "run"]

logger = logging.getLogger(__name__)

#: Identifier registered for the catch-up ingestion job.
CATCH_UP_JOB_ID: str = "catch_up_ingestion"

#: Interval, in minutes, between scheduled catch-up runs (Requirement 15.1).
INTERVAL_MINUTES: int = 5


async def _run_catch_up_job(settings: Settings) -> None:
    """Execute one scheduled catch-up ingestion run.

    Wraps :func:`ge_pipeline.ingestion.run_catch_up` so that a failure in one
    run is logged but never propagates out of the scheduler, keeping the daemon
    alive for the next interval. The next run backfills any windows this run
    failed to ingest.

    Args:
        settings: The application settings passed to ``run_catch_up``.
    """
    logger.info("Starting scheduled catch-up ingestion")
    try:
        result = await ingestion.run_catch_up(settings)
    except Exception:  # noqa: BLE001 - keep the daemon alive across run failures
        logger.exception(
            "Scheduled catch-up ingestion failed; the next run will backfill "
            "any gaps left behind"
        )
        return

    logger.info("Scheduled catch-up ingestion finished: %s", result)


def build_scheduler(settings: Settings | None = None) -> AsyncIOScheduler:
    """Build and configure the catch-up ingestion scheduler.

    Creates an :class:`AsyncIOScheduler` and registers
    :func:`ge_pipeline.ingestion.run_catch_up` (via :func:`_run_catch_up_job`)
    on a 5-minute :class:`IntervalTrigger`. Because ``run_catch_up`` is a
    coroutine, an asyncio-based scheduler is used so the job runs on the event
    loop. The returned scheduler is configured but **not** started; callers
    invoke :meth:`AsyncIOScheduler.start` when ready.

    Args:
        settings: Application settings to pass to the ingestion job. When
            ``None``, :func:`ge_pipeline.config.get_settings` is called to load
            them (deferred so importing this module has no side effects).

    Returns:
        A configured :class:`AsyncIOScheduler` with the catch-up job registered
        on a 5-minute interval trigger.
    """
    if settings is None:
        settings = get_settings()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        _run_catch_up_job,
        trigger=IntervalTrigger(minutes=INTERVAL_MINUTES),
        args=[settings],
        id=CATCH_UP_JOB_ID,
        name="OSRS GE catch-up ingestion",
        replace_existing=True,
        # If a run is missed (for example the machine was asleep), collapse the
        # backlog into a single catch-up run rather than firing repeatedly; the
        # single run still backfills every missing window.
        coalesce=True,
        max_instances=1,
    )
    return scheduler


async def _serve(settings: Settings | None = None) -> None:
    """Run the scheduler until a shutdown signal is received.

    Starts the scheduler on the running event loop, installs SIGINT/SIGTERM
    handlers for graceful shutdown, and blocks until one of those signals is
    delivered. On shutdown the scheduler is stopped so in-flight jobs can
    finish.

    Args:
        settings: Application settings forwarded to :func:`build_scheduler`.
    """
    scheduler = build_scheduler(settings)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_shutdown() -> None:
        logger.info("Shutdown signal received; stopping scheduler")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except NotImplementedError:  # pragma: no cover - non-POSIX platforms
            signal.signal(sig, lambda *_: _request_shutdown())

    scheduler.start()
    logger.info(
        "Scheduler started; catch-up ingestion will run every %d minutes",
        INTERVAL_MINUTES,
    )
    try:
        await stop_event.wait()
    finally:
        scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")


def main() -> None:
    """Entry point for running the scheduler as a long-running daemon.

    Configures basic logging and runs the scheduler event loop forever, until
    interrupted by SIGINT or SIGTERM. Suitable as the ``ExecStart`` target of a
    systemd service unit, for example::

        [Unit]
        Description=OSRS GE catch-up ingestion scheduler
        After=network-online.target

        [Service]
        Type=simple
        ExecStart=/path/to/.venv/bin/python -m ge_pipeline.scheduler
        Restart=on-failure
        Environment=INFLUX_URL=... INFLUX_TOKEN=... INFLUX_ORG=... INFLUX_BUCKET=...

        [Install]
        WantedBy=multi-user.target
    """
    level = logging.getLevelName(os.environ.get("LOG_LEVEL", "INFO").upper())
    if not isinstance(level, int):
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_serve())
    except (KeyboardInterrupt, SystemExit):  # pragma: no cover - signal path
        logger.info("Scheduler daemon exiting")


#: Alias so callers/systemd can reference either ``main`` or ``run``.
run = main


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    main()
