"""Typer command-line interface for :mod:`ge_pipeline`.

Exposes the ``ingest``, ``backfill``, ``setup``, ``serve``, and ``export``
commands as a Typer application. The module-level :data:`app` object is the
console entry point declared in ``pyproject.toml``
(``ge-pipeline = "ge_pipeline.cli:app"``).

Commands that require configuration read it lazily through
:func:`ge_pipeline.config.get_settings`. When a required setting is missing the
command prints remediation guidance (copy ``.env.example`` to ``.env`` and set
the InfluxDB connection values) and exits with a non-zero status. The shared
:func:`_require_settings` helper centralizes that behavior so every command
reports missing configuration identically.
"""

from __future__ import annotations

import asyncio
import logging
import time

import typer

from .config import Settings, get_settings
from .errors import ConfigError

__all__ = ["app"]

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="OSRS Grand Exchange price pipeline: ingest, serve, and export data.",
    no_args_is_help=True,
)

#: Guidance printed when a required configuration value is missing. Names the
#: remediation step and the required variables so the operator can recover.
_REMEDIATION = (
    "Configuration missing: copy .env.example to .env and set INFLUX_URL/"
    "INFLUX_TOKEN/INFLUX_ORG/INFLUX_BUCKET.\n"
    "  cp .env.example .env"
)

#: Duration suffixes accepted by ``--range`` (e.g. ``7d``), mapped to seconds.
_RANGE_UNITS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}

#: Earliest snapshot timestamp to fall back to for ``--range all`` exports.
_EARLIEST_TIMESTAMP = 1615733100


def _require_settings() -> Settings:
    """Return the application settings or exit with remediation guidance.

    Wraps :func:`ge_pipeline.config.get_settings` so that a missing or empty
    required variable is reported uniformly across every command: the
    remediation message is written to stderr and the process exits with a
    non-zero status instead of surfacing a raw traceback.

    Returns:
        The loaded, cached :class:`Settings`.

    Raises:
        typer.Exit: With code ``1`` when configuration is missing
            (:class:`~ge_pipeline.errors.ConfigError`).
    """
    try:
        return get_settings()
    except ConfigError as exc:
        typer.echo(f"{exc}", err=True)
        typer.echo(_REMEDIATION, err=True)
        raise typer.Exit(code=1) from exc


def _parse_range(value: str) -> int:
    """Convert a ``--range`` duration string into a number of seconds.

    Args:
        value: A duration such as ``"30s"``, ``"5m"``, ``"7d"``, or ``"1w"``. A
            bare integer is treated as seconds.

    Returns:
        The duration in seconds.

    Raises:
        typer.BadParameter: If ``value`` is empty or not a recognized duration.
    """
    text = value.strip()
    if not text:
        raise typer.BadParameter("range must be a non-empty duration (e.g. 7d)")

    unit = text[-1]
    try:
        if unit.isdigit():
            seconds = int(text)
        else:
            if unit not in _RANGE_UNITS:
                raise ValueError(unit)
            seconds = int(text[:-1]) * _RANGE_UNITS[unit]
    except ValueError as exc:
        raise typer.BadParameter(
            f"invalid range {value!r}; expected a duration like 7d, 12h, 90m"
        ) from exc

    if seconds <= 0:
        raise typer.BadParameter("range must be a positive duration")
    return seconds


def _resolve_window(
    range_: str | None,
    start: int | None,
    stop: int | None,
) -> tuple[int, int]:
    """Resolve export time bounds from ``--range`` or explicit ``--start/--stop``.

    Precedence: explicit ``start``/``stop`` win when provided; otherwise a
    ``--range`` value is interpreted as "the last N seconds up to now". When
    nothing is supplied the window spans from the project's earliest snapshot to
    now.

    Args:
        range_: A duration string (e.g. ``"7d"``) or ``"all"``, or ``None``.
        start: Explicit inclusive start (unix seconds), or ``None``.
        stop: Explicit exclusive stop (unix seconds), or ``None``.

    Returns:
        A ``(start, stop)`` tuple of unix-second bounds with ``start <= stop``.

    Raises:
        typer.BadParameter: If the resolved ``start`` is after ``stop``.
    """
    now = int(time.time())
    resolved_stop = stop if stop is not None else now

    if start is not None:
        resolved_start = start
    elif range_ is not None and range_.strip().lower() != "all":
        resolved_start = resolved_stop - _parse_range(range_)
    else:
        resolved_start = _EARLIEST_TIMESTAMP

    if resolved_start > resolved_stop:
        raise typer.BadParameter(
            f"start ({resolved_start}) must be <= stop ({resolved_stop})"
        )
    return resolved_start, resolved_stop


def _run_catch_up_and_report(label: str) -> None:
    """Run catch-up ingestion and echo the resulting counts.

    Shared by :func:`ingest` and :func:`backfill` because both drive the same
    catch-up mechanism (a catch-up run naturally backfills any windows missing
    between the latest stored timestamp and now).

    Args:
        label: A human-readable label for the run, used in the output header.
    """
    # Imported lazily so importing the CLI module has no heavy side effects.
    from .ingestion import run_catch_up

    settings = _require_settings()
    result = asyncio.run(run_catch_up(settings))

    typer.echo(f"{label} complete:")
    typer.echo(f"  records written:      {result.records_written}")
    typer.echo(f"  timestamps processed: {result.timestamps_processed}")
    typer.echo(f"  timestamps skipped:   {result.timestamps_skipped}")
    typer.echo(f"  failures:             {result.failures}")


@app.command()
def ingest() -> None:
    """Run catch-up ingestion and report written/processed/failure counts.

    Fetches every missing 5-minute window between the latest stored timestamp
    and now, writes the resulting records to InfluxDB, and echoes the counts of
    records written, timestamps processed, and failures.
    """
    _run_catch_up_and_report("Ingestion")


@app.command()
def backfill() -> None:
    """Backfill missing windows via the shared catch-up mechanism.

    This shares the same catch-up path as :func:`ingest`: ``run_catch_up``
    detects the latest stored timestamp and fetches every missing 5-minute
    window up to now, so any gaps left by prior failed runs are filled in.
    """
    _run_catch_up_and_report("Backfill")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Host interface to bind."),
    port: int = typer.Option(8000, help="TCP port to listen on."),
    reload: bool = typer.Option(
        False, "--reload", help="Enable auto-reload (development only)."
    ),
) -> None:
    """Start the FastAPI query service with uvicorn on the given host and port.

    Args:
        host: The host interface to bind (defaults to localhost).
        port: The TCP port to listen on.
        reload: Enable uvicorn's auto-reload for development.
    """
    import uvicorn

    typer.echo(f"Starting FastAPI query service on http://{host}:{port}")
    uvicorn.run("ge_pipeline.api:app", host=host, port=port, reload=reload)


@app.command()
def export(
    items: str = typer.Option(
        ..., help="Comma-separated item IDs to export (e.g. 554,565)."
    ),
    output: str = typer.Option(
        ..., "--output", "-o", help="Path of the file to write the dataset to."
    ),
    fmt: str = typer.Option(
        "ndjson",
        "--format",
        "-f",
        help="Export format: ndjson, csv, or parquet.",
    ),
    interval: str = typer.Option(
        "1h", help="Downsample interval (e.g. 5m, 1h, 1d)."
    ),
    range_: str | None = typer.Option(
        None,
        "--range",
        help="Relative range ending now (e.g. 7d) or 'all'.",
    ),
    start: int | None = typer.Option(
        None, help="Explicit inclusive start (unix seconds); overrides --range."
    ),
    stop: int | None = typer.Option(
        None, help="Explicit exclusive stop (unix seconds); defaults to now."
    ),
) -> None:
    """Stream a bulk dataset to a file using bounded memory.

    Resolves the requested items, time window, interval, and format, then
    iterates :func:`ge_pipeline.data_access.stream_dataset` and writes each byte
    chunk straight to ``output``. Only one time-chunk's rows are held in memory
    at a time, so exporting years of data never buffers the full result set.

    Args:
        items: Comma-separated item IDs.
        output: Destination file path.
        fmt: One of ``ndjson``, ``csv``, or ``parquet``.
        interval: Downsample interval passed to the query layer.
        range_: Relative range ending now, or ``all``.
        start: Explicit inclusive start (unix seconds).
        stop: Explicit exclusive stop (unix seconds).

    Raises:
        typer.Exit: With code ``1`` on missing configuration, an unsupported
            format, or an empty item list.
    """
    from .data_access import SUPPORTED_EXPORT_FORMATS, stream_dataset

    if fmt not in SUPPORTED_EXPORT_FORMATS:
        typer.echo(
            f"Unsupported format {fmt!r}; expected one of "
            f"{list(SUPPORTED_EXPORT_FORMATS)}.",
            err=True,
        )
        raise typer.Exit(code=1)

    item_ids = [item.strip() for item in items.split(",") if item.strip()]
    if not item_ids:
        typer.echo("No item IDs provided; pass --items 554,565", err=True)
        raise typer.Exit(code=1)

    settings = _require_settings()

    # Imported lazily to keep module import lightweight.
    from . import influx

    client = influx.get_client(settings)
    resolved_start, resolved_stop = _resolve_window(range_, start, stop)

    typer.echo(
        f"Exporting {len(item_ids)} item(s) [{resolved_start}, {resolved_stop}) "
        f"interval={interval} format={fmt} -> {output}"
    )

    bytes_written = 0
    try:
        with open(output, "wb") as handle:
            for chunk in stream_dataset(
                client,
                item_ids,
                resolved_start,
                resolved_stop,
                interval,
                fmt,
            ):
                handle.write(chunk)
                bytes_written += len(chunk)
    except ValueError as exc:
        typer.echo(f"Export failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Export complete: wrote {bytes_written} byte(s) to {output}")


@app.command()
def setup() -> None:
    """Prepare InfluxDB: health check, ensure the bucket, verify write access.

    Folds in the standalone ``setup_influxdb.py`` logic using the package's
    settings and reusable client: pings InfluxDB, creates the target bucket with
    infinite retention when it does not already exist, and writes a test point
    to confirm the token has write access to the bucket.

    Raises:
        typer.Exit: With code ``1`` on missing configuration or a failed health
            check / write-access check.
    """
    from influxdb_client import BucketRetentionRules
    from influxdb_client.client.write_api import SYNCHRONOUS
    from influxdb_client.rest import ApiException

    from . import influx

    settings = _require_settings()
    client = influx.get_client(settings)

    # 1. Health check.
    typer.echo(f"Checking InfluxDB health at {settings.influx_url} ...")
    health = client.health()
    if health.status != "pass":
        typer.echo(
            f"InfluxDB health check failed: {health.message}", err=True
        )
        typer.echo(
            f"Cannot reach InfluxDB. Make sure it is running at "
            f"{settings.influx_url}.",
            err=True,
        )
        raise typer.Exit(code=1)
    typer.echo(f"InfluxDB is healthy (version {health.version}).")

    # 2. Ensure the bucket exists.
    buckets_api = client.buckets_api()
    existing = buckets_api.find_bucket_by_name(settings.influx_bucket)
    if existing is not None:
        typer.echo(
            f"Bucket {settings.influx_bucket!r} already exists "
            f"(id: {existing.id})."
        )
    else:
        typer.echo(
            f"Creating bucket {settings.influx_bucket!r} in org "
            f"{settings.influx_org!r} ..."
        )
        retention = BucketRetentionRules(type="expire", every_seconds=0)
        buckets_api.create_bucket(
            bucket_name=settings.influx_bucket,
            retention_rules=retention,
            org=settings.influx_org,
        )
        typer.echo(f"Bucket {settings.influx_bucket!r} created successfully.")

    # 3. Verify write access with a test point.
    write_api = client.write_api(write_options=SYNCHRONOUS)
    test_record = {
        "measurement": "setup_test",
        "tags": {"source": "ge_pipeline_cli"},
        "fields": {"value": 1},
    }
    try:
        write_api.write(
            bucket=settings.influx_bucket,
            write_precision="s",
            record=test_record,
        )
    except (ApiException, OSError) as exc:
        typer.echo(f"Test write failed: {exc}", err=True)
        typer.echo(
            f"Check that INFLUX_TOKEN has write permission to "
            f"{settings.influx_bucket!r}.",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    typer.echo(f"Test write to bucket {settings.influx_bucket!r} succeeded.")
    typer.echo("Setup complete. You can now run: ge-pipeline ingest")


if __name__ == "__main__":
    app()
