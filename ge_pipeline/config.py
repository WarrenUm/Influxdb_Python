"""Lazy, cached configuration service for :mod:`ge_pipeline`.

Exposes :class:`Settings` (a frozen dataclass of connection and tuning values)
and :func:`get_settings`, which loads settings from the environment/``.env`` on
first call only and caches the result. No environment reads occur at import
time, so importing this module has no side effects and raises no errors.

The pipeline targets **InfluxDB 3 Core**. The three v3 target variables
(``INFLUXDB3_HOST_URL``, ``INFLUXDB3_AUTH_TOKEN``, ``INFLUXDB3_DATABASE_NAME``)
are required for every command and validated eagerly on first
:func:`get_settings`. The four v2 source variables (``V2_INFLUX_URL``,
``V2_INFLUX_TOKEN``, ``V2_INFLUX_ORG``, ``V2_INFLUX_BUCKET``) are needed only by
the migration tool and are therefore validated lazily via
:func:`require_migration_source`, so ``ingest``/``serve``/``export`` never
require the source configuration.

A required variable that is unset or empty causes the validating function to
raise :class:`ge_pipeline.errors.ConfigError`, naming the first missing
variable in declared order.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from .errors import ConfigError

__all__ = ["MigrationSource", "Settings", "get_settings", "require_migration_source"]

# V3 target variables. Required for every command; checked in this order so the
# error names the *first* missing one (Requirement 2.2).
_REQUIRED_V3_VARS: tuple[str, ...] = (
    "INFLUXDB3_HOST_URL",
    "INFLUXDB3_AUTH_TOKEN",
    "INFLUXDB3_DATABASE_NAME",
)

# V2 source variables. Required only when the migration tool runs, so these are
# validated lazily by require_migration_source() in this order (Requirement 2.4).
_REQUIRED_V2_VARS: tuple[str, ...] = (
    "V2_INFLUX_URL",
    "V2_INFLUX_TOKEN",
    "V2_INFLUX_ORG",
    "V2_INFLUX_BUCKET",
)


@dataclass(frozen=True)
class Settings:
    """Immutable application settings for the pipeline.

    Attributes:
        influx3_host: Base URL of the InfluxDB 3 Core server, for example
            ``"http://localhost:8181"`` (required; ``INFLUXDB3_HOST_URL``).
        influx3_token: InfluxDB 3 authentication token used for all v3
            requests (required; ``INFLUXDB3_AUTH_TOKEN``).
        influx3_database: Name of the InfluxDB 3 database that holds the price
            data (required; ``INFLUXDB3_DATABASE_NAME``).
        v2_url: Base URL of the InfluxDB v2 source server, used only by the
            migration tool (optional; ``V2_INFLUX_URL``).
        v2_token: InfluxDB v2 source API token, used only by the migration
            tool (optional; ``V2_INFLUX_TOKEN``).
        v2_org: InfluxDB v2 source organization, used only by the migration
            tool (optional; ``V2_INFLUX_ORG``).
        v2_bucket: InfluxDB v2 source bucket, used only by the migration tool
            (optional; ``V2_INFLUX_BUCKET``).
        user_agent: ``User-Agent`` header sent to the Wiki API. Defaults to
            ``"GEoutlier-detection"``.
        api_base_url: Base URL of the RuneScape Wiki OSRS price API. Defaults to
            ``"https://prices.runescape.wiki/api/v1/osrs"``.
        max_concurrency: Maximum number of concurrent Wiki API fetches during
            ingestion. Defaults to ``8``.
        batch_size: Number of processed timestamps per InfluxDB write batch.
            Defaults to ``50``.
    """

    # --- V3 target (required for all commands) ---
    influx3_host: str
    influx3_token: str
    influx3_database: str

    # --- V3 source / v2 (required ONLY for migration) ---
    v2_url: str | None = None
    v2_token: str | None = None
    v2_org: str | None = None
    v2_bucket: str | None = None

    # --- unchanged tuning / Wiki API ---
    user_agent: str = "GEoutlier-detection"
    api_base_url: str = "https://prices.runescape.wiki/api/v1/osrs"
    max_concurrency: int = 8
    batch_size: int = 50


@dataclass(frozen=True)
class MigrationSource:
    """Validated InfluxDB v2 source settings for the migration tool.

    All four values are guaranteed present and non-empty; this type is only
    produced by :func:`require_migration_source`.

    Attributes:
        url: Base URL of the InfluxDB v2 source server (``V2_INFLUX_URL``).
        token: InfluxDB v2 source API token (``V2_INFLUX_TOKEN``).
        org: InfluxDB v2 source organization (``V2_INFLUX_ORG``).
        bucket: InfluxDB v2 source bucket (``V2_INFLUX_BUCKET``).
    """

    url: str
    token: str
    org: str
    bucket: str


# Process-wide cache. Populated on the first successful call to get_settings().
_cached_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the cached :class:`Settings`, loading them on first call only.

    On the first call, ``.env`` (if present) and the process environment are
    read, the three required v3 target variables are validated, and a
    fully-populated :class:`Settings` is built and cached. Any optional v2
    source values that happen to be set are captured too, but they are *not*
    validated here; that is deferred to :func:`require_migration_source`.
    Subsequent calls return the identical cached object without re-reading the
    environment.

    Returns:
        The shared, cached :class:`Settings` instance.

    Raises:
        ConfigError: If any of ``INFLUXDB3_HOST_URL``, ``INFLUXDB3_AUTH_TOKEN``,
            or ``INFLUXDB3_DATABASE_NAME`` is unset or empty at first access.
            The message names the first missing variable in declared order.
    """
    global _cached_settings

    if _cached_settings is not None:
        return _cached_settings

    # Load .env into the environment without overriding values already set in
    # the process environment. Missing .env files are silently ignored.
    load_dotenv(override=False)

    for name in _REQUIRED_V3_VARS:
        value = os.environ.get(name)
        if value is None or value == "":
            raise ConfigError(
                f"Required environment variable {name!r} is unset or empty. "
                "Copy .env.example to .env and set the InfluxDB 3 connection values."
            )

    settings = Settings(
        influx3_host=os.environ["INFLUXDB3_HOST_URL"],
        influx3_token=os.environ["INFLUXDB3_AUTH_TOKEN"],
        influx3_database=os.environ["INFLUXDB3_DATABASE_NAME"],
        v2_url=os.environ.get("V2_INFLUX_URL") or None,
        v2_token=os.environ.get("V2_INFLUX_TOKEN") or None,
        v2_org=os.environ.get("V2_INFLUX_ORG") or None,
        v2_bucket=os.environ.get("V2_INFLUX_BUCKET") or None,
    )

    _cached_settings = settings
    return settings


def require_migration_source(settings: Settings) -> MigrationSource:
    """Validate and return the InfluxDB v2 source settings for migration.

    Unlike the v3 target values, the ``V2_*`` source values are validated only
    when the migration tool asks for them, so non-migration commands never fail
    just because the source is not configured (Requirement 2.4).

    Args:
        settings: The application settings, as returned by
            :func:`get_settings`, carrying the optional v2 source values.

    Returns:
        A :class:`MigrationSource` with all four source values guaranteed
        present and non-empty.

    Raises:
        ConfigError: If any of ``V2_INFLUX_URL``, ``V2_INFLUX_TOKEN``,
            ``V2_INFLUX_ORG``, or ``V2_INFLUX_BUCKET`` is unset or empty. The
            message names the first missing variable in declared order.
    """
    values = {
        "V2_INFLUX_URL": settings.v2_url,
        "V2_INFLUX_TOKEN": settings.v2_token,
        "V2_INFLUX_ORG": settings.v2_org,
        "V2_INFLUX_BUCKET": settings.v2_bucket,
    }

    for name in _REQUIRED_V2_VARS:
        value = values[name]
        if value is None or value == "":
            raise ConfigError(
                f"Required environment variable {name!r} is unset or empty. "
                "Set the InfluxDB v2 source values before running migration."
            )

    return MigrationSource(
        url=settings.v2_url,  # type: ignore[arg-type]
        token=settings.v2_token,  # type: ignore[arg-type]
        org=settings.v2_org,  # type: ignore[arg-type]
        bucket=settings.v2_bucket,  # type: ignore[arg-type]
    )
