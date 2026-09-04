"""Lazy, cached configuration service for :mod:`ge_pipeline`.

Exposes :class:`Settings` (a frozen dataclass of connection and tuning values)
and :func:`get_settings`, which loads settings from the environment/``.env`` on
first call only and caches the result. No environment reads occur at import
time, so importing this module has no side effects and raises no errors.

A required variable that is unset or empty at first access causes
:func:`get_settings` to raise :class:`ge_pipeline.errors.ConfigError`, naming
the first missing variable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from .errors import ConfigError

__all__ = ["Settings", "get_settings"]

# Environment variables that must be present and non-empty at first access,
# checked in this order so the error names the *first* missing one.
_REQUIRED_ENV_VARS: tuple[str, ...] = (
    "INFLUX_URL",
    "INFLUX_TOKEN",
    "INFLUX_ORG",
    "INFLUX_BUCKET",
)


@dataclass(frozen=True)
class Settings:
    """Immutable application settings for the pipeline.

    Attributes:
        influx_url: Base URL of the InfluxDB v2 server (required).
        influx_token: InfluxDB API token used for authentication (required).
        influx_org: InfluxDB organization name (required).
        influx_bucket: InfluxDB bucket that holds the price data (required).
        user_agent: ``User-Agent`` header sent to the Wiki API. Defaults to
            ``"GEoutlier-detection"``.
        api_base_url: Base URL of the RuneScape Wiki OSRS price API. Defaults to
            ``"https://prices.runescape.wiki/api/v1/osrs"``.
        max_concurrency: Maximum number of concurrent Wiki API fetches during
            ingestion. Defaults to ``8``.
        batch_size: Number of processed timestamps per InfluxDB write batch.
            Defaults to ``50``.
    """

    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket: str
    user_agent: str = "GEoutlier-detection"
    api_base_url: str = "https://prices.runescape.wiki/api/v1/osrs"
    max_concurrency: int = 8
    batch_size: int = 50


# Process-wide cache. Populated on the first successful call to get_settings().
_cached_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the cached :class:`Settings`, loading them on first call only.

    On the first call, ``.env`` (if present) and the process environment are
    read, the four required variables are validated, and a fully-populated
    :class:`Settings` is built and cached. Subsequent calls return the identical
    cached object without re-reading the environment.

    Returns:
        The shared, cached :class:`Settings` instance.

    Raises:
        ConfigError: If any of ``INFLUX_URL``, ``INFLUX_TOKEN``, ``INFLUX_ORG``,
            or ``INFLUX_BUCKET`` is unset or empty at first access. The message
            names the first missing variable.
    """
    global _cached_settings

    if _cached_settings is not None:
        return _cached_settings

    # Load .env into the environment without overriding values already set in
    # the process environment. Missing .env files are silently ignored.
    load_dotenv(override=False)

    for name in _REQUIRED_ENV_VARS:
        value = os.environ.get(name)
        if value is None or value == "":
            raise ConfigError(
                f"Required environment variable {name!r} is unset or empty. "
                "Copy .env.example to .env and set the InfluxDB connection values."
            )

    settings = Settings(
        influx_url=os.environ["INFLUX_URL"],
        influx_token=os.environ["INFLUX_TOKEN"],
        influx_org=os.environ["INFLUX_ORG"],
        influx_bucket=os.environ["INFLUX_BUCKET"],
    )

    _cached_settings = settings
    return settings
