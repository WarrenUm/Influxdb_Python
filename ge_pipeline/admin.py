"""InfluxDB 3 Core database administration for :mod:`ge_pipeline`.

Provides :func:`setup_v3_database`, the helper behind the CLI ``setup`` command
(Requirement 4). It ensures the configured v3 target database exists before
ingestion or migration runs.

InfluxDB 3 Core exposes database administration over its HTTP management API
rather than through the ``influxdb3-python`` query client, so this module talks
to the server directly with :mod:`httpx` (already a project dependency):

* ``GET /health`` -- a lightweight preflight to confirm the server is
  reachable (Requirement 4.3).
* ``POST /api/v3/configure/database`` with a JSON body ``{"db": <name>}`` --
  creates a database. The server responds ``409 Conflict`` when the database
  already exists, which is treated as success and reported as ``"exists"``
  without creating a duplicate (Requirement 4.2).

The HTTP layer is isolated behind an injectable ``client_factory`` so the
function can be unit-tested by mocking the transport without a live server.
"""

from __future__ import annotations

import logging

import httpx

from .config import Settings

__all__ = ["CREATE_DATABASE_PATH", "HEALTH_PATH", "setup_v3_database"]

logger = logging.getLogger(__name__)

#: Lightweight preflight endpoint used to confirm the server is reachable.
HEALTH_PATH = "/health"

#: InfluxDB 3 Core management endpoint for creating a database. A ``POST`` with
#: a JSON body ``{"db": <name>}`` creates the database; a ``409`` response means
#: it already exists.
CREATE_DATABASE_PATH = "/api/v3/configure/database"

#: HTTP status returned by the create endpoint when the database already exists.
_HTTP_CONFLICT = 409

#: Timeout (seconds) for the preflight and create requests.
_REQUEST_TIMEOUT = 10.0


def setup_v3_database(
    settings: Settings,
    *,
    client_factory: type[httpx.Client] | None = None,
) -> str:
    """Create the configured v3 target database if absent (idempotent).

    Preflights the InfluxDB 3 Core server with a lightweight health call and,
    if it cannot be reached, raises a connection error naming
    ``settings.influx3_host`` (Requirement 4.3). Otherwise it issues a create
    request for ``settings.influx3_database``: a success creates the database
    (Requirement 4.1), while an "already exists" response is caught and reported
    without creating a duplicate (Requirement 4.2).

    Args:
        settings: The application settings holding the InfluxDB 3 connection
            values (``influx3_host``, ``influx3_token``, ``influx3_database``).
        client_factory: Optional :class:`httpx.Client` factory, injected for
            testing so the HTTP transport can be mocked. Defaults to
            :class:`httpx.Client`.

    Returns:
        ``"created"`` when the database was created, or ``"exists"`` when it was
        already present.

    Raises:
        ConnectionError: If the InfluxDB 3 server at ``settings.influx3_host`` is
            unreachable, or the create request fails for any reason other than
            the database already existing. The message names the host.
    """
    factory = client_factory if client_factory is not None else httpx.Client
    host = settings.influx3_host
    headers = {"Authorization": f"Bearer {settings.influx3_token}"}

    with factory(
        base_url=host, headers=headers, timeout=_REQUEST_TIMEOUT
    ) as client:
        _preflight(client, host)
        return _create_database(client, host, settings.influx3_database)


def _preflight(client: httpx.Client, host: str) -> None:
    """Confirm the server is reachable with a lightweight health call.

    Args:
        client: The configured :class:`httpx.Client` bound to ``host``.
        host: The InfluxDB 3 host URL, used only in the error message.

    Raises:
        ConnectionError: If the health call raises a transport error (the server
            is unreachable). The message names ``host``.
    """
    try:
        client.get(HEALTH_PATH)
    except httpx.HTTPError as exc:
        raise ConnectionError(
            f"Cannot reach InfluxDB 3 server at {host!r}: {exc}"
        ) from exc
    logger.debug("InfluxDB 3 server at %s is reachable", host)


def _create_database(client: httpx.Client, host: str, database: str) -> str:
    """Create ``database``, treating an "already exists" response as success.

    Args:
        client: The configured :class:`httpx.Client` bound to ``host``.
        host: The InfluxDB 3 host URL, used only in error messages.
        database: The database name to create.

    Returns:
        ``"created"`` if the create request succeeded, or ``"exists"`` if the
        server reported the database already exists (HTTP 409).

    Raises:
        ConnectionError: If the create request raises a transport error or the
            server returns an unexpected error status. The message names
            ``host``.
    """
    try:
        response = client.post(CREATE_DATABASE_PATH, json={"db": database})
    except httpx.HTTPError as exc:
        raise ConnectionError(
            f"Cannot reach InfluxDB 3 server at {host!r}: {exc}"
        ) from exc

    if response.status_code == _HTTP_CONFLICT:
        logger.info("Database %r already exists on %s", database, host)
        return "exists"

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise ConnectionError(
            f"Failed to create database {database!r} on InfluxDB 3 server "
            f"at {host!r}: {exc}"
        ) from exc

    logger.info("Created database %r on %s", database, host)
    return "created"
