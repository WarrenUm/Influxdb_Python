"""Unit tests for the InfluxDB 3 Core database setup helper.

These tests exercise :func:`ge_pipeline.admin.setup_v3_database` without a live
server. The HTTP layer is driven by :class:`httpx.MockTransport`, injected via
the ``client_factory`` seam, so each test controls exactly how the ``/health``
preflight and ``/api/v3/configure/database`` create endpoints respond.

Covered behavior (Requirement 4):

* created outcome -- preflight succeeds and the create endpoint returns 2xx, so
  the helper returns ``"created"`` (Requirement 4.1).
* exists outcome -- the create endpoint returns ``409 Conflict``, so the helper
  returns ``"exists"`` without issuing a duplicate create (Requirement 4.2).
* unreachable host -- the preflight raises a transport error, so the helper
  raises :class:`ConnectionError` naming ``settings.influx3_host`` and stops
  before attempting the create (Requirement 4.3).
* create-endpoint failures -- a transport error or an unexpected non-2xx status
  on the create call raises :class:`ConnectionError` naming the host.
"""

from __future__ import annotations

import httpx
import pytest

from ge_pipeline import admin
from ge_pipeline.admin import CREATE_DATABASE_PATH, HEALTH_PATH, setup_v3_database
from ge_pipeline.config import Settings

HOST = "http://influx3.example:8181"


@pytest.fixture
def settings() -> Settings:
    """Return Settings with dummy v3 connection values."""
    return Settings(
        influx3_host=HOST,
        influx3_token="test-token",
        influx3_database="ge_test",
    )


def _factory_from_handler(handler):
    """Build a ``client_factory`` whose client uses a MockTransport.

    The returned callable matches the signature ``setup_v3_database`` uses to
    construct its client (``base_url``, ``headers``, ``timeout``), and routes
    every request through ``handler`` instead of the network.
    """

    def factory(*, base_url, headers, timeout):
        return httpx.Client(
            transport=httpx.MockTransport(handler),
            base_url=base_url,
            headers=headers,
            timeout=timeout,
        )

    return factory


def test_returns_created_when_create_succeeds(settings: Settings) -> None:
    """Preflight OK + create 2xx returns ``"created"`` (Requirement 4.1)."""
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.url.path == HEALTH_PATH:
            return httpx.Response(200, text="OK")
        if request.url.path == CREATE_DATABASE_PATH:
            return httpx.Response(201, json={"db": "ge_test"})
        raise AssertionError(f"unexpected path {request.url.path!r}")

    result = setup_v3_database(
        settings, client_factory=_factory_from_handler(handler)
    )

    assert result == "created"
    # Preflight then create, in that order, and nothing else.
    assert seen == [
        ("GET", HEALTH_PATH),
        ("POST", CREATE_DATABASE_PATH),
    ]


def test_returns_exists_on_conflict_without_duplicate(settings: Settings) -> None:
    """Create returning 409 yields ``"exists"`` and a single create (Req 4.2)."""
    create_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal create_calls
        if request.url.path == HEALTH_PATH:
            return httpx.Response(200, text="OK")
        if request.url.path == CREATE_DATABASE_PATH:
            create_calls += 1
            return httpx.Response(409, json={"error": "database already exists"})
        raise AssertionError(f"unexpected path {request.url.path!r}")

    result = setup_v3_database(
        settings, client_factory=_factory_from_handler(handler)
    )

    assert result == "exists"
    # No duplicate create attempt was made.
    assert create_calls == 1


def test_unreachable_host_raises_naming_host_and_skips_create(
    settings: Settings,
) -> None:
    """Preflight transport error -> ConnectionError naming host, no create.

    The create endpoint must never be reached because the preflight fails first
    (Requirement 4.3).
    """
    create_attempted = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal create_attempted
        if request.url.path == HEALTH_PATH:
            raise httpx.ConnectError("connection refused", request=request)
        if request.url.path == CREATE_DATABASE_PATH:
            create_attempted = True
            return httpx.Response(201)
        raise AssertionError(f"unexpected path {request.url.path!r}")

    with pytest.raises(ConnectionError) as excinfo:
        setup_v3_database(settings, client_factory=_factory_from_handler(handler))

    assert HOST in str(excinfo.value)
    assert create_attempted is False


def test_create_transport_error_raises_naming_host(settings: Settings) -> None:
    """A transport error on create raises ConnectionError naming the host."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == HEALTH_PATH:
            return httpx.Response(200, text="OK")
        if request.url.path == CREATE_DATABASE_PATH:
            raise httpx.ConnectError("connection reset", request=request)
        raise AssertionError(f"unexpected path {request.url.path!r}")

    with pytest.raises(ConnectionError) as excinfo:
        setup_v3_database(settings, client_factory=_factory_from_handler(handler))

    assert HOST in str(excinfo.value)


def test_create_unexpected_status_raises_naming_host(settings: Settings) -> None:
    """An unexpected non-2xx create status raises ConnectionError naming host."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == HEALTH_PATH:
            return httpx.Response(200, text="OK")
        if request.url.path == CREATE_DATABASE_PATH:
            return httpx.Response(500, json={"error": "internal error"})
        raise AssertionError(f"unexpected path {request.url.path!r}")

    with pytest.raises(ConnectionError) as excinfo:
        setup_v3_database(settings, client_factory=_factory_from_handler(handler))

    assert HOST in str(excinfo.value)


def test_defaults_to_httpx_client_factory(monkeypatch, settings: Settings) -> None:
    """When no factory is given, httpx.Client is used (default seam).

    The client is constructed with a MockTransport substituted for the real
    network transport, confirming the default path is exercised end-to-end.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == HEALTH_PATH:
            return httpx.Response(200, text="OK")
        return httpx.Response(201, json={"db": "ge_test"})

    real_client = httpx.Client

    def patched_client(*args, **kwargs):
        kwargs.setdefault("transport", httpx.MockTransport(handler))
        return real_client(*args, **kwargs)

    monkeypatch.setattr(admin.httpx, "Client", patched_client)

    result = setup_v3_database(settings)

    assert result == "created"
