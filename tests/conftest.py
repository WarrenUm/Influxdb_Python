"""Shared pytest fixtures and configuration for the ge_pipeline test suite.

Registers a Hypothesis profile named ``ge`` that runs at least 100 examples per
property (per design Requirement 21.2) and activates it for the session. Shared
fixtures (mocked httpx clients, mocked InfluxDB clients, seeded stores, and
Hypothesis strategies) are added as the corresponding features are implemented
in later tasks.
"""

from __future__ import annotations

from hypothesis import HealthCheck, settings

# Property-based tests must run >= 100 iterations (design Requirement 21.2).
settings.register_profile(
    "ge",
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("ge")


# ---------------------------------------------------------------------------
# InfluxDB 3 Core testcontainer fixtures (Task 11.1, Requirements 10.1, 10.2)
# ---------------------------------------------------------------------------
#
# These fixtures serve a live, ephemeral **InfluxDB 3 Core** instance via
# ``testcontainers`` so the query/write and setup/migration integration tests
# (design "Testing Strategy": "A live InfluxDB 3 Core instance via
# ``testcontainers`` backs the query/write and setup/migration integration
# tests") can run against a real server. They are centralized here so every
# integration module shares one container definition rather than each defining
# its own.
#
# When Docker or ``testcontainers`` is unavailable, the fixtures ``skip`` (not
# error) so the suite stays green in environments without a container runtime.

import time as _time

import pytest

# InfluxDB 3 Core image and the port its HTTP/Flight endpoint listens on.
INFLUXDB3_IMAGE = "quay.io/influxdb/influxdb3-core:latest"
INFLUXDB3_PORT = 8181
# Any non-empty token is accepted because the server runs with auth disabled.
INFLUXDB3_TEST_TOKEN = "test-token"
INFLUXDB3_TEST_DATABASE = "ge_test"


def _influx3_server_ready(host: str, token: str, database: str) -> bool:
    """Return True once the v3 server answers a trivial write+query.

    Rather than parse log lines (which can drift between releases), this probes
    readiness by doing a tiny write and reading it back through the same client
    the tests use.
    """
    from ge_pipeline import influx
    from ge_pipeline.config import Settings

    settings_obj = Settings(
        influx3_host=host, influx3_token=token, influx3_database=database
    )
    influx._client_cache.clear()
    client = influx.get_client(settings_obj)
    probe = {
        "measurement": influx.MEASUREMENT,
        "tags": {"itemID": "__readiness__"},
        "time": 1,
        "fields": {"avgHighPrice": 1},
    }
    try:
        client.write(
            record=[influx._record_to_point(probe)],
            write_precision=influx.WRITE_PRECISION,
        )
        client.query(
            query=f"SELECT 1 FROM {influx._QUOTED_MEASUREMENT} LIMIT 1",
            language="sql",
            query_parameters={},
        )
    except Exception:  # noqa: BLE001 - any failure means "not ready yet"
        return False
    return True


@pytest.fixture(scope="session")
def influx3_container():
    """Start an ephemeral in-memory InfluxDB 3 Core server for the session.

    Runs ``influxdb3 serve`` with an in-memory object store and auth disabled so
    tests need no token provisioning. If ``testcontainers`` (or Docker) is
    unavailable, or the image cannot be pulled / the container cannot start
    (no network), the requesting test is skipped rather than failed.

    Session-scoped so the container is started once and shared across every
    integration module (query/write and setup/migration), rather than restarted
    per module.
    """
    try:  # pragma: no cover - exercised only by presence/absence of the dep
        from testcontainers.core.container import DockerContainer
    except Exception as exc:  # noqa: BLE001 - any import/runtime failure means skip # pragma: no cover
        pytest.skip(
            f"testcontainers is unavailable ({exc}); skipping InfluxDB 3 "
            "integration tests"
        )

    from ge_pipeline import influx

    try:
        container = (
            DockerContainer(INFLUXDB3_IMAGE)
            .with_exposed_ports(INFLUXDB3_PORT)
            .with_command(
                "serve --node-id test-node --object-store memory "
                f"--without-auth --http-bind 0.0.0.0:{INFLUXDB3_PORT}"
            )
        )
        container.start()
    except Exception as exc:  # noqa: BLE001 - any startup failure means skip # pragma: no cover
        pytest.skip(
            f"could not start InfluxDB 3 Core container ({exc}); skipping "
            "integration tests"
        )

    try:
        # Give the process a moment to bind, then wait for a real read/write to
        # succeed before handing the container to tests.
        host_ip = container.get_container_host_ip()
        mapped_port = int(container.get_exposed_port(INFLUXDB3_PORT))
        host_url = f"http://{host_ip}:{mapped_port}"

        deadline = _time.time() + 60
        ready = False
        while _time.time() < deadline:
            if _influx3_server_ready(
                host_url, INFLUXDB3_TEST_TOKEN, INFLUXDB3_TEST_DATABASE
            ):
                ready = True
                break
            _time.sleep(1)
        if not ready:  # pragma: no cover - environment dependent
            pytest.skip(
                "InfluxDB 3 Core container did not become ready in time; "
                "skipping integration tests"
            )
        yield host_url
    finally:
        influx._client_cache.clear()
        container.stop()


@pytest.fixture
def influx3_client(influx3_container):
    """Return a v3 client bound to the running container's database.

    The module-level client cache is cleared around each test so a test's
    writes/reads run against a fresh client bound to the container.
    """
    from ge_pipeline import influx
    from ge_pipeline.config import Settings

    host_url = influx3_container
    settings_obj = Settings(
        influx3_host=host_url,
        influx3_token=INFLUXDB3_TEST_TOKEN,
        influx3_database=INFLUXDB3_TEST_DATABASE,
    )
    influx._client_cache.clear()
    client = influx.get_client(settings_obj)
    yield client, settings_obj
