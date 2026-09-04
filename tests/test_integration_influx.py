"""InfluxDB test-container round-trip integration test (design Requirement 21.4).

Spins up a real InfluxDB v2 container, writes a batch of price records through
:func:`ge_pipeline.influx.write_batch`, then reads them back through the query
helpers to assert the storage schema and data survive a full round-trip:

* :func:`ge_pipeline.influx.get_latest_timestamp` returns the max timestamp.
* :func:`ge_pipeline.influx.query_price_series` returns the written points in
  ascending time order with all fields present.
* :func:`ge_pipeline.influx.query_chunk` returns a combined multi-item frame.
* :func:`ge_pipeline.influx.list_item_ids` returns the distinct item ids.

The test requires Docker and network access to pull the ``influxdb:2.7`` image.
Because that is not available in every environment, the whole module skips
gracefully: ``testcontainers`` is imported via ``pytest.importorskip`` and any
failure to start the container (Docker not running, image unpullable) turns into
a ``pytest.skip`` rather than a failure, keeping the unit/property suite green.
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest

# Skip the whole module cleanly if testcontainers (or its deps) is unavailable.
pytest.importorskip("testcontainers")

from testcontainers.core.container import DockerContainer  # noqa: E402
from testcontainers.core.waiting_utils import wait_for_logs  # noqa: E402

from ge_pipeline import influx  # noqa: E402
from ge_pipeline.config import Settings  # noqa: E402

# --- Container / seed constants -------------------------------------------

_INFLUX_IMAGE = "influxdb:2.7"
_INFLUX_PORT = 8086
_ORG = "Ge-data-project"
_BUCKET = "GEItemPrices"
_USERNAME = "ci-admin"
_PASSWORD = "ci-password-123"
_ADMIN_TOKEN = "ci-admin-token-please-change"

# Base timestamp (2021-03-14 15:25:00 UTC) plus 5-minute steps for two items.
_BASE_TS = 1_615_735_500
_STEP = 300
_ITEM_A = "554"
_ITEM_B = "565"


def _seed_records() -> list[dict]:
    """Return a batch of records across two items and multiple timestamps."""
    records: list[dict] = []
    for offset in range(3):
        ts = _BASE_TS + offset * _STEP
        records.append(
            {
                "measurement": "itemPrice",
                "tags": {"itemID": _ITEM_A},
                "time": ts,
                "fields": {
                    "avgHighPrice": 5 + offset,
                    "avgLowPrice": 4 + offset,
                    "highPriceVolume": 100 + offset,
                    "lowPriceVolume": 90 + offset,
                },
            }
        )
    for offset in range(2):
        ts = _BASE_TS + offset * _STEP
        records.append(
            {
                "measurement": "itemPrice",
                "tags": {"itemID": _ITEM_B},
                "time": ts,
                "fields": {
                    "avgHighPrice": 1000 + offset,
                    "avgLowPrice": 950 + offset,
                    "highPriceVolume": 20 + offset,
                    "lowPriceVolume": 15 + offset,
                },
            }
        )
    return records


@pytest.fixture(scope="module")
def influx_settings() -> Iterator[Settings]:
    """Start an InfluxDB v2 container and yield Settings pointing at it.

    Skips the test (rather than failing) when Docker is unavailable or the
    image cannot be pulled, so the suite stays green in Docker-less
    environments.
    """
    influx._client_cache.clear()

    # Constructing DockerContainer eagerly connects to the Docker daemon, so it
    # (and start/wait) must live inside the try to skip cleanly when Docker is
    # unavailable.
    container = None
    try:
        container = (
            DockerContainer(_INFLUX_IMAGE)
            .with_exposed_ports(_INFLUX_PORT)
            .with_env("DOCKER_INFLUXDB_INIT_MODE", "setup")
            .with_env("DOCKER_INFLUXDB_INIT_USERNAME", _USERNAME)
            .with_env("DOCKER_INFLUXDB_INIT_PASSWORD", _PASSWORD)
            .with_env("DOCKER_INFLUXDB_INIT_ORG", _ORG)
            .with_env("DOCKER_INFLUXDB_INIT_BUCKET", _BUCKET)
            .with_env("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", _ADMIN_TOKEN)
        )
        container.start()
        # Wait until InfluxDB reports it is ready to serve requests.
        wait_for_logs(container, "Listening", timeout=60)
    except Exception as exc:  # noqa: BLE001 - any startup failure => skip
        if container is not None:
            try:
                container.stop()
            except Exception:  # noqa: BLE001
                pass
        pytest.skip(
            "InfluxDB test container unavailable (Docker not running or image "
            f"cannot be pulled): {exc}"
        )

    host = container.get_container_host_ip()
    port = container.get_exposed_port(_INFLUX_PORT)
    settings = Settings(
        influx_url=f"http://{host}:{port}",
        influx_token=_ADMIN_TOKEN,
        influx_org=_ORG,
        influx_bucket=_BUCKET,
    )

    # Give the HTTP API a brief moment past the log line to accept writes.
    time.sleep(1)

    try:
        yield settings
    finally:
        influx._client_cache.clear()
        try:
            container.stop()
        except Exception:  # noqa: BLE001
            pass


@pytest.fixture(scope="module")
def seeded_client(influx_settings: Settings):
    """Write the seed batch, then return the reusable client for reads."""
    client = influx.get_client(influx_settings)
    influx.write_batch(client, influx_settings.influx_bucket, _seed_records())
    # Writes are synchronous, but allow a moment for indexing/visibility.
    time.sleep(1)
    return client


def test_get_latest_timestamp_returns_max_for_item(seeded_client):
    """get_latest_timestamp returns the newest stored timestamp for an item."""
    latest_a = influx.get_latest_timestamp(seeded_client, _ITEM_A)
    latest_b = influx.get_latest_timestamp(seeded_client, _ITEM_B)

    assert latest_a == _BASE_TS + 2 * _STEP
    assert latest_b == _BASE_TS + 1 * _STEP
    assert influx.get_latest_timestamp(seeded_client, "does-not-exist") is None


def test_query_price_series_round_trips_written_points(seeded_client):
    """query_price_series returns the written points, ascending, fields present."""
    start = _BASE_TS - _STEP
    stop = _BASE_TS + 5 * _STEP

    series = influx.query_price_series(seeded_client, _ITEM_A, start, stop)

    assert len(series) == 3
    times = [point["time"] for point in series]
    assert times == sorted(times), "points must be time-ascending"
    assert times == [_BASE_TS, _BASE_TS + _STEP, _BASE_TS + 2 * _STEP]

    # Schema round-trip: every field is present on every point.
    for point in series:
        for field in influx.PRICE_FIELDS:
            assert field in point

    # Data round-trip: first point matches what we wrote.
    first = series[0]
    assert first["avgHighPrice"] == 5
    assert first["avgLowPrice"] == 4
    assert first["highPriceVolume"] == 100
    assert first["lowPriceVolume"] == 90


def test_query_chunk_returns_combined_multi_item_frame(seeded_client):
    """query_chunk returns a combined DataFrame spanning both items."""
    start = _BASE_TS - _STEP
    stop = _BASE_TS + 5 * _STEP

    frame = influx.query_chunk(seeded_client, [_ITEM_A, _ITEM_B], start, stop)

    assert not frame.empty
    # 3 rows for item A + 2 rows for item B = 5 combined rows.
    assert len(frame) == 5
    assert set(frame["itemID"].astype(str)) == {_ITEM_A, _ITEM_B}
    for field in influx.PRICE_FIELDS:
        assert field in frame.columns


def test_list_item_ids_returns_distinct_ids(seeded_client):
    """list_item_ids returns the distinct itemID tag values that were written."""
    ids = influx.list_item_ids(seeded_client)

    assert set(ids) >= {_ITEM_A, _ITEM_B}
    # No duplicates.
    assert len(ids) == len(set(ids))
