"""Property-based tests for the InfluxDB 3 client caching seam.

Implements the client-lifecycle correctness property from the design's
Correctness Properties section:

* **Property 1: Client caching identity** (Task 3.2)

:func:`ge_pipeline.influx.get_client` caches an :class:`InfluxDBClient3` keyed
by the v3 connection identity ``(host, database, token)``. Two calls must
return the identical client instance if and only if their connection tuples are
equal (Requirement 1.3).

Each property runs under the ``ge`` Hypothesis profile (``max_examples>=100``)
registered in ``tests/conftest.py``. A lightweight stand-in replaces
``InfluxDBClient3`` so no real connection is ever opened.
"""

from __future__ import annotations

from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

from ge_pipeline import influx
from ge_pipeline.config import Settings


class _FakeClient3:
    """Stand-in for ``InfluxDBClient3`` that records its connection kwargs.

    Constructing this never touches the network, so the property can exercise
    :func:`ge_pipeline.influx.get_client` purely as a caching function while
    still giving each created client a distinct object identity.
    """

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


def _settings(host: str, database: str, token: str) -> Settings:
    """Build Settings carrying the given v3 connection identity."""
    return Settings(
        influx3_host=host,
        influx3_token=token,
        influx3_database=database,
    )


# Small, overlapping value pools so generated pairs frequently both match and
# differ on each component, exercising the "iff" in both directions.
_HOSTS = st.sampled_from(
    ["http://localhost:8181", "http://influx:8181", "https://remote:443"]
)
_DATABASES = st.sampled_from(["ge", "prices", "other"])
_TOKENS = st.sampled_from(["token-a", "token-b", "token-c"])


@given(
    host1=_HOSTS,
    database1=_DATABASES,
    token1=_TOKENS,
    host2=_HOSTS,
    database2=_DATABASES,
    token2=_TOKENS,
)
def test_get_client_caching_identity(
    host1: str,
    database1: str,
    token1: str,
    host2: str,
    database2: str,
    token2: str,
) -> None:
    """Two ``get_client`` calls share an instance iff their identities match.

    **Feature: influxdb-v3-migration, Property 1: Client caching identity**

    **Validates: Requirements 1.3**
    """
    settings1 = _settings(host1, database1, token1)
    settings2 = _settings(host2, database2, token2)

    # ``unittest.mock.patch`` (not the monkeypatch fixture) because Hypothesis
    # re-runs this body many times and function-scoped fixtures are not reset
    # between generated inputs. The stand-in guarantees no real client is ever
    # created, and the cache is cleared so identity comparisons reflect only
    # this example's calls.
    with patch.object(influx, "InfluxDBClient3", _FakeClient3):
        influx._client_cache.clear()

        client1 = influx.get_client(settings1)
        client2 = influx.get_client(settings2)

        same_identity = (host1, database1, token1) == (host2, database2, token2)

        if same_identity:
            assert client1 is client2
        else:
            assert client1 is not client2

        # Repeating a call with an already-seen identity must return the cached
        # instance, never a freshly constructed one.
        assert influx.get_client(settings1) is client1
        assert influx.get_client(settings2) is client2
