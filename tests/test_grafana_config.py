"""Config-assertion tests for the Grafana v3 datasource and dashboard files.

Feature: influxdb-v3-migration (task 9.3)

These tests read the provisioned Grafana datasource YAML and the dashboard JSON
and assert they describe an InfluxDB 3 (SQL/FlightSQL) setup rather than the old
v2 InfluxDB/Flux setup:

- Datasource (Requirements 9.1, 9.2): uses the v3 FlightSQL datasource type, the
  auth token is env-injected (never a hardcoded secret), and it references the
  v3 database and host via environment variables. The datasource ``uid`` is
  asserted so it can be matched against the dashboard's references.
- Dashboard (Requirement 9.3): every datasource reference matches the
  provisioned datasource ``uid``/``type``; the ``$itemID`` template variable
  query lists distinct item ids from ``"itemPrice"``; and the panel queries
  select the double-quoted camelCase price/volume fields from ``"itemPrice"``.
- Neither file retains any Flux/v2 remnants.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

# --- Paths ----------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DATASOURCE_YAML = (
    _REPO_ROOT / "grafana" / "provisioning" / "datasources" / "influxdb.yaml"
)
_DASHBOARD_JSON = _REPO_ROOT / "grafana" / "dashboards" / "ge-prices.json"

# The single UID both files must agree on (reconciled in task 9.3).
_EXPECTED_UID = "influxdb-v3-ge"
_V3_DATASOURCE_TYPE = "influxdata-flightsql-datasource"

# camelCase price/volume fields that must survive from the v2 schema.
_PRICE_FIELDS = (
    "avgHighPrice",
    "avgLowPrice",
    "highPriceVolume",
    "lowPriceVolume",
)

# Env-var placeholders the datasource must inject rather than hardcode.
_TOKEN_ENV = "${INFLUXDB3_AUTH_TOKEN}"
_DATABASE_ENV = "${INFLUXDB3_DATABASE_NAME}"
_HOST_ENV = "${INFLUXDB3_HOST_URL}"

# Flux / v2 tokens that must NOT appear anywhere in either file.
_FLUX_REMNANTS = (
    "from(bucket",
    "aggregateWindow",
    "_field",
    "version: Flux",
)


# --- Fixtures / loaders ---------------------------------------------------


def _load_datasource() -> dict:
    with _DATASOURCE_YAML.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _load_dashboard() -> dict:
    with _DASHBOARD_JSON.open(encoding="utf-8") as fh:
        return json.load(fh)


def _first_datasource() -> dict:
    doc = _load_datasource()
    datasources = doc["datasources"]
    assert datasources, "datasource provisioning file lists no datasources"
    return datasources[0]


def _iter_query_texts(dashboard: dict):
    """Yield every SQL query string referenced by the dashboard.

    Covers both the ``$itemID`` template variable query and each panel target's
    ``queryText`` / ``rawSql``.
    """
    for var in dashboard.get("templating", {}).get("list", []):
        if var.get("definition"):
            yield var["definition"]
        query = var.get("query")
        if isinstance(query, dict):
            for key in ("queryText", "rawSql"):
                if query.get(key):
                    yield query[key]
    for panel in dashboard.get("panels", []):
        for target in panel.get("targets", []):
            for key in ("queryText", "rawSql"):
                if target.get(key):
                    yield target[key]


def _iter_datasource_refs(dashboard: dict):
    """Yield every ``{"type","uid"}`` datasource reference for the InfluxDB
    v3 datasource (skipping built-in Grafana refs like ``-- Grafana --``)."""
    def _walk(node):
        if isinstance(node, dict):
            if "type" in node and "uid" in node and node.get("uid") != "-- Grafana --":
                yield node
            for value in node.values():
                yield from _walk(value)
        elif isinstance(node, list):
            for item in node:
                yield from _walk(item)

    yield from _walk(dashboard)


# --- Datasource assertions (Requirements 9.1, 9.2) ------------------------


def test_datasource_uses_v3_flightsql_type() -> None:
    ds = _first_datasource()
    assert ds["type"] == _V3_DATASOURCE_TYPE, (
        "datasource must use the InfluxDB v3 FlightSQL type, not the v2 "
        "influxdb/Flux datasource"
    )


def test_datasource_is_not_v2_influxdb() -> None:
    ds = _first_datasource()
    assert ds["type"] != "influxdb", "datasource must not be the v2 influxdb type"


def test_datasource_uid_is_standardized() -> None:
    ds = _first_datasource()
    assert ds["uid"] == _EXPECTED_UID


def test_datasource_token_is_env_injected_not_hardcoded() -> None:
    ds = _first_datasource()
    token = ds["secureJsonData"]["token"]
    assert token == _TOKEN_ENV, "auth token must be injected from the environment"


def test_datasource_has_no_hardcoded_secret() -> None:
    """The raw YAML text must never contain a literal token value."""
    raw = _DATASOURCE_YAML.read_text(encoding="utf-8")
    # The only place a token appears must be the env placeholder.
    assert raw.count("token:") >= 1
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("token:") and not stripped.startswith("#"):
            value = stripped.split(":", 1)[1].strip()
            assert value == _TOKEN_ENV, (
                f"token must be env-injected, found hardcoded value: {value!r}"
            )


def test_datasource_references_v3_database() -> None:
    ds = _first_datasource()
    json_data = ds["jsonData"]
    assert json_data["database"] == _DATABASE_ENV
    # The FlightSQL connection metadata must also carry the v3 database.
    metadata = {m["key"]: m["value"] for m in json_data.get("metadata", [])}
    assert metadata.get("database") == _DATABASE_ENV


def test_datasource_references_v3_host() -> None:
    ds = _first_datasource()
    assert ds["url"] == _HOST_ENV
    assert ds["jsonData"]["host"] == _HOST_ENV


def test_datasource_has_no_flux_v2_remnants() -> None:
    raw = _DATASOURCE_YAML.read_text(encoding="utf-8")
    for remnant in _FLUX_REMNANTS:
        assert remnant not in raw, f"datasource still references Flux/v2 remnant: {remnant!r}"


# --- Dashboard assertions (Requirement 9.3) -------------------------------


def test_dashboard_datasource_refs_match_provisioned_datasource() -> None:
    dashboard = _load_dashboard()
    refs = list(_iter_datasource_refs(dashboard))
    assert refs, "dashboard has no InfluxDB datasource references"
    for ref in refs:
        assert ref["type"] == _V3_DATASOURCE_TYPE, (
            f"dashboard datasource ref uses wrong type: {ref['type']!r}"
        )
        assert ref["uid"] == _EXPECTED_UID, (
            f"dashboard datasource uid {ref['uid']!r} does not match the "
            f"provisioned datasource uid {_EXPECTED_UID!r}"
        )


def test_dashboard_uid_matches_datasource_uid() -> None:
    """The dashboard's datasource references must resolve to the provisioned
    datasource, i.e. share the same uid."""
    ds_uid = _first_datasource()["uid"]
    dashboard = _load_dashboard()
    ref_uids = {ref["uid"] for ref in _iter_datasource_refs(dashboard)}
    assert ref_uids == {ds_uid}, (
        f"dashboard references {ref_uids} but provisioned datasource is {ds_uid!r}"
    )


def test_dashboard_itemid_variable_query() -> None:
    dashboard = _load_dashboard()
    variables = {v["name"]: v for v in dashboard["templating"]["list"]}
    assert "itemID" in variables, "dashboard is missing the $itemID template variable"
    item_var = variables["itemID"]
    expected = 'SELECT DISTINCT "itemID" FROM "itemPrice"'
    assert item_var["definition"] == expected
    assert item_var["query"]["rawSql"] == expected
    assert item_var["query"]["queryText"] == expected


def test_dashboard_queries_reference_double_quoted_camelcase_fields() -> None:
    dashboard = _load_dashboard()
    queries = list(_iter_query_texts(dashboard))
    assert queries, "dashboard has no queries"

    # Every panel/variable query must read from the double-quoted measurement.
    for query in queries:
        assert '"itemPrice"' in query, (
            f'query does not reference "itemPrice": {query!r}'
        )

    # Across the panel queries, all four camelCase fields must appear
    # double-quoted at least once.
    combined = "\n".join(queries)
    for field in _PRICE_FIELDS:
        assert f'"{field}"' in combined, (
            f'no query references the double-quoted field "{field}"'
        )


def test_dashboard_has_no_flux_v2_remnants() -> None:
    raw = _DASHBOARD_JSON.read_text(encoding="utf-8")
    for remnant in _FLUX_REMNANTS:
        assert remnant not in raw, f"dashboard still references Flux/v2 remnant: {remnant!r}"


def test_dashboard_candlestick_and_gauge_keep_camelcase_field_mappings() -> None:
    """Field mappings must keep the camelCase names (schema preserved)."""
    dashboard = _load_dashboard()
    panels = {p["type"]: p for p in dashboard["panels"]}
    candle = panels["candlestick"]["options"]["fields"]
    assert candle["high"] == "avgHighPrice"
    assert candle["low"] == "avgLowPrice"
