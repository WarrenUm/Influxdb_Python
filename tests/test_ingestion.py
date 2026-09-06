"""Unit tests for :mod:`ge_pipeline.ingestion` record construction.

Task 4.2 (InfluxDB v3 migration): assert that ``build_price_records`` still
emits the *unchanged* v2 storage record shape after the v2 -> v3 cutover.
Because the V3_Schema is preserved 1:1 from v2, ingestion needs no field-name
changes and must keep producing:

    - measurement ``itemPrice`` (:data:`MEASUREMENT_NAME`),
    - a single ``itemID`` tag whose value is the item id as a string,
    - the four camelCase field keys ``avgHighPrice``, ``avgLowPrice``,
      ``highPriceVolume``, ``lowPriceVolume``,
    - null-field filtering (partial-null items drop only the null fields;
      all-null items are skipped entirely), and
    - a unix-second integer timestamp copied from the snapshot.

_Requirements: 3.1, 3.2, 7.1, 7.2_
"""

from __future__ import annotations

from ge_pipeline.ingestion import (
    _FIELD_NAME_MAP,
    MEASUREMENT_NAME,
    build_price_records,
)
from ge_pipeline.models import FiveMinuteSnapshot

# The four camelCase storage field names, unchanged from the v2 schema.
_CAMEL_FIELDS = ("avgHighPrice", "avgLowPrice", "highPriceVolume", "lowPriceVolume")


def _snapshot(timestamp: int, data: dict) -> FiveMinuteSnapshot:
    """Build a validated snapshot from raw camelCase API-shaped input."""
    return FiveMinuteSnapshot.model_validate({"timestamp": timestamp, "data": data})


# --- measurement / tag shape (Req 3.1) -----------------------------------


def test_measurement_is_itemprice():
    """Every emitted record uses the unchanged ``itemPrice`` measurement (Req 3.1)."""
    snapshot = _snapshot(
        1615733100, {"554": {"avgHighPrice": 180, "avgLowPrice": 176}}
    )

    records = build_price_records(snapshot)

    assert MEASUREMENT_NAME == "itemPrice"
    assert records
    assert all(record["measurement"] == "itemPrice" for record in records)


def test_tag_is_item_id_as_string():
    """The sole tag is ``itemID`` carrying the item id as a string (Req 3.1)."""
    snapshot = _snapshot(1615733100, {"554": {"avgHighPrice": 180}})

    records = build_price_records(snapshot)

    assert len(records) == 1
    assert records[0]["tags"] == {"itemID": "554"}
    assert isinstance(records[0]["tags"]["itemID"], str)


def test_field_keys_are_the_four_camelcase_names():
    """A full item emits exactly the four camelCase field keys (Req 3.1)."""
    snapshot = _snapshot(
        1615733100,
        {
            "2": {
                "avgHighPrice": 5,
                "avgLowPrice": 4,
                "highPriceVolume": 100,
                "lowPriceVolume": 200,
            }
        },
    )

    records = build_price_records(snapshot)

    assert set(records[0]["fields"].keys()) == set(_CAMEL_FIELDS)
    # The identity mapping's storage names are exactly the camelCase keys.
    assert set(_FIELD_NAME_MAP.values()) == set(_CAMEL_FIELDS)


def test_field_values_are_preserved_by_camelcase_name():
    """Field values land under their camelCase storage names unchanged (Req 3.1)."""
    snapshot = _snapshot(
        1615733100,
        {
            "2": {
                "avgHighPrice": 5,
                "avgLowPrice": 4,
                "highPriceVolume": 100,
                "lowPriceVolume": 200,
            }
        },
    )

    records = build_price_records(snapshot)

    assert records[0]["fields"] == {
        "avgHighPrice": 5,
        "avgLowPrice": 4,
        "highPriceVolume": 100,
        "lowPriceVolume": 200,
    }


# --- null filtering (Req 3.2, 7.2) ---------------------------------------


def test_partial_null_fields_are_dropped_but_record_kept():
    """A point with some None fields emits only the non-null ones (Req 3.2)."""
    snapshot = _snapshot(
        1700000000,
        {
            "10": {
                "avgHighPrice": 42,
                "avgLowPrice": None,
                "highPriceVolume": 7,
                "lowPriceVolume": None,
            }
        },
    )

    records = build_price_records(snapshot)

    assert len(records) == 1
    assert records[0]["fields"] == {"avgHighPrice": 42, "highPriceVolume": 7}
    assert all(value is not None for value in records[0]["fields"].values())


def test_all_null_item_is_skipped_entirely():
    """A point whose every field is None produces no record (Req 3.2)."""
    snapshot = _snapshot(
        1700000000,
        {
            "1": {
                "avgHighPrice": None,
                "avgLowPrice": None,
                "highPriceVolume": None,
                "lowPriceVolume": None,
            },
            "2": {"avgLowPrice": 9},
        },
    )

    records = build_price_records(snapshot)

    produced_ids = {record["tags"]["itemID"] for record in records}
    assert produced_ids == {"2"}
    assert records[0]["fields"] == {"avgLowPrice": 9}


def test_zero_is_not_treated_as_null():
    """A field equal to 0 is a real value and must be retained (Req 3.2)."""
    snapshot = _snapshot(1700000000, {"3": {"highPriceVolume": 0}})

    records = build_price_records(snapshot)

    assert records[0]["fields"] == {"highPriceVolume": 0}


def test_empty_snapshot_yields_no_records():
    """An empty data mapping produces an empty batch (Req 7.2 upstream)."""
    assert build_price_records(_snapshot(1700000000, {})) == []


# --- timestamp semantics (Req 7.1) ---------------------------------------


def test_time_is_unix_seconds_int_preserved_from_snapshot():
    """Each record's time is the snapshot's unix-second timestamp, as an int (Req 7.1)."""
    snapshot = _snapshot(
        1615733100,
        {
            "554": {"avgHighPrice": 180},
            "555": {"avgLowPrice": 12},
        },
    )

    records = build_price_records(snapshot)

    assert len(records) == 2
    for record in records:
        assert record["time"] == 1615733100
        assert isinstance(record["time"], int)


def test_full_record_shape_matches_v2_layout():
    """End-to-end: a single full item matches the exact v2 record dict (Req 3.1, 7.1)."""
    snapshot = _snapshot(
        1615733100,
        {
            "554": {
                "avgHighPrice": 180,
                "avgLowPrice": 176,
                "highPriceVolume": 1200,
                "lowPriceVolume": 980,
            }
        },
    )

    assert build_price_records(snapshot) == [
        {
            "measurement": "itemPrice",
            "tags": {"itemID": "554"},
            "time": 1615733100,
            "fields": {
                "avgHighPrice": 180,
                "avgLowPrice": 176,
                "highPriceVolume": 1200,
                "lowPriceVolume": 980,
            },
        }
    ]
