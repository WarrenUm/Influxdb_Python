"""Tests for :func:`ge_pipeline.ingestion.build_price_records`.

Covers Requirements 3.1-3.4 and the associated correctness properties:
    - Property 1: every produced record has a non-empty ``fields`` dict with
      only non-null values.
    - Property 2: exactly the items with >=1 non-null price field are emitted;
      all-null items are excluded.
    - Property 3: each record is stamped with the fixed ``measurement``, the
      ``itemID`` tag, and the snapshot timestamp.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from ge_pipeline.ingestion import build_price_records
from ge_pipeline.models import FiveMinuteSnapshot

_STORAGE_FIELDS = ("avgHighPrice", "avgLowPrice", "highPriceVolume", "lowPriceVolume")


# --- Unit tests -----------------------------------------------------------


def test_item_with_all_fields_maps_to_camelcase():
    """An item with all fields set produces one full camelCase record (Req 3.2, 3.4)."""
    snapshot = FiveMinuteSnapshot.model_validate(
        {
            "timestamp": 1615733100,
            "data": {
                "554": {
                    "avgHighPrice": 5,
                    "avgLowPrice": 4,
                    "highPriceVolume": 100,
                    "lowPriceVolume": 200,
                }
            },
        }
    )

    records = build_price_records(snapshot)

    assert records == [
        {
            "measurement": "itemPrice",
            "tags": {"itemID": "554"},
            "time": 1615733100,
            "fields": {
                "avgHighPrice": 5,
                "avgLowPrice": 4,
                "highPriceVolume": 100,
                "lowPriceVolume": 200,
            },
        }
    ]


def test_partial_nulls_are_filtered_out():
    """Only non-null fields appear in the record (Req 3.3)."""
    snapshot = FiveMinuteSnapshot.model_validate(
        {
            "timestamp": 1700000000,
            "data": {"2": {"avgHighPrice": 10, "avgLowPrice": None}},
        }
    )

    records = build_price_records(snapshot)

    assert len(records) == 1
    assert records[0]["fields"] == {"avgHighPrice": 10}


def test_all_null_item_is_excluded():
    """An item whose every field is None is excluded (Req 3.2)."""
    snapshot = FiveMinuteSnapshot.model_validate(
        {
            "timestamp": 1700000000,
            "data": {
                "1": {
                    "avgHighPrice": None,
                    "avgLowPrice": None,
                    "highPriceVolume": None,
                    "lowPriceVolume": None,
                },
                "2": {"avgHighPrice": 7},
            },
        }
    )

    records = build_price_records(snapshot)

    assert len(records) == 1
    assert records[0]["tags"]["itemID"] == "2"


def test_empty_snapshot_produces_no_records():
    """An empty data mapping yields an empty list."""
    snapshot = FiveMinuteSnapshot.model_validate(
        {"timestamp": 1700000000, "data": {}}
    )
    assert build_price_records(snapshot) == []


def test_zero_valued_field_is_kept():
    """A field equal to 0 is non-null and must be retained (Req 3.3)."""
    snapshot = FiveMinuteSnapshot.model_validate(
        {"timestamp": 1700000000, "data": {"3": {"highPriceVolume": 0}}}
    )

    records = build_price_records(snapshot)

    assert records[0]["fields"] == {"highPriceVolume": 0}


# --- Property-based tests -------------------------------------------------

_optional_price = st.one_of(st.none(), st.integers(min_value=0, max_value=2**31))

_item_strategy = st.fixed_dictionaries(
    {
        "avgHighPrice": _optional_price,
        "avgLowPrice": _optional_price,
        "highPriceVolume": _optional_price,
        "lowPriceVolume": _optional_price,
    }
)

_snapshot_strategy = st.builds(
    lambda ts, data: FiveMinuteSnapshot.model_validate({"timestamp": ts, "data": data}),
    ts=st.integers(min_value=0, max_value=2**32),
    data=st.dictionaries(
        keys=st.integers(min_value=1, max_value=100000).map(str),
        values=_item_strategy,
        max_size=15,
    ),
)


@given(_snapshot_strategy)
def test_property_records_are_null_free_and_nonempty(snapshot):
    """Every record has a non-empty fields dict with no None values.

    **Validates: Requirements 3.1, 3.3** (Property 1)
    """
    for record in build_price_records(snapshot):
        assert record["fields"], "fields dict must be non-empty"
        assert all(value is not None for value in record["fields"].values())


@given(_snapshot_strategy)
def test_property_emits_exactly_items_with_non_null_field(snapshot):
    """Exactly the items with >=1 non-null field are emitted; all-null excluded.

    **Validates: Requirements 3.1, 3.2** (Property 2)
    """
    expected_ids = {
        item_id
        for item_id, point in snapshot.data.items()
        if any(
            getattr(point, name) is not None
            for name in (
                "avg_high_price",
                "avg_low_price",
                "high_price_volume",
                "low_price_volume",
            )
        )
    }

    records = build_price_records(snapshot)
    produced_ids = {record["tags"]["itemID"] for record in records}

    assert produced_ids == expected_ids
    assert len(records) == len(expected_ids)


@given(_snapshot_strategy)
def test_property_record_stamping_and_field_names(snapshot):
    """Each record is stamped with measurement, itemID tag, and timestamp.

    **Validates: Requirements 3.4** (Property 3)
    """
    for record in build_price_records(snapshot):
        assert record["measurement"] == "itemPrice"
        assert record["time"] == snapshot.timestamp
        assert set(record["tags"].keys()) == {"itemID"}
        assert isinstance(record["tags"]["itemID"], str)
        assert set(record["fields"].keys()).issubset(_STORAGE_FIELDS)
