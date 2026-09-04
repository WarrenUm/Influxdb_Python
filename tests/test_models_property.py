"""Property-based tests for the pydantic validation models.

Implements **Property 8: pydantic validation round-trip** from the design's
Correctness Properties. This is the MANDATORY schema validation round-trip test
required by Requirement 21.5: well-formed ``/5m`` API JSON must validate into a
:class:`FiveMinuteSnapshot`, and every non-null numeric field on every
:class:`ItemPricePoint` must be an ``int`` greater than or equal to 0.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from ge_pipeline.models import FiveMinuteSnapshot, ItemPricePoint

# The API-supplied alias keys for the four numeric price fields.
_ALIAS_KEYS = ("avgHighPrice", "avgLowPrice", "highPriceVolume", "lowPriceVolume")

# The corresponding snake_case domain attribute names on ItemPricePoint.
_FIELD_NAMES = (
    "avg_high_price",
    "avg_low_price",
    "high_price_volume",
    "low_price_volume",
)

# A non-null numeric value is a non-negative integer; None models an inactive item.
_numeric_value = st.one_of(st.none(), st.integers(min_value=0))


def _item_entries() -> st.SearchStrategy[dict[str, object]]:
    """Generate one well-formed item entry keyed by API aliases.

    Each alias key is optionally present (absent keys model missing fields) and
    maps to either ``None`` or a non-negative integer.
    """
    return st.fixed_dictionaries(
        {}, optional={alias: _numeric_value for alias in _ALIAS_KEYS}
    )


def _well_formed_api_json() -> st.SearchStrategy[dict[str, object]]:
    """Generate a well-formed ``/5m`` API response using alias keys."""
    return st.fixed_dictionaries(
        {
            "timestamp": st.integers(min_value=0),
            "data": st.dictionaries(
                keys=st.integers(min_value=1).map(str),
                values=_item_entries(),
                max_size=8,
            ),
        }
    )


@given(api_json=_well_formed_api_json())
def test_schema_validation_round_trip(api_json: dict[str, object]) -> None:
    """Well-formed API JSON validates and yields non-negative int|None fields.

    **Property 8: pydantic validation round-trip**

    **Validates: Requirements 2.1, 2.3, 21.5**
    """
    snapshot = FiveMinuteSnapshot.model_validate(api_json)

    assert isinstance(snapshot.timestamp, int)

    for item_id, point in snapshot.data.items():
        assert isinstance(item_id, str)
        assert isinstance(point, ItemPricePoint)
        for field_name in _FIELD_NAMES:
            value = getattr(point, field_name)
            assert value is None or (isinstance(value, int) and value >= 0)
