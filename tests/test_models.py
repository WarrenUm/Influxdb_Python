"""Unit tests for the pydantic validation models in ``ge_pipeline.models``.

Covers alias coercion (camelCase -> snake_case), ``None`` preservation for
absent/null fields, unknown-key ignoring, and negative-value rejection.

Validates: Requirements 2.2, 2.4, 2.5, 2.6, 21.1
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ge_pipeline.models import FiveMinuteSnapshot, ItemPricePoint


class TestAliasCoercion:
    """Requirement 2.2 - coerce camelCase API aliases to snake_case fields."""

    def test_all_aliases_mapped_to_snake_case(self) -> None:
        point = ItemPricePoint.model_validate(
            {
                "avgHighPrice": 100,
                "avgLowPrice": 90,
                "highPriceVolume": 5,
                "lowPriceVolume": 7,
            }
        )

        assert point.avg_high_price == 100
        assert point.avg_low_price == 90
        assert point.high_price_volume == 5
        assert point.low_price_volume == 7

    def test_populate_by_field_name_also_supported(self) -> None:
        point = ItemPricePoint.model_validate(
            {
                "avg_high_price": 100,
                "avg_low_price": 90,
                "high_price_volume": 5,
                "low_price_volume": 7,
            }
        )

        assert point.avg_high_price == 100
        assert point.avg_low_price == 90
        assert point.high_price_volume == 5
        assert point.low_price_volume == 7

    def test_snapshot_coerces_nested_item_aliases(self) -> None:
        snapshot = FiveMinuteSnapshot.model_validate(
            {
                "timestamp": 1700000000,
                "data": {"554": {"avgHighPrice": 3, "lowPriceVolume": 42}},
            }
        )

        assert snapshot.timestamp == 1700000000
        item = snapshot.data["554"]
        assert item.avg_high_price == 3
        assert item.low_price_volume == 42


class TestNonePreservation:
    """Requirement 2.4 - preserve absent/null fields as ``None``."""

    def test_absent_fields_default_to_none(self) -> None:
        point = ItemPricePoint.model_validate({})

        assert point.avg_high_price is None
        assert point.avg_low_price is None
        assert point.high_price_volume is None
        assert point.low_price_volume is None

    def test_explicit_null_fields_preserved_as_none(self) -> None:
        point = ItemPricePoint.model_validate(
            {
                "avgHighPrice": None,
                "avgLowPrice": None,
                "highPriceVolume": None,
                "lowPriceVolume": None,
            }
        )

        assert point.avg_high_price is None
        assert point.avg_low_price is None
        assert point.high_price_volume is None
        assert point.low_price_volume is None

    def test_partial_nulls_preserved_alongside_values(self) -> None:
        point = ItemPricePoint.model_validate(
            {"avgHighPrice": 100, "avgLowPrice": None}
        )

        assert point.avg_high_price == 100
        assert point.avg_low_price is None
        assert point.high_price_volume is None
        assert point.low_price_volume is None

    def test_zero_is_preserved_and_not_treated_as_none(self) -> None:
        point = ItemPricePoint.model_validate({"avgHighPrice": 0})

        assert point.avg_high_price == 0
        assert point.avg_high_price is not None


class TestUnknownKeysIgnored:
    """Requirement 2.5 - ignore unknown keys, validate recognized fields."""

    def test_unknown_keys_ignored_on_item(self) -> None:
        point = ItemPricePoint.model_validate(
            {
                "avgHighPrice": 100,
                "someFutureField": "surprise",
                "extra": 999,
            }
        )

        assert point.avg_high_price == 100
        assert not hasattr(point, "someFutureField")
        assert not hasattr(point, "extra")

    def test_unknown_keys_do_not_appear_in_dump(self) -> None:
        point = ItemPricePoint.model_validate(
            {"avgLowPrice": 5, "unexpected": True}
        )

        dumped = point.model_dump()
        assert "unexpected" not in dumped
        assert dumped["avg_low_price"] == 5


class TestNegativeValueRejection:
    """Requirement 2.3/2.6 - non-null numeric fields must be >= 0."""

    @pytest.mark.parametrize(
        "alias",
        ["avgHighPrice", "avgLowPrice", "highPriceVolume", "lowPriceVolume"],
    )
    def test_negative_value_rejected(self, alias: str) -> None:
        with pytest.raises(ValidationError):
            ItemPricePoint.model_validate({alias: -1})

    def test_negative_value_inside_snapshot_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FiveMinuteSnapshot.model_validate(
                {
                    "timestamp": 1700000000,
                    "data": {"554": {"avgHighPrice": -5}},
                }
            )
