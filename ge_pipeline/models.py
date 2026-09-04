"""pydantic v2 validation models for the Wiki API responses.

Exposes ``ItemPricePoint`` and ``FiveMinuteSnapshot`` which validate and coerce
raw RuneScape Wiki ``/5m`` JSON, centralize null handling, and reject malformed
responses with descriptive validation errors.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

__all__ = ["ItemPricePoint", "FiveMinuteSnapshot"]


class ItemPricePoint(BaseModel):
    """A single item's price data within a 5-minute snapshot.

    Validates and coerces one entry of the RuneScape Wiki ``/5m`` response.
    The API supplies camelCase field aliases (for example ``avgHighPrice``)
    which are mapped to the snake_case domain fields below. Every field is
    optional and may be ``None`` for inactive items, so null handling is
    centralized here and downstream code can filter on ``None`` uniformly.

    Attributes:
        avg_high_price: Average high (insta-sell) price, or ``None``.
        avg_low_price: Average low (insta-buy) price, or ``None``.
        high_price_volume: Volume traded at the high price, or ``None``.
        low_price_volume: Volume traded at the low price, or ``None``.

    Validation rules:
        - Non-null numeric fields must be integers greater than or equal to 0.
        - Unknown keys are ignored, keeping the model forward-compatible with
          future API additions.
        - Fields may be populated by either their alias or their field name.
    """

    avg_high_price: int | None = Field(default=None, alias="avgHighPrice", ge=0)
    avg_low_price: int | None = Field(default=None, alias="avgLowPrice", ge=0)
    high_price_volume: int | None = Field(
        default=None, alias="highPriceVolume", ge=0
    )
    low_price_volume: int | None = Field(default=None, alias="lowPriceVolume", ge=0)

    model_config = {"populate_by_name": True, "extra": "ignore"}


class FiveMinuteSnapshot(BaseModel):
    """A validated full ``/5m`` Wiki API response for one time window.

    Attributes:
        timestamp: Unix timestamp (seconds) marking the 5-minute window.
        data: Mapping of item id (as a string) to its validated
            :class:`ItemPricePoint`.

    Malformed input (for example a missing ``timestamp`` or a non-mapping
    ``data`` value) raises a descriptive :class:`pydantic.ValidationError`.
    """

    timestamp: int
    data: dict[str, ItemPricePoint]
