"""Tests for :func:`ge_pipeline.ingestion.iter_missing_timestamps`.

Covers Requirements 4.2-4.4 and the associated correctness properties:
    - Property 4: yielded timestamps are strictly increasing, aligned to the
      start, and consecutive values differ by exactly ``interval``; every
      yielded value ``t`` satisfies ``start <= t < now``.
    - Property 5: the yielded set equals exactly
      ``{start + k*interval : k >= 0 and start + k*interval < now}`` with no
      skipped and no extra timestamps.

The ``start`` boundary is ``earliest`` when ``latest is None`` and
``latest + interval`` otherwise. Both cases are exercised below.
"""

from __future__ import annotations

from hypothesis import assume, given
from hypothesis import strategies as st

from ge_pipeline.ingestion import iter_missing_timestamps

_EARLIEST = 1615733100


def _start_for(latest: int | None, interval: int, earliest: int) -> int:
    """Compute the inclusive lower bound the same way the function specifies."""
    return earliest if latest is None else latest + interval


def _expected_set(start: int, now: int, interval: int) -> set[int]:
    """The exact set of timestamps the generator is required to produce."""
    result: set[int] = set()
    t = start
    while t < now:
        result.add(t)
        t += interval
    return result


# --- Unit tests -----------------------------------------------------------


def test_latest_none_starts_at_earliest():
    """When latest is None the first yielded value is ``earliest`` (Req 4.3)."""
    values = list(iter_missing_timestamps(None, _EARLIEST + 3 * 300))
    assert values == [_EARLIEST, _EARLIEST + 300, _EARLIEST + 600]


def test_latest_provided_starts_after_latest():
    """When latest is provided the first value is ``latest + interval`` (Req 4.3)."""
    latest = _EARLIEST + 1000 * 300
    values = list(iter_missing_timestamps(latest, latest + 3 * 300))
    assert values == [latest + 300, latest + 600]


def test_empty_when_now_at_start():
    """No timestamps are produced when ``now == start`` (half-open upper bound)."""
    assert list(iter_missing_timestamps(None, _EARLIEST)) == []


def test_upper_bound_is_exclusive():
    """A value exactly equal to ``now`` is never yielded (Req 4.3)."""
    now = _EARLIEST + 2 * 300
    values = list(iter_missing_timestamps(None, now))
    assert now not in values
    assert values == [_EARLIEST, _EARLIEST + 300]


# --- Property-based tests -------------------------------------------------

# latest is either None or an aligned-or-arbitrary value >= earliest.
_latest_strategy = st.one_of(
    st.none(),
    st.integers(min_value=_EARLIEST, max_value=_EARLIEST + 5_000_000),
)
_interval_strategy = st.integers(min_value=1, max_value=100_000)


@given(
    latest=_latest_strategy,
    span=st.integers(min_value=0, max_value=5_000_000),
    interval=_interval_strategy,
)
def test_property_strictly_increasing_and_aligned(latest, span, interval):
    """Yielded values are strictly increasing, aligned, and within [start, now).

    Consecutive values differ by exactly ``interval``; every value ``t``
    satisfies ``start <= t < now`` and ``(t - start) % interval == 0``.

    **Validates: Requirements 4.2, 4.3** (Property 4)
    """
    start = _start_for(latest, interval, _EARLIEST)
    now = max(start, _EARLIEST) + span
    assume(now >= _EARLIEST)

    values = list(iter_missing_timestamps(latest, now, interval=interval))

    # Strictly increasing with an exact interval step.
    for prev, curr in zip(values, values[1:]):
        assert curr > prev
        assert curr - prev == interval

    # Alignment to start and half-open bounds.
    for t in values:
        assert start <= t < now
        assert (t - start) % interval == 0


@given(
    latest=_latest_strategy,
    span=st.integers(min_value=0, max_value=5_000_000),
    interval=_interval_strategy,
)
def test_property_completeness_exact_set(latest, span, interval):
    """The yielded set equals exactly the expected set, no skips or extras.

    Produces exactly ``{start + k*interval : k >= 0 and start + k*interval < now}``.

    **Validates: Requirements 4.3, 4.4** (Property 5)
    """
    start = _start_for(latest, interval, _EARLIEST)
    now = max(start, _EARLIEST) + span
    assume(now >= _EARLIEST)

    values = list(iter_missing_timestamps(latest, now, interval=interval))
    expected = _expected_set(start, now, interval)

    # Exact set equality proves no skips and no extras.
    assert set(values) == expected
    # No duplicates were produced.
    assert len(values) == len(expected)
