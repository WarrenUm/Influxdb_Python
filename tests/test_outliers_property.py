"""Property-based tests for the pluggable outlier-detection registry.

Implements the outlier-detection correctness properties from the design's
Correctness Properties section:

* **Property 6: detect_outliers length and None-safety** (Task 11.2)
* **Property 7: detect_outliers zero-variance stability** (Task 11.3)
* **Property 13: get_detector returns a length-preserving detector** across every
  registered detector (Task 11.4)

Each property runs under the ``ge`` Hypothesis profile (``max_examples>=100``)
registered in ``tests/conftest.py``.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from ge_pipeline.outliers import (
    available_detectors,
    detect_outliers,
    get_detector,
)

# The two built-in registered detector methods.
_BUILT_IN_METHODS = ("zscore", "iqr")

# A single value is either missing (None) or a finite numeric price. Floats are
# constrained to be finite (no NaN/inf) so the numeric contract is well-defined.
_value = st.one_of(
    st.none(),
    st.integers(min_value=-1_000_000, max_value=1_000_000),
    st.floats(
        min_value=-1_000_000.0,
        max_value=1_000_000.0,
        allow_nan=False,
        allow_infinity=False,
    ),
)

# A mixed sequence of Nones and finite numbers, including the empty list.
_value_lists = st.lists(_value, min_size=0, max_size=60)


@given(
    values=_value_lists,
    method=st.sampled_from(_BUILT_IN_METHODS),
    threshold=st.floats(
        min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False
    ),
    window=st.integers(min_value=2, max_value=40),
)
def test_detect_outliers_length_and_none_safety(
    values: list[float | None],
    method: str,
    threshold: float,
    window: int,
) -> None:
    """Flags match input length and every None index maps to False.

    **Property 6: detect_outliers length and None-safety**

    **Validates: Requirements 13.2, 13.3**
    """
    flags = detect_outliers(values, method=method, threshold=threshold, window=window)

    # Length is preserved and every flag is a genuine boolean.
    assert len(flags) == len(values)
    assert all(isinstance(flag, bool) for flag in flags)

    # None inputs never flag as outliers.
    for value, flag in zip(values, flags):
        if value is None:
            assert flag is False


@given(
    constant=st.one_of(
        st.integers(min_value=-1_000_000, max_value=1_000_000),
        st.floats(
            min_value=-1_000_000.0,
            max_value=1_000_000.0,
            allow_nan=False,
            allow_infinity=False,
        ),
    ),
    length=st.integers(min_value=1, max_value=60),
    threshold=st.floats(
        min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False
    ),
    window=st.integers(min_value=2, max_value=40),
)
def test_detect_outliers_zero_variance_stability(
    constant: float,
    length: int,
    threshold: float,
    window: int,
) -> None:
    """A flat (zero-variance) sequence flags nothing under zscore.

    **Property 7: detect_outliers zero-variance stability**

    **Validates: Requirements 13.4, 13.5**
    """
    values: list[float | None] = [constant] * length

    flags = detect_outliers(
        values, method="zscore", threshold=threshold, window=window
    )

    assert len(flags) == length
    # sigma == 0 across every trailing window, so no index is ever flagged.
    assert not any(flags)


@given(
    values=_value_lists,
    threshold=st.floats(
        min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False
    ),
    window=st.integers(min_value=2, max_value=40),
)
def test_registered_detectors_preserve_length_and_none_safety(
    values: list[float | None],
    threshold: float,
    window: int,
) -> None:
    """Every registered detector preserves length and is None-safe.

    Iterates over ``available_detectors()`` so the contract is enforced across
    all current and future registered detectors, not just the built-ins.

    **Property 13: get_detector returns a length-preserving detector**

    **Validates: Requirements 13.2, 13.3, 13.6**
    """
    methods = available_detectors()
    # The registry always exposes at least the two built-in detectors.
    assert set(_BUILT_IN_METHODS).issubset(set(methods))

    for method in methods:
        detector = get_detector(method, threshold=threshold, window=window)
        flags = detector.detect(values)

        assert len(flags) == len(values)
        assert all(isinstance(flag, bool) for flag in flags)
        for value, flag in zip(values, flags):
            if value is None:
                assert flag is False
