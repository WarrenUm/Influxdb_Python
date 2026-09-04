"""Unit tests for the pluggable outlier-detection registry.

Covers example-based and edge-case behavior of the registry lookup, in
particular the descriptive error raised for an unregistered method name
(Task 11.5). Property-based coverage of the detector contracts lives in
``tests/test_outliers_property.py``.
"""

from __future__ import annotations

import pytest

from ge_pipeline.outliers import available_detectors, get_detector


def test_get_detector_unknown_method_raises_descriptive_error() -> None:
    """An unregistered method name raises a descriptive ValueError.

    The message must reference the offending method name and list the known
    registered methods so callers can self-correct.

    **Validates: Requirements 13.7, 21.1**
    """
    unknown = "unknown_method_xyz"

    with pytest.raises(ValueError) as exc_info:
        get_detector(unknown)

    message = str(exc_info.value)
    # References the offending method name.
    assert unknown in message
    # Lists the known/registered methods to guide the caller.
    for known in available_detectors():
        assert known in message


def test_get_detector_returns_registered_built_ins() -> None:
    """The built-in ``zscore`` and ``iqr`` methods resolve to detectors."""
    for method in ("zscore", "iqr"):
        detector = get_detector(method)
        # Every detector satisfies the OutlierDetector protocol (has detect()).
        assert hasattr(detector, "detect")
        assert detector.detect([]) == []
