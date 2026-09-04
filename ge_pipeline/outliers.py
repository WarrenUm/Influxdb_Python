"""Pluggable outlier-detection registry.

Exposes the ``OutlierDetector`` protocol, a name-keyed registry with
``get_detector(method, **params)``, and built-in detectors (rolling z-score and
IQR). Every registered detector returns a ``list[bool]`` equal in length to its
input and maps ``None`` inputs to ``False``.
"""

from __future__ import annotations

import logging
import statistics
from typing import Callable, Protocol, runtime_checkable

__all__ = [
    "OutlierDetector",
    "ZScoreDetector",
    "IQRDetector",
    "register_detector",
    "get_detector",
    "available_detectors",
    "detect_outliers",
]

logger = logging.getLogger(__name__)


@runtime_checkable
class OutlierDetector(Protocol):
    """Protocol implemented by every registered outlier detector.

    A detector maps a sequence of optional float values to a list of booleans
    of the same length, where ``True`` marks the corresponding value as an
    outlier. ``None`` inputs must always map to ``False``.
    """

    def detect(self, values: list[float | None]) -> list[bool]:
        """Return a boolean outlier flag for each input value."""
        ...


class ZScoreDetector:
    """Trailing-window z-score outlier detector.

    For index ``i``, computes the mean (``μ``) and population standard
    deviation (``σ``) over the trailing window of non-``None`` values ending at
    ``i`` (inclusive) and flags the point when ``|values[i] - μ| / σ >
    threshold``. When ``σ == 0`` (a flat window) or when there is insufficient
    data, the point is not flagged.
    """

    def __init__(self, threshold: float = 3.0, window: int = 20) -> None:
        """Initialize the detector.

        Args:
            threshold: Positive z-score cut-off above which a point is flagged.
            window: Trailing window size (must be ``>= 2``).

        Raises:
            ValueError: If ``threshold <= 0`` or ``window < 2``.
        """
        if threshold <= 0:
            raise ValueError(f"threshold must be > 0, got {threshold!r}")
        if window < 2:
            raise ValueError(f"window must be >= 2, got {window!r}")
        self.threshold = float(threshold)
        self.window = int(window)

    def detect(self, values: list[float | None]) -> list[bool]:
        """Flag outliers using a trailing-window z-score.

        Args:
            values: Sequence of price values; ``None`` marks missing data.

        Returns:
            A ``list[bool]`` equal in length to ``values``. ``None`` inputs map
            to ``False``.
        """
        result: list[bool] = [False] * len(values)
        for i, value in enumerate(values):
            if value is None:
                continue
            # Trailing window of non-null values ending at i (inclusive).
            window_values = [
                v for v in values[max(0, i - self.window + 1) : i + 1] if v is not None
            ]
            if len(window_values) < 2:
                continue
            mean = statistics.fmean(window_values)
            sigma = statistics.pstdev(window_values)
            if sigma == 0:
                continue
            if abs(value - mean) / sigma > self.threshold:
                result[i] = True
        return result


class IQRDetector:
    """Trailing-window inter-quartile-range (IQR) outlier detector.

    For index ``i``, computes the first and third quartiles (``Q1``, ``Q3``)
    over the trailing window of non-``None`` values ending at ``i`` and flags
    the point when it falls outside ``[Q1 - k*IQR, Q3 + k*IQR]``. When the IQR
    is ``0`` (a flat window) or there is insufficient data, the point is not
    flagged.
    """

    def __init__(self, threshold: float = 1.5, window: int = 20) -> None:
        """Initialize the detector.

        Args:
            threshold: Positive IQR multiplier (``k``) defining the fence width.
            window: Trailing window size (must be ``>= 2``).

        Raises:
            ValueError: If ``threshold <= 0`` or ``window < 2``.
        """
        if threshold <= 0:
            raise ValueError(f"threshold must be > 0, got {threshold!r}")
        if window < 2:
            raise ValueError(f"window must be >= 2, got {window!r}")
        self.threshold = float(threshold)
        self.window = int(window)

    def detect(self, values: list[float | None]) -> list[bool]:
        """Flag outliers using a trailing-window IQR fence.

        Args:
            values: Sequence of price values; ``None`` marks missing data.

        Returns:
            A ``list[bool]`` equal in length to ``values``. ``None`` inputs map
            to ``False``.
        """
        result: list[bool] = [False] * len(values)
        for i, value in enumerate(values):
            if value is None:
                continue
            window_values = [
                v for v in values[max(0, i - self.window + 1) : i + 1] if v is not None
            ]
            # statistics.quantiles requires at least 2 data points.
            if len(window_values) < 2:
                continue
            q1, _, q3 = statistics.quantiles(window_values, n=4, method="inclusive")
            iqr = q3 - q1
            if iqr == 0:
                continue
            lower = q1 - self.threshold * iqr
            upper = q3 + self.threshold * iqr
            if value < lower or value > upper:
                result[i] = True
        return result


#: Name-keyed registry mapping detector method names to their factories. A
#: factory accepts keyword parameters and returns an ``OutlierDetector``.
_REGISTRY: dict[str, Callable[..., OutlierDetector]] = {}


def register_detector(
    method: str, factory: Callable[..., OutlierDetector]
) -> None:
    """Register a detector factory under ``method``.

    Args:
        method: Unique, non-empty name used to look the detector up.
        factory: Callable returning an ``OutlierDetector`` for given params.

    Raises:
        ValueError: If ``method`` is empty.
    """
    if not method:
        raise ValueError("detector method name must be a non-empty string")
    if method in _REGISTRY:
        logger.warning("Overriding already-registered detector %r", method)
    _REGISTRY[method] = factory
    logger.debug("Registered outlier detector %r", method)


def get_detector(method: str, **params) -> OutlierDetector:
    """Look up and instantiate a registered detector.

    Args:
        method: Registered detector name (e.g. ``"zscore"`` or ``"iqr"``).
        **params: Keyword parameters forwarded to the detector factory.

    Returns:
        An ``OutlierDetector`` instance.

    Raises:
        ValueError: If ``method`` is not registered.
    """
    try:
        factory = _REGISTRY[method]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise ValueError(
            f"Unknown outlier detection method {method!r}. "
            f"Registered methods: {known}."
        ) from None
    return factory(**params)


def available_detectors() -> list[str]:
    """Return the sorted list of registered detector names."""
    return sorted(_REGISTRY)


def detect_outliers(
    values: list[float | None],
    method: str = "zscore",
    threshold: float = 3.0,
    window: int = 20,
) -> list[bool]:
    """Detect outliers in ``values`` using the named detector.

    Convenience wrapper around :func:`get_detector` that builds a detector for
    ``method`` with the given ``threshold`` and ``window`` and applies it.

    Args:
        values: Sequence of price values; ``None`` marks missing data.
        method: Registered detector name (``"zscore"`` or ``"iqr"``).
        threshold: Positive detector-specific cut-off (must be ``> 0``).
        window: Trailing window size (must be ``>= 2``).

    Returns:
        A ``list[bool]`` equal in length to ``values``. ``None`` inputs map to
        ``False``.

    Raises:
        ValueError: If ``method`` is unregistered, ``threshold <= 0``, or
            ``window < 2``.
    """
    detector = get_detector(method, threshold=threshold, window=window)
    return detector.detect(values)


# Register built-in detectors.
register_detector("zscore", ZScoreDetector)
register_detector("iqr", IQRDetector)
