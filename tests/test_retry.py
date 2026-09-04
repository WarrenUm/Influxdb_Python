"""Unit tests for :mod:`ge_pipeline.retry`.

Covers the retry-with-backoff behavior for both synchronous and asynchronous
callables: exponential delay growth (with clamping to ``max_delay``), honoring an
explicit ``Retry-After`` wait carried on a :class:`TransientError`, and the
final-failure logging plus propagation once retries are exhausted.

Requirements: 6.1 (retry with exponential backoff), 6.2 (honor ``Retry-After``
on HTTP 429), 21.1 (pytest unit tests).
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from ge_pipeline.errors import NonTransientError, TransientError
from ge_pipeline.retry import (
    RETRY_AFTER_ATTR,
    compute_backoff_delay,
    retry_with_backoff,
)


def _run(coro):
    """Run a coroutine to completion without requiring pytest-asyncio."""
    return asyncio.run(coro)


class _AlwaysFail:
    """Callable that always raises a fresh ``TransientError``.

    Records the number of times it was invoked so tests can assert the total
    number of attempts.
    """

    def __init__(self, retry_after=None):
        self.calls = 0
        self._retry_after = retry_after

    def _make_error(self) -> TransientError:
        exc = TransientError(f"boom #{self.calls}")
        if self._retry_after is not None:
            setattr(exc, RETRY_AFTER_ATTR, self._retry_after)
        return exc

    def __call__(self, *args, **kwargs):
        self.calls += 1
        raise self._make_error()


# ---------------------------------------------------------------------------
# compute_backoff_delay
# ---------------------------------------------------------------------------


def test_compute_backoff_delay_grows_geometrically():
    delays = [compute_backoff_delay(a, base_delay=1.0, backoff_factor=2.0) for a in range(1, 5)]
    assert delays == [1.0, 2.0, 4.0, 8.0]


def test_compute_backoff_delay_clamped_to_max_delay():
    assert compute_backoff_delay(10, base_delay=1.0, backoff_factor=2.0, max_delay=5.0) == 5.0


def test_compute_backoff_delay_floors_attempt_at_one():
    assert compute_backoff_delay(0, base_delay=1.0, backoff_factor=2.0) == 1.0


# ---------------------------------------------------------------------------
# Synchronous: exponential delay growth
# ---------------------------------------------------------------------------


def test_sync_exponential_delay_growth():
    captured: list[float] = []
    failer = _AlwaysFail()

    @retry_with_backoff(
        max_attempts=5,
        base_delay=1.0,
        backoff_factor=2.0,
        max_delay=60.0,
        sleep=captured.append,
    )
    def op():
        return failer()

    with pytest.raises(TransientError):
        op()

    # 5 attempts => 4 waits between them, growing geometrically.
    assert failer.calls == 5
    assert captured == [1.0, 2.0, 4.0, 8.0]


def test_sync_delay_clamped_to_max_delay():
    captured: list[float] = []
    failer = _AlwaysFail()

    @retry_with_backoff(
        max_attempts=5,
        base_delay=10.0,
        backoff_factor=10.0,
        max_delay=25.0,
        sleep=captured.append,
    )
    def op():
        return failer()

    with pytest.raises(TransientError):
        op()

    # base 10 -> 100 -> ... all clamped to 25 after the first.
    assert captured == [10.0, 25.0, 25.0, 25.0]


def test_sync_succeeds_after_transient_then_no_more_retries():
    captured: list[float] = []
    calls = {"n": 0}

    @retry_with_backoff(max_attempts=5, base_delay=1.0, sleep=captured.append)
    def op():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("temporary")
        return "ok"

    assert op() == "ok"
    assert calls["n"] == 3
    # Two failures before success => two waits.
    assert captured == [1.0, 2.0]


# ---------------------------------------------------------------------------
# Synchronous: Retry-After honoring
# ---------------------------------------------------------------------------


def test_sync_retry_after_overrides_computed_delay():
    captured: list[float] = []
    failer = _AlwaysFail(retry_after=7.5)

    @retry_with_backoff(
        max_attempts=3,
        base_delay=1.0,
        backoff_factor=2.0,
        sleep=captured.append,
    )
    def op():
        return failer()

    with pytest.raises(TransientError):
        op()

    # Every wait honors the explicit Retry-After value rather than backoff.
    assert captured == [7.5, 7.5]


def test_sync_negative_retry_after_falls_back_to_backoff():
    captured: list[float] = []
    failer = _AlwaysFail(retry_after=-1)

    @retry_with_backoff(
        max_attempts=3,
        base_delay=1.0,
        backoff_factor=2.0,
        sleep=captured.append,
    )
    def op():
        return failer()

    with pytest.raises(TransientError):
        op()

    # Negative Retry-After is ignored; exponential backoff is used instead.
    assert captured == [1.0, 2.0]


def test_sync_zero_retry_after_skips_sleep():
    captured: list[float] = []
    failer = _AlwaysFail(retry_after=0)

    @retry_with_backoff(max_attempts=3, base_delay=1.0, sleep=captured.append)
    def op():
        return failer()

    with pytest.raises(TransientError):
        op()

    # A zero wait must not call sleep (delay > 0 guard), but attempts still run.
    assert captured == []
    assert failer.calls == 3


# ---------------------------------------------------------------------------
# Synchronous: final-failure logging + propagation
# ---------------------------------------------------------------------------


def test_sync_final_failure_logs_and_propagates(caplog):
    failer = _AlwaysFail()

    @retry_with_backoff(max_attempts=3, base_delay=1.0, sleep=lambda _d: None)
    def op():
        return failer()

    with caplog.at_level(logging.WARNING, logger="ge_pipeline.retry"):
        with pytest.raises(TransientError):
            op()

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]

    # One warning per retry (attempts - 1) and exactly one final error.
    assert len(warnings) == 2
    assert len(errors) == 1
    assert "giving up" in errors[0].getMessage()


def test_non_transient_error_is_not_retried():
    captured: list[float] = []
    calls = {"n": 0}

    @retry_with_backoff(max_attempts=5, base_delay=1.0, sleep=captured.append)
    def op():
        calls["n"] += 1
        raise NonTransientError("permanent")

    with pytest.raises(NonTransientError):
        op()

    assert calls["n"] == 1
    assert captured == []


def test_max_attempts_must_be_positive():
    with pytest.raises(ValueError):
        retry_with_backoff(max_attempts=0)


# ---------------------------------------------------------------------------
# Asynchronous: exponential delay growth
# ---------------------------------------------------------------------------


def test_async_exponential_delay_growth():
    captured: list[float] = []
    calls = {"n": 0}

    async def _asleep(delay: float) -> None:
        captured.append(delay)

    @retry_with_backoff(
        max_attempts=5,
        base_delay=1.0,
        backoff_factor=2.0,
        async_sleep=_asleep,
    )
    async def op():
        calls["n"] += 1
        raise TransientError("boom")

    with pytest.raises(TransientError):
        _run(op())

    assert calls["n"] == 5
    assert captured == [1.0, 2.0, 4.0, 8.0]


def test_async_succeeds_after_transient():
    captured: list[float] = []
    calls = {"n": 0}

    async def _asleep(delay: float) -> None:
        captured.append(delay)

    @retry_with_backoff(max_attempts=5, base_delay=1.0, async_sleep=_asleep)
    async def op():
        calls["n"] += 1
        if calls["n"] < 2:
            raise TransientError("temporary")
        return "done"

    assert _run(op()) == "done"
    assert calls["n"] == 2
    assert captured == [1.0]


# ---------------------------------------------------------------------------
# Asynchronous: Retry-After honoring
# ---------------------------------------------------------------------------


def test_async_retry_after_overrides_computed_delay():
    captured: list[float] = []

    async def _asleep(delay: float) -> None:
        captured.append(delay)

    @retry_with_backoff(
        max_attempts=3,
        base_delay=1.0,
        backoff_factor=2.0,
        async_sleep=_asleep,
    )
    async def op():
        exc = TransientError("rate limited")
        setattr(exc, RETRY_AFTER_ATTR, 3.0)
        raise exc

    with pytest.raises(TransientError):
        _run(op())

    assert captured == [3.0, 3.0]


# ---------------------------------------------------------------------------
# Asynchronous: final-failure logging + propagation
# ---------------------------------------------------------------------------


def test_async_final_failure_logs_and_propagates(caplog):
    async def _asleep(_delay: float) -> None:
        return None

    @retry_with_backoff(max_attempts=3, base_delay=1.0, async_sleep=_asleep)
    async def op():
        raise TransientError("boom")

    with caplog.at_level(logging.WARNING, logger="ge_pipeline.retry"):
        with pytest.raises(TransientError):
            _run(op())

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]

    assert len(warnings) == 2
    assert len(errors) == 1
    assert "giving up" in errors[0].getMessage()
