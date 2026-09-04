"""Retry helper with exponential backoff for transient failures.

Provides a decorator/utility that retries operations raising
:class:`~ge_pipeline.errors.TransientError` using exponential backoff, honors an
explicit wait for HTTP 429 ``Retry-After`` (carried on the exception as a
``retry_after`` attribute), logs each retry attempt, and logs a final error once
retries are exhausted before re-raising.

The decorator supports both synchronous and asynchronous callables so it can wrap
the synchronous ``write_batch`` and the asynchronous ``fetch_snapshot`` used by
the ingestion layer.

Example
-------
Synchronous use::

    @retry_with_backoff(max_attempts=5)
    def write(records):
        ...

Asynchronous use::

    @retry_with_backoff(max_attempts=5)
    async def fetch(client, ts):
        ...
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from .errors import TransientError

__all__ = ["retry_with_backoff", "compute_backoff_delay", "RETRY_AFTER_ATTR"]

logger = logging.getLogger(__name__)

#: Name of the attribute a :class:`TransientError` may carry to request an
#: explicit wait (in seconds) before the next attempt, e.g. HTTP 429
#: ``Retry-After``. When present and non-negative it overrides the computed
#: exponential-backoff delay.
RETRY_AFTER_ATTR = "retry_after"

# Default backoff configuration.
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BASE_DELAY = 1.0
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_MAX_DELAY = 60.0

F = TypeVar("F", bound=Callable[..., Any])


def compute_backoff_delay(
    attempt: int,
    *,
    base_delay: float = DEFAULT_BASE_DELAY,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    max_delay: float = DEFAULT_MAX_DELAY,
) -> float:
    """Compute the exponential-backoff delay for a given retry attempt.

    The delay grows geometrically as ``base_delay * backoff_factor ** (attempt - 1)``
    and is clamped to ``max_delay``.

    Args:
        attempt: The 1-based attempt number that just failed (``1`` for the first
            failure, ``2`` for the second, and so on).
        base_delay: The delay in seconds after the first failed attempt.
        backoff_factor: The multiplier applied for each subsequent attempt.
        max_delay: The upper bound on the returned delay, in seconds.

    Returns:
        The number of seconds to wait before the next attempt.
    """
    if attempt < 1:
        attempt = 1
    delay = base_delay * (backoff_factor ** (attempt - 1))
    return min(delay, max_delay)


def _resolve_delay(
    exc: TransientError,
    attempt: int,
    *,
    base_delay: float,
    backoff_factor: float,
    max_delay: float,
) -> float:
    """Return the wait before the next attempt, honoring an explicit ``retry_after``.

    If the exception carries a non-negative ``retry_after`` attribute (for example
    an HTTP 429 ``Retry-After`` value), that explicit wait is used. Otherwise the
    exponential-backoff delay is computed.

    Args:
        exc: The transient error that was raised.
        attempt: The 1-based attempt number that just failed.
        base_delay: The delay in seconds after the first failed attempt.
        backoff_factor: The multiplier applied for each subsequent attempt.
        max_delay: The upper bound on the computed backoff delay, in seconds.

    Returns:
        The number of seconds to wait before retrying.
    """
    retry_after = getattr(exc, RETRY_AFTER_ATTR, None)
    if retry_after is not None:
        try:
            explicit = float(retry_after)
        except (TypeError, ValueError):
            explicit = -1.0
        if explicit >= 0:
            return explicit
    return compute_backoff_delay(
        attempt,
        base_delay=base_delay,
        backoff_factor=backoff_factor,
        max_delay=max_delay,
    )


def retry_with_backoff(
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    *,
    base_delay: float = DEFAULT_BASE_DELAY,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    max_delay: float = DEFAULT_MAX_DELAY,
    sleep: Callable[[float], None] | None = None,
    async_sleep: Callable[[float], Awaitable[None]] | None = None,
) -> Callable[[F], F]:
    """Decorate a callable to retry on :class:`TransientError` with backoff.

    Retryable failures (those raising ``TransientError``) trigger a wait followed
    by another attempt, up to ``max_attempts`` total attempts. Each retry is
    logged. When the attempts are exhausted a final error is logged and the last
    ``TransientError`` is re-raised. Non-transient exceptions propagate
    immediately without retrying.

    The wait between attempts is the exponential-backoff delay, unless the raised
    ``TransientError`` carries a non-negative ``retry_after`` attribute (for
    example an HTTP 429 ``Retry-After`` value), in which case that explicit wait is
    honored.

    The decorator detects whether the wrapped callable is a coroutine function and
    returns an appropriate synchronous or asynchronous wrapper, so it can wrap both
    the synchronous ``write_batch`` and the asynchronous ``fetch_snapshot``.

    Args:
        max_attempts: The maximum number of attempts (initial try plus retries).
            Must be at least ``1``.
        base_delay: The delay in seconds after the first failed attempt.
        backoff_factor: The multiplier applied to the delay for each subsequent
            attempt.
        max_delay: The upper bound on any computed backoff delay, in seconds.
        sleep: Optional injectable synchronous sleep function (defaults to
            :func:`time.sleep`). Primarily useful for testing.
        async_sleep: Optional injectable asynchronous sleep coroutine function
            (defaults to :func:`asyncio.sleep`). Primarily useful for testing.

    Returns:
        A decorator that wraps the target callable with retry behavior.

    Raises:
        ValueError: If ``max_attempts`` is less than ``1``.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    sync_sleep = sleep if sleep is not None else time.sleep
    a_sleep = async_sleep if async_sleep is not None else asyncio.sleep

    def decorator(func: F) -> F:
        func_name = getattr(func, "__qualname__", getattr(func, "__name__", repr(func)))

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(1, max_attempts + 1):
                    try:
                        return await func(*args, **kwargs)
                    except TransientError as exc:
                        if attempt >= max_attempts:
                            logger.error(
                                "%s failed after %d attempt(s); giving up: %s",
                                func_name,
                                attempt,
                                exc,
                            )
                            raise
                        delay = _resolve_delay(
                            exc,
                            attempt,
                            base_delay=base_delay,
                            backoff_factor=backoff_factor,
                            max_delay=max_delay,
                        )
                        logger.warning(
                            "%s transient failure on attempt %d/%d: %s; "
                            "retrying in %.3fs",
                            func_name,
                            attempt,
                            max_attempts,
                            exc,
                            delay,
                        )
                        if delay > 0:
                            await a_sleep(delay)
                # Unreachable: the loop either returns or raises.
                raise AssertionError("retry loop exited without returning or raising")

            return cast(F, async_wrapper)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except TransientError as exc:
                    if attempt >= max_attempts:
                        logger.error(
                            "%s failed after %d attempt(s); giving up: %s",
                            func_name,
                            attempt,
                            exc,
                        )
                        raise
                    delay = _resolve_delay(
                        exc,
                        attempt,
                        base_delay=base_delay,
                        backoff_factor=backoff_factor,
                        max_delay=max_delay,
                    )
                    logger.warning(
                        "%s transient failure on attempt %d/%d: %s; "
                        "retrying in %.3fs",
                        func_name,
                        attempt,
                        max_attempts,
                        exc,
                        delay,
                    )
                    if delay > 0:
                        sync_sleep(delay)
            # Unreachable: the loop either returns or raises.
            raise AssertionError("retry loop exited without returning or raising")

        return cast(F, sync_wrapper)

    return decorator
