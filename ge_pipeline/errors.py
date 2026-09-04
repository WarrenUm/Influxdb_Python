"""Shared exception types for the :mod:`ge_pipeline` package.

These exceptions provide a common vocabulary for failures across the
configuration, ingestion, influx, data-access, outlier, and API layers so that
callers can distinguish retryable (transient) failures from permanent
(non-transient) ones, and configuration problems from operational ones.
"""

from __future__ import annotations

__all__ = ["ConfigError", "TransientError", "NonTransientError"]


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid.

    This is raised lazily, only when settings are actually needed (for example
    on the first call to :func:`ge_pipeline.config.get_settings`), never merely
    by importing a module.
    """


class TransientError(Exception):
    """Raised for retryable failures.

    Transient failures include network timeouts, connection errors, HTTP 5xx
    responses, HTTP 429 rate-limit responses, and transient InfluxDB write
    errors. Operations that raise this should be retried with exponential
    backoff.
    """


class NonTransientError(Exception):
    """Raised for non-retryable failures.

    Non-transient failures include non-429 HTTP 4xx responses and similar
    permanent errors that will not succeed on retry.
    """
