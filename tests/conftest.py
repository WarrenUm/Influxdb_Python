"""Shared pytest fixtures and configuration for the ge_pipeline test suite.

Registers a Hypothesis profile named ``ge`` that runs at least 100 examples per
property (per design Requirement 21.2) and activates it for the session. Shared
fixtures (mocked httpx clients, mocked InfluxDB clients, seeded stores, and
Hypothesis strategies) are added as the corresponding features are implemented
in later tasks.
"""

from __future__ import annotations

from hypothesis import HealthCheck, settings

# Property-based tests must run >= 100 iterations (design Requirement 21.2).
settings.register_profile(
    "ge",
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("ge")
