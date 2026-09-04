"""ge_pipeline: modernized OSRS Grand Exchange price collection pipeline.

This package repackages the legacy ingestion scripts into a cohesive, testable
Python package with lazy/cached configuration, schema validation, async
ingestion, a scale-oriented data access layer, a pluggable outlier-detection
registry, a Typer CLI, an APScheduler daemon, and a FastAPI query service.

Importing this package (or any submodule) has no side effects: no environment
reads, no network calls, and no configuration errors are raised at import time.
Configuration is loaded lazily on first use via
:func:`ge_pipeline.config.get_settings`.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
