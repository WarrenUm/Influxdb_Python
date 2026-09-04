"""Unit tests for :mod:`ge_pipeline.config`.

Covers lazy loading, caching, required-variable validation, and the absence of
import-time side effects (Requirements 1.1-1.5, 20.3).
"""

from __future__ import annotations

import subprocess
import sys

import pytest

import ge_pipeline.config as config_module
from ge_pipeline.config import Settings, get_settings
from ge_pipeline.errors import ConfigError

_REQUIRED = ("INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET")


@pytest.fixture(autouse=True)
def _reset_cache_and_env(monkeypatch):
    """Isolate each test: clear the module cache and required env vars.

    Also neutralize ``load_dotenv`` so tests never read a real ``.env`` file on
    the developer/CI machine.
    """
    monkeypatch.setattr(config_module, "_cached_settings", None)
    monkeypatch.setattr(config_module, "load_dotenv", lambda *a, **k: False)
    for name in _REQUIRED:
        monkeypatch.delenv(name, raising=False)
    yield
    monkeypatch.setattr(config_module, "_cached_settings", None)


def _set_all_required(monkeypatch) -> None:
    monkeypatch.setenv("INFLUX_URL", "http://localhost:8086")
    monkeypatch.setenv("INFLUX_TOKEN", "test-token")
    monkeypatch.setenv("INFLUX_ORG", "Ge-data-project")
    monkeypatch.setenv("INFLUX_BUCKET", "GEItemPrices")


def test_import_has_no_side_effects_and_no_reads():
    """Importing the module performs no env reads and raises nothing (Req 1.1).

    Run in a clean subprocess with all required vars removed so import purity is
    verified in isolation from the test process' own imports.
    """
    code = (
        "import os\n"
        "for n in ('INFLUX_URL','INFLUX_TOKEN','INFLUX_ORG','INFLUX_BUCKET'):\n"
        "    os.environ.pop(n, None)\n"
        "import ge_pipeline.config\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_get_settings_loads_all_fields_with_defaults(monkeypatch):
    """First call returns a fully-populated Settings with documented defaults (Req 1.2)."""
    _set_all_required(monkeypatch)

    settings = get_settings()

    assert settings.influx_url == "http://localhost:8086"
    assert settings.influx_token == "test-token"
    assert settings.influx_org == "Ge-data-project"
    assert settings.influx_bucket == "GEItemPrices"
    # Documented defaults.
    assert settings.user_agent == "GEoutlier-detection"
    assert settings.api_base_url == "https://prices.runescape.wiki/api/v1/osrs"
    assert settings.max_concurrency == 8
    assert settings.batch_size == 50


def test_settings_is_frozen(monkeypatch):
    """Settings is immutable (frozen dataclass)."""
    _set_all_required(monkeypatch)
    settings = get_settings()
    with pytest.raises(Exception):
        settings.influx_url = "changed"  # type: ignore[misc]


def test_get_settings_caches_identical_object(monkeypatch):
    """Second call returns the identical cached object without re-reading (Req 1.3)."""
    _set_all_required(monkeypatch)
    first = get_settings()

    # Change the environment; the cached value must not reflect the change.
    monkeypatch.setenv("INFLUX_URL", "http://changed:9999")
    second = get_settings()

    assert first is second
    assert second.influx_url == "http://localhost:8086"


@pytest.mark.parametrize("missing", _REQUIRED)
def test_missing_required_variable_raises_config_error(monkeypatch, missing):
    """A missing required var raises ConfigError naming that variable (Req 1.4)."""
    _set_all_required(monkeypatch)
    monkeypatch.delenv(missing, raising=False)

    with pytest.raises(ConfigError) as exc_info:
        get_settings()

    assert missing in str(exc_info.value)


def test_empty_required_variable_raises_config_error(monkeypatch):
    """An empty required var is treated as missing (Req 1.4)."""
    _set_all_required(monkeypatch)
    monkeypatch.setenv("INFLUX_TOKEN", "")

    with pytest.raises(ConfigError) as exc_info:
        get_settings()

    assert "INFLUX_TOKEN" in str(exc_info.value)


def test_config_error_names_first_missing_variable(monkeypatch):
    """When several are missing, the error names the first in required order (Req 1.4)."""
    # Only set the last two; URL and TOKEN missing -> URL reported first.
    monkeypatch.setenv("INFLUX_ORG", "Ge-data-project")
    monkeypatch.setenv("INFLUX_BUCKET", "GEItemPrices")

    with pytest.raises(ConfigError) as exc_info:
        get_settings()

    assert "INFLUX_URL" in str(exc_info.value)


def test_settings_type_matches_returned_object(monkeypatch):
    """get_settings returns a Settings instance (Req 1.2)."""
    _set_all_required(monkeypatch)
    assert isinstance(get_settings(), Settings)
