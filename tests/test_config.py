"""Unit tests for :mod:`ge_pipeline.config`.

Covers lazy loading, caching, required-variable validation, and the absence of
import-time side effects (Requirements 1.1-1.5, 20.3).
"""

from __future__ import annotations

import os
import subprocess
import sys
from unittest.mock import patch

import pytest

import ge_pipeline.config as config_module
from ge_pipeline.config import (
    MigrationSource,
    Settings,
    get_settings,
    require_migration_source,
)
from ge_pipeline.errors import ConfigError

_REQUIRED = ("INFLUXDB3_HOST_URL", "INFLUXDB3_AUTH_TOKEN", "INFLUXDB3_DATABASE_NAME")


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
    monkeypatch.setenv("INFLUXDB3_HOST_URL", "http://localhost:8181")
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "test-token")
    monkeypatch.setenv("INFLUXDB3_DATABASE_NAME", "GEItemPrices")


def test_import_has_no_side_effects_and_no_reads():
    """Importing the module performs no env reads and raises nothing (Req 1.1).

    Run in a clean subprocess with all required vars removed so import purity is
    verified in isolation from the test process' own imports.
    """
    code = (
        "import os\n"
        "for n in ('INFLUXDB3_HOST_URL','INFLUXDB3_AUTH_TOKEN','INFLUXDB3_DATABASE_NAME'):\n"
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

    assert settings.influx3_host == "http://localhost:8181"
    assert settings.influx3_token == "test-token"
    assert settings.influx3_database == "GEItemPrices"
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
        settings.influx3_host = "changed"  # type: ignore[misc]


def test_get_settings_caches_identical_object(monkeypatch):
    """Second call returns the identical cached object without re-reading (Req 1.3)."""
    _set_all_required(monkeypatch)
    first = get_settings()

    # Change the environment; the cached value must not reflect the change.
    monkeypatch.setenv("INFLUXDB3_HOST_URL", "http://changed:9999")
    second = get_settings()

    assert first is second
    assert second.influx3_host == "http://localhost:8181"


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
    monkeypatch.setenv("INFLUXDB3_AUTH_TOKEN", "")

    with pytest.raises(ConfigError) as exc_info:
        get_settings()

    assert "INFLUXDB3_AUTH_TOKEN" in str(exc_info.value)


def test_config_error_names_first_missing_variable(monkeypatch):
    """When several are missing, the error names the first in required order (Req 1.4)."""
    # Only set the last var; HOST_URL and AUTH_TOKEN missing -> HOST_URL first.
    monkeypatch.setenv("INFLUXDB3_DATABASE_NAME", "GEItemPrices")

    with pytest.raises(ConfigError) as exc_info:
        get_settings()

    assert "INFLUXDB3_HOST_URL" in str(exc_info.value)


def test_settings_type_matches_returned_object(monkeypatch):
    """get_settings returns a Settings instance (Req 1.2)."""
    _set_all_required(monkeypatch)
    assert isinstance(get_settings(), Settings)


def test_require_migration_source_happy_path(monkeypatch):
    """With all V2_* source vars set, require_migration_source resolves cleanly."""
    _set_all_required(monkeypatch)
    monkeypatch.setenv("V2_INFLUX_URL", "http://localhost:8086")
    monkeypatch.setenv("V2_INFLUX_TOKEN", "v2-token")
    monkeypatch.setenv("V2_INFLUX_ORG", "Ge-data-project")
    monkeypatch.setenv("V2_INFLUX_BUCKET", "GEItemPrices")

    source = require_migration_source(get_settings())

    assert isinstance(source, MigrationSource)
    assert source.url == "http://localhost:8086"
    assert source.token == "v2-token"
    assert source.org == "Ge-data-project"
    assert source.bucket == "GEItemPrices"


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------
#
# Feature: influxdb-v3-migration, Property 2: Config names the first missing
# target variable
#
# For any subset of the required v3 target variables (INFLUXDB3_HOST_URL,
# INFLUXDB3_AUTH_TOKEN, INFLUXDB3_DATABASE_NAME) being unset or empty,
# get_settings raises a ConfigError naming the FIRST missing variable in
# declared order; when all are present it returns settings without error.
#
# Validates: Requirements 2.1, 2.2

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ge_pipeline.config import _REQUIRED_V3_VARS

# A non-empty, valid-ish value for a present variable.
_PRESENT = "present-value"

# Each required var is independently "present", "unset", or "empty". The
# empty-string case must be treated the same as unset (Requirement 2.2).
_var_state = st.sampled_from(("present", "unset", "empty"))


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(states=st.lists(_var_state, min_size=3, max_size=3))
def test_property_first_missing_v3_variable_is_named(monkeypatch, states):
    """Property 2: the error names the first missing v3 target variable.

    Feature: influxdb-v3-migration, Property 2: Config names the first missing
    target variable.

    Validates: Requirements 2.1, 2.2
    """
    # Isolate every generated example: reset the module cache and neutralize
    # load_dotenv so no real .env file leaks in, then set the environment to
    # exactly the generated combination of present/unset/empty values.
    monkeypatch.setattr(config_module, "_cached_settings", None)
    monkeypatch.setattr(config_module, "load_dotenv", lambda *a, **k: False)

    missing_names: list[str] = []
    for name, state in zip(_REQUIRED_V3_VARS, states):
        if state == "present":
            monkeypatch.setenv(name, _PRESENT)
        elif state == "empty":
            monkeypatch.setenv(name, "")
            missing_names.append(name)
        else:  # "unset"
            monkeypatch.delenv(name, raising=False)
            missing_names.append(name)

    if not missing_names:
        # All present -> settings returned without error, fields populated.
        settings = get_settings()
        assert settings.influx3_host == _PRESENT
        assert settings.influx3_token == _PRESENT
        assert settings.influx3_database == _PRESENT
    else:
        # At least one missing -> ConfigError naming the FIRST missing var in
        # declared order.
        expected_first = next(n for n in _REQUIRED_V3_VARS if n in missing_names)
        with pytest.raises(ConfigError) as exc_info:
            get_settings()

        message = str(exc_info.value)
        assert expected_first in message
        # The error must name the FIRST missing one, not a later missing one.
        for later in missing_names:
            if later != expected_first:
                assert later not in message


# ---------------------------------------------------------------------------
#
# Feature: influxdb-v3-migration, Property 3: Migration source validated only
# on demand
#
# For any environment where the v3 target variables are present but some V2_*
# source variables (V2_INFLUX_URL, V2_INFLUX_TOKEN, V2_INFLUX_ORG,
# V2_INFLUX_BUCKET) are missing or empty, non-migration settings access
# (get_settings) succeeds, while require_migration_source(settings) raises a
# ConfigError naming the FIRST missing V2_* variable in declared order.
#
# Validates: Requirements 2.4

from ge_pipeline.config import _REQUIRED_V2_VARS


def _build_env(states) -> dict[str, str]:
    """Build a complete os.environ replacement for one generated example.

    The three v3 target variables are always present so the non-migration
    ``get_settings`` path never fails for a target-config reason. Each V2_*
    source variable is independently present / empty / omitted per ``states``;
    an empty string is treated the same as unset (Requirement 2.4).
    """
    env: dict[str, str] = {
        "INFLUXDB3_HOST_URL": _PRESENT,
        "INFLUXDB3_AUTH_TOKEN": _PRESENT,
        "INFLUXDB3_DATABASE_NAME": _PRESENT,
    }
    for name, state in zip(_REQUIRED_V2_VARS, states):
        if state == "present":
            env[name] = _PRESENT
        elif state == "empty":
            env[name] = ""
        # "unset" -> intentionally left out of the environment
    return env


@given(states=st.lists(_var_state, min_size=4, max_size=4))
def test_property_migration_source_validated_only_on_demand(states):
    """Property 3: source config is required only when migration asks for it.

    Feature: influxdb-v3-migration, Property 3: Migration source validated only
    on demand.

    Validates: Requirements 2.4
    """
    missing_names = [
        name
        for name, state in zip(_REQUIRED_V2_VARS, states)
        if state != "present"
    ]

    # Isolate every generated example with context managers (not the
    # function-scoped monkeypatch fixture, which Hypothesis does not reset
    # between generated inputs): reset the module cache, neutralize load_dotenv
    # so no real .env leaks in, and replace os.environ wholesale.
    with (
        patch.object(config_module, "_cached_settings", None),
        patch.object(config_module, "load_dotenv", lambda *a, **k: False),
        patch.dict(os.environ, _build_env(states), clear=True),
    ):
        _run_migration_source_assertions(missing_names)


def _run_migration_source_assertions(missing_names) -> None:
    # Non-migration access always succeeds regardless of V2_* state: the source
    # variables are never validated here.
    settings = get_settings()
    assert settings.influx3_host == _PRESENT
    assert settings.influx3_token == _PRESENT
    assert settings.influx3_database == _PRESENT

    if not missing_names:
        # All source vars present -> migration source resolves cleanly.
        source = require_migration_source(settings)
        assert isinstance(source, MigrationSource)
        assert source.url == _PRESENT
        assert source.token == _PRESENT
        assert source.org == _PRESENT
        assert source.bucket == _PRESENT
    else:
        # At least one source var missing -> ConfigError naming the FIRST
        # missing V2_* variable in declared order, and only when asked.
        expected_first = next(n for n in _REQUIRED_V2_VARS if n in missing_names)
        with pytest.raises(ConfigError) as exc_info:
            require_migration_source(settings)

        message = str(exc_info.value)
        assert expected_first in message
        # It must name the FIRST missing one, not a later missing one.
        for later in missing_names:
            if later != expected_first:
                assert later not in message

# ---------------------------------------------------------------------------
#
# Feature: influxdb-v3-migration, Property 4: Process environment overrides
# `.env`
#
# For any variable set in BOTH the process environment and a `.env` file, the
# loaded configuration value equals the process-environment value. This holds
# because get_settings() calls load_dotenv(override=False), so values already
# present in os.environ are never clobbered by the file.
#
# Validates: Requirements 2.6

import contextlib
import functools

from dotenv import load_dotenv as _real_load_dotenv

# Reuse the same three-state generator as Property 2 but bias toward "present"
# so the interesting overlap case (var in both env and .env) is exercised
# often. A separate value is used for the process env vs. the .env file so a
# mismatch is detectable.
_ENV_VALUE = "from-process-env"
_DOTENV_VALUE = "from-dotenv-file"


@st.composite
def _env_and_dotenv_states(draw: st.DrawFn) -> tuple[dict[str, str], dict[str, str]]:
    """Draw the process-env and .env-file contents for the v3 target vars.

    For each of the three required v3 variables, independently decide whether
    it appears in the process environment and whether it appears in the ``.env``
    file. At least one location must supply every variable (so ``get_settings``
    does not raise), and the two locations use distinct values so precedence is
    observable.
    """
    env: dict[str, str] = {}
    dotenv: dict[str, str] = {}
    for name in _REQUIRED_V3_VARS:
        # ("env_only", "dotenv_only", "both") — every case supplies the var.
        placement = draw(st.sampled_from(("env_only", "dotenv_only", "both")))
        if placement in ("env_only", "both"):
            env[name] = _ENV_VALUE
        if placement in ("dotenv_only", "both"):
            dotenv[name] = _DOTENV_VALUE
    return env, dotenv


@contextlib.contextmanager
def _isolated_v3_env(env: dict[str, str]):
    """Set the process env to exactly ``env`` for the v3 target vars, then restore.

    Any values ``load_dotenv`` writes into ``os.environ`` during the body are
    also cleaned up on exit, so nothing leaks between Hypothesis examples.
    """
    saved = {name: os.environ.get(name) for name in _REQUIRED_V3_VARS}
    try:
        for name in _REQUIRED_V3_VARS:
            os.environ.pop(name, None)
        for name, value in env.items():
            os.environ[name] = value
        yield
    finally:
        for name, original in saved.items():
            if original is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = original


@given(data=_env_and_dotenv_states())
def test_property_process_env_overrides_dotenv(tmp_path_factory, data):
    """Property 4: process environment wins over ``.env`` for shared variables.

    Feature: influxdb-v3-migration, Property 4: Process environment overrides
    `.env`.

    Writes a real ``.env`` file whose values differ from the process-env values
    for the same keys, then loads settings and asserts each field reflects the
    process-env value wherever the process env set it (and the ``.env`` value
    only where the process env did not).

    ``os.environ`` and ``config._cached_settings`` are isolated with context
    managers rather than the ``monkeypatch`` fixture because Hypothesis reuses a
    function-scoped fixture across every generated example.

    Validates: Requirements 2.6
    """
    env, dotenv = data

    # Write a real .env file with the generated file values.
    dotenv_dir = tmp_path_factory.mktemp("dotenv")
    dotenv_path = dotenv_dir / ".env"
    dotenv_path.write_text(
        "".join(f"{name}={value}\n" for name, value in dotenv.items()),
        encoding="utf-8",
    )

    # Point config's load_dotenv at this specific file (instead of cwd
    # discovery) while preserving its real override=False semantics, which is
    # exactly the behavior under test.
    patched_load_dotenv = functools.partial(
        _real_load_dotenv, dotenv_path=str(dotenv_path)
    )

    saved_cache = config_module._cached_settings
    with _isolated_v3_env(env), patch.object(
        config_module, "load_dotenv", patched_load_dotenv
    ):
        config_module._cached_settings = None
        try:
            settings = get_settings()
        finally:
            config_module._cached_settings = saved_cache

    loaded = {
        "INFLUXDB3_HOST_URL": settings.influx3_host,
        "INFLUXDB3_AUTH_TOKEN": settings.influx3_token,
        "INFLUXDB3_DATABASE_NAME": settings.influx3_database,
    }

    for name in _REQUIRED_V3_VARS:
        if name in env:
            # Set in the process environment (possibly also in .env): the
            # process-env value must win — .env must NOT override it.
            assert loaded[name] == _ENV_VALUE, (
                f"{name} should keep its process-env value, not the .env value"
            )
        else:
            # Only in the .env file: the file value is used as the fallback.
            assert loaded[name] == _DOTENV_VALUE
