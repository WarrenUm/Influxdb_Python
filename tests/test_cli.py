"""Unit tests for :mod:`ge_pipeline.cli` using Typer's :class:`CliRunner`.

Covers command wiring, ``ingest`` count output, ``export`` file streaming, the
``setup`` command's InfluxDB 3 database-creation outcomes, the ``migrate``
command wrapping the one-pass migration tool, and non-zero exit with
remediation guidance on missing configuration (Requirements 4.x, 8.x, 14.x).
"""

from __future__ import annotations

from typer.testing import CliRunner

import ge_pipeline.admin as admin_module
import ge_pipeline.cli as cli_module
import ge_pipeline.data_access as data_access_module
import ge_pipeline.influx as influx_module
import ge_pipeline.ingestion as ingestion_module
import ge_pipeline.migrate as migrate_module
from ge_pipeline.cli import app
from ge_pipeline.config import Settings
from ge_pipeline.errors import ConfigError
from ge_pipeline.ingestion import IngestionResult
from ge_pipeline.migrate import MigrationResult

runner = CliRunner()


def _dummy_settings() -> Settings:
    """Build a fully-populated Settings that requires no environment access."""
    return Settings(
        influx3_host="http://localhost:8181",
        influx3_token="test-token",
        influx3_database="GEItemPrices",
    )


# --------------------------------------------------------------------------- #
# Command wiring (Requirement 8.3, 21.1)
# --------------------------------------------------------------------------- #
def test_help_lists_all_commands():
    """``--help`` exits 0 and lists every registered command."""
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in (
        "ingest", "backfill", "rollup", "serve", "export", "setup", "migrate"
    ):
        assert command in result.output


def test_all_commands_are_registered():
    """Every v3 command is registered on the Typer app (Req 8.1, 8.2, 8.3).

    Asserts against ``app.registered_commands`` directly (not just ``--help``
    text) so a command that stops being wired up is caught even if help output
    changes. Typer derives a command's name from its callback when ``name`` is
    unset, so resolve each name the same way.
    """
    registered = {
        cmd.name or cmd.callback.__name__ for cmd in app.registered_commands
    }
    assert registered == {
        "ingest",
        "backfill",
        "rollup",
        "serve",
        "export",
        "setup",
        "migrate",
    }


# --------------------------------------------------------------------------- #
# ingest count output (Requirement 14.2)
# --------------------------------------------------------------------------- #
def test_ingest_reports_counts(monkeypatch):
    """``ingest`` echoes the written/processed/skipped/failure counts."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())

    async def _fake_run_catch_up(settings):
        assert isinstance(settings, Settings)
        return IngestionResult(
            timestamps_processed=3,
            records_written=6,
            timestamps_skipped=0,
            failures=1,
        )

    # cli imports run_catch_up lazily via `from .ingestion import run_catch_up`,
    # so patch the source binding.
    monkeypatch.setattr(ingestion_module, "run_catch_up", _fake_run_catch_up)

    result = runner.invoke(app, ["ingest"])

    assert result.exit_code == 0
    assert "records written:      6" in result.output
    assert "timestamps processed: 3" in result.output
    assert "timestamps skipped:   0" in result.output
    assert "failures:             1" in result.output


def test_backfill_reports_counts(monkeypatch):
    """``backfill`` shares the catch-up path and echoes the same counts (Req 8.1).

    ``backfill`` drives the same ``run_catch_up`` mechanism as ``ingest`` (a
    catch-up run naturally backfills missing windows), so it must report the
    written/processed/skipped/failure counts too.
    """
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())

    async def _fake_run_catch_up(settings):
        assert isinstance(settings, Settings)
        return IngestionResult(
            timestamps_processed=5,
            records_written=9,
            timestamps_skipped=2,
            failures=0,
        )

    monkeypatch.setattr(ingestion_module, "run_catch_up", _fake_run_catch_up)

    result = runner.invoke(app, ["backfill"])

    assert result.exit_code == 0, result.output
    assert "Backfill complete:" in result.output
    assert "records written:      9" in result.output
    assert "timestamps processed: 5" in result.output
    assert "timestamps skipped:   2" in result.output
    assert "failures:             0" in result.output


# --------------------------------------------------------------------------- #
# export file streaming (Requirement 14.4)
# --------------------------------------------------------------------------- #
def test_export_streams_chunks_to_file(monkeypatch, tmp_path):
    """``export`` writes each streamed byte chunk straight to the output file."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())
    monkeypatch.setattr(
        influx_module, "get_client", lambda settings: object()
    )

    chunks = [b'{"itemID":"554"}\n', b'{"itemID":"565"}\n']

    def _fake_stream_dataset(client, item_ids, start, stop, interval, fmt):
        assert item_ids == ["554"]
        assert fmt == "ndjson"
        yield from chunks

    monkeypatch.setattr(
        data_access_module, "stream_dataset", _fake_stream_dataset
    )

    out_path = tmp_path / "out.ndjson"
    result = runner.invoke(
        app,
        [
            "export",
            "--items",
            "554",
            "--output",
            str(out_path),
            "--format",
            "ndjson",
        ],
    )

    assert result.exit_code == 0, result.output
    assert out_path.read_bytes() == b"".join(chunks)


# --------------------------------------------------------------------------- #
# setup: v3 database creation outcomes (Requirements 4.1, 4.2, 4.3, 8.2, 8.4)
# --------------------------------------------------------------------------- #
def test_setup_reports_created(monkeypatch):
    """``setup`` delegates to admin.setup_v3_database and reports 'created'."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())

    def _fake_setup(settings):
        assert isinstance(settings, Settings)
        return "created"

    monkeypatch.setattr(admin_module, "setup_v3_database", _fake_setup)

    result = runner.invoke(app, ["setup"])

    assert result.exit_code == 0, result.output
    assert "created successfully" in result.output


def test_setup_reports_exists(monkeypatch):
    """``setup`` reports 'already exists' when the database is present."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())
    monkeypatch.setattr(
        admin_module, "setup_v3_database", lambda settings: "exists"
    )

    result = runner.invoke(app, ["setup"])

    assert result.exit_code == 0, result.output
    assert "already exists" in result.output


def test_setup_connection_error_exits_nonzero_naming_host(monkeypatch):
    """An unreachable v3 server exits non-zero and names the host (Req 4.3, 8.4)."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())

    def _raise_connection_error(settings):
        raise ConnectionError(
            "Cannot reach InfluxDB 3 server at 'http://localhost:8181'"
        )

    monkeypatch.setattr(admin_module, "setup_v3_database", _raise_connection_error)

    result = runner.invoke(app, ["setup"])

    assert result.exit_code != 0
    assert "http://localhost:8181" in result.output


# --------------------------------------------------------------------------- #
# migrate: wraps run_migration and echoes read/written counts (Req 5.5, 8.3)
# --------------------------------------------------------------------------- #
def test_migrate_reports_read_and_written_counts(monkeypatch):
    """``migrate`` delegates to run_migration and echoes the counts."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())

    def _fake_run_migration(settings):
        assert isinstance(settings, Settings)
        return MigrationResult(records_read=42, records_written=42)

    monkeypatch.setattr(migrate_module, "run_migration", _fake_run_migration)

    result = runner.invoke(app, ["migrate"])

    assert result.exit_code == 0, result.output
    assert "records read:    42" in result.output
    assert "records written: 42" in result.output


def test_migrate_missing_source_config_exits_nonzero(monkeypatch):
    """A missing V2_* variable surfaces remediation and a non-zero exit."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())

    def _raise_config_error(settings):
        raise ConfigError(
            "Required environment variable 'V2_INFLUX_URL' is unset or empty."
        )

    monkeypatch.setattr(migrate_module, "run_migration", _raise_config_error)

    result = runner.invoke(app, ["migrate"])

    assert result.exit_code != 0
    assert "V2_INFLUX_URL" in result.output


def test_migrate_connection_error_exits_nonzero(monkeypatch):
    """An unreachable v2/v3 endpoint exits non-zero naming the server."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: _dummy_settings())

    def _raise_connection_error(settings):
        raise ConnectionError("Cannot reach InfluxDB v2 source at 'http://v2:8086'")

    monkeypatch.setattr(migrate_module, "run_migration", _raise_connection_error)

    result = runner.invoke(app, ["migrate"])

    assert result.exit_code != 0
    assert "http://v2:8086" in result.output


# --------------------------------------------------------------------------- #
# Non-zero exit with remediation on missing config (Requirement 8.4, 14.5)
# --------------------------------------------------------------------------- #
def _raise_config_error():
    raise ConfigError(
        "Required environment variable 'INFLUXDB3_HOST_URL' is unset or empty."
    )


def test_ingest_missing_config_exits_nonzero_with_remediation(monkeypatch):
    """``ingest`` exits non-zero and prints remediation when config is missing."""
    monkeypatch.setattr(cli_module, "get_settings", _raise_config_error)

    result = runner.invoke(app, ["ingest"])

    assert result.exit_code != 0
    combined = result.output
    assert "Configuration missing" in combined
    assert "cp .env.example .env" in combined
    # Remediation names the v3 INFLUXDB3_* variables.
    assert "INFLUXDB3_HOST_URL" in combined


def test_setup_missing_config_exits_nonzero_with_remediation(monkeypatch):
    """``setup`` also surfaces remediation guidance on missing config."""
    monkeypatch.setattr(cli_module, "get_settings", _raise_config_error)

    result = runner.invoke(app, ["setup"])

    assert result.exit_code != 0
    assert "Configuration missing" in result.output
    assert "INFLUXDB3_HOST_URL" in result.output


def test_export_missing_config_exits_nonzero_with_remediation(
    monkeypatch, tmp_path
):
    """``export`` reports remediation when config is missing (after validation)."""
    monkeypatch.setattr(cli_module, "get_settings", _raise_config_error)

    result = runner.invoke(
        app,
        [
            "export",
            "--items",
            "554",
            "--output",
            str(tmp_path / "out.ndjson"),
            "--format",
            "ndjson",
        ],
    )

    assert result.exit_code != 0
    assert "Configuration missing" in result.output
