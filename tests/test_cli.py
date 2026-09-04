"""Unit tests for :mod:`ge_pipeline.cli` using Typer's :class:`CliRunner`.

Covers command wiring, ``ingest`` count output, ``export`` file streaming, and
non-zero exit with remediation guidance on missing configuration
(Requirements 14.2, 14.4, 14.5, 21.1).
"""

from __future__ import annotations

import ge_pipeline.data_access as data_access_module
import ge_pipeline.influx as influx_module
import ge_pipeline.ingestion as ingestion_module
from typer.testing import CliRunner

import ge_pipeline.cli as cli_module
from ge_pipeline.cli import app
from ge_pipeline.config import Settings
from ge_pipeline.errors import ConfigError
from ge_pipeline.ingestion import IngestionResult

runner = CliRunner()


def _dummy_settings() -> Settings:
    """Build a fully-populated Settings that requires no environment access."""
    return Settings(
        influx_url="http://localhost:8086",
        influx_token="test-token",
        influx_org="Ge-data-project",
        influx_bucket="GEItemPrices",
    )


# --------------------------------------------------------------------------- #
# Command wiring (Requirement 21.1)
# --------------------------------------------------------------------------- #
def test_help_lists_all_commands():
    """``--help`` exits 0 and lists every registered command."""
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in ("ingest", "backfill", "serve", "export", "setup"):
        assert command in result.output


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
# Non-zero exit with remediation on missing config (Requirement 14.5)
# --------------------------------------------------------------------------- #
def _raise_config_error():
    raise ConfigError(
        "Required environment variable 'INFLUX_URL' is unset or empty."
    )


def test_ingest_missing_config_exits_nonzero_with_remediation(monkeypatch):
    """``ingest`` exits non-zero and prints remediation when config is missing."""
    monkeypatch.setattr(cli_module, "get_settings", _raise_config_error)

    result = runner.invoke(app, ["ingest"])

    assert result.exit_code != 0
    combined = result.output
    assert "Configuration missing" in combined
    assert "cp .env.example .env" in combined


def test_setup_missing_config_exits_nonzero_with_remediation(monkeypatch):
    """``setup`` also surfaces remediation guidance on missing config."""
    monkeypatch.setattr(cli_module, "get_settings", _raise_config_error)

    result = runner.invoke(app, ["setup"])

    assert result.exit_code != 0
    assert "Configuration missing" in result.output


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
