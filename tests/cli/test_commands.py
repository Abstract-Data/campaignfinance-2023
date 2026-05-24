"""Tests for the `cf` Typer CLI commands."""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from app.cli.main import app

runner = CliRunner()


@dataclass
class FakeStateContext:
    config: object = field(default_factory=object)
    temp_folder: Path = field(default_factory=lambda: Path("/tmp/texas"))


class FakeDownloadError(Exception):
    pass


class FakeDownloader:
    def __init__(self, config: object) -> None:
        self.config = config

    def download(self, *, overwrite: bool = False, headless: bool = False) -> Path:
        return Path("/tmp/texas")


@dataclass
class FakeConvertResult:
    converted: int = 1
    skipped: int = 0
    failed: list[tuple[Path, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed


@dataclass
class FakeCoverageRow:
    record_type: str
    files: list[Path]
    row_count: int
    status: Literal["present", "missing", "empty"]


@dataclass
class FakeCoverageReport:
    rows: list[FakeCoverageRow] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(row.status == "present" and row.row_count > 0 for row in self.rows)


@pytest.fixture
def fake_state_context() -> FakeStateContext:
    return FakeStateContext()


@pytest.fixture
def patch_resolve_state(monkeypatch: pytest.MonkeyPatch, fake_state_context: FakeStateContext):
    def _resolve(_state: object) -> FakeStateContext:
        return fake_state_context

    for module in ("app.cli.download", "app.cli.convert", "app.cli.verify"):
        monkeypatch.setattr(f"{module}.resolve_state", _resolve)


@pytest.fixture
def patch_texas_downloader(monkeypatch: pytest.MonkeyPatch):
    texas_mod = types.ModuleType("app.states.texas")
    texas_mod.TECDownloader = FakeDownloader
    texas_mod.DownloadError = FakeDownloadError
    texas_mod.TEXAS_CONFIGURATION = MagicMock()
    monkeypatch.setitem(sys.modules, "app.states.texas", texas_mod)


@pytest.fixture
def patch_texas_converter(monkeypatch: pytest.MonkeyPatch):
    converter_mod = types.ModuleType("app.states.texas.texas_converter")

    def convert_folder(
        folder: Path,
        *,
        overwrite: bool = False,
        keep_csv: bool = True,
        on_progress=None,
    ) -> FakeConvertResult:
        if on_progress is not None:
            on_progress(folder / "sample.csv")
        return FakeConvertResult()

    converter_mod.convert_folder = convert_folder
    monkeypatch.setitem(sys.modules, "app.states.texas.texas_converter", converter_mod)


@pytest.fixture
def patch_texas_coverage(monkeypatch: pytest.MonkeyPatch):
    coverage_mod = types.ModuleType("app.states.texas.texas_coverage")

    def verify_coverage(folder: Path) -> FakeCoverageReport:
        return FakeCoverageReport(
            rows=[
                FakeCoverageRow(
                    record_type="RCPT",
                    files=[folder / "rcpt.parquet"],
                    row_count=10,
                    status="present",
                )
            ]
        )

    coverage_mod.verify_coverage = verify_coverage
    monkeypatch.setitem(sys.modules, "app.states.texas.texas_coverage", coverage_mod)


def test_version_exits_zero() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.stdout


def test_download_texas_success(
    patch_resolve_state,
    patch_texas_downloader,
) -> None:
    result = runner.invoke(app, ["download", "texas"])
    assert result.exit_code == 0
    assert "Download complete" in result.stdout


def test_download_texas_failure(
    monkeypatch: pytest.MonkeyPatch,
    patch_resolve_state,
    patch_texas_downloader,
) -> None:
    class FailingDownloader(FakeDownloader):
        def download(self, *, overwrite: bool = False, headless: bool = False) -> Path:
            raise FakeDownloadError("network timeout")

    texas_mod = sys.modules["app.states.texas"]
    texas_mod.TECDownloader = FailingDownloader

    result = runner.invoke(app, ["download", "texas"])
    assert result.exit_code == 1
    assert "Download failed" in result.stdout


def test_convert_texas_success(
    patch_resolve_state,
    patch_texas_converter,
) -> None:
    result = runner.invoke(app, ["convert", "texas"])
    assert result.exit_code == 0
    assert "Conversion complete" in result.stdout


def test_convert_texas_failure(
    monkeypatch: pytest.MonkeyPatch,
    patch_resolve_state,
    patch_texas_converter,
) -> None:
    converter_mod = sys.modules["app.states.texas.texas_converter"]

    def failing_convert_folder(folder: Path, **kwargs) -> FakeConvertResult:
        return FakeConvertResult(failed=[(folder / "bad.csv", "parse error")])

    converter_mod.convert_folder = failing_convert_folder

    result = runner.invoke(app, ["convert", "texas"])
    assert result.exit_code == 1
    assert "Conversion completed with failures" in result.stdout


def test_verify_texas_success(
    patch_resolve_state,
    patch_texas_coverage,
) -> None:
    result = runner.invoke(app, ["verify", "texas"])
    assert result.exit_code == 0
    assert "Coverage verification passed" in result.stdout


def test_verify_texas_failure(
    monkeypatch: pytest.MonkeyPatch,
    patch_resolve_state,
    patch_texas_coverage,
) -> None:
    coverage_mod = sys.modules["app.states.texas.texas_coverage"]

    def failing_verify(folder: Path) -> FakeCoverageReport:
        return FakeCoverageReport(
            rows=[
                FakeCoverageRow(
                    record_type="RCPT",
                    files=[],
                    row_count=0,
                    status="missing",
                )
            ]
        )

    coverage_mod.verify_coverage = failing_verify

    result = runner.invoke(app, ["verify", "texas"])
    assert result.exit_code == 1
    assert "Coverage verification failed" in result.stdout


def test_prepare_texas_success(
    patch_resolve_state,
    patch_texas_downloader,
    patch_texas_converter,
    patch_texas_coverage,
) -> None:
    result = runner.invoke(app, ["prepare", "texas"])
    assert result.exit_code == 0
    assert "Prepare complete" in result.stdout


def test_prepare_stops_at_first_failing_stage(
    monkeypatch: pytest.MonkeyPatch,
    patch_resolve_state,
    patch_texas_downloader,
    patch_texas_converter,
    patch_texas_coverage,
) -> None:
    convert_called = {"value": False}

    converter_mod = sys.modules["app.states.texas.texas_converter"]

    def failing_convert(folder: Path, **kwargs) -> FakeConvertResult:
        convert_called["value"] = True
        return FakeConvertResult(failed=[(folder / "bad.csv", "parse error")])

    converter_mod.convert_folder = failing_convert

    coverage_mod = sys.modules["app.states.texas.texas_coverage"]
    verify_called = {"value": False}

    def verify_coverage(folder: Path) -> FakeCoverageReport:
        verify_called["value"] = True
        return FakeCoverageReport()

    coverage_mod.verify_coverage = verify_coverage

    result = runner.invoke(app, ["prepare", "texas"])
    assert result.exit_code == 1
    assert "Prepare failed at convert stage" in result.stdout
    assert convert_called["value"] is True
    assert verify_called["value"] is False


def test_download_unknown_state_usage_error() -> None:
    result = runner.invoke(app, ["download", "utah"])
    assert result.exit_code != 0
    assert result.exception is not None or "texas" in result.stdout.lower() or "utah" in result.stdout.lower()
