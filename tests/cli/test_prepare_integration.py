"""Integration tests for `cf prepare texas` using real converter and verifier."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from app.cli.main import app
from app.cli.state import StateContext
from app.states.texas import TEXAS_CONFIGURATION

runner = CliRunner()

_REQUIRED_CSVS: dict[str, str] = {
    "contribs.csv": "id,amount\n1,100\n",
    "expend.csv": "id,amount\n1,50\n",
    "loans.csv": "id,amount\n1,25\n",
    "filers.csv": "id,name\n1,Committee A\n",
    "cover.csv": "id,period\n1,2024\n",
}


def _build_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in _REQUIRED_CSVS.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def _seed_required_csvs(folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    for name, content in _REQUIRED_CSVS.items():
        (folder / name).write_text(content, encoding="utf-8")


@pytest.fixture
def texas_data_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "texas"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def patch_texas_context(texas_data_dir: Path, monkeypatch: pytest.MonkeyPatch):
    context = StateContext(config=TEXAS_CONFIGURATION, temp_folder=texas_data_dir)

    def _resolve(_state: object, *, data_folder: Path | None = None) -> StateContext:
        if data_folder is not None:
            return StateContext(config=TEXAS_CONFIGURATION, temp_folder=data_folder)
        return context

    for module in ("app.cli.download", "app.cli.convert", "app.cli.verify"):
        monkeypatch.setattr(f"{module}.resolve_state", _resolve)
    monkeypatch.setattr("app.cli.state.resolve_state", _resolve)


@pytest.fixture
def patch_selenium_download(texas_data_dir: Path):
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())
    mock_driver = MagicMock()

    def _fake_wait_for_download(_self: object, folder: Path) -> None:
        zip_path = folder / "TEC_CF_CSV.zip"
        zip_path.write_bytes(_build_zip_bytes())

    with (
        patch("app.states.texas.texas_downloader.webdriver.Chrome", return_value=mock_driver),
        patch("app.states.texas.texas_downloader.WebDriverWait", return_value=wait),
        patch("app.states.texas.texas_downloader.time.sleep"),
        patch(
            "app.states.texas.texas_downloader.TECDownloader._wait_for_download",
            _fake_wait_for_download,
        ),
    ):
        yield mock_driver


def test_prepare_texas_writes_parquet_and_passes_verify(
    patch_texas_context,
    patch_selenium_download,
    texas_data_dir: Path,
) -> None:
    result = runner.invoke(app, ["prepare", "texas"])

    assert result.exit_code == 0, result.stdout
    assert "Prepare complete" in result.stdout
    assert "Coverage verification passed" in result.stdout

    for csv_name in _REQUIRED_CSVS:
        stem = Path(csv_name).stem
        matches = list(texas_data_dir.glob(f"{stem}*.parquet"))
        assert matches, f"Expected parquet for {stem} in {texas_data_dir}"


def test_prepare_texas_fails_verify_when_required_type_missing(
    patch_texas_context,
    texas_data_dir: Path,
) -> None:
    _seed_required_csvs(texas_data_dir)
    (texas_data_dir / "loans.csv").unlink()

    result = runner.invoke(app, ["prepare", "texas", "--skip-download"])

    assert result.exit_code == 1
    assert "Prepare failed at verify stage" in result.stdout
    assert "Coverage verification failed" in result.stdout
