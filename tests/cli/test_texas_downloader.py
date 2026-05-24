"""Tests for TECDownloader refactor (task A)."""

from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from abcs.abc_state_config import CSVReaderConfig, StateConfig
from states.texas.texas_downloader import (
    DOWNLOAD_WAIT_TIMEOUT_SECONDS,
    DownloadError,
    TECDownloader,
)


def _make_config(tmp_path: Path) -> StateConfig:
    config = StateConfig(
        STATE_NAME="texas",
        STATE_ABBREVIATION="TX",
        CSV_CONFIG=CSVReaderConfig(),
    )
    return config


def _patch_temp_folder(monkeypatch: pytest.MonkeyPatch, config: StateConfig, folder: Path) -> None:
    monkeypatch.setattr(type(config), "TEMP_FOLDER", property(lambda self: folder))


def test_temp_folder_auto_created_no_input(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    temp_folder = tmp_path / "missing-texas"
    assert not temp_folder.exists()

    monkeypatch.setattr(
        "builtins.input",
        lambda *_args, **_kwargs: pytest.fail("input() must not be called"),
    )

    config = _make_config(tmp_path)
    _patch_temp_folder(monkeypatch, config, temp_folder)

    downloader = TECDownloader(config=config)

    assert temp_folder.is_dir()
    assert downloader.folder == temp_folder


def test_headless_adds_chrome_argument(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    temp_folder = tmp_path / "texas"
    temp_folder.mkdir()
    config = _make_config(tmp_path)
    _patch_temp_folder(monkeypatch, config, temp_folder)

    captured_options: list[object] = []

    wait_mock = MagicMock()
    wait_mock.until.side_effect = lambda _fn: MagicMock(click=MagicMock())
    monkeypatch.setattr(
        "states.texas.texas_downloader.WebDriverWait",
        lambda _driver, _timeout: wait_mock,
    )
    monkeypatch.setattr("states.texas.texas_downloader.time.sleep", lambda _seconds: None)

    def fake_chrome(*, options=None, **kwargs):
        captured_options.append(options)
        return MagicMock()

    monkeypatch.setattr("states.texas.texas_downloader.webdriver.Chrome", fake_chrome)

    zip_path = temp_folder / "data.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("sample.csv", "a,b\n1,2\n")

    downloader = TECDownloader(config=config)
    result = downloader.download(headless=True)

    assert result == temp_folder
    assert len(captured_options) == 1
    arguments = captured_options[0].arguments
    assert "--headless" in arguments


def test_wait_loop_timeout_raises_download_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    temp_folder = tmp_path / "texas"
    temp_folder.mkdir()
    config = _make_config(tmp_path)
    _patch_temp_folder(monkeypatch, config, temp_folder)

    def fake_chrome(*, options=None, **kwargs):
        driver = MagicMock()
        wait_mock = MagicMock()
        wait_mock.until.side_effect = lambda fn: MagicMock(click=MagicMock())
        monkeypatch.setattr(
            "states.texas.texas_downloader.WebDriverWait",
            lambda _driver, _timeout: wait_mock,
        )
        monkeypatch.setattr("states.texas.texas_downloader.time.sleep", lambda _seconds: None)
        return driver

    monkeypatch.setattr("states.texas.texas_downloader.webdriver.Chrome", fake_chrome)

    crdownload = temp_folder / "file.csv.crdownload"
    crdownload.touch()

    start = 0.0

    def fake_monotonic() -> float:
        nonlocal start
        start += DOWNLOAD_WAIT_TIMEOUT_SECONDS
        return start

    monkeypatch.setattr("states.texas.texas_downloader.time.monotonic", fake_monotonic)

    original_glob = Path.glob

    def glob_with_crdownload(self: Path, pattern: str):
        if pattern == "*.crdownload":
            return iter([crdownload])
        return original_glob(self, pattern)

    monkeypatch.setattr(Path, "glob", glob_with_crdownload)

    downloader = TECDownloader(config=config)

    with pytest.raises(DownloadError, match="timed out"):
        downloader.download()
