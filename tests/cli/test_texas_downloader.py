"""Tests for Texas TEC downloader refactor (task A)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.states.texas.texas_downloader import DownloadError, TECDownloader


@pytest.fixture
def downloader_config(tmp_path: Path) -> MagicMock:
    config = MagicMock()
    config.TEMP_FOLDER = tmp_path / "tec_download"
    config.FILE_COUNTS = None
    return config


def test_temp_folder_created_on_init_without_prompt(
    downloader_config: MagicMock,
) -> None:
    fresh_dir = downloader_config.TEMP_FOLDER
    assert not fresh_dir.exists()

    with patch("builtins.input") as mock_input:
        TECDownloader(config=downloader_config)

    mock_input.assert_not_called()
    assert fresh_dir.is_dir()


def test_download_headless_adds_chrome_argument(downloader_config: MagicMock) -> None:
    captured_options: list = []
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())

    def fake_chrome(*, options):  # noqa: ANN001
        captured_options.append(options)
        return MagicMock()

    downloader = TECDownloader(config=downloader_config)
    tmp_path = downloader_config.TEMP_FOLDER
    tmp_path.mkdir(parents=True, exist_ok=True)
    zip_path = tmp_path / "TEC_CF_CSV.zip"
    zip_path.write_bytes(b"placeholder")

    with (
        patch("app.states.texas.texas_downloader.webdriver.Chrome", side_effect=fake_chrome),
        patch("app.states.texas.texas_downloader.WebDriverWait", return_value=wait),
        patch("app.states.texas.texas_downloader.time.sleep"),
        patch.object(downloader, "_wait_for_download"),
        patch("app.states.texas.texas_downloader.zipfile.ZipFile") as mock_zip,
    ):
        mock_zip.return_value.__enter__.return_value.infolist.return_value = []
        downloader.download(headless=True)

    assert captured_options
    args = captured_options[0].arguments
    assert "--headless" in args


def test_wait_loop_timeout_raises_download_error(downloader_config: MagicMock) -> None:
    downloader = TECDownloader(config=downloader_config)
    tmp_path = downloader_config.TEMP_FOLDER
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "file.crdownload").write_text("partial")

    with (
        patch(
            "app.states.texas.texas_downloader._DOWNLOAD_TIMEOUT_SECONDS",
            0,
        ),
        patch(
            "app.states.texas.texas_downloader.time.monotonic",
            side_effect=[0.0, 1.0],
        ),
    ):
        with pytest.raises(DownloadError, match="timed out"):
            downloader._wait_for_download(tmp_path)


def test_download_returns_temp_folder_path(downloader_config: MagicMock) -> None:
    downloader = TECDownloader(config=downloader_config)
    tmp_path = downloader_config.TEMP_FOLDER
    tmp_path.mkdir(parents=True, exist_ok=True)
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())
    mock_driver = MagicMock()

    with (
        patch("app.states.texas.texas_downloader.webdriver.Chrome", return_value=mock_driver),
        patch("app.states.texas.texas_downloader.WebDriverWait", return_value=wait),
        patch("app.states.texas.texas_downloader.time.sleep"),
        patch.object(downloader, "_wait_for_download"),
        patch("app.states.texas.texas_downloader.zipfile.ZipFile") as mock_zip,
    ):
        mock_zip.return_value.__enter__.return_value.infolist.return_value = []
        (tmp_path / "TEC_CF_CSV.zip").write_bytes(b"placeholder")
        result = downloader.download()

    assert result == tmp_path
    mock_driver.quit.assert_called_once()


def test_download_quits_driver_on_download_error(downloader_config: MagicMock) -> None:
    downloader = TECDownloader(config=downloader_config)
    tmp_path = downloader_config.TEMP_FOLDER
    tmp_path.mkdir(parents=True, exist_ok=True)
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())
    mock_driver = MagicMock()

    with (
        patch("app.states.texas.texas_downloader.webdriver.Chrome", return_value=mock_driver),
        patch("app.states.texas.texas_downloader.WebDriverWait", return_value=wait),
        patch("app.states.texas.texas_downloader.time.sleep"),
        patch.object(downloader, "_wait_for_download"),
    ):
        with pytest.raises(DownloadError, match="No zip file found"):
            downloader.download()

    mock_driver.quit.assert_called_once()


def test_download_uses_output_dir_override(downloader_config: MagicMock, tmp_path: Path) -> None:
    downloader = TECDownloader(config=downloader_config)
    custom_dir = tmp_path / "custom_out"
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())
    mock_driver = MagicMock()

    with (
        patch("app.states.texas.texas_downloader.webdriver.Chrome", return_value=mock_driver),
        patch("app.states.texas.texas_downloader.WebDriverWait", return_value=wait),
        patch("app.states.texas.texas_downloader.time.sleep"),
        patch.object(downloader, "_wait_for_download"),
        patch("app.states.texas.texas_downloader.zipfile.ZipFile") as mock_zip,
    ):
        custom_dir.mkdir(parents=True, exist_ok=True)
        (custom_dir / "TEC_CF_CSV.zip").write_bytes(b"placeholder")
        mock_zip.return_value.__enter__.return_value.infolist.return_value = []
        result = downloader.download(output_dir=custom_dir)

    assert result == custom_dir
