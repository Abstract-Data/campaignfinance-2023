"""Tests for Texas TEC downloader refactor (task A)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.states.texas.texas_downloader import DownloadError, TECDownloader, _is_safe_zip_member

TEXAS_TEC_PORTAL_GOOD_HTML = (
    Path(__file__).resolve().parents[1] / "scrapers" / "fixtures" / "texas_tec_portal_good.html"
).read_text(encoding="utf-8")


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
        driver = MagicMock()
        driver.page_source = TEXAS_TEC_PORTAL_GOOD_HTML
        return driver

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


def test_is_safe_zip_member_rejects_path_traversal(tmp_path: Path) -> None:
    destination = tmp_path / "dest"
    destination.mkdir()

    assert _is_safe_zip_member("../etc/passwd", destination) is False
    assert _is_safe_zip_member("..\\etc\\passwd", destination) is False
    assert _is_safe_zip_member("/etc/passwd", destination) is False
    assert _is_safe_zip_member("subdir/../../outside.txt", destination) is False


def test_is_safe_zip_member_allows_safe_relative_paths(tmp_path: Path) -> None:
    destination = tmp_path / "dest"
    destination.mkdir()

    assert _is_safe_zip_member("contribs.csv", destination) is True
    assert _is_safe_zip_member("data/contribs.csv", destination) is True


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


def test_wait_loop_waits_for_stable_zip(downloader_config: MagicMock) -> None:
    downloader = TECDownloader(config=downloader_config)
    tmp_path = downloader_config.TEMP_FOLDER
    tmp_path.mkdir(parents=True, exist_ok=True)
    zip_path = tmp_path / "TEC_CF_CSV.zip"
    zip_path.write_bytes(b"partial")

    crdownload_seen = {"count": 0}
    stable_checks: list[bool] = []

    def fake_glob(self: Path, pattern: str) -> list[Path]:
        if self != tmp_path:
            return list(Path.glob(self, pattern))
        if pattern == "*.crdownload":
            crdownload_seen["count"] += 1
            if crdownload_seen["count"] <= 1:
                return [tmp_path / "file.crdownload"]
            return []
        if pattern == "*.zip":
            return [zip_path]
        return []

    def fake_stable(_path: Path, **_kwargs: object) -> bool:
        stable_checks.append(True)
        return len(stable_checks) >= 2

    monotonic_values = iter([float(i) for i in range(20)])

    with (
        patch("app.states.texas.texas_downloader._DOWNLOAD_TIMEOUT_SECONDS", 30),
        patch(
            "app.states.texas.texas_downloader.time.monotonic",
            side_effect=lambda: next(monotonic_values),
        ),
        patch("app.states.texas.texas_downloader.time.sleep"),
        patch.object(Path, "glob", fake_glob),
        patch.object(downloader, "_is_file_size_stable", side_effect=fake_stable),
    ):
        downloader._wait_for_download(tmp_path)

    assert len(stable_checks) >= 2


def test_download_overwrite_removes_existing_zip(downloader_config: MagicMock) -> None:
    downloader = TECDownloader(config=downloader_config)
    tmp_path = downloader_config.TEMP_FOLDER
    tmp_path.mkdir(parents=True, exist_ok=True)
    stale_zip = tmp_path / "stale.zip"
    stale_zip.write_bytes(b"old")
    mock_driver = MagicMock()
    mock_driver.page_source = TEXAS_TEC_PORTAL_GOOD_HTML
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())

    def _seed_download_zip(_self: TECDownloader, folder: Path) -> None:
        (folder / "TEC_CF_CSV.zip").write_bytes(b"fresh")

    with (
        patch("app.states.texas.texas_downloader.webdriver.Chrome", return_value=mock_driver),
        patch("app.states.texas.texas_downloader.WebDriverWait", return_value=wait),
        patch("app.states.texas.texas_downloader.time.sleep"),
        patch.object(TECDownloader, "_wait_for_download", _seed_download_zip),
        patch("app.states.texas.texas_downloader.zipfile.ZipFile") as mock_zip,
    ):
        mock_zip.return_value.__enter__.return_value.infolist.return_value = []
        downloader.download(overwrite=True)

    assert not stale_zip.exists()


def test_download_returns_temp_folder_path(downloader_config: MagicMock) -> None:
    downloader = TECDownloader(config=downloader_config)
    tmp_path = downloader_config.TEMP_FOLDER
    tmp_path.mkdir(parents=True, exist_ok=True)
    mock_driver = MagicMock()
    mock_driver.page_source = TEXAS_TEC_PORTAL_GOOD_HTML
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())

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
    mock_driver = MagicMock()
    mock_driver.page_source = TEXAS_TEC_PORTAL_GOOD_HTML
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())

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
    mock_driver = MagicMock()
    mock_driver.page_source = TEXAS_TEC_PORTAL_GOOD_HTML
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())

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
