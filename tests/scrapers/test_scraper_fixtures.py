"""Fixture-based scraper markup drift tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.scrapers.drift_detector import (
    ScraperExpectation,
    ScraperMarkupError,
    structural_fingerprint,
    verify_markup,
)
from app.scrapers.expectations import TEXAS_TEC_PORTAL

_FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def texas_good_html() -> str:
    return (_FIXTURES / "texas_tec_portal_good.html").read_text(encoding="utf-8")


@pytest.fixture
def texas_drifted_html() -> str:
    return (_FIXTURES / "texas_tec_portal_drifted.html").read_text(encoding="utf-8")


def test_structural_fingerprint_is_stable(texas_good_html: str) -> None:
    first = structural_fingerprint(texas_good_html)
    second = structural_fingerprint(texas_good_html)
    assert first == second
    assert len(first) == 64


def test_verify_markup_passes_on_good_fixture(texas_good_html: str) -> None:
    verify_markup(texas_good_html, expectation=TEXAS_TEC_PORTAL)


def test_verify_markup_raises_on_drifted_fixture(texas_drifted_html: str) -> None:
    with pytest.raises(ScraperMarkupError, match="Portal markup drift detected"):
        verify_markup(texas_drifted_html, expectation=TEXAS_TEC_PORTAL)


def test_verify_markup_raises_when_required_links_missing(texas_good_html: str) -> None:
    broken = texas_good_html.replace("Campaign Finance CSV Database", "CSV Export")
    expectation = ScraperExpectation(
        scraper_id="texas-tec-portal",
        fixture_path=_FIXTURES / "texas_tec_portal_good.html",
        required_link_texts=("Campaign Finance CSV Database",),
    )
    with pytest.raises(ScraperMarkupError, match="missing required link text"):
        verify_markup(broken, expectation=expectation)


def test_verify_markup_raises_when_fixture_file_missing(tmp_path: Path) -> None:
    expectation = ScraperExpectation(
        scraper_id="missing-fixture",
        fixture_path=tmp_path / "does-not-exist.html",
    )
    with pytest.raises(ScraperMarkupError, match="Missing markup fixture"):
        verify_markup("<html></html>", expectation=expectation)


def test_texas_downloader_verifies_markup_before_navigation(
    texas_good_html: str,
    tmp_path: Path,
) -> None:
    from app.states.texas.texas_downloader import TECDownloader

    config = MagicMock()
    config.TEMP_FOLDER = tmp_path / "tec"
    downloader = TECDownloader(config=config)

    mock_driver = MagicMock()
    mock_driver.page_source = texas_good_html
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())

    verified: list[str] = []

    def _capture_verify(_self: TECDownloader, html: str, *, step: str) -> None:
        verified.append(step)
        verify_markup(html, expectation=TEXAS_TEC_PORTAL)

    with (
        patch("app.states.texas.texas_downloader.webdriver.Chrome", return_value=mock_driver),
        patch("app.states.texas.texas_downloader.WebDriverWait", return_value=wait),
        patch("app.states.texas.texas_downloader.time.sleep"),
        patch.object(TECDownloader, "_wait_for_download"),
        patch.object(TECDownloader, "_verify_portal_markup", _capture_verify),
        patch("app.states.texas.texas_downloader.zipfile.ZipFile") as mock_zip,
    ):
        config.TEMP_FOLDER.mkdir(parents=True, exist_ok=True)
        (config.TEMP_FOLDER / "TEC_CF_CSV.zip").write_bytes(b"zip")
        mock_zip.return_value.__enter__.return_value.infolist.return_value = []
        downloader.download()

    assert verified == ["landing"]


def test_texas_downloader_raises_on_drifted_markup(
    texas_drifted_html: str,
    tmp_path: Path,
) -> None:
    from app.states.texas.texas_downloader import DownloadError, TECDownloader

    config = MagicMock()
    config.TEMP_FOLDER = tmp_path / "tec-drift"
    downloader = TECDownloader(config=config)

    mock_driver = MagicMock()
    mock_driver.page_source = texas_drifted_html
    wait = MagicMock()
    wait.until.side_effect = lambda _: MagicMock(click=MagicMock())

    with (
        patch("app.states.texas.texas_downloader.webdriver.Chrome", return_value=mock_driver),
        patch("app.states.texas.texas_downloader.WebDriverWait", return_value=wait),
        patch("app.states.texas.texas_downloader.time.sleep"),
    ):
        with pytest.raises(DownloadError, match="markup drift"):
            downloader.download()
