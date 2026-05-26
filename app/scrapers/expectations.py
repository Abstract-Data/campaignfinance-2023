"""Registered scraper markup expectations."""

from __future__ import annotations

from pathlib import Path

from app.scrapers.drift_detector import ScraperExpectation, load_expectation

_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "tests" / "scrapers" / "fixtures"

TEXAS_TEC_PORTAL = load_expectation(
    "texas-tec-portal",
    fixture_name="texas_tec_portal_good.html",
    required_link_texts=(
        "Search",
        "Campaign Finance Reports",
        "Database of Campaign Finance Reports",
        "Campaign Finance CSV Database",
    ),
    fixtures_dir=_FIXTURES_DIR,
)

__all__ = ["TEXAS_TEC_PORTAL", "ScraperExpectation"]
