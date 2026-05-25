"""Shared HTML fixture content for scraper tests."""

from __future__ import annotations

from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"
TEXAS_TEC_PORTAL_GOOD_HTML = (FIXTURES_DIR / "texas_tec_portal_good.html").read_text(
    encoding="utf-8"
)
