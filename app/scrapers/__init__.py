"""Scraper utilities — markup drift detection and portal expectations."""

from app.scrapers.drift_detector import (
    ScraperExpectation,
    ScraperMarkupError,
    structural_fingerprint,
    verify_markup,
)

__all__ = [
    "ScraperExpectation",
    "ScraperMarkupError",
    "structural_fingerprint",
    "verify_markup",
]
