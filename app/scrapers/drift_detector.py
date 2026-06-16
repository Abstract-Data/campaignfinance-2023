"""Structural HTML fingerprinting for campaign-finance portal scrapers."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path

from app.logger import Logger

logger = Logger(__name__)

_WHITESPACE_RE = re.compile(r"\s+")


class ScraperMarkupError(Exception):
    """Raised when live portal HTML no longer matches the saved fixture fingerprint."""


@dataclass(frozen=True)
class ScraperExpectation:
    """Known-good portal markup contract for a scraper."""

    scraper_id: str
    fixture_path: Path
    required_link_texts: tuple[str, ...] = ()


class _StructureCollector(HTMLParser):
    """Collect a normalized tag/link skeleton from HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_attrs = sorted(
            (name, value or "")
            for name, value in attrs
            if name in {"id", "class", "href", "name", "role", "type"}
        )
        attr_blob = ",".join(f"{name}={value}" for name, value in normalized_attrs)
        self._parts.append(f"<{tag}{(' ' + attr_blob) if attr_blob else ''}>")

    def handle_endtag(self, tag: str) -> None:
        self._parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        text = _WHITESPACE_RE.sub(" ", data).strip()
        if text:
            self._parts.append(f"#{text}")

    def skeleton(self) -> str:
        return "\n".join(self._parts)


def structural_fingerprint(html: str) -> str:
    """Return a stable SHA-256 hex digest of normalized page structure."""
    parser = _StructureCollector()
    parser.feed(html)
    parser.close()
    skeleton = parser.skeleton()
    return hashlib.sha256(skeleton.encode("utf-8")).hexdigest()


def _missing_link_texts(html: str, required_link_texts: tuple[str, ...]) -> list[str]:
    if not required_link_texts:
        return []

    parser = _StructureCollector()
    parser.feed(html)
    parser.close()
    page_text = "\n".join(
        part[1:] for part in parser.skeleton().splitlines() if part.startswith("#")
    )
    return [text for text in required_link_texts if text not in page_text]


def verify_markup(
    html: str,
    *,
    expectation: ScraperExpectation,
    compare_fingerprint: bool = True,
    logger_instance: Logger | None = None,
) -> None:
    """Verify live HTML against a fixture fingerprint and required navigation links."""
    log = logger_instance or logger

    if compare_fingerprint and not expectation.fixture_path.is_file():
        msg = (
            f"Missing markup fixture for scraper {expectation.scraper_id!r}: "
            f"{expectation.fixture_path}"
        )
        log.error(msg)
        raise ScraperMarkupError(msg)

    missing_links = _missing_link_texts(html, expectation.required_link_texts)

    fingerprint_mismatch = False
    if compare_fingerprint:
        fixture_html = expectation.fixture_path.read_text(encoding="utf-8")
        expected_fingerprint = structural_fingerprint(fixture_html)
        actual_fingerprint = structural_fingerprint(html)
        fingerprint_mismatch = actual_fingerprint != expected_fingerprint

    if not fingerprint_mismatch and not missing_links:
        return

    details: list[str] = []
    if fingerprint_mismatch:
        details.append("structural fingerprint mismatch vs fixture")
    if missing_links:
        details.append(f"missing required link text: {', '.join(missing_links)}")

    msg = f"Portal markup drift detected for {expectation.scraper_id}: " + "; ".join(details)
    log.error(msg)
    raise ScraperMarkupError(msg)


def load_expectation(
    scraper_id: str,
    *,
    fixture_name: str,
    required_link_texts: tuple[str, ...] = (),
    fixtures_dir: Path | None = None,
) -> ScraperExpectation:
    """Build a :class:`ScraperExpectation` from a fixture file name."""
    base = fixtures_dir or Path(__file__).resolve().parents[2] / "tests" / "scrapers" / "fixtures"
    return ScraperExpectation(
        scraper_id=scraper_id,
        fixture_path=base / fixture_name,
        required_link_texts=required_link_texts,
    )
