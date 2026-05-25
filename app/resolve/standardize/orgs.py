"""Organization name normalization helpers."""

from __future__ import annotations

import re

_PUNCTUATION_RE = re.compile(r"[^\w\s]")
_AND_RE = re.compile(r"\b(and|&)\b", flags=re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")
_LEGAL_SUFFIXES_RE = re.compile(
    r"\b("
    r"llc|l\.l\.c|inc|incorporated|corp|corporation|co|company|"
    r"lp|l\.p|llp|l\.l\.p|pllc|p\.l\.l\.c"
    r")\b",
    flags=re.IGNORECASE,
)


def normalize_org_name(raw: str) -> str:
    """Lowercase and strip legal suffix/punctuation noise from org names."""
    cleaned = raw.strip().lower()
    if not cleaned:
        return ""

    cleaned = _AND_RE.sub(" and ", cleaned)
    cleaned = _LEGAL_SUFFIXES_RE.sub(" ", cleaned)
    cleaned = _PUNCTUATION_RE.sub(" ", cleaned)
    cleaned = _MULTISPACE_RE.sub(" ", cleaned).strip()
    return cleaned
