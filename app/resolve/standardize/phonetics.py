"""Phonetic feature helpers."""

from __future__ import annotations

import jellyfish


def phonetic_code(token: str) -> str:
    """Return a metaphone code for a token."""
    cleaned = token.strip()
    if not cleaned:
        return ""
    return jellyfish.metaphone(cleaned)
