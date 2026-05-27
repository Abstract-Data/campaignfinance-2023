"""Phonetic feature helpers."""

from __future__ import annotations

import jellyfish

# Matches ``resolution_input.*_phonetic`` column width in staging.py.
_PHONETIC_MAX_LENGTH = 50


def phonetic_code(token: str) -> str:
    """Return a metaphone code for a token."""
    cleaned = token.strip()
    if not cleaned:
        return ""
    return jellyfish.metaphone(cleaned)[:_PHONETIC_MAX_LENGTH]
