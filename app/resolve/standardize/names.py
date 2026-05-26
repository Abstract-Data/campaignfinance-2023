"""Name standardization helpers for resolution feature prep."""

from __future__ import annotations

from dataclasses import dataclass

import probablepeople


@dataclass(frozen=True, slots=True)
class StandardizedName:
    first: str | None = None
    middle: str | None = None
    last: str | None = None
    suffix: str | None = None
    is_organization: bool = False


_ORG_TAGS = {
    "CorporationName",
    "CorporationLegalType",
    "CompanyName",
    "ShortForm",
}


def _clean_token(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def standardize_name(raw: str | StandardizedName) -> StandardizedName:
    """Normalize person/org names into canonical parts."""
    if isinstance(raw, StandardizedName):
        return raw

    cleaned = raw.strip()
    if not cleaned:
        return StandardizedName()

    try:
        tagged, classification = probablepeople.tag(cleaned)
    except probablepeople.RepeatedLabelError:
        return StandardizedName(last=cleaned)

    is_organization = classification in {"Corporation", "Household", "Unknown"}
    if any(tag in tagged for tag in _ORG_TAGS):
        is_organization = True

    first = _clean_token(tagged.get("GivenName"))
    middle = _clean_token(tagged.get("MiddleName") or tagged.get("MiddleInitial"))
    last = _clean_token(tagged.get("Surname") or tagged.get("CorporationName") or cleaned)
    suffix = _clean_token(
        tagged.get("SuffixGenerational")
        or tagged.get("SuffixOther")
        or tagged.get("CorporationLegalType")
    )

    return StandardizedName(
        first=first,
        middle=middle,
        last=last,
        suffix=suffix,
        is_organization=is_organization,
    )
