"""Address standardization helpers for resolution feature prep."""

from __future__ import annotations

from dataclasses import dataclass

import usaddress
from scourgify import normalize_address_record
from scourgify.exceptions import UnParseableAddressError


@dataclass(frozen=True, slots=True)
class StandardizedAddress:
    line_1: str | None = None
    line_2: str | None = None
    city: str | None = None
    state: str | None = None
    zip5: str | None = None
    zip4: str | None = None
    parse_status: str = "unparsed"


def _clean_token(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _split_zip(postal_code: str | None) -> tuple[str | None, str | None]:
    if not postal_code:
        return None, None
    cleaned = postal_code.strip()
    if "-" in cleaned:
        zip5, zip4 = cleaned.split("-", maxsplit=1)
        return zip5[:5], zip4[:4]
    return cleaned[:5], None


def _stringify_address(raw: str | dict[str, str | None]) -> str:
    if isinstance(raw, str):
        return raw.strip()

    direct_line_1 = raw.get("line_1")
    if direct_line_1 is not None:
        parts = [
            _clean_token(str(raw.get("line_1", ""))),
            _clean_token(str(raw.get("line_2", ""))),
            _clean_token(str(raw.get("city", ""))),
            _clean_token(str(raw.get("state", ""))),
            _clean_token(str(raw.get("zip5", ""))),
        ]
        return ", ".join(part for part in parts if part)

    street_1 = raw.get("street_1") or raw.get("address_line_1")
    street_2 = raw.get("street_2") or raw.get("address_line_2")
    zip_code = raw.get("zip_code") or raw.get("postal_code")
    parts = [
        _clean_token(str(street_1 or "")),
        _clean_token(str(street_2 or "")),
        _clean_token(str(raw.get("city") or "")),
        _clean_token(str(raw.get("state") or "")),
        _clean_token(str(zip_code or "")),
    ]
    return ", ".join(part for part in parts if part)


def standardize_address(raw: str | dict[str, str | None] | StandardizedAddress) -> StandardizedAddress:
    """Normalize and parse a raw address into comparable parts."""
    if isinstance(raw, StandardizedAddress):
        return raw

    address_text = _stringify_address(raw)
    if not address_text:
        return StandardizedAddress(parse_status="unparsed")

    try:
        normalized = normalize_address_record(address_text)
    except (UnParseableAddressError, ValueError):
        normalized = None

    if normalized:
        zip5, zip4 = _split_zip(normalized.get("postal_code"))
        return StandardizedAddress(
            line_1=_clean_token(normalized.get("address_line_1")),
            line_2=_clean_token(normalized.get("address_line_2")),
            city=_clean_token(normalized.get("city")),
            state=_clean_token(normalized.get("state")),
            zip5=zip5,
            zip4=zip4,
            parse_status="parsed",
        )

    try:
        tagged, _ = usaddress.tag(address_text)
    except usaddress.RepeatedLabelError:
        return StandardizedAddress(parse_status="unparsed")

    line_1_parts = [
        tagged.get("AddressNumber"),
        tagged.get("StreetNamePreDirectional"),
        tagged.get("StreetName"),
        tagged.get("StreetNamePostType"),
        tagged.get("StreetNamePostDirectional"),
    ]
    line_2_parts = [
        tagged.get("OccupancyType"),
        tagged.get("OccupancyIdentifier"),
    ]
    zip5, zip4 = _split_zip(tagged.get("ZipCode"))
    line_1 = _clean_token(" ".join(part for part in line_1_parts if part))
    line_2 = _clean_token(" ".join(part for part in line_2_parts if part))

    if line_1 is None and tagged.get("PlaceName") is None:
        return StandardizedAddress(parse_status="unparsed")

    return StandardizedAddress(
        line_1=line_1,
        line_2=line_2,
        city=_clean_token(tagged.get("PlaceName")),
        state=_clean_token(tagged.get("StateName")),
        zip5=zip5,
        zip4=zip4,
        parse_status="partial",
    )
