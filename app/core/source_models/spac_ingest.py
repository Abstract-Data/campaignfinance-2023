"""Ingest helpers for SPAC linkage records."""

from __future__ import annotations

import json

from app.core.source_models.spac import SpacLink

_POSITION_MAP = {
    "SUPPORT": "support",
    "OPPOSE": "oppose",
}


def build_spac_link(raw: dict, *, state_id: int) -> SpacLink:
    """Build a SpacLink from a raw TEC SPAC (SpacData) record."""
    support_type = _derive_support_type(raw)
    supported_filer_id = _clean_str(raw.get("candidateFilerIdent"))
    supported_name = _derive_supported_name(raw, support_type, supported_filer_id)
    spac_filer_id = _clean_str(raw.get("spacFilerIdent"))
    if spac_filer_id is None:
        msg = "SPAC record is missing spacFilerIdent"
        raise ValueError(msg)

    return SpacLink(
        spac_filer_id=spac_filer_id,
        supported_filer_id=supported_filer_id,
        supported_name=supported_name,
        support_type=support_type,
        position=_derive_position(raw.get("spacPositionCd")),
        state_id=state_id,
        raw_data=json.dumps(raw),
    )


def _clean_str(value: object | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _derive_support_type(raw: dict) -> str | None:
    if _clean_str(raw.get("candidateFilerIdent")) or _clean_str(raw.get("candidateFilerName")):
        return "candidate"
    if _clean_str(raw.get("ctaSeekOfficeDescr")) or _clean_str(raw.get("ctaSeekOfficeCd")):
        return "measure"
    return None


def _derive_supported_name(
    raw: dict,
    support_type: str | None,
    supported_filer_id: str | None,
) -> str | None:
    if support_type == "candidate":
        return _clean_str(raw.get("candidateFilerName"))
    if support_type == "measure":
        return _clean_str(raw.get("ctaSeekOfficeDescr")) or _clean_str(raw.get("ctaSeekOfficeCd"))
    if supported_filer_id is None:
        return _clean_str(raw.get("candidateFilerName")) or _clean_str(raw.get("ctaSeekOfficeDescr"))
    return _clean_str(raw.get("candidateFilerName"))


def _derive_position(position_code: object | None) -> str | None:
    cleaned = _clean_str(position_code)
    if cleaned is None:
        return None
    return _POSITION_MAP.get(cleaned.upper())
