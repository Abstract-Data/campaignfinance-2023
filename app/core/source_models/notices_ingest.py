"""Ingest builders for TEC CVR2 notice records."""

from __future__ import annotations

import json
from datetime import date

from app.core.source_models.notices import UnifiedNotice

_MAPPED_KEYS = frozenset(
    {
        "filerIdent",
        "reportInfoIdent",
        "receivedDt",
        "notifierPersentTypeCd",
        "notifierNameOrganization",
        "notifierNameFirst",
        "notifierNameLast",
        "notifierNamePrefixCd",
        "notifierNameSuffixCd",
        "notifierNameShort",
        "notifierCommactPersentKindCd",
    }
)


def build_notice(raw: dict, *, state_id: int) -> UnifiedNotice:
    """Build a UnifiedNotice from a raw TEC CVR2 (CoverSheet2Data) record."""
    raw_data = {key: value for key, value in raw.items() if key not in _MAPPED_KEYS}
    return UnifiedNotice(
        committee_id=_optional_str(raw.get("filerIdent")),
        report_ident=_optional_str(raw.get("reportInfoIdent")),
        state_id=state_id,
        notice_date=_parse_date(raw.get("receivedDt")),
        notice_from=_derive_notice_from(raw),
        description=_optional_str(raw.get("notifierCommactPersentKindCd")),
        raw_data=json.dumps(raw_data) if raw_data else None,
    )


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_date(value: object) -> date | None:
    text = _optional_str(value)
    if text is None or len(text) != 8 or not text.isdigit():
        return None
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _derive_notice_from(raw: dict) -> str | None:
    persent_type = _optional_str(raw.get("notifierPersentTypeCd"))
    if persent_type == "ENTITY":
        return _optional_str(raw.get("notifierNameOrganization"))
    if persent_type == "INDIVIDUAL":
        parts = [
            _optional_str(raw.get("notifierNamePrefixCd")),
            _optional_str(raw.get("notifierNameFirst")),
            _optional_str(raw.get("notifierNameLast")),
            _optional_str(raw.get("notifierNameSuffixCd")),
        ]
        name = " ".join(part for part in parts if part)
        return name or _optional_str(raw.get("notifierNameShort"))
    return _optional_str(raw.get("notifierNameOrganization")) or _optional_str(
        raw.get("notifierNameLast")
    )
