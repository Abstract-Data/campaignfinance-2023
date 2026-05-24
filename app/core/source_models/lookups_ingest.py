"""Ingest builders for TEC lookup records (EXCAT, CVR3)."""

from __future__ import annotations

from app.core.source_models.lookups import CommitteePurpose, ExpenditureCategory


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def build_expenditure_category(raw: dict) -> ExpenditureCategory:
    return ExpenditureCategory(
        code=str(raw["expendCategoryCodeValue"]).strip(),
        description=_optional_str(raw.get("expendCategoryCodeLabel")),
    )


def build_committee_purpose(raw: dict, *, state_id: int) -> CommitteePurpose:
    return CommitteePurpose(
        committee_id=str(raw["filerIdent"]).strip(),
        report_ident=_optional_str(raw.get("reportInfoIdent")),
        state_id=state_id,
        purpose_text=_optional_str(raw.get("subjectDescr")),
        form_type=_optional_str(raw.get("formTypeCd")),
    )
