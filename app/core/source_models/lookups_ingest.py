"""Ingest builders for TEC lookup records (EXCAT, CVR3)."""

from __future__ import annotations

from app.core.source_models.lookups import CommitteePurpose, ExpenditureCategory


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def build_expenditure_category(raw: dict, *, state_id: int | None = None) -> ExpenditureCategory:
    return ExpenditureCategory(
        code=str(raw["expendCategoryCodeValue"]).strip(),
        description=_optional_str(raw.get("expendCategoryCodeLabel")),
    )


def build_committee_purpose(raw: dict, *, state_id: int | None = None) -> CommitteePurpose:
    subject_descr = _optional_str(raw.get("subjectDescr"))
    return CommitteePurpose(
        committee_id=_optional_str(raw.get("filerIdent")),
        report_ident=_optional_str(raw.get("reportInfoIdent")),
        state_id=state_id,
        form_type=_optional_str(raw.get("formTypeCd")),
        activity_id=_optional_str(raw.get("committeeActivityId")),
        subject_category=_optional_str(raw.get("subjectCategoryCd")),
        subject_position=_optional_str(raw.get("subjectPositionCd")),
        subject_descr=subject_descr,
        ballot_number=_optional_str(raw.get("subjectBallotNumber")),
        election_date=_optional_str(raw.get("subjectElectionDt")),
        activity_hold_office_cd=_optional_str(raw.get("activityHoldOfficeCd")),
        activity_hold_office_district=_optional_str(raw.get("activityHoldOfficeDistrict")),
        activity_hold_office_place=_optional_str(raw.get("activityHoldOfficePlace")),
        activity_hold_office_descr=_optional_str(raw.get("activityHoldOfficeDescr")),
        activity_hold_office_county_cd=_optional_str(raw.get("activityHoldOfficeCountyCd")),
        activity_hold_office_county_descr=_optional_str(raw.get("activityHoldOfficeCountyDescr")),
        activity_seek_office_cd=_optional_str(raw.get("activitySeekOfficeCd")),
        activity_seek_office_district=_optional_str(raw.get("activitySeekOfficeDistrict")),
        activity_seek_office_place=_optional_str(raw.get("activitySeekOfficePlace")),
        activity_seek_office_descr=_optional_str(raw.get("activitySeekOfficeDescr")),
        activity_seek_office_county_cd=_optional_str(raw.get("activitySeekOfficeCountyCd")),
        activity_seek_office_county_descr=_optional_str(raw.get("activitySeekOfficeCountyDescr")),
        activity_name=_optional_str(raw.get("commActivityName")),
        purpose_text=subject_descr,  # backward-compat alias
    )
