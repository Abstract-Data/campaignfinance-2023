"""Lookup tables for TEC reference data (EXCAT, CVR3)."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlmodel import Field, SQLModel


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ExpenditureCategory(SQLModel, table=True):
    __tablename__ = "expenditure_categories"

    code: str = Field(primary_key=True, max_length=30)
    description: str | None = Field(default=None, max_length=100)
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)


class CommitteePurpose(SQLModel, table=True):
    """TEC CVR3 — committee activity declaration (purpose file).

    Each row declares one candidate or measure a committee supported/opposed
    during a given report period.  A single report typically has many rows.

    Column mapping from TEC purpose_*.csv:
        committeeActivityId       → activity_id
        subjectCategoryCd         → subject_category  (CANDIDATE | MEASURE | OTHER)
        subjectPositionCd         → subject_position  (SUPPORT | OPPOSE | NEUTRAL)
        subjectDescr              → subject_descr
        subjectBallotNumber       → ballot_number
        subjectElectionDt         → election_date
        activityHoldOffice*       → activity_hold_office_* fields
        activitySeekOffice*       → activity_seek_office_* fields
        commActivityName          → activity_name  (candidate or measure name)
    """

    __tablename__ = "committee_purposes"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid4()), max_length=36, index=True)
    committee_id: str = Field(
        foreign_key="unified_committees.filer_id",
        max_length=100,
        index=True,
    )
    report_ident: str | None = Field(default=None, max_length=20, index=True)
    state_id: int | None = Field(default=None, foreign_key="states.id")
    form_type: str | None = Field(default=None, max_length=20)

    # TEC committeeActivityId — unique identifier within a filer/report
    activity_id: str | None = Field(default=None, max_length=20, index=True)

    # Subject classification
    subject_category: str | None = Field(
        default=None, max_length=30, description="CANDIDATE, MEASURE, or OTHER"
    )
    subject_position: str | None = Field(
        default=None, max_length=30, description="SUPPORT, OPPOSE, or NEUTRAL"
    )
    subject_descr: str | None = Field(default=None, max_length=500)
    ballot_number: str | None = Field(default=None, max_length=50)
    election_date: str | None = Field(default=None, max_length=20)

    # Office the activity relates to — current holder
    activity_hold_office_cd: str | None = Field(default=None, max_length=30)
    activity_hold_office_district: str | None = Field(default=None, max_length=50)
    activity_hold_office_place: str | None = Field(default=None, max_length=50)
    activity_hold_office_descr: str | None = Field(default=None, max_length=100)
    activity_hold_office_county_cd: str | None = Field(default=None, max_length=10)
    activity_hold_office_county_descr: str | None = Field(default=None, max_length=50)

    # Office being sought by the candidate
    activity_seek_office_cd: str | None = Field(default=None, max_length=30)
    activity_seek_office_district: str | None = Field(default=None, max_length=50)
    activity_seek_office_place: str | None = Field(default=None, max_length=50)
    activity_seek_office_descr: str | None = Field(default=None, max_length=100)
    activity_seek_office_county_cd: str | None = Field(default=None, max_length=10)
    activity_seek_office_county_descr: str | None = Field(default=None, max_length=50)

    # The candidate or measure name
    activity_name: str | None = Field(default=None, max_length=200)

    # Kept for backward compat; same content as subject_descr
    purpose_text: str | None = Field(default=None, max_length=500)

    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)
