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
    __tablename__ = "committee_purposes"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid4()), max_length=36, index=True)
    committee_id: str = Field(
        foreign_key="unified_committees.filer_id",
        max_length=100,
        index=True,
    )
    report_ident: str | None = Field(default=None, max_length=11, index=True)
    state_id: int | None = Field(default=None, foreign_key="states.id")
    purpose_text: str | None = Field(default=None, max_length=500)
    form_type: str | None = Field(default=None, max_length=20)
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)
