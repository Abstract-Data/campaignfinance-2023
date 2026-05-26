"""Unified notice source model (TEC CVR2 / CoverSheet2Data)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

from sqlalchemy import Column, Date, Text
from sqlmodel import Field, SQLModel


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class UnifiedNotice(SQLModel, table=True):
    """Notices received by candidates and officeholders (CVR2)."""

    __tablename__ = "unified_notices"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid4()), max_length=36, index=True)
    committee_id: str | None = Field(
        default=None,
        foreign_key="unified_committees.filer_id",
        max_length=100,
        index=True,
    )
    report_ident: str | None = Field(default=None, max_length=11, index=True)
    state_id: int | None = Field(default=None, foreign_key="states.id")
    notice_date: date | None = Field(default=None, sa_column=Column(Date))
    notice_from: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, sa_column=Column(Text))
    raw_data: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)
