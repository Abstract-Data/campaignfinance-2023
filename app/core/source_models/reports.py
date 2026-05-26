"""UnifiedReport — normalized cover-sheet record (CVR1) for campaign filings."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import Column, Numeric, Text
from sqlmodel import Field, SQLModel


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class UnifiedReport(SQLModel, table=True):
    """A single campaign finance report filing (TEC CVR1 / CoverSheetData).

    Every TEC transaction row carries a ``reportInfoIdent`` that identifies
    the cover-sheet report it was submitted under.  This table normalises
    those references so transactions can be linked back to their filings via
    ``unified_transactions.report_id``.

    Declared totals are nullable because ``cover_ss`` and ``cover_t`` records
    (special-session / pre-election) do not include totals.
    """

    __tablename__ = "unified_reports"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(
        default_factory=lambda: str(uuid4()),
        max_length=36,
        index=True,
        unique=True,
    )

    state_id: int | None = Field(default=None, foreign_key="states.id", index=True)

    # String FK to unified_committees.filer_id — matches the convention used
    # by unified_transactions.committee_id.
    committee_id: str | None = Field(
        default=None,
        foreign_key="unified_committees.filer_id",
        max_length=100,
        index=True,
    )

    # TEC reportInfoIdent — the report's unique identifier used for linking.
    report_ident: str = Field(max_length=20, index=True, unique=True)

    form_type: str | None = Field(default=None, max_length=30)
    filed_date: date | None = Field(default=None)
    period_start: date | None = Field(default=None)
    period_end: date | None = Field(default=None)

    # Set to True when a FINL record references this report.
    is_final: bool = Field(default=False)

    # Declared totals from the cover sheet (nullable for _ss / _t variants).
    total_contributions: Decimal | None = Field(
        default=None,
        sa_column=Column(Numeric(15, 2), nullable=True),
    )
    total_unitemized_contributions: Decimal | None = Field(
        default=None,
        sa_column=Column(Numeric(15, 2), nullable=True),
    )
    total_expenditures: Decimal | None = Field(
        default=None,
        sa_column=Column(Numeric(15, 2), nullable=True),
    )
    total_unitemized_expenditures: Decimal | None = Field(
        default=None,
        sa_column=Column(Numeric(15, 2), nullable=True),
    )
    loan_balance: Decimal | None = Field(
        default=None,
        sa_column=Column(Numeric(15, 2), nullable=True),
    )
    contributions_maintained: Decimal | None = Field(
        default=None,
        sa_column=Column(Numeric(15, 2), nullable=True),
    )
    cash_on_hand: Decimal | None = Field(
        default=None,
        sa_column=Column(Numeric(15, 2), nullable=True),
    )

    file_origin_id: str | None = Field(
        default=None,
        foreign_key="file_origins.id",
        max_length=64,
    )

    raw_data: str | None = Field(default=None, sa_column=Column(Text))

    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)
