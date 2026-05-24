"""UnifiedPledge detail table for PLDG transactions."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import Column, ForeignKey, Integer, Numeric, Text
from sqlmodel import Field, SQLModel


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class UnifiedPledge(SQLModel, table=True):
    """Normalized pledge detail extracted from PLDG transactions."""

    __tablename__ = "unified_pledges"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        unique=True,
        index=True,
    )
    transaction_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("unified_transactions.id"),
            unique=True,
        )
    )
    pledgor_entity_id: int | None = Field(default=None, foreign_key="unified_entities.id")
    recipient_entity_id: int | None = Field(default=None, foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")
    amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
    pledge_date: date | None = Field(default=None, index=True)
    is_fulfilled: bool = Field(default=False)
    description: str | None = Field(default=None, sa_column=Column(Text))
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)
