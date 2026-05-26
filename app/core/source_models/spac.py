"""SPAC linkage source model."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, Text
from sqlmodel import Field, SQLModel


class SpacLink(SQLModel, table=True):
    """Links a specific-purpose committee to a supported or opposed target."""

    __tablename__ = "spac_links"

    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    spac_filer_id: str = Field(foreign_key="unified_committees.filer_id", index=True)
    supported_filer_id: Optional[str] = Field(
        default=None,
        foreign_key="unified_committees.filer_id",
        index=True,
    )
    supported_name: Optional[str] = Field(default=None, sa_column=Column(Text))
    support_type: Optional[str] = Field(default=None, sa_column=Column(String(20)))
    position: Optional[str] = Field(default=None, sa_column=Column(String(20)))
    state_id: Optional[int] = Field(default=None, foreign_key="states.id")
    raw_data: Optional[str] = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
