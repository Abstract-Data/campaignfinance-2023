"""Stage-1 resolution staging models."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, String
from sqlmodel import Field, SQLModel


class ResolutionInput(SQLModel, table=True):
    """Prepared matching features for one source record."""

    __tablename__ = "resolution_input"

    id: int | None = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    source_type: str = Field(sa_column=Column(String(64), nullable=False, index=True))
    source_id: str = Field(sa_column=Column(String(128), nullable=False, index=True))
    entity_type: str = Field(sa_column=Column(String(64), nullable=False, index=True))

    first_name: str | None = Field(default=None, sa_column=Column(String(200)))
    middle_name: str | None = Field(default=None, sa_column=Column(String(200)))
    last_name: str | None = Field(default=None, sa_column=Column(String(200)))
    suffix: str | None = Field(default=None, sa_column=Column(String(50)))
    is_organization: bool = Field(default=False)

    line_1: str | None = Field(default=None, sa_column=Column(String(500)))
    line_2: str | None = Field(default=None, sa_column=Column(String(500)))
    city: str | None = Field(default=None, sa_column=Column(String(200)))
    state: str | None = Field(default=None, sa_column=Column(String(50)))
    zip5: str | None = Field(default=None, sa_column=Column(String(10)))
    zip4: str | None = Field(default=None, sa_column=Column(String(10)))
    parse_status: str = Field(default="unparsed", sa_column=Column(String(20), nullable=False))

    normalized_org: str | None = Field(default=None, sa_column=Column(String(500)))
    first_name_phonetic: str | None = Field(default=None, sa_column=Column(String(50)))
    last_name_phonetic: str | None = Field(default=None, sa_column=Column(String(50)))
    org_name_phonetic: str | None = Field(default=None, sa_column=Column(String(50)))

    raw_name: str | None = Field(default=None, sa_column=Column(String(500)))
    raw_address: str | None = Field(default=None, sa_column=Column(String(1000)))

    created_at: datetime = Field(default_factory=datetime.utcnow)
