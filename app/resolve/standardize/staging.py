"""Stage-1 resolution staging models."""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import Column, Date, Integer, String
from sqlmodel import Field, SQLModel

from app.resolve.models.canonical import map_unified_to_canonical_entity_type
from app.resolve.models.resolution import SOURCE_ID_MAX_LENGTH


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def coerce_staging_entity_type(unified_entity_type: str) -> str:
    """Map a unified ``entity_type`` string to a canonical staging value.

    Used by the stage-1 path before writing ``resolution_input.entity_type``.
    Raises :class:`UnmappedEntityTypeError` for types that belong on another
    pass (e.g. ``campaign``).
    """
    return map_unified_to_canonical_entity_type(unified_entity_type).value


class ResolutionInput(SQLModel, table=True):
    """Prepared matching features for one source record."""

    __tablename__ = "resolution_input"

    id: int | None = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    source_type: str = Field(sa_column=Column(String(64), nullable=False, index=True))
    source_id: str = Field(
        sa_column=Column(String(SOURCE_ID_MAX_LENGTH), nullable=False, index=True)
    )
    entity_type: str = Field(sa_column=Column(String(64), nullable=False, index=True))

    # Deterministic 1:1 link carried from unified_entities so the fast path can merge a
    # unified_entity with the source unified_person / unified_committee it represents
    # (they are the same real-world entity). Null for unified_person / unified_committee
    # source rows themselves.
    linked_person_id: int | None = Field(default=None, sa_column=Column(Integer))
    linked_committee_id: str | None = Field(default=None, sa_column=Column(String(128)))

    first_name: str | None = Field(default=None, sa_column=Column(String(200)))
    middle_name: str | None = Field(default=None, sa_column=Column(String(200)))
    last_name: str | None = Field(default=None, sa_column=Column(String(200)))
    suffix: str | None = Field(default=None, sa_column=Column(String(50)))
    is_organization: bool = Field(default=False)

    line_1: str | None = Field(default=None, sa_column=Column(String(500)))
    line_2: str | None = Field(default=None, sa_column=Column(String(500)))
    city: str | None = Field(default=None, sa_column=Column(String(200)))
    state: str | None = Field(default=None, sa_column=Column(String(50)))
    zip5: str | None = Field(default=None, sa_column=Column(String(5)))
    zip4: str | None = Field(default=None, sa_column=Column(String(4)))
    parse_status: str = Field(default="unparsed", sa_column=Column(String(20), nullable=False))

    normalized_org: str | None = Field(default=None, sa_column=Column(String(500)))
    first_name_phonetic: str | None = Field(default=None, sa_column=Column(String(50)))
    last_name_phonetic: str | None = Field(default=None, sa_column=Column(String(50)))
    org_name_phonetic: str | None = Field(default=None, sa_column=Column(String(50)))

    raw_name: str | None = Field(default=None, sa_column=Column(String(500)))
    raw_address: str | None = Field(default=None, sa_column=Column(String(1000)))

    # Real activity window for this source record, derived from the transaction
    # dates that reference it (NOT the ETL load time).  Survivorship uses these
    # for canonical_entity / canonical_name_history first/last-seen so name
    # windows reflect actual filing periods.
    first_activity_date: date | None = Field(default=None, sa_column=Column(Date))
    last_activity_date: date | None = Field(default=None, sa_column=Column(Date))

    created_at: datetime = Field(default_factory=_utc_now)
