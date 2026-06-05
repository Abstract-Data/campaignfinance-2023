"""Canonical-layer SQLModel schema.

Four tables — one row per real-world thing — that the resolution pipeline
populates.  Schema only; no pipeline logic.

Spec reference: docs/superpowers/specs/2026-05-23-data-resolution-pipeline-design.md
  § "Schema design → Canonical layer" and "Address-as-shared-hub model".
"""

from __future__ import annotations

import uuid as _uuid_mod
from datetime import date, datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import VARCHAR, Column, UniqueConstraint
from sqlalchemy.dialects.postgresql import ENUM as PG_ENUM
from sqlalchemy.types import TypeDecorator
from sqlmodel import Field, SQLModel

if TYPE_CHECKING:
    from app.core.enums import EntityType as UnifiedEntityType


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class EntityType(str, Enum):
    """Canonical entity types stored on ``canonical_entity``.

    The unified source layer (``app.core.unified_sqlmodels.EntityType``) adds
    counting values — ``person``, ``organization``, ``committee``, ``campaign``,
    ``vendor``, and ``other``.  Only the first three are canonical types; see
    :func:`map_unified_to_canonical_entity_type` for the full mapping.

    Python values stay lowercase; Postgres ``entitytype`` labels are uppercase
    (``PERSON``, ``ORGANIZATION``, ``COMMITTEE``) — see
    :class:`CanonicalEntityTypeType`.
    """

    person = "person"
    organization = "organization"
    committee = "committee"


class CanonicalEntityTypeType(TypeDecorator):
    """Bind :class:`EntityType` to the shared Postgres ``entitytype`` enum."""

    impl = VARCHAR(12)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(
                PG_ENUM(
                    "PERSON",
                    "ORGANIZATION",
                    "COMMITTEE",
                    name="entitytype",
                    create_type=False,
                )
            )
        return dialect.type_descriptor(VARCHAR(12))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, EntityType):
            raw = value.value
        else:
            raw = str(value)
        return raw.upper()

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return EntityType(str(value).lower())


class UnmappedEntityTypeError(ValueError):
    """Raised when a unified ``EntityType`` cannot map to a canonical type."""


# Unified EntityType.value → canonical EntityType.
# Per spec: vendors are organizations in a payee role, not a separate type.
# Campaign rows belong on ``canonical_campaign`` (campaign pass), not here.
UNIFIED_TO_CANONICAL_ENTITY_TYPE: dict[str, EntityType] = {
    "person": EntityType.person,
    "organization": EntityType.organization,
    "committee": EntityType.committee,
    "vendor": EntityType.organization,
    "other": EntityType.organization,
}


def map_unified_to_canonical_entity_type(
    unified_type: str | UnifiedEntityType,
) -> EntityType:
    """Map a unified-layer entity type to a canonical ``EntityType``.

    ``campaign`` is intentionally unmapped — those records use the campaign
    resolution pass and ``canonical_campaign``, not ``canonical_entity``.
    """
    key = unified_type.value if hasattr(unified_type, "value") else str(unified_type)
    key = key.lower()
    if key == "campaign":
        raise UnmappedEntityTypeError(
            "unified EntityType 'campaign' is not mapped to canonical_entity; "
            "use the campaign resolution pass instead"
        )
    try:
        return UNIFIED_TO_CANONICAL_ENTITY_TYPE[key]
    except KeyError as exc:
        raise UnmappedEntityTypeError(
            f"unified EntityType {key!r} has no canonical mapping"
        ) from exc


class ParseStatus(str, Enum):
    parsed = "parsed"
    partial = "partial"
    unparsed = "unparsed"


class NameHistorySubjectType(str, Enum):
    entity = "entity"
    campaign = "campaign"


# ---------------------------------------------------------------------------
# CanonicalAddress
# Must be defined before CanonicalEntity (which holds the FK).
# ---------------------------------------------------------------------------


class CanonicalAddress(SQLModel, table=True):
    """One row = physical location."""

    __tablename__ = "canonical_address"

    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: str = Field(
        default_factory=lambda: str(_uuid_mod.uuid4()),
        unique=True,
        index=True,
        max_length=36,
    )

    standardized_line_1: Optional[str] = Field(default=None, max_length=500)
    standardized_line_2: Optional[str] = Field(default=None, max_length=500)
    city: Optional[str] = Field(default=None, max_length=200)
    state: Optional[str] = Field(default=None, max_length=2)
    zip5: Optional[str] = Field(default=None, max_length=5)
    zip4: Optional[str] = Field(default=None, max_length=4)

    parse_status: ParseStatus = Field(default=ParseStatus.unparsed, max_length=10)

    # Derived count for display / query only; Splink never reads this.
    frequency: int = Field(default=0)

    # Nullable int — no DB-level FK; match_run is defined in task-1b.
    last_run_id: Optional[int] = Field(default=None, index=True)

    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)


# ---------------------------------------------------------------------------
# CanonicalEntity
# ---------------------------------------------------------------------------


class CanonicalEntity(SQLModel, table=True):
    """One row per resolved person, organization, or committee."""

    __tablename__ = "canonical_entity"

    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: str = Field(
        default_factory=lambda: str(_uuid_mod.uuid4()),
        unique=True,
        index=True,
        max_length=36,
    )

    entity_type: EntityType = Field(
        sa_column=Column(CanonicalEntityTypeType(), nullable=False),
    )
    canonical_name: str = Field(max_length=500)
    normalized_name: str = Field(max_length=500, index=True)

    # Many entities → one address; nullable, no unique constraint.
    canonical_address_id: Optional[int] = Field(
        default=None,
        foreign_key="canonical_address.id",
        index=True,
    )

    state_code: str = Field(max_length=2)

    # Self-FK reserved for future cross-state linking; unused in Phase 1.
    master_entity_id: Optional[int] = Field(
        default=None,
        foreign_key="canonical_entity.id",
        index=True,
    )

    first_seen_date: Optional[date] = Field(default=None)
    last_seen_date: Optional[date] = Field(default=None)
    source_record_count: int = Field(default=0)

    # JSON string mapping field name → {source_type, source_id} of the record
    # whose value was selected during survivorship.  Added in Phase 2 (task-2d);
    # the canonical tables are rebuilt each run so the column simply appears on
    # the next freshly-created staging table without a migration.
    provenance_json: Optional[str] = Field(default=None)

    last_run_id: Optional[int] = Field(default=None, index=True)

    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)


# ---------------------------------------------------------------------------
# CanonicalCampaign
# ---------------------------------------------------------------------------


class CanonicalCampaign(SQLModel, table=True):
    """One row per campaign.

    Identity tuple: (committee_entity_id, office_normalized, election_cycle).
    """

    __tablename__ = "canonical_campaign"
    __table_args__ = (
        UniqueConstraint(
            "committee_entity_id",
            "office_normalized",
            "election_cycle",
            name="uq_campaign_identity",
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: str = Field(
        default_factory=lambda: str(_uuid_mod.uuid4()),
        unique=True,
        index=True,
        max_length=36,
    )

    # Identity anchor — committee that runs this campaign.
    committee_entity_id: int = Field(foreign_key="canonical_entity.id", index=True)

    office_normalized: Optional[str] = Field(default=None, max_length=200)
    district: Optional[str] = Field(default=None, max_length=100)

    # Derived from report period_end, never from individual transaction dates.
    # Required (non-null): identity tuple member; derived upstream from report period.
    election_cycle: int = Field(nullable=False)

    # Nullable: not every campaign row has a linked candidate yet.
    candidate_entity_id: Optional[int] = Field(
        default=None,
        foreign_key="canonical_entity.id",
        index=True,
    )

    canonical_name: Optional[str] = Field(default=None, max_length=500)
    state_code: str = Field(max_length=2)

    last_run_id: Optional[int] = Field(default=None, index=True)

    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)


# ---------------------------------------------------------------------------
# CanonicalNameHistory
# ---------------------------------------------------------------------------


class CanonicalNameHistory(SQLModel, table=True):
    """Every name a canonical entity or campaign has filed under."""

    __tablename__ = "canonical_name_history"
    __table_args__ = (
        UniqueConstraint(
            "subject_type",
            "subject_id",
            "normalized_name",
            name="uq_name_history_subject_normalized",
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)

    subject_type: NameHistorySubjectType = Field(max_length=8)
    # Polymorphic int FK — points at canonical_entity.id or canonical_campaign.id
    # depending on subject_type.  No DB-level FK to keep the schema simple and
    # avoid a circular dependency with the campaign table.
    subject_id: int = Field(index=True)

    name: str = Field(max_length=500)
    normalized_name: Optional[str] = Field(default=None, max_length=500, index=True)

    first_seen_date: Optional[date] = Field(default=None)
    last_seen_date: Optional[date] = Field(default=None)
    occurrence_count: int = Field(default=1)

    # File origin or report reference.
    source: Optional[str] = Field(default=None, max_length=200)

    created_at: datetime = Field(default_factory=_utc_now)
