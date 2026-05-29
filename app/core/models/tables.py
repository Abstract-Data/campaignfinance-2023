"""SQLModel table classes and indexes (TASK-3a)."""

import hashlib
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import Column, ForeignKey, Index, Integer, Numeric, String, Text
from sqlmodel import Field, Relationship, SQLModel

from app.core.constants import MONEY_TYPE
from app.core.enums import (
    AssociationType,
    CampaignRole,
    CommitteeRole,
    EntityType,
    PersonRole,
    PersonType,
    TransactionType,
)

class State(SQLModel, table=True):
    """Reference table containing US states."""

    __tablename__ = "states"

    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(sa_column=Column(String(2), unique=True, nullable=False))
    name: str = Field(sa_column=Column(String(100), unique=True, nullable=False))

    transactions: List["UnifiedTransaction"] = Relationship(back_populates="state")
    persons: List["UnifiedPerson"] = Relationship(back_populates="state")
    committees: List["UnifiedCommittee"] = Relationship(back_populates="state")
    entities: List["UnifiedEntity"] = Relationship(back_populates="state")
    campaigns: List["UnifiedCampaign"] = Relationship(back_populates="state")
    contributions: List["UnifiedContribution"] = Relationship(back_populates="state")
    loans: List["UnifiedLoan"] = Relationship(back_populates="state")
    debts: List["UnifiedDebt"] = Relationship(back_populates="state")
    credits: List["UnifiedCredit"] = Relationship(back_populates="state")
    travel_records: List["UnifiedTravel"] = Relationship(back_populates="state")
    assets: List["UnifiedAsset"] = Relationship(back_populates="state")
    expenditures: List["UnifiedExpenditure"] = Relationship(back_populates="state")
    campaign_entities: List["UnifiedCampaignEntity"] = Relationship(back_populates="state")
    transaction_persons: List["UnifiedTransactionPerson"] = Relationship(back_populates="state")
    committee_persons: List["UnifiedCommitteePerson"] = Relationship(back_populates="state")
    file_origins: List["FileOrigin"] = Relationship(back_populates="state")


class FileOrigin(SQLModel, table=True):
    """Normalized file origin references for ingested data."""

    __tablename__ = "file_origins"

    id: str = Field(default=None, primary_key=True, max_length=64)
    state_id: int = Field(foreign_key="states.id")
    filename: str = Field(sa_column=Column(String(500), nullable=False))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    state: State = Relationship(back_populates="file_origins")
    transactions: List["UnifiedTransaction"] = Relationship(back_populates="file_origin")

    @staticmethod
    def build_key(state_id: int, filename: str) -> str:
        base = f"{state_id}:{filename}".encode("utf-8")
        return hashlib.sha256(base).hexdigest()


class UnifiedAddress(SQLModel, table=True):
    """Unified address model with database table"""

    __tablename__ = "unified_addresses"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)

    # Address fields
    street_1: str | None = Field(default=None, sa_column=Column(String(500)))
    street_2: str | None = Field(default=None, sa_column=Column(String(500)))
    city: str | None = Field(default=None, sa_column=Column(String(200)))
    state: str | None = Field(default=None, sa_column=Column(String(50)))
    zip_code: str | None = Field(default=None, sa_column=Column(String(50)))
    country: str | None = Field(default=None, sa_column=Column(String(100)))
    county: str | None = Field(default=None, sa_column=Column(String(200)))

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    persons: List["UnifiedPerson"] = Relationship(back_populates="address")
    entities: List["UnifiedEntity"] = Relationship(back_populates="address")

    # RF-SMELL-001 fix: SQLModel ``table=True`` skips Pydantic validators on
    # __init__, so the brief's @model_validator(mode="after") and even
    # @field_validator(mode="before") never run when callers do
    # ``UnifiedAddress(state=" tx ")``.  Override __init__ to normalize before
    # delegating to SQLModel/SQLAlchemy.  Replaces the inert __post_init__.
    def __init__(self, **data):
        if isinstance(data.get("state"), str):
            data["state"] = data["state"].upper().strip()
        if isinstance(data.get("city"), str):
            data["city"] = data["city"].strip()
        if data.get("zip_code") is not None:
            data["zip_code"] = str(data["zip_code"]).strip()
        super().__init__(**data)

    @property
    def full_address(self) -> str:
        """Get the full formatted address"""
        parts = []
        if self.street_1:
            parts.append(self.street_1)
        if self.street_2:
            parts.append(self.street_2)
        if self.city:
            parts.append(self.city)
        if self.state:
            parts.append(self.state)
        if self.zip_code:
            parts.append(self.zip_code)

        return ", ".join(parts) if parts else "No address"


class UnifiedPerson(SQLModel, table=True):
    """Unified person model with database table"""

    __tablename__ = "unified_persons"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)

    # Person fields
    first_name: str | None = Field(default=None, sa_column=Column(String(200)))
    last_name: str | None = Field(default=None, sa_column=Column(String(200)))
    middle_name: str | None = Field(default=None, sa_column=Column(String(200)))
    suffix: str | None = Field(default=None, sa_column=Column(String(50)))
    organization: str | None = Field(default=None, sa_column=Column(String(500)))
    employer: str | None = Field(default=None, sa_column=Column(String(500)))
    occupation: str | None = Field(default=None, sa_column=Column(String(500)))
    job_title: str | None = Field(default=None, sa_column=Column(String(500)))
    person_type: PersonType = Field(default=PersonType.UNKNOWN)

    # Foreign keys
    address_id: int | None = Field(default=None, foreign_key="unified_addresses.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    address: UnifiedAddress | None = Relationship(back_populates="persons")
    entity: Optional["UnifiedEntity"] = Relationship(back_populates="person")
    state: State | None = Relationship(back_populates="persons")
    campaigns: List["UnifiedCampaign"] = Relationship(back_populates="candidate")
    transaction_contributions: List["UnifiedTransactionPerson"] = Relationship(
        back_populates="person",
        sa_relationship_kwargs={"foreign_keys": "UnifiedTransactionPerson.person_id"},
    )

    # RF-SMELL-001 fix — replaces inert ``__post_init__``; see UnifiedAddress
    # note above re: SQLModel table=True validator semantics.
    def __init__(self, **data):
        for fld in (
            "first_name",
            "last_name",
            "middle_name",
            "suffix",
            "organization",
            "employer",
            "occupation",
            "job_title",
        ):
            if isinstance(data.get(fld), str):
                data[fld] = data[fld].strip()
        super().__init__(**data)

    @property
    def full_name(self) -> str:
        """Get the full name of the person"""
        parts = []
        if self.first_name:
            parts.append(self.first_name)
        if self.middle_name:
            parts.append(self.middle_name)
        if self.last_name:
            parts.append(self.last_name)
        if self.suffix:
            parts.append(self.suffix)

        if parts:
            return " ".join(parts)
        elif self.organization:
            return self.organization
        else:
            return "Unknown"


class UnifiedCommittee(SQLModel, table=True):
    """Unified committee model with database table"""

    __tablename__ = "unified_committees"

    # Use filer_id as primary key since it's the unique identifier from state systems
    filer_id: str = Field(sa_column=Column(String(200), primary_key=True))
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)

    # Committee fields
    name: str | None = Field(default=None, sa_column=Column(String(500)))
    committee_type: str | None = Field(default=None, sa_column=Column(String(200)))

    # Foreign keys
    address_id: int | None = Field(default=None, foreign_key="unified_addresses.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    address: UnifiedAddress | None = Relationship()
    transactions: List["UnifiedTransaction"] = Relationship(back_populates="committee")
    entity: Optional["UnifiedEntity"] = Relationship(back_populates="committee")
    campaigns: List["UnifiedCampaign"] = Relationship(back_populates="primary_committee")
    state: State | None = Relationship(back_populates="committees")

    # RF-SMELL-001 fix — replaces inert ``__post_init__``; see UnifiedAddress
    # note above re: SQLModel table=True validator semantics.
    def __init__(self, **data):
        for fld in ("name", "committee_type"):
            if isinstance(data.get(fld), str):
                data[fld] = data[fld].strip()
        super().__init__(**data)


class UnifiedEntity(SQLModel, table=True):
    """Unified entity representing people, committees, vendors, and campaigns."""

    __tablename__ = "unified_entities"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    entity_type: EntityType = Field(default=EntityType.PERSON, index=True)
    name: str | None = Field(default=None, sa_column=Column(String(500)))
    normalized_name: str | None = Field(default=None, sa_column=Column(String(500)))
    person_id: int | None = Field(
        default=None, sa_column=Column(Integer, ForeignKey("unified_persons.id"), unique=True)
    )
    committee_id: str | None = Field(
        default=None,
        sa_column=Column(String(200), ForeignKey("unified_committees.filer_id"), unique=True),
    )
    address_id: int | None = Field(default=None, foreign_key="unified_addresses.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")
    notes: str | None = Field(default=None, sa_column=Column(Text))
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    address: UnifiedAddress | None = Relationship(back_populates="entities")
    person: UnifiedPerson | None = Relationship(back_populates="entity")
    committee: UnifiedCommittee | None = Relationship(back_populates="entity")
    campaign_memberships: List["UnifiedCampaignEntity"] = Relationship(back_populates="entity")
    association_sources: List["UnifiedEntityAssociation"] = Relationship(
        back_populates="source_entity",
        sa_relationship_kwargs={"foreign_keys": "UnifiedEntityAssociation.source_entity_id"},
    )
    association_targets: List["UnifiedEntityAssociation"] = Relationship(
        back_populates="target_entity",
        sa_relationship_kwargs={"foreign_keys": "UnifiedEntityAssociation.target_entity_id"},
    )
    contributions_given: List["UnifiedContribution"] = Relationship(
        back_populates="contributor",
        sa_relationship_kwargs={"foreign_keys": "UnifiedContribution.contributor_entity_id"},
    )
    contributions_received: List["UnifiedContribution"] = Relationship(
        back_populates="recipient",
        sa_relationship_kwargs={"foreign_keys": "UnifiedContribution.recipient_entity_id"},
    )
    loans_lent: List["UnifiedLoan"] = Relationship(
        back_populates="lender",
        sa_relationship_kwargs={"foreign_keys": "UnifiedLoan.lender_entity_id"},
    )
    loans_borrowed: List["UnifiedLoan"] = Relationship(
        back_populates="borrower",
        sa_relationship_kwargs={"foreign_keys": "UnifiedLoan.borrower_entity_id"},
    )
    debts_owed_to: List["UnifiedDebt"] = Relationship(
        back_populates="creditor",
        sa_relationship_kwargs={"foreign_keys": "UnifiedDebt.creditor_entity_id"},
    )
    debts_owed_by: List["UnifiedDebt"] = Relationship(
        back_populates="debtor",
        sa_relationship_kwargs={"foreign_keys": "UnifiedDebt.debtor_entity_id"},
    )
    credits_from: List["UnifiedCredit"] = Relationship(
        back_populates="payor",
        sa_relationship_kwargs={"foreign_keys": "UnifiedCredit.payor_entity_id"},
    )
    credits_to: List["UnifiedCredit"] = Relationship(
        back_populates="recipient",
        sa_relationship_kwargs={"foreign_keys": "UnifiedCredit.recipient_entity_id"},
    )
    state: State | None = Relationship(back_populates="entities")


class UnifiedTransaction(SQLModel, table=True):
    """Unified transaction model with database table and change tracking"""

    __tablename__ = "unified_transactions"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)

    # Core transaction fields
    transaction_id: str | None = Field(default=None, sa_column=Column(String(500)))
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    transaction_date: date | None = Field(default=None, index=True)
    description: str | None = Field(default=None, sa_column=Column(Text))
    transaction_type: TransactionType = Field(default=TransactionType.OTHER, index=True)

    # Foreign keys
    committee_id: str | None = Field(default=None, foreign_key="unified_committees.filer_id")
    campaign_id: int | None = Field(default=None, foreign_key="unified_campaigns.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")
    file_origin_id: str | None = Field(default=None, foreign_key="file_origins.id")
    report_id: int | None = Field(default=None, foreign_key="unified_reports.id", index=True)

    # Matching field populated from TEC reportInfoIdent (used by link_transactions_to_reports)
    report_ident: str | None = Field(default=None, sa_column=Column(String(20), index=True))

    # Administrative fields
    filed_date: date | None = Field(default=None, index=True)
    amended: bool = Field(default=False, index=True)

    # Metadata fields
    download_date: str | None = Field(default=None, sa_column=Column(String(100)))

    # Raw data for debugging (stored as JSON string)
    raw_data: str | None = Field(default=None, sa_column=Column(Text))

    # Change tracking fields
    last_modified_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), index=True
    )
    last_modified_by: str | None = Field(default=None, sa_column=Column(String(200)))
    change_reason: str | None = Field(default=None, sa_column=Column(String(500)))
    amendment_details: str | None = Field(default=None, sa_column=Column(Text))

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    committee: UnifiedCommittee | None = Relationship(back_populates="transactions")
    campaign: Optional["UnifiedCampaign"] = Relationship(back_populates="transactions")
    persons: List["UnifiedTransactionPerson"] = Relationship(back_populates="transaction")
    contribution: Optional["UnifiedContribution"] = Relationship(
        back_populates="transaction", sa_relationship_kwargs={"uselist": False}
    )
    loan: Optional["UnifiedLoan"] = Relationship(
        back_populates="transaction", sa_relationship_kwargs={"uselist": False}
    )
    debt: Optional["UnifiedDebt"] = Relationship(
        back_populates="transaction", sa_relationship_kwargs={"uselist": False}
    )
    credit: Optional["UnifiedCredit"] = Relationship(
        back_populates="transaction", sa_relationship_kwargs={"uselist": False}
    )
    travel: Optional["UnifiedTravel"] = Relationship(
        back_populates="transaction", sa_relationship_kwargs={"uselist": False}
    )
    asset: Optional["UnifiedAsset"] = Relationship(
        back_populates="transaction", sa_relationship_kwargs={"uselist": False}
    )
    expenditure: Optional["UnifiedExpenditure"] = Relationship(
        back_populates="transaction", sa_relationship_kwargs={"uselist": False}
    )
    state: State | None = Relationship(back_populates="transactions")
    file_origin: FileOrigin | None = Relationship(back_populates="transactions")
    report: Optional["UnifiedReport"] = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[UnifiedTransaction.report_id]"}
    )


class UnifiedTransactionPerson(SQLModel, table=True):
    """Junction table for transaction-person relationships with roles"""

    __tablename__ = "unified_transaction_persons"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)

    # Foreign keys
    transaction_id: int = Field(foreign_key="unified_transactions.id")
    person_id: int = Field(foreign_key="unified_persons.id")
    entity_id: int | None = Field(default=None, foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")

    # Link to committee role (optional - for tracking officer activities)
    committee_person_id: int | None = Field(
        default=None, foreign_key="unified_committee_persons.id"
    )

    # Role in the transaction
    role: PersonRole = Field(index=True)

    # Additional metadata for the relationship
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    notes: str | None = Field(default=None, sa_column=Column(Text))

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: UnifiedTransaction = Relationship(back_populates="persons")
    person: UnifiedPerson = Relationship(back_populates="transaction_contributions")
    entity: UnifiedEntity | None = Relationship()
    state: State | None = Relationship(back_populates="transaction_persons")
    committee_person: Optional["UnifiedCommitteePerson"] = Relationship()


class UnifiedTransactionVersion(SQLModel, table=True):
    """Versioning/history table for UnifiedTransaction"""

    __tablename__ = "unified_transaction_versions"

    id: int | None = Field(default=None, primary_key=True)
    transaction_id: int = Field(foreign_key="unified_transactions.id")
    version_number: int = Field(index=True)
    data: str = Field(sa_column=Column(Text))  # JSON snapshot of the transaction
    changed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), index=True)
    changed_by: str | None = Field(default=None, sa_column=Column(String(200)))
    change_reason: str | None = Field(default=None, sa_column=Column(String(500)))
    amendment_details: str | None = Field(default=None, sa_column=Column(Text))

    # Relationships
    transaction: UnifiedTransaction | None = Relationship()


class UnifiedPersonVersion(SQLModel, table=True):
    """Versioning/history table for UnifiedPerson"""

    __tablename__ = "unified_person_versions"
    id: int | None = Field(default=None, primary_key=True)
    person_id: int = Field(foreign_key="unified_persons.id")
    version_number: int = Field(index=True)
    data: str = Field(sa_column=Column(Text))  # JSON snapshot of the person
    changed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), index=True)
    changed_by: str | None = Field(default=None, sa_column=Column(String(200)))
    change_reason: str | None = Field(default=None, sa_column=Column(String(500)))
    amendment_details: str | None = Field(default=None, sa_column=Column(Text))
    person: UnifiedPerson | None = Relationship()


class UnifiedCommitteeVersion(SQLModel, table=True):
    """Versioning/history table for UnifiedCommittee"""

    __tablename__ = "unified_committee_versions"
    id: int | None = Field(default=None, primary_key=True)
    committee_id: str = Field(foreign_key="unified_committees.filer_id")
    version_number: int = Field(index=True)
    data: str = Field(sa_column=Column(Text))  # JSON snapshot of the committee
    changed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), index=True)
    changed_by: str | None = Field(default=None, sa_column=Column(String(200)))
    change_reason: str | None = Field(default=None, sa_column=Column(String(500)))
    amendment_details: str | None = Field(default=None, sa_column=Column(Text))
    committee: UnifiedCommittee | None = Relationship()


class UnifiedAddressVersion(SQLModel, table=True):
    """Versioning/history table for UnifiedAddress"""

    __tablename__ = "unified_address_versions"
    id: int | None = Field(default=None, primary_key=True)
    address_id: int = Field(foreign_key="unified_addresses.id")
    version_number: int = Field(index=True)
    data: str = Field(sa_column=Column(Text))  # JSON snapshot of the address
    changed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), index=True)
    changed_by: str | None = Field(default=None, sa_column=Column(String(200)))
    change_reason: str | None = Field(default=None, sa_column=Column(String(500)))
    amendment_details: str | None = Field(default=None, sa_column=Column(Text))
    address: UnifiedAddress | None = Relationship()


class UnifiedCommitteePerson(SQLModel, table=True):
    """Junction table for committee-person relationships with roles"""

    __tablename__ = "unified_committee_persons"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)

    # Foreign keys
    committee_id: str = Field(foreign_key="unified_committees.filer_id")
    person_id: int = Field(foreign_key="unified_persons.id")
    entity_id: int | None = Field(default=None, foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")

    # Role information
    role: CommitteeRole = Field(index=True)
    start_date: date | None = Field(default=None, index=True)
    end_date: date | None = Field(default=None, index=True)
    is_active: bool = Field(default=True, index=True)

    # Additional metadata
    notes: str | None = Field(default=None, sa_column=Column(Text))

    # Change tracking fields
    last_modified_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), index=True
    )
    last_modified_by: str | None = Field(default=None, sa_column=Column(String(200)))
    change_reason: str | None = Field(default=None, sa_column=Column(String(500)))

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    committee: UnifiedCommittee = Relationship()
    person: UnifiedPerson = Relationship()
    entity: UnifiedEntity | None = Relationship()
    state: State | None = Relationship(back_populates="committee_persons")


class UnifiedCommitteePersonVersion(SQLModel, table=True):
    """Versioning/history table for UnifiedCommitteePerson"""

    __tablename__ = "unified_committee_person_versions"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    committee_person_id: int = Field(foreign_key="unified_committee_persons.id")
    version_number: int = Field(index=True)
    data: str = Field(sa_column=Column(Text))  # JSON snapshot
    changed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), index=True)
    changed_by: str | None = Field(default=None, sa_column=Column(String(200)))
    change_reason: str | None = Field(default=None, sa_column=Column(String(500)))
    amendment_details: str | None = Field(default=None, sa_column=Column(Text))

    # Relationships
    committee_person: UnifiedCommitteePerson | None = Relationship()


class UnifiedEntityAssociation(SQLModel, table=True):
    """Associations between unified entities (e.g., treasurer-of, vendor-for)."""

    __tablename__ = "unified_entity_associations"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    source_entity_id: int = Field(foreign_key="unified_entities.id")
    target_entity_id: int = Field(foreign_key="unified_entities.id")
    association_type: AssociationType = Field(index=True)
    start_date: date | None = Field(default=None, index=True)
    end_date: date | None = Field(default=None, index=True)
    description: str | None = Field(default=None, sa_column=Column(Text))
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    source_entity: UnifiedEntity = Relationship(
        back_populates="association_sources",
        sa_relationship_kwargs={"foreign_keys": "UnifiedEntityAssociation.source_entity_id"},
    )
    target_entity: UnifiedEntity = Relationship(
        back_populates="association_targets",
        sa_relationship_kwargs={"foreign_keys": "UnifiedEntityAssociation.target_entity_id"},
    )


class UnifiedCampaign(SQLModel, table=True):
    """Unified campaign metadata allowing cross-committee associations."""

    __tablename__ = "unified_campaigns"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    name: str | None = Field(default=None, sa_column=Column(String(500)))
    normalized_name: str | None = Field(default=None, sa_column=Column(String(500)))
    election_year: int | None = Field(default=None, index=True)
    office_sought: str | None = Field(default=None, sa_column=Column(String(200)))
    district: str | None = Field(default=None, sa_column=Column(String(200)))
    candidate_person_id: int | None = Field(default=None, foreign_key="unified_persons.id")
    primary_committee_id: str | None = Field(
        default=None, foreign_key="unified_committees.filer_id"
    )
    state_id: int | None = Field(default=None, foreign_key="states.id")
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    candidate: Optional["UnifiedPerson"] = Relationship(back_populates="campaigns")
    primary_committee: Optional["UnifiedCommittee"] = Relationship(back_populates="campaigns")
    entities: List["UnifiedCampaignEntity"] = Relationship(back_populates="campaign")
    transactions: List["UnifiedTransaction"] = Relationship(back_populates="campaign")
    state: State | None = Relationship(back_populates="campaigns")


class UnifiedCampaignEntity(SQLModel, table=True):
    """Links unified entities to campaigns with specific roles."""

    __tablename__ = "unified_campaign_entities"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    campaign_id: int = Field(foreign_key="unified_campaigns.id")
    entity_id: int = Field(foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")
    role: CampaignRole = Field(index=True)
    is_primary: bool = Field(default=False, index=True)
    start_date: date | None = Field(default=None, index=True)
    end_date: date | None = Field(default=None, index=True)
    notes: str | None = Field(default=None, sa_column=Column(Text))
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    campaign: "UnifiedCampaign" = Relationship(back_populates="entities")
    entity: "UnifiedEntity" = Relationship(back_populates="campaign_memberships")
    state: State | None = Relationship(back_populates="campaign_entities")


class UnifiedContribution(SQLModel, table=True):
    """Normalized contribution detail extracted from transactions."""

    __tablename__ = "unified_contributions"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    transaction_id: int = Field(
        sa_column=Column(Integer, ForeignKey("unified_transactions.id"), unique=True)
    )
    contributor_entity_id: int = Field(foreign_key="unified_entities.id")
    recipient_entity_id: int = Field(foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    receipt_date: date | None = Field(default=None, index=True)
    contribution_type: str | None = Field(default=None, sa_column=Column(String(200)))
    is_anonymous: bool = Field(default=False, index=True)
    description: str | None = Field(default=None, sa_column=Column(Text))
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="contribution")
    contributor: "UnifiedEntity" = Relationship(
        back_populates="contributions_given",
        sa_relationship_kwargs={"foreign_keys": "UnifiedContribution.contributor_entity_id"},
    )
    recipient: "UnifiedEntity" = Relationship(
        back_populates="contributions_received",
        sa_relationship_kwargs={"foreign_keys": "UnifiedContribution.recipient_entity_id"},
    )
    state: State | None = Relationship(back_populates="contributions")


class UnifiedLoan(SQLModel, table=True):
    """Normalized loan detail extracted from transactions."""

    __tablename__ = "unified_loans"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    transaction_id: int = Field(
        sa_column=Column(Integer, ForeignKey("unified_transactions.id"), unique=True)
    )
    lender_entity_id: int = Field(foreign_key="unified_entities.id")
    borrower_entity_id: int = Field(foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    loan_date: date | None = Field(default=None, index=True)
    due_date: date | None = Field(default=None, index=True)
    interest_rate: Decimal | None = Field(default=None, sa_column=Column(Numeric(9, 4)))
    is_forgiven: bool = Field(default=False, index=True)
    collateral: str | None = Field(default=None, sa_column=Column(Text))
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="loan")
    lender: "UnifiedEntity" = Relationship(
        back_populates="loans_lent",
        sa_relationship_kwargs={"foreign_keys": "UnifiedLoan.lender_entity_id"},
    )
    borrower: "UnifiedEntity" = Relationship(
        back_populates="loans_borrowed",
        sa_relationship_kwargs={"foreign_keys": "UnifiedLoan.borrower_entity_id"},
    )
    state: State | None = Relationship(back_populates="loans")


class UnifiedDebt(SQLModel, table=True):
    """Normalized debt detail extracted from transactions.

    Tracks outstanding debts owed by the campaign/committee.
    Similar to loans but specifically for debt obligations.
    """

    __tablename__ = "unified_debts"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    transaction_id: int = Field(
        sa_column=Column(Integer, ForeignKey("unified_transactions.id"), unique=True)
    )
    creditor_entity_id: int = Field(foreign_key="unified_entities.id")
    debtor_entity_id: int = Field(foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")

    # Debt details
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    original_amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    debt_date: date | None = Field(default=None, index=True)
    due_date: date | None = Field(default=None, index=True)
    description: str | None = Field(default=None, sa_column=Column(Text))

    # Guarantor information
    is_guaranteed: bool = Field(default=False, index=True)
    guarantor_name: str | None = Field(default=None, sa_column=Column(String(200)))
    guarantee_amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))

    # Status
    is_paid: bool = Field(default=False, index=True)
    payment_amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    payment_date: date | None = Field(default=None)

    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="debt")
    creditor: "UnifiedEntity" = Relationship(
        back_populates="debts_owed_to",
        sa_relationship_kwargs={"foreign_keys": "UnifiedDebt.creditor_entity_id"},
    )
    debtor: "UnifiedEntity" = Relationship(
        back_populates="debts_owed_by",
        sa_relationship_kwargs={"foreign_keys": "UnifiedDebt.debtor_entity_id"},
    )
    state: State | None = Relationship(back_populates="debts")


class UnifiedCredit(SQLModel, table=True):
    """Normalized credit/refund detail extracted from transactions.

    Tracks credits, refunds, and returns received by the campaign/committee.
    """

    __tablename__ = "unified_credits"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    transaction_id: int = Field(
        sa_column=Column(Integer, ForeignKey("unified_transactions.id"), unique=True)
    )
    payor_entity_id: int = Field(foreign_key="unified_entities.id")
    recipient_entity_id: int = Field(foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")

    # Credit details
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    credit_date: date | None = Field(default=None, index=True)
    credit_type: str | None = Field(
        default=None, sa_column=Column(String(100))
    )  # refund, return, adjustment, etc.
    description: str | None = Field(default=None, sa_column=Column(Text))

    # Related transaction (what was the credit for)
    related_transaction_id: str | None = Field(default=None, sa_column=Column(String(500)))

    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="credit")
    payor: "UnifiedEntity" = Relationship(
        back_populates="credits_from",
        sa_relationship_kwargs={"foreign_keys": "UnifiedCredit.payor_entity_id"},
    )
    recipient: "UnifiedEntity" = Relationship(
        back_populates="credits_to",
        sa_relationship_kwargs={"foreign_keys": "UnifiedCredit.recipient_entity_id"},
    )
    state: State | None = Relationship(back_populates="credits")


class UnifiedTravel(SQLModel, table=True):
    """Normalized travel detail extracted from transactions.

    Tracks travel expenses, including transportation, lodging, and meals
    associated with campaign activities.
    """

    __tablename__ = "unified_travel"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    transaction_id: int = Field(
        sa_column=Column(Integer, ForeignKey("unified_transactions.id"), unique=True)
    )
    traveler_person_id: int | None = Field(default=None, foreign_key="unified_persons.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")

    # Parent transaction info (travel is often a sub-item of contribution/expenditure)
    parent_transaction_type: str | None = Field(default=None, sa_column=Column(String(50)))
    parent_transaction_id: str | None = Field(default=None, sa_column=Column(String(500)))
    parent_amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))

    # Travel details
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    travel_date: date | None = Field(default=None, index=True)

    # Transportation
    transportation_type: str | None = Field(
        default=None, sa_column=Column(String(100))
    )  # air, car, rail, etc.
    transportation_description: str | None = Field(default=None, sa_column=Column(String(255)))

    # Itinerary
    departure_city: str | None = Field(default=None, sa_column=Column(String(100)))
    departure_state: str | None = Field(default=None, sa_column=Column(String(50)))
    arrival_city: str | None = Field(default=None, sa_column=Column(String(100)))
    arrival_state: str | None = Field(default=None, sa_column=Column(String(50)))
    departure_date: date | None = Field(default=None, index=True)
    arrival_date: date | None = Field(default=None)

    # Purpose
    travel_purpose: str | None = Field(default=None, sa_column=Column(Text))

    # Traveler info (denormalized for quick access)
    traveler_name: str | None = Field(default=None, sa_column=Column(String(200)))

    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="travel")
    traveler: Optional["UnifiedPerson"] = Relationship()
    state: State | None = Relationship(back_populates="travel_records")


class UnifiedAsset(SQLModel, table=True):
    """Normalized asset detail extracted from transactions.

    Tracks campaign assets such as equipment, property, and other
    items of value owned by the campaign/committee.
    """

    __tablename__ = "unified_assets"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    transaction_id: int = Field(
        sa_column=Column(Integer, ForeignKey("unified_transactions.id"), unique=True)
    )
    committee_id: str | None = Field(default=None, foreign_key="unified_committees.filer_id")
    state_id: int | None = Field(default=None, foreign_key="states.id")

    # Asset details
    asset_type: str | None = Field(
        default=None, sa_column=Column(String(100))
    )  # equipment, property, vehicle, etc.
    description: str | None = Field(default=None, sa_column=Column(Text))

    # Valuation
    acquisition_date: date | None = Field(default=None, index=True)
    acquisition_cost: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    current_value: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    valuation_date: date | None = Field(default=None)

    # Disposition (if sold/disposed)
    disposition_date: date | None = Field(default=None)
    disposition_amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    is_disposed: bool = Field(default=False, index=True)

    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="asset")
    committee: Optional["UnifiedCommittee"] = Relationship()
    state: State | None = Relationship(back_populates="assets")


class UnifiedExpenditure(SQLModel, table=True):
    """Normalized expenditure detail extracted from transactions.

    Tracks vendor payments and other disbursements made by the committee.
    One row per EXPN transaction — payer is always the committee entity,
    payee is the vendor/person who received the funds.
    """

    __tablename__ = "unified_expenditures"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    transaction_id: int = Field(
        sa_column=Column(Integer, ForeignKey("unified_transactions.id"), unique=True)
    )
    payer_entity_id: int = Field(foreign_key="unified_entities.id")
    payee_entity_id: int = Field(foreign_key="unified_entities.id")
    state_id: int | None = Field(default=None, foreign_key="states.id")
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    expenditure_date: date | None = Field(default=None, index=True)
    expenditure_type: str | None = Field(default=None, sa_column=Column(String(200)))
    description: str | None = Field(default=None, sa_column=Column(Text))
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="expenditure")
    payer: "UnifiedEntity" = Relationship(
        sa_relationship_kwargs={"foreign_keys": "UnifiedExpenditure.payer_entity_id"}
    )
    payee: "UnifiedEntity" = Relationship(
        sa_relationship_kwargs={"foreign_keys": "UnifiedExpenditure.payee_entity_id"}
    )
    state: State | None = Relationship(back_populates="expenditures")


# Database indexes for performance
class UnifiedTransactionIndexes:
    """Database indexes for the unified transaction system"""

    # Transaction indexes
    idx_transactions_state = Index("idx_transactions_state", UnifiedTransaction.state_id)
    idx_transactions_type = Index("idx_transactions_type", UnifiedTransaction.transaction_type)
    idx_transactions_date = Index("idx_transactions_date", UnifiedTransaction.transaction_date)
    idx_transactions_amount = Index("idx_transactions_amount", UnifiedTransaction.amount)
    idx_transactions_committee = Index(
        "idx_transactions_committee", UnifiedTransaction.committee_id
    )
    idx_transactions_id = Index("idx_transactions_id", UnifiedTransaction.transaction_id)
    idx_transactions_file_origin = Index(
        "idx_transactions_file_origin", UnifiedTransaction.file_origin_id
    )

    # Person indexes
    idx_persons_name = Index("idx_persons_name", UnifiedPerson.last_name, UnifiedPerson.first_name)
    idx_persons_organization = Index("idx_persons_organization", UnifiedPerson.organization)
    idx_persons_type = Index("idx_persons_type", UnifiedPerson.person_type)

    # Transaction-Person relationship indexes
    idx_transaction_persons_role = Index(
        "idx_transaction_persons_role", UnifiedTransactionPerson.role
    )
    idx_transaction_persons_transaction = Index(
        "idx_transaction_persons_transaction", UnifiedTransactionPerson.transaction_id
    )
    idx_transaction_persons_person = Index(
        "idx_transaction_persons_person", UnifiedTransactionPerson.person_id
    )

    # Address indexes
    idx_addresses_state = Index("idx_addresses_state", UnifiedAddress.state)
    idx_addresses_city = Index("idx_addresses_city", UnifiedAddress.city)

    # Committee indexes
    idx_committees_name = Index("idx_committees_name", UnifiedCommittee.name)
    idx_committees_type = Index("idx_committees_type", UnifiedCommittee.committee_type)
    # Note: filer_id is already indexed as primary key

    # Entity indexes
    idx_entities_type = Index("idx_entities_type", UnifiedEntity.entity_type)
    idx_entities_name = Index("idx_entities_name", UnifiedEntity.normalized_name)

    # Campaign indexes
    idx_campaigns_year = Index("idx_campaigns_year", UnifiedCampaign.election_year)
    idx_campaigns_office = Index("idx_campaigns_office", UnifiedCampaign.office_sought)
    idx_campaigns_name = Index("idx_campaigns_name", UnifiedCampaign.normalized_name)
    idx_campaign_entity_role = Index("idx_campaign_entity_role", UnifiedCampaignEntity.role)

    # Contribution indexes
    idx_contributions_date = Index("idx_contributions_date", UnifiedContribution.receipt_date)
    idx_contributions_amount = Index("idx_contributions_amount", UnifiedContribution.amount)

    # Loan indexes
    idx_loans_date = Index("idx_loans_date", UnifiedLoan.loan_date)
    idx_loans_due_date = Index("idx_loans_due_date", UnifiedLoan.due_date)

    # Debt indexes
    idx_debts_date = Index("idx_debts_date", UnifiedDebt.debt_date)
    idx_debts_due_date = Index("idx_debts_due_date", UnifiedDebt.due_date)
    idx_debts_amount = Index("idx_debts_amount", UnifiedDebt.amount)
    idx_debts_is_paid = Index("idx_debts_is_paid", UnifiedDebt.is_paid)

    # Credit indexes
    idx_credits_date = Index("idx_credits_date", UnifiedCredit.credit_date)
    idx_credits_amount = Index("idx_credits_amount", UnifiedCredit.amount)
    idx_credits_type = Index("idx_credits_type", UnifiedCredit.credit_type)

    # Travel indexes
    idx_travel_date = Index("idx_travel_date", UnifiedTravel.travel_date)
    idx_travel_departure = Index("idx_travel_departure", UnifiedTravel.departure_date)
    idx_travel_departure_city = Index("idx_travel_departure_city", UnifiedTravel.departure_city)
    idx_travel_arrival_city = Index("idx_travel_arrival_city", UnifiedTravel.arrival_city)

    # Asset indexes
    idx_assets_acquisition_date = Index(
        "idx_assets_acquisition_date", UnifiedAsset.acquisition_date
    )
    idx_assets_type = Index("idx_assets_type", UnifiedAsset.asset_type)
    idx_assets_is_disposed = Index("idx_assets_is_disposed", UnifiedAsset.is_disposed)


