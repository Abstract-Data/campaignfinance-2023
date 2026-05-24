"""
Unified SQLModels for Campaign Finance Data

These SQLModel-based models provide database relationships and ORM capabilities
for campaign finance data from any state.
"""

import hashlib
import json
import re
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, List, Optional  # Optional only for SQLModel forward-ref relationships

from sqlalchemy import Column, ForeignKey, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import selectinload
from sqlmodel import Field, Relationship, SQLModel, select

from .unified_field_library import field_library


class TransactionType(str, Enum):
    """Types of campaign finance transactions"""

    CONTRIBUTION = "contribution"
    EXPENDITURE = "expenditure"
    LOAN = "loan"
    PLEDGE = "pledge"
    DEBT = "debt"
    CREDIT = "credit"
    TRAVEL = "travel"
    ASSET = "asset"
    REFUND = "refund"
    TRANSFER = "transfer"
    OTHER = "other"


class PersonType(str, Enum):
    """Types of persons in campaign finance data"""

    INDIVIDUAL = "individual"
    ORGANIZATION = "organization"
    COMMITTEE = "committee"
    CANDIDATE = "candidate"
    UNKNOWN = "unknown"


class PersonRole(str, Enum):
    """Roles of persons in transactions"""

    CONTRIBUTOR = "contributor"
    RECIPIENT = "recipient"
    PAYEE = "payee"
    CANDIDATE = "candidate"
    TREASURER = "treasurer"
    CHAIR = "chair"


class CommitteeRole(str, Enum):
    """Roles that people can have within committees"""

    TREASURER = "treasurer"
    ASSISTANT_TREASURER = "assistant_treasurer"
    CHAIR = "chair"
    VICE_CHAIR = "vice_chair"
    SECRETARY = "secretary"
    ASSISTANT_SECRETARY = "assistant_secretary"
    CANDIDATE = "candidate"
    DEPUTY_TREASURER = "deputy_treasurer"
    OTHER = "other"


class EntityType(str, Enum):
    """Types of unified entities used for deduplication"""

    PERSON = "person"
    ORGANIZATION = "organization"
    COMMITTEE = "committee"
    CAMPAIGN = "campaign"
    VENDOR = "vendor"
    OTHER = "other"


class AssociationType(str, Enum):
    """Association types between unified entities"""

    TREASURER_OF = "treasurer_of"
    DONOR_TO = "donor_to"
    VENDOR_FOR = "vendor_for"
    OFFICER_OF = "officer_of"
    AFFILIATED_WITH = "affiliated_with"
    EMPLOYED_BY = "employed_by"
    OTHER = "other"


class CampaignRole(str, Enum):
    """Roles that entities can have within a campaign context"""

    CANDIDATE = "candidate"
    TREASURER = "treasurer"
    CHAIR = "chair"
    DONOR = "donor"
    VENDOR = "vendor"
    CONSULTANT = "consultant"
    STAFF = "staff"
    SUPPORTER = "supporter"
    COMMITTEE = "committee"
    OTHER = "other"


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
    amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
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
    amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
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
    amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
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
    amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
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
    amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
    original_amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
    debt_date: date | None = Field(default=None, index=True)
    due_date: date | None = Field(default=None, index=True)
    description: str | None = Field(default=None, sa_column=Column(Text))

    # Guarantor information
    is_guaranteed: bool = Field(default=False, index=True)
    guarantor_name: str | None = Field(default=None, sa_column=Column(String(200)))
    guarantee_amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))

    # Status
    is_paid: bool = Field(default=False, index=True)
    payment_amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
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
    amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
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
    parent_amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))

    # Travel details
    amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
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
    acquisition_cost: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
    current_value: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
    valuation_date: date | None = Field(default=None)

    # Disposition (if sold/disposed)
    disposition_date: date | None = Field(default=None)
    disposition_amount: Decimal | None = Field(default=None, sa_column=Column(Numeric(15, 2)))
    is_disposed: bool = Field(default=False, index=True)

    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="asset")
    committee: Optional["UnifiedCommittee"] = Relationship()
    state: State | None = Relationship(back_populates="assets")


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


class UnifiedSQLModelBuilder:
    """
    Builder class that creates SQLModel instances from state-specific data
    by automatically mapping fields using the field library.
    """

    def __init__(self, state: str, state_id: int | None, state_code: str | None = None):
        self.state_slug = state
        self.state_id = state_id
        self.state_code = state_code
        self.field_mappings = {
            mapping.state_field: mapping.unified_field
            for mapping in field_library.get_state_mappings(state)
        }

    def build_transaction(self, raw_data: dict[str, Any]) -> UnifiedTransaction:
        """
        Build a unified transaction from raw state-specific data.

        Args:
            raw_data: Dictionary containing state-specific field data

        Returns:
            UnifiedTransaction object with normalized data
        """
        # Initialize the transaction
        transaction = UnifiedTransaction(
            state_id=self.state_id, raw_data=json.dumps(raw_data.copy(), default=self._json_default)
        )

        # Map core transaction fields
        transaction.transaction_id = self._get_field_value(raw_data, "transaction_id")
        transaction.amount = self._parse_amount(self._get_field_value(raw_data, "amount"))
        transaction.transaction_date = self._parse_date(
            self._get_field_value(raw_data, "transaction_date")
        )
        transaction.description = self._get_field_value(raw_data, "description")
        transaction.transaction_type = self._determine_transaction_type(raw_data)

        # Map administrative fields
        transaction.filed_date = self._parse_date(self._get_field_value(raw_data, "filed_date"))
        transaction.amended = self._parse_boolean(self._get_field_value(raw_data, "amended"))

        # Map metadata fields
        transaction.download_date = raw_data.get("download_date")

        return transaction

    def build_person(self, raw_data: dict[str, Any], role: PersonRole) -> UnifiedPerson | None:
        """Build a unified person from raw data"""
        person_data = {}

        # Map person fields using the unified field names from the field library
        person_fields = {
            "first_name": "person_first_name",
            "last_name": "person_last_name",
            "middle_name": "person_middle_name",
            "suffix": "person_suffix",
            "organization": "person_organization",
            "employer": "person_employer",
            "occupation": "person_occupation",
        }

        for unified_field, field_name in person_fields.items():
            value = self._get_field_value(raw_data, field_name)
            if value:
                person_data[unified_field] = value

        # If we found any person data, create the person
        if person_data:
            # Determine person type
            person_type = PersonType.UNKNOWN

            # Check for special cases first
            last_name = person_data.get("last_name", "").strip()
            first_name = person_data.get("first_name", "").strip()

            # Handle special placeholder cases
            if last_name.upper() in [
                "NON-ITEMIZED CONTRIBUTOR",
                "NON-ITEMIZED",
                "UNKNOWN",
                "ANONYMOUS",
            ]:
                person_type = PersonType.UNKNOWN
            elif person_data.get("organization"):
                person_type = PersonType.ORGANIZATION
            elif first_name and last_name:
                person_type = PersonType.INDIVIDUAL
            elif last_name and not first_name:
                # Only last name - could be organization or incomplete individual
                person_type = PersonType.UNKNOWN

            # Build address
            address = self.build_address(raw_data, role.value)

            person = UnifiedPerson(
                **person_data, person_type=person_type, state_id=self.state_id, address=address
            )

            entity_type = (
                EntityType.ORGANIZATION
                if person_type == PersonType.ORGANIZATION
                else EntityType.PERSON
            )
            entity_name = person.organization if person.organization else person.full_name
            entity = self._get_or_create_entity(
                entity_type=entity_type, name=entity_name, address=address, person=person
            )
            if entity:
                person.entity = entity

            return person

        return None

    def build_address(self, raw_data: dict[str, Any], entity_role: str) -> UnifiedAddress | None:
        """Build a unified address from raw data"""
        address_data = {}

        # Map address fields using the unified field names from the field library
        address_fields = {
            "street_1": "address_street_1",
            "street_2": "address_street_2",
            "city": "address_city",
            "state": "address_state",
            "zip_code": "address_zip",
            "country": "address_country",
            "county": "address_county",
        }

        for unified_field, field_name in address_fields.items():
            value = self._get_field_value(raw_data, field_name)
            if value:
                address_data[unified_field] = value

        # If we found any address data, create or find the address
        if address_data:
            # Check if address already exists
            existing_address = self._find_address_by_fields(address_data)
            if existing_address:
                return existing_address

            # Create new address
            return UnifiedAddress(**address_data)

        return None

    def build_committee(self, raw_data: dict[str, Any]) -> UnifiedCommittee | None:
        """Build a unified committee from raw data"""
        committee_data = {}

        # Map committee fields
        committee_fields = {
            "name": "committee_name",
            "committee_type": "committee_type",
            "filer_id": "committee_filer_id",
        }

        for unified_field, field_name in committee_fields.items():
            value = self._get_field_value(raw_data, field_name)
            if value:
                committee_data[unified_field] = value

        # Handle missing committee name - for candidate committees, use candidate name
        if not committee_data.get("name"):
            committee_type = committee_data.get("committee_type", "").lower()
            candidate_name = raw_data.get("Candidate Name", "")

            if "candidate" in committee_type and candidate_name:
                # For candidate committees, use candidate name as committee name
                committee_data["name"] = f"Candidate Committee - {candidate_name}"
            else:
                # Use filer_id as fallback name if no committee name is available
                filer_id = committee_data.get("filer_id", "UNKNOWN")
                committee_data["name"] = f"Committee {filer_id}"

        committee_address = self.build_address(raw_data, "committee")

        # If we found any committee data, create or find the committee
        if committee_data:
            # Check if committee already exists by filer_id
            existing_committee = self._find_committee_by_filer_id(committee_data.get("filer_id"))
            if existing_committee:
                if self.state_id and not existing_committee.state_id:
                    existing_committee.state_id = self.state_id
                if committee_address and not existing_committee.address:
                    existing_committee.address = committee_address
                if not existing_committee.entity:
                    entity = self._get_or_create_entity(
                        entity_type=EntityType.COMMITTEE,
                        name=existing_committee.name,
                        address=existing_committee.address,
                        committee=existing_committee,
                    )
                    if entity:
                        existing_committee.entity = entity
                return existing_committee

            committee = UnifiedCommittee(**committee_data)
            committee.state_id = self.state_id
            if committee_address:
                committee.address = committee_address
            entity = self._get_or_create_entity(
                entity_type=EntityType.COMMITTEE,
                name=committee.name,
                address=committee.address,
                committee=committee,
            )
            if entity:
                committee.entity = entity

            return committee

        return None

    def _get_field_value(self, raw_data: dict[str, Any], unified_field: str) -> Any | None:
        """Get the value for a unified field from raw data"""
        # Handle None unified_field
        if unified_field is None:
            return None

        # First check if the unified field name is directly in raw_data
        # This handles cases where GenericFileReader already normalized the field names
        if unified_field in raw_data:
            return raw_data[unified_field]

        # Then try direct mapping from state-specific field names
        for state_field, mapped_field in self.field_mappings.items():
            if mapped_field == unified_field and state_field in raw_data:
                return raw_data[state_field]

        # If no direct mapping, try fuzzy matching (but avoid matching other unified field names)
        for field_name, value in raw_data.items():
            if field_name is not None:
                # Skip fuzzy matching on fields that look like unified field names
                # (they start with common prefixes like 'person_', 'address_', 'committee_')
                if field_name.startswith(("person_", "address_", "committee_", "transaction_")):
                    continue
                if self._fuzzy_match(field_name, unified_field):
                    return value

        return None

    def _fuzzy_match(self, state_field: str, unified_field: str) -> bool:
        """Check if a state field roughly matches a unified field"""
        try:
            state_normalized = self._normalize_field_name(state_field)
            unified_normalized = self._normalize_field_name(unified_field)

            # Exact match after normalization
            if state_normalized == unified_normalized:
                return True

            # Check for exact word matches (more precise)
            state_words = set(state_normalized.split("_"))
            unified_words = set(unified_normalized.split("_"))

            if state_words and unified_words:
                # Require at least 2 words to match for better precision
                overlap = len(state_words.intersection(unified_words))
                return overlap >= 2

            return False
        except Exception:
            return False

    def _find_committee_by_filer_id(self, filer_id: str) -> UnifiedCommittee | None:
        """Find an existing committee by filer_id"""
        if not filer_id:
            return None

        try:
            from app.core.unified_database import db_manager

            with db_manager.get_session() as session:
                stmt = (
                    select(UnifiedCommittee)
                    .options(
                        selectinload(UnifiedCommittee.address),
                        selectinload(UnifiedCommittee.entity).selectinload(UnifiedEntity.address),
                    )
                    .where(UnifiedCommittee.filer_id == filer_id)
                )
                return session.exec(stmt).first()
        except Exception:
            return None

    def _normalize_entity_name(self, value: str | None) -> str:
        if not value:
            return ""
        normalized = value.strip().lower()
        normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    def _find_entity(
        self, entity_type: EntityType, normalized_name: str, address: UnifiedAddress | None
    ) -> UnifiedEntity | None:
        if not normalized_name:
            return None
        try:
            from app.core.unified_database import db_manager

            with db_manager.get_session() as session:
                query = select(UnifiedEntity).where(
                    UnifiedEntity.entity_type == entity_type,
                    UnifiedEntity.normalized_name == normalized_name,
                )
                if address and getattr(address, "id", None):
                    query = query.where(UnifiedEntity.address_id == address.id)
                return session.exec(query).first()
        except Exception:
            return None

    def _get_or_create_entity(
        self,
        entity_type: EntityType,
        name: str | None,
        address: UnifiedAddress | None,
        person: UnifiedPerson | None = None,
        committee: UnifiedCommittee | None = None,
    ) -> UnifiedEntity | None:
        normalized_name = self._normalize_entity_name(name)
        existing = self._find_entity(entity_type, normalized_name, address)
        if existing:
            if person and not existing.person:
                existing.person = person
            if committee and not existing.committee:
                existing.committee = committee
            if address and not existing.address:
                existing.address = address
            if self.state_id and not existing.state_id:
                existing.state_id = self.state_id
            return existing
        if not normalized_name and not name:
            return None
        entity = UnifiedEntity(
            entity_type=entity_type,
            name=name or normalized_name or None,
            normalized_name=normalized_name or None,
            address=address,
            person=person,
            committee=committee,
            state_id=self.state_id,
        )
        return entity

    def _find_campaign(
        self,
        normalized_name: str,
        committee: UnifiedCommittee | None,
        candidate: UnifiedPerson | None,
        election_year: int | None,
    ) -> UnifiedCampaign | None:
        if not normalized_name:
            return None
        try:
            from app.core.unified_database import db_manager

            with db_manager.get_session() as session:
                query = select(UnifiedCampaign).where(
                    UnifiedCampaign.normalized_name == normalized_name
                )
                if committee and committee.filer_id:
                    query = query.where(UnifiedCampaign.primary_committee_id == committee.filer_id)
                if candidate and getattr(candidate, "id", None):
                    query = query.where(UnifiedCampaign.candidate_person_id == candidate.id)
                if election_year:
                    query = query.where(UnifiedCampaign.election_year == election_year)
                return session.exec(query).first()
        except Exception:
            return None

    def build_campaign(
        self,
        raw_data: dict[str, Any],
        committee: UnifiedCommittee | None,
        candidate: UnifiedPerson | None,
        transaction: UnifiedTransaction | None,
    ) -> UnifiedCampaign | None:
        campaign_name = self._get_field_value(raw_data, "campaign_name")
        if not campaign_name:
            campaign_name = candidate.full_name if candidate else None
        if not campaign_name and committee:
            campaign_name = committee.name
        normalized_name = self._normalize_entity_name(campaign_name)
        if not normalized_name:
            return None
        transaction_date = transaction.transaction_date if transaction else None
        if not transaction_date:
            transaction_date = self._parse_date(self._get_field_value(raw_data, "transaction_date"))
        election_year = transaction_date.year if isinstance(transaction_date, date) else None
        campaign = self._find_campaign(normalized_name, committee, candidate, election_year)
        if campaign:
            return campaign
        campaign = UnifiedCampaign(
            name=campaign_name,
            normalized_name=normalized_name,
            election_year=election_year,
            office_sought=self._get_field_value(raw_data, "office_sought"),
            district=self._get_field_value(raw_data, "district_info"),
            candidate=candidate,
            primary_committee=committee,
            state_id=self.state_id,
        )
        if candidate and candidate.entity:
            campaign.entities.append(
                UnifiedCampaignEntity(
                    campaign=campaign,
                    entity=candidate.entity,
                    state_id=self.state_id,
                    role=CampaignRole.CANDIDATE,
                    is_primary=True,
                )
            )
        if committee and committee.entity:
            campaign.entities.append(
                UnifiedCampaignEntity(
                    campaign=campaign,
                    entity=committee.entity,
                    state_id=self.state_id,
                    role=CampaignRole.COMMITTEE,
                    is_primary=True,
                )
            )
        return campaign

    def _find_address_by_fields(self, address_data: dict[str, Any]) -> UnifiedAddress | None:
        """Find an existing address by key fields"""
        if not address_data:
            return None

        try:
            from app.core.unified_database import db_manager

            with db_manager.get_session() as session:
                # Build query based on available fields
                conditions = []
                params = {}
                
                if address_data.get("street_1"):
                    conditions.append("street_1 = :street_1")
                    params["street_1"] = address_data["street_1"]
                
                if address_data.get("city"):
                    conditions.append("city = :city")
                    params["city"] = address_data["city"]
                
                if address_data.get("state"):
                    conditions.append("state = :state")
                    params["state"] = address_data["state"]
                
                if address_data.get("zip_code"):
                    conditions.append("zip_code = :zip_code")
                    params["zip_code"] = address_data["zip_code"]
                
                # Need at least street_1 and city to find a match
                if len(conditions) >= 2:
                    query = f"SELECT * FROM unified_addresses WHERE {' AND '.join(conditions)} LIMIT 1"
                    result = session.exec(text(query), params).first()
                    if result:
                        return UnifiedAddress(**dict(result))

                return None
        except Exception:
            return None

    def _normalize_field_name(self, field_name: str) -> str:
        """Normalize a field name for comparison"""
        if field_name is None:
            return ""
        try:
            normalized = str(field_name).lower()
            normalized = re.sub(r"[^a-z0-9]", "_", normalized)
            normalized = re.sub(r"_+", "_", normalized)
            return normalized.strip("_")
        except Exception:
            return ""

    def _parse_amount(self, value: Any) -> Decimal | None:
        """Parse an amount value to Decimal"""
        if value is None:
            return None

        try:
            # Remove currency symbols and commas
            if isinstance(value, str):
                value = re.sub(r"[^\d.-]", "", value)

            return Decimal(str(value))
        except (ValueError, TypeError):
            return None

    def _parse_date(self, value: Any) -> date | None:
        """Parse a date value"""
        if value is None:
            return None

        try:
            if isinstance(value, str):
                # Try common date formats
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"]:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
            elif isinstance(value, (date, datetime)):
                return value.date() if isinstance(value, datetime) else value

            return None
        except (ValueError, TypeError):
            return None

    def _parse_boolean(self, value: Any) -> bool:
        """Parse a boolean value"""
        if value is None:
            return False

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            return value.lower() in ["true", "yes", "y", "1", "t"]

        if isinstance(value, (int, float)):
            return bool(value)

        return False

    def _determine_transaction_type(self, raw_data: dict[str, Any]) -> TransactionType:
        """Determine the transaction type from raw data"""
        # Check for explicit transaction type
        type_value = self._get_field_value(raw_data, "transaction_type")
        if type_value:
            type_str = str(type_value).lower()
            type_synonyms = {
                "monetary": TransactionType.CONTRIBUTION,
                "money": TransactionType.CONTRIBUTION,
                "in-kind": TransactionType.CONTRIBUTION,
                "inkind": TransactionType.CONTRIBUTION,
                "loan": TransactionType.LOAN,
                "expenditure": TransactionType.EXPENDITURE,
                "expense": TransactionType.EXPENDITURE,
                "debt": TransactionType.DEBT,
                "obligation": TransactionType.DEBT,
                "credit": TransactionType.CREDIT,
                "refund": TransactionType.CREDIT,
                "return": TransactionType.CREDIT,
                "travel": TransactionType.TRAVEL,
                "trip": TransactionType.TRAVEL,
                "transportation": TransactionType.TRAVEL,
                "asset": TransactionType.ASSET,
                "equipment": TransactionType.ASSET,
                "property": TransactionType.ASSET,
            }
            if type_str in type_synonyms:
                return type_synonyms[type_str]
            for transaction_type in TransactionType:
                if transaction_type.value in type_str:
                    return transaction_type

        # Check for explicit record_type field (Texas uses this)
        record_type = raw_data.get("record_type", "").upper()
        record_type_map = {
            "RCPT": TransactionType.CONTRIBUTION,
            "EXPN": TransactionType.EXPENDITURE,
            "LOAN": TransactionType.LOAN,
            "PLDG": TransactionType.PLEDGE,
            "DEBT": TransactionType.DEBT,
            "CRED": TransactionType.CREDIT,
            "TRVL": TransactionType.TRAVEL,
            "ASSET": TransactionType.ASSET,
        }
        if record_type in record_type_map:
            return record_type_map[record_type]

        # Infer from field names
        field_names = [k.lower() for k in raw_data.keys()]

        if any("contribution" in name for name in field_names):
            return TransactionType.CONTRIBUTION
        elif any("expenditure" in name or "expend" in name for name in field_names):
            return TransactionType.EXPENDITURE
        elif any("loan" in name for name in field_names):
            return TransactionType.LOAN
        elif any("pledge" in name for name in field_names):
            return TransactionType.PLEDGE
        elif any("debt" in name for name in field_names):
            return TransactionType.DEBT
        elif any("credit" in name for name in field_names):
            return TransactionType.CREDIT
        elif any("travel" in name for name in field_names):
            return TransactionType.TRAVEL
        elif any("asset" in name for name in field_names):
            return TransactionType.ASSET
        elif any("refund" in name for name in field_names):
            return TransactionType.REFUND
        elif any("transfer" in name for name in field_names):
            return TransactionType.TRANSFER

        return TransactionType.OTHER

    def _json_default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return str(obj)


class UnifiedSQLDataProcessor:
    """
    High-level processor for converting state-specific data to SQLModel instances.
    """

    def __init__(self):
        self.builders = {}

    def get_builder(
        self, state: str, state_id: int | None = None, state_code: str | None = None
    ) -> UnifiedSQLModelBuilder:
        """Get or create a model builder for a specific state"""
        if state not in self.builders:
            self.builders[state] = UnifiedSQLModelBuilder(state, state_id, state_code)
        builder = self.builders[state]
        builder.state_id = state_id
        builder.state_code = state_code
        return builder

    def process_record(
        self,
        raw_data: dict[str, Any],
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
    ) -> UnifiedTransaction:
        """
        Process a single record from any state into a unified transaction.

        Args:
            raw_data: Raw data dictionary from the state
            state: State identifier (e.g., 'texas', 'oklahoma')

        Returns:
            UnifiedTransaction object
        """
        builder = self.get_builder(state, state_id=state_id, state_code=state_code)

        # Build related entities first (committee, persons, addresses)
        contributor = builder.build_person(raw_data, PersonRole.CONTRIBUTOR)
        recipient = builder.build_person(raw_data, PersonRole.RECIPIENT)
        payee = builder.build_person(raw_data, PersonRole.PAYEE)
        candidate = builder.build_person(raw_data, PersonRole.CANDIDATE)
        committee = builder.build_committee(raw_data)

        # Build the transaction
        transaction = builder.build_transaction(raw_data)

        # Build campaign if possible
        campaign = builder.build_campaign(raw_data, committee, candidate, transaction)
        if campaign:
            transaction.campaign = campaign

        # Set committee relationship
        if committee:
            transaction.committee_id = committee.filer_id
            transaction.committee = committee

        # Create transaction-person relationships
        if contributor:
            tx_person = UnifiedTransactionPerson(
                transaction=transaction,
                person=contributor,
                entity=contributor.entity,
                state_id=builder.state_id,
                role=PersonRole.CONTRIBUTOR,
            )
            transaction.persons.append(tx_person)

        if recipient:
            tx_person = UnifiedTransactionPerson(
                transaction=transaction,
                person=recipient,
                entity=recipient.entity,
                state_id=builder.state_id,
                role=PersonRole.RECIPIENT,
            )
            transaction.persons.append(tx_person)

        if payee:
            tx_person = UnifiedTransactionPerson(
                transaction=transaction,
                person=payee,
                entity=payee.entity,
                state_id=builder.state_id,
                role=PersonRole.PAYEE,
            )
            transaction.persons.append(tx_person)

        if candidate:
            tx_person = UnifiedTransactionPerson(
                transaction=transaction,
                person=candidate,
                entity=candidate.entity,
                state_id=builder.state_id,
                role=PersonRole.CANDIDATE,
            )
            transaction.persons.append(tx_person)

        # Create specialized financial records
        contributor_entity = contributor.entity if contributor and contributor.entity else None
        recipient_entity = None
        if committee and committee.entity:
            recipient_entity = committee.entity
        elif recipient and recipient.entity:
            recipient_entity = recipient.entity

        if transaction.transaction_type == TransactionType.CONTRIBUTION:
            if not contributor_entity and committee and committee.entity:
                contributor_entity = committee.entity
            if not recipient_entity and recipient and recipient.entity:
                recipient_entity = recipient.entity
            if contributor_entity and recipient_entity:
                contribution = UnifiedContribution(
                    transaction=transaction,
                    contributor=contributor_entity,
                    recipient=recipient_entity,
                    amount=transaction.amount,
                    receipt_date=transaction.transaction_date,
                    contribution_type=builder._get_field_value(raw_data, "contribution_type"),
                    description=transaction.description,
                    state_id=builder.state_id,
                )
                transaction.contribution = contribution

        if transaction.transaction_type == TransactionType.LOAN:
            if not contributor_entity and recipient_entity:
                contributor_entity = recipient_entity
            if contributor_entity and recipient_entity:
                loan = UnifiedLoan(
                    transaction=transaction,
                    lender=contributor_entity,
                    borrower=recipient_entity,
                    amount=transaction.amount,
                    loan_date=transaction.transaction_date,
                    due_date=builder._parse_date(
                        builder._get_field_value(raw_data, "loan_due_date")
                    ),
                    interest_rate=builder._parse_amount(
                        builder._get_field_value(raw_data, "loan_interest_rate")
                    ),
                    collateral=builder._get_field_value(raw_data, "loan_collateral"),
                    state_id=builder.state_id,
                )
                transaction.loan = loan

        # Create debt detail record
        if transaction.transaction_type == TransactionType.DEBT:
            # For debts, the contributor is the creditor (who is owed money)
            # and the committee/campaign is the debtor
            creditor_entity = contributor_entity
            debtor_entity = recipient_entity or (
                committee.entity if committee and hasattr(committee, "entity") else None
            )

            if creditor_entity:
                debt = UnifiedDebt(
                    transaction=transaction,
                    creditor=creditor_entity,
                    debtor=debtor_entity or creditor_entity,  # Fallback if no debtor
                    amount=transaction.amount,
                    original_amount=builder._parse_amount(
                        builder._get_field_value(raw_data, "debt_original_amount")
                    )
                    or transaction.amount,
                    debt_date=transaction.transaction_date,
                    due_date=builder._parse_date(
                        builder._get_field_value(raw_data, "debt_due_date")
                    ),
                    description=transaction.description,
                    is_guaranteed=builder._parse_boolean(
                        builder._get_field_value(raw_data, "loan_guaranteed_flag")
                    ),
                    guarantor_name=builder._get_field_value(raw_data, "guarantor_name"),
                    guarantee_amount=builder._parse_amount(
                        builder._get_field_value(raw_data, "loan_guarantee_amount")
                    ),
                    is_paid=builder._parse_boolean(
                        builder._get_field_value(raw_data, "debt_paid_flag")
                    ),
                    payment_amount=builder._parse_amount(
                        builder._get_field_value(raw_data, "debt_payment_amount")
                    ),
                    payment_date=builder._parse_date(
                        builder._get_field_value(raw_data, "debt_payment_date")
                    ),
                    state_id=builder.state_id,
                )
                transaction.debt = debt

        # Create credit detail record
        if transaction.transaction_type == TransactionType.CREDIT:
            payor_entity = contributor_entity  # Who is giving the credit/refund
            recipient_ent = recipient_entity or (
                committee.entity if committee and hasattr(committee, "entity") else None
            )

            if payor_entity:
                credit = UnifiedCredit(
                    transaction=transaction,
                    payor=payor_entity,
                    recipient=recipient_ent or payor_entity,  # Fallback
                    amount=transaction.amount,
                    credit_date=transaction.transaction_date,
                    credit_type=builder._get_field_value(raw_data, "credit_type"),
                    description=transaction.description,
                    related_transaction_id=builder._get_field_value(
                        raw_data, "related_transaction_id"
                    ),
                    state_id=builder.state_id,
                )
                transaction.credit = credit

        # Create travel detail record
        if transaction.transaction_type == TransactionType.TRAVEL:
            # Get traveler info
            traveler_name = builder._get_field_value(
                raw_data, "traveler_name"
            ) or builder._get_field_value(raw_data, "parent_full_name")

            travel = UnifiedTravel(
                transaction=transaction,
                traveler=contributor if contributor else None,
                state_id=builder.state_id,
                # Parent transaction info
                parent_transaction_type=builder._get_field_value(raw_data, "parent_type"),
                parent_transaction_id=builder._get_field_value(raw_data, "parent_id"),
                parent_amount=builder._parse_amount(
                    builder._get_field_value(raw_data, "parent_amount")
                ),
                # Travel details
                amount=transaction.amount,
                travel_date=transaction.transaction_date,
                transportation_type=builder._get_field_value(raw_data, "transportation_type_cd")
                or builder._get_field_value(raw_data, "transportation_type"),
                transportation_description=builder._get_field_value(
                    raw_data, "transportation_type_descr"
                ),
                # Itinerary
                departure_city=builder._get_field_value(raw_data, "departure_city"),
                departure_state=builder._get_field_value(raw_data, "departure_state"),
                arrival_city=builder._get_field_value(raw_data, "arrival_city"),
                arrival_state=builder._get_field_value(raw_data, "arrival_state"),
                departure_date=builder._parse_date(
                    builder._get_field_value(raw_data, "departure_dt")
                ),
                arrival_date=builder._parse_date(builder._get_field_value(raw_data, "arrival_dt")),
                # Purpose
                travel_purpose=builder._get_field_value(raw_data, "travel_purpose")
                or transaction.description,
                traveler_name=traveler_name,
            )
            transaction.travel = travel

        # Create asset detail record
        if transaction.transaction_type == TransactionType.ASSET:
            asset = UnifiedAsset(
                transaction=transaction,
                committee=committee,
                state_id=builder.state_id,
                # Asset details
                asset_type=builder._get_field_value(raw_data, "asset_type"),
                description=transaction.description
                or builder._get_field_value(raw_data, "asset_descr"),
                # Valuation
                acquisition_date=transaction.transaction_date,
                acquisition_cost=transaction.amount,
                current_value=builder._parse_amount(
                    builder._get_field_value(raw_data, "asset_current_value")
                ),
                valuation_date=builder._parse_date(
                    builder._get_field_value(raw_data, "asset_valuation_date")
                ),
                # Disposition
                disposition_date=builder._parse_date(
                    builder._get_field_value(raw_data, "asset_disposition_date")
                ),
                disposition_amount=builder._parse_amount(
                    builder._get_field_value(raw_data, "asset_disposition_amount")
                ),
                is_disposed=builder._parse_boolean(
                    builder._get_field_value(raw_data, "asset_disposed_flag")
                ),
            )
            transaction.asset = asset

        return transaction

    def process_records(
        self,
        records: List[dict[str, Any]],
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
    ) -> list[UnifiedTransaction]:
        """
        Process multiple records from any state into unified transactions.

        Args:
            records: List of raw data dictionaries
            state: State identifier

        Returns:
            List of UnifiedTransaction objects
        """
        return [
            self.process_record(record, state, state_id=state_id, state_code=state_code)
            for record in records
        ]


# Global processor instance
unified_sql_processor = UnifiedSQLDataProcessor()


# Resolve forward-string relationship references at module load time so the
# SQLAlchemy mapper registry can locate models declared in sibling modules
# (e.g. ``UnifiedReport`` lives in ``app.core.source_models.reports``).
# Without this import the mapper raises InvalidRequestError when any
# unified model is instantiated outside the loader pipeline (e.g. in tests).
from app.core.source_models.reports import UnifiedReport as _UnifiedReport  # noqa: E402,F401
