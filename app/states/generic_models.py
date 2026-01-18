from typing import Optional, List
from datetime import date
from decimal import Decimal
from pydantic import Field
from sqlmodel import SQLModel, Field, Relationship
import app.funcs.validator_functions as funcs

# Generic Address Model
class CampaignAddress(SQLModel, table=True):
    __tablename__ = "campaign_addresses"
    __table_args__ = {"schema": "campaign_finance"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    address_hash: str = Field(unique=True, index=True, description="Hash of address for deduplication")
    
    # Standard address fields (common across states)
    address_1: Optional[str] = Field(default=None, max_length=100, description="Primary address line")
    address_2: Optional[str] = Field(default=None, max_length=100, description="Secondary address line")
    city: Optional[str] = Field(default=None, max_length=50, description="City")
    state: Optional[str] = Field(default=None, max_length=2, description="State code")
    zip_code: Optional[str] = Field(default=None, max_length=20, description="ZIP/Postal code")
    country: Optional[str] = Field(default=None, max_length=3, description="Country code")
    
    # Additional address fields (state-specific variations)
    county: Optional[str] = Field(default=None, max_length=50, description="County")
    region: Optional[str] = Field(default=None, max_length=50, description="Region/Province")
    
    # Phone information
    phone: Optional[str] = Field(default=None, max_length=20, description="Phone number")
    phone_ext: Optional[str] = Field(default=None, max_length=10, description="Phone extension")
    
    # Relationships
    persons: List["CampaignPerson"] = Relationship(back_populates="address")
    contributions: List["CampaignContribution"] = Relationship(back_populates="contributor_address")
    expenditures: List["CampaignExpenditure"] = Relationship(back_populates="payee_address")
    loans: List["CampaignLoan"] = Relationship(back_populates="lender_address")
    committees: List["CampaignCommittee"] = Relationship(back_populates="address")

# Generic Person Model
class CampaignPerson(SQLModel, table=True):
    __tablename__ = "campaign_persons"
    __table_args__ = {"schema": "campaign_finance"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    person_hash: str = Field(unique=True, index=True, description="Hash of person data for deduplication")
    
    # Person type
    person_type: str = Field(description="INDIVIDUAL, ENTITY, BUSINESS, PAC, etc.")
    
    # Name fields (standardized across states)
    name_organization: Optional[str] = Field(default=None, max_length=200, description="Organization name for entities")
    name_last: Optional[str] = Field(default=None, max_length=100, description="Last name for individuals")
    name_first: Optional[str] = Field(default=None, max_length=100, description="First name for individuals")
    name_middle: Optional[str] = Field(default=None, max_length=100, description="Middle name for individuals")
    name_suffix: Optional[str] = Field(default=None, max_length=20, description="Name suffix (Jr, Sr, III, etc.)")
    name_prefix: Optional[str] = Field(default=None, max_length=20, description="Name prefix (Mr, Mrs, Dr, etc.)")
    
    # Employment information
    employer: Optional[str] = Field(default=None, max_length=200, description="Employer")
    occupation: Optional[str] = Field(default=None, max_length=200, description="Occupation")
    job_title: Optional[str] = Field(default=None, max_length=200, description="Job title")
    
    # PAC/Committee specific fields
    pac_id: Optional[str] = Field(default=None, max_length=50, description="PAC identification number")
    fein: Optional[str] = Field(default=None, max_length=20, description="Federal Employer Identification Number")
    
    # Foreign key to address
    address_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_addresses.id")
    address: Optional[CampaignAddress] = Relationship(back_populates="persons")
    
    # Relationships to various record types
    contributions: List["CampaignContribution"] = Relationship(back_populates="contributor")
    expenditures: List["CampaignExpenditure"] = Relationship(back_populates="payee")
    loans: List["CampaignLoan"] = Relationship(back_populates="lender")
    committees: List["CampaignCommittee"] = Relationship(back_populates="treasurer")
    committee_chairs: List["CampaignCommittee"] = Relationship(back_populates="chair")

# Generic Committee Model
class CampaignCommittee(SQLModel, table=True):
    __tablename__ = "campaign_committees"
    __table_args__ = {"schema": "campaign_finance"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Committee identification
    committee_id: str = Field(unique=True, description="Unique committee identifier")
    committee_name: str = Field(max_length=200, description="Committee name")
    committee_type: str = Field(max_length=100, description="Type of committee")
    
    # State and jurisdiction
    state: str = Field(max_length=2, description="State where committee operates")
    jurisdiction: Optional[str] = Field(default=None, max_length=100, description="Jurisdiction (State, County, etc.)")
    
    # Committee details
    candidate_name: Optional[str] = Field(default=None, max_length=200, description="Candidate name if candidate committee")
    office_sought: Optional[str] = Field(default=None, max_length=100, description="Office being sought")
    district: Optional[str] = Field(default=None, max_length=50, description="District number")
    
    # Status and dates
    status: Optional[str] = Field(default=None, max_length=50, description="Committee status")
    registration_date: Optional[date] = Field(default=None, description="Date of registration")
    termination_date: Optional[date] = Field(default=None, description="Date of termination")
    
    # Foreign keys to normalized tables
    treasurer_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_persons.id")
    treasurer: Optional[CampaignPerson] = Relationship(back_populates="committees")
    
    chair_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_persons.id")
    chair: Optional[CampaignPerson] = Relationship(back_populates="committee_chairs")
    
    address_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_addresses.id")
    address: Optional[CampaignAddress] = Relationship(back_populates="committees")
    
    # Relationships to transactions
    contributions: List["CampaignContribution"] = Relationship(back_populates="committee")
    expenditures: List["CampaignExpenditure"] = Relationship(back_populates="committee")
    loans: List["CampaignLoan"] = Relationship(back_populates="committee")

# Generic Transaction Base Model
class CampaignTransaction(SQLModel):
    """Base model for all campaign finance transactions"""
    
    # Transaction identification
    transaction_id: str = Field(description="Unique transaction identifier")
    transaction_type: str = Field(description="Type of transaction (CONTRIBUTION, EXPENDITURE, LOAN, etc.)")
    
    # Dates
    transaction_date: date = Field(description="Date of transaction")
    filed_date: Optional[date] = Field(default=None, description="Date filed with authority")
    
    # Amounts
    amount: Decimal = Field(description="Transaction amount")
    description: Optional[str] = Field(default=None, max_length=500, description="Transaction description")
    
    # Source information
    source_type: Optional[str] = Field(default=None, max_length=100, description="Source type (Individual, Business, PAC, etc.)")
    
    # Amendment tracking
    amended: Optional[bool] = Field(default=None, description="Whether this is an amended filing")
    amendment_reason: Optional[str] = Field(default=None, max_length=200, description="Reason for amendment")
    
    # State-specific fields
    state: str = Field(description="State where transaction occurred")
    raw_data: Optional[dict] = Field(default=None, description="Original raw data from source")

# Generic Contribution Model
class CampaignContribution(SQLModel, table=True):
    __tablename__ = "campaign_contributions"
    __table_args__ = {"schema": "campaign_finance"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Transaction identification
    transaction_id: str = Field(unique=True, description="Unique contribution identifier")
    contribution_type: str = Field(description="Type of contribution (Monetary, In-Kind, Loan, etc.)")
    
    # Dates
    contribution_date: date = Field(description="Date of contribution")
    filed_date: Optional[date] = Field(default=None, description="Date filed with authority")
    
    # Amounts
    amount: Decimal = Field(description="Contribution amount")
    description: Optional[str] = Field(default=None, max_length=500, description="Contribution description")
    
    # Source information
    source_type: Optional[str] = Field(default=None, max_length=100, description="Source type (Individual, Business, PAC, etc.)")
    
    # Amendment tracking
    amended: Optional[bool] = Field(default=None, description="Whether this is an amended filing")
    
    # Foreign keys to normalized tables
    committee_id: int = Field(foreign_key="campaign_finance.campaign_committees.id")
    committee: CampaignCommittee = Relationship(back_populates="contributions")
    
    contributor_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_persons.id")
    contributor: Optional[CampaignPerson] = Relationship(back_populates="contributions")
    
    contributor_address_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_addresses.id")
    contributor_address: Optional[CampaignAddress] = Relationship(back_populates="contributions")
    
    # State-specific fields
    state: str = Field(description="State where contribution occurred")
    raw_data: Optional[dict] = Field(default=None, description="Original raw data from source")

# Generic Expenditure Model
class CampaignExpenditure(SQLModel, table=True):
    __tablename__ = "campaign_expenditures"
    __table_args__ = {"schema": "campaign_finance"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Transaction identification
    transaction_id: str = Field(unique=True, description="Unique expenditure identifier")
    expenditure_type: str = Field(description="Type of expenditure")
    
    # Dates
    expenditure_date: date = Field(description="Date of expenditure")
    filed_date: Optional[date] = Field(default=None, description="Date filed with authority")
    
    # Amounts
    amount: Decimal = Field(description="Expenditure amount")
    description: Optional[str] = Field(default=None, max_length=500, description="Expenditure description")
    purpose: Optional[str] = Field(default=None, max_length=200, description="Purpose of expenditure")
    
    # Amendment tracking
    amended: Optional[bool] = Field(default=None, description="Whether this is an amended filing")
    
    # Foreign keys to normalized tables
    committee_id: int = Field(foreign_key="campaign_finance.campaign_committees.id")
    committee: CampaignCommittee = Relationship(back_populates="expenditures")
    
    payee_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_persons.id")
    payee: Optional[CampaignPerson] = Relationship(back_populates="expenditures")
    
    payee_address_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_addresses.id")
    payee_address: Optional[CampaignAddress] = Relationship(back_populates="expenditures")
    
    # State-specific fields
    state: str = Field(description="State where expenditure occurred")
    raw_data: Optional[dict] = Field(default=None, description="Original raw data from source")

# Generic Loan Model
class CampaignLoan(SQLModel, table=True):
    __tablename__ = "campaign_loans"
    __table_args__ = {"schema": "campaign_finance"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Transaction identification
    transaction_id: str = Field(unique=True, description="Unique loan identifier")
    loan_type: str = Field(description="Type of loan")
    
    # Dates
    loan_date: date = Field(description="Date of loan")
    filed_date: Optional[date] = Field(default=None, description="Date filed with authority")
    maturity_date: Optional[date] = Field(default=None, description="Loan maturity date")
    
    # Amounts
    amount: Decimal = Field(description="Loan amount")
    description: Optional[str] = Field(default=None, max_length=500, description="Loan description")
    
    # Loan terms
    interest_rate: Optional[str] = Field(default=None, max_length=20, description="Interest rate")
    collateral: Optional[bool] = Field(default=None, description="Whether loan is collateralized")
    collateral_description: Optional[str] = Field(default=None, max_length=200, description="Collateral description")
    
    # Loan status
    status: Optional[str] = Field(default=None, max_length=50, description="Loan status")
    payment_made: Optional[bool] = Field(default=None, description="Whether payment has been made")
    payment_amount: Optional[Decimal] = Field(default=None, description="Payment amount")
    
    # Amendment tracking
    amended: Optional[bool] = Field(default=None, description="Whether this is an amended filing")
    
    # Foreign keys to normalized tables
    committee_id: int = Field(foreign_key="campaign_finance.campaign_committees.id")
    committee: CampaignCommittee = Relationship(back_populates="loans")
    
    lender_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_persons.id")
    lender: Optional[CampaignPerson] = Relationship(back_populates="loans")
    
    lender_address_id: Optional[int] = Field(default=None, foreign_key="campaign_finance.campaign_addresses.id")
    lender_address: Optional[CampaignAddress] = Relationship(back_populates="loans")
    
    # State-specific fields
    state: str = Field(description="State where loan occurred")
    raw_data: Optional[dict] = Field(default=None, description="Original raw data from source")

# Generic Report Model
class CampaignReport(SQLModel, table=True):
    __tablename__ = "campaign_reports"
    __table_args__ = {"schema": "campaign_finance"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Report identification
    report_id: str = Field(unique=True, description="Unique report identifier")
    report_type: str = Field(description="Type of report")
    
    # Dates
    report_date: date = Field(description="Date of report")
    period_start: Optional[date] = Field(default=None, description="Reporting period start")
    period_end: Optional[date] = Field(default=None, description="Reporting period end")
    
    # Committee
    committee_id: int = Field(foreign_key="campaign_finance.campaign_committees.id")
    
    # Financial totals
    total_contributions: Optional[Decimal] = Field(default=None, description="Total contributions")
    total_expenditures: Optional[Decimal] = Field(default=None, description="Total expenditures")
    total_loans: Optional[Decimal] = Field(default=None, description="Total loans")
    cash_on_hand: Optional[Decimal] = Field(default=None, description="Cash on hand")
    
    # State-specific fields
    state: str = Field(description="State where report was filed")
    raw_data: Optional[dict] = Field(default=None, description="Original raw data from source")

# Update relationships in base models
CampaignPerson.model_rebuild()
CampaignAddress.model_rebuild() 