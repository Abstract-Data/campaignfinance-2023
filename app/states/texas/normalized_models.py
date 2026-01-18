from typing import Optional, List
from datetime import date
from decimal import Decimal
from pydantic import ConfigDict, Field
from sqlmodel import SQLModel, Field, Relationship
from .validators.texas_settings import TECSettings
import app.funcs.validator_functions as funcs

# Base Address Model
class TECAddress(SQLModel, table=True):
    __tablename__ = "tx_addresses"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    address_hash: str = Field(unique=True, index=True, description="Hash of address for deduplication")
    
    # Address fields
    street_addr1: Optional[str] = Field(default=None, max_length=55)
    street_addr2: Optional[str] = Field(default=None, max_length=55)
    city: Optional[str] = Field(default=None, max_length=30)
    state_cd: Optional[str] = Field(default=None, max_length=2)
    county_cd: Optional[str] = Field(default=None, max_length=5)
    country_cd: Optional[str] = Field(default=None, max_length=3)
    postal_code: Optional[str] = Field(default=None, max_length=20)
    region: Optional[str] = Field(default=None, max_length=30)
    
    # Mailing address fields (if different from street)
    mailing_addr1: Optional[str] = Field(default=None, max_length=55)
    mailing_addr2: Optional[str] = Field(default=None, max_length=55)
    mailing_city: Optional[str] = Field(default=None, max_length=30)
    mailing_state_cd: Optional[str] = Field(default=None, max_length=2)
    mailing_county_cd: Optional[str] = Field(default=None, max_length=5)
    mailing_country_cd: Optional[str] = Field(default=None, max_length=3)
    mailing_postal_code: Optional[str] = Field(default=None, max_length=20)
    mailing_region: Optional[str] = Field(default=None, max_length=30)
    
    # Phone fields
    primary_usa_phone_flag: Optional[bool] = Field(default=None)
    primary_phone_number: Optional[str] = Field(default=None, max_length=20)
    primary_phone_ext: Optional[str] = Field(default=None, max_length=10)
    
    # Relationships
    persons: List["TECPerson"] = Relationship(back_populates="address")
    contributors: List["TECContribution"] = Relationship(back_populates="contributor_address")
    payees: List["TECExpenditure"] = Relationship(back_populates="payee_address")
    lenders: List["TECLoan"] = Relationship(back_populates="lender_address")
    pledgers: List["TECPledge"] = Relationship(back_populates="pledger_address")
    filers: List["TECFiler"] = Relationship(back_populates="filer_address")
    treasurers: List["TECFiler"] = Relationship(back_populates="treasurer_address")
    chairs: List["TECFiler"] = Relationship(back_populates="chair_address")
    debt_lenders: List["TECDebt"] = Relationship(back_populates="lender_address")
    credit_payors: List["TECCredit"] = Relationship(back_populates="payor_address")
    cover_sheet1_filer: List["TECCoverSheet1"] = Relationship(back_populates="filer_address")
    cover_sheet1_treasurer: List["TECCoverSheet1"] = Relationship(back_populates="treasurer_address")
    cover_sheet1_chair: List["TECCoverSheet1"] = Relationship(back_populates="chair_address")

# Base Person Model
class TECPerson(SQLModel, table=True):
    __tablename__ = "tx_persons"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    person_hash: str = Field(unique=True, index=True, description="Hash of person data for deduplication")
    
    # Person type
    person_type: str = Field(description="INDIVIDUAL or ENTITY")
    
    # Name fields
    name_organization: Optional[str] = Field(default=None, max_length=100)
    name_last: Optional[str] = Field(default=None, max_length=100)
    name_first: Optional[str] = Field(default=None, max_length=45)
    name_prefix_cd: Optional[str] = Field(default=None, max_length=30)
    name_suffix_cd: Optional[str] = Field(default=None, max_length=30)
    name_short: Optional[str] = Field(default=None, max_length=25)
    
    # Employment fields
    employer: Optional[str] = Field(default=None, max_length=60)
    occupation: Optional[str] = Field(default=None, max_length=60)
    job_title: Optional[str] = Field(default=None, max_length=60)
    
    # PAC fields
    pac_fein: Optional[str] = Field(default=None, max_length=12)
    oos_pac_flag: Optional[bool] = Field(default=None)
    
    # Law firm fields
    law_firm_name: Optional[str] = Field(default=None, max_length=60)
    spouse_law_firm_name: Optional[str] = Field(default=None, max_length=60)
    parent1_law_firm_name: Optional[str] = Field(default=None, max_length=60)
    parent2_law_firm_name: Optional[str] = Field(default=None, max_length=60)
    
    # Foreign key to address
    address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    address: Optional[TECAddress] = Relationship(back_populates="persons")
    
    # Relationships to various record types
    contributions: List["TECContribution"] = Relationship(back_populates="contributor")
    expenditures: List["TECExpenditure"] = Relationship(back_populates="payee")
    loans: List["TECLoan"] = Relationship(back_populates="lender")
    pledges: List["TECPledge"] = Relationship(back_populates="pledger")
    debts: List["TECDebt"] = Relationship(back_populates="lender")
    credits: List["TECCredit"] = Relationship(back_populates="payor")
    travel: List["TECTravel"] = Relationship(back_populates="traveller")
    filers: List["TECFiler"] = Relationship(back_populates="filer_person")
    treasurers: List["TECFiler"] = Relationship(back_populates="treasurer_person")
    chairs: List["TECFiler"] = Relationship(back_populates="chair_person")
    guarantors: List["TECLoanGuarantor"] = Relationship(back_populates="guarantor")
    debt_guarantors: List["TECDebtGuarantor"] = Relationship(back_populates="guarantor")
    cover_sheet1_filer: List["TECCoverSheet1"] = Relationship(back_populates="filer_person")
    cover_sheet1_treasurer: List["TECCoverSheet1"] = Relationship(back_populates="treasurer_person")
    cover_sheet1_chair: List["TECCoverSheet1"] = Relationship(back_populates="chair_person")

# Main Record Models
class TECContribution(SQLModel, table=True):
    __tablename__ = "tx_contributions"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="RCPT", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Contribution details
    contribution_info_id: Optional[int] = Field(default=None)
    contribution_dt: Optional[date] = Field(default=None)
    contribution_amount: Optional[Decimal] = Field(default=None)
    contribution_descr: Optional[str] = Field(default=None, max_length=100)
    itemize_flag: Optional[bool] = Field(default=None)
    travel_flag: Optional[bool] = Field(default=None)
    
    # Foreign keys to normalized tables
    contributor_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    contributor: Optional[TECPerson] = Relationship(back_populates="contributions")
    
    contributor_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    contributor_address: Optional[TECAddress] = Relationship(back_populates="contributors")

    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="contributions")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="contributions")

class TECExpenditure(SQLModel, table=True):
    __tablename__ = "tx_expenditures"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="EXPN", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Expenditure details
    expend_info_id: Optional[int] = Field(default=None)
    expend_dt: Optional[date] = Field(default=None)
    expend_amount: Optional[Decimal] = Field(default=None)
    expend_descr: Optional[str] = Field(default=None, max_length=100)
    expend_cat_cd: Optional[str] = Field(default=None, max_length=30)
    expend_cat_descr: Optional[str] = Field(default=None, max_length=100)
    itemize_flag: Optional[bool] = Field(default=None)
    travel_flag: Optional[bool] = Field(default=None)
    political_expend_cd: Optional[str] = Field(default=None, max_length=30)
    reimburse_intended_flag: Optional[bool] = Field(default=None)
    src_corp_contrib_flag: Optional[bool] = Field(default=None)
    capital_living_exp_flag: Optional[bool] = Field(default=None)
    credit_card_issuer: Optional[str] = Field(default=None, max_length=100)
    repayment_dt: Optional[date] = Field(default=None)
    
    # Foreign keys to normalized tables
    payee_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    payee: Optional[TECPerson] = Relationship(back_populates="expenditures")
    
    payee_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    payee_address: Optional[TECAddress] = Relationship(back_populates="payees")

    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="expenditures")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="expenditures")

class TECLoan(SQLModel, table=True):
    __tablename__ = "tx_loans"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="LOAN", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Loan details
    loan_info_id: Optional[int] = Field(default=None)
    loan_dt: Optional[date] = Field(default=None)
    loan_amount: Optional[Decimal] = Field(default=None)
    loan_descr: Optional[str] = Field(default=None, max_length=100)
    interest_rate: Optional[str] = Field(default=None, max_length=15)
    maturity_dt: Optional[date] = Field(default=None)
    collateral_flag: Optional[bool] = Field(default=None)
    collateral_descr: Optional[str] = Field(default=None, max_length=100)
    loan_status_cd: Optional[str] = Field(default=None, max_length=30)
    payment_made_flag: Optional[bool] = Field(default=None)
    payment_amount: Optional[Decimal] = Field(default=None)
    payment_source: Optional[str] = Field(default=None, max_length=100)
    loan_guaranteed_flag: Optional[bool] = Field(default=None)
    financial_institution_flag: Optional[bool] = Field(default=None)
    loan_guarantee_amount: Optional[Decimal] = Field(default=None)
    
    # Foreign keys to normalized tables
    lender_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    lender: Optional[TECPerson] = Relationship(back_populates="loans")
    
    lender_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    lender_address: Optional[TECAddress] = Relationship(back_populates="lenders")

    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="loans")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="loans")
    
    # Guarantors relationship
    guarantors: List["TECLoanGuarantor"] = Relationship(back_populates="loan")

class TECPledge(SQLModel, table=True):
    __tablename__ = "tx_pledges"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="PLDG", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Pledge details
    pledge_info_id: Optional[int] = Field(default=None)
    pledge_dt: Optional[date] = Field(default=None)
    pledge_amount: Optional[Decimal] = Field(default=None)
    pledge_descr: Optional[str] = Field(default=None, max_length=100)
    itemize_flag: Optional[bool] = Field(default=None)
    travel_flag: Optional[bool] = Field(default=None)
    
    # Foreign keys to normalized tables
    pledger_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    pledger: Optional[TECPerson] = Relationship(back_populates="pledges")
    
    pledger_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    pledger_address: Optional[TECAddress] = Relationship(back_populates="pledgers")

    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="pledges")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="pledges")

class TECFiler(SQLModel, table=True):
    __tablename__ = "tx_filers"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="FILER", description="Record type code")
    filer_ident: str = Field(max_length=100, unique=True)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Filer status
    unexpend_contrib_filer_flag: Optional[bool] = Field(default=None)
    modified_elect_cycle_flag: Optional[bool] = Field(default=None)
    filer_jdi_cd: Optional[str] = Field(default=None, max_length=30)
    committee_status_cd: Optional[str] = Field(default=None, max_length=30)
    
    # Office information
    filer_hold_office_cd: Optional[str] = Field(default=None, max_length=30)
    filer_hold_office_district: Optional[str] = Field(default=None, max_length=11)
    filer_hold_office_place: Optional[str] = Field(default=None, max_length=11)
    filer_hold_office_descr: Optional[str] = Field(default=None, max_length=100)
    filer_hold_office_county_cd: Optional[str] = Field(default=None, max_length=5)
    filer_hold_office_county_descr: Optional[str] = Field(default=None, max_length=100)
    
    filer_seek_office_cd: Optional[str] = Field(default=None, max_length=30)
    filer_seek_office_district: Optional[str] = Field(default=None, max_length=11)
    filer_seek_office_place: Optional[str] = Field(default=None, max_length=11)
    filer_seek_office_descr: Optional[str] = Field(default=None, max_length=100)
    filer_seek_office_county_cd: Optional[str] = Field(default=None, max_length=5)
    filer_seek_office_county_descr: Optional[str] = Field(default=None, max_length=100)
    
    # Status and dates
    filer_status_cd: Optional[str] = Field(default=None, max_length=30)
    filer_eff_start_dt: Optional[date] = Field(default=None)
    filer_eff_stop_dt: Optional[date] = Field(default=None)
    
    # Foreign keys to normalized tables
    filer_person_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    filer_person: Optional[TECPerson] = Relationship(back_populates="filers")
    
    filer_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    filer_address: Optional[TECAddress] = Relationship(back_populates="filers")
    
    treasurer_person_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    treasurer_person: Optional[TECPerson] = Relationship(back_populates="treasurers")
    
    treasurer_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    treasurer_address: Optional[TECAddress] = Relationship(back_populates="treasurers")
    
    chair_person_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    chair_person: Optional[TECPerson] = Relationship(back_populates="chairs")
    
    chair_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    chair_address: Optional[TECAddress] = Relationship(back_populates="chairs") 

    # Reverse relationships
    cover_sheets: List["TECCoverSheet1"] = Relationship(back_populates="filer")
    contributions: List["TECContribution"] = Relationship(back_populates="filer")
    expenditures: List["TECExpenditure"] = Relationship(back_populates="filer")
    loans: List["TECLoan"] = Relationship(back_populates="filer")
    pledges: List["TECPledge"] = Relationship(back_populates="filer")
    debts: List["TECDebt"] = Relationship(back_populates="filer")
    credits: List["TECCredit"] = Relationship(back_populates="filer")
    assets: List["TECAsset"] = Relationship(back_populates="filer")
    candidates: List["TECCandidate"] = Relationship(back_populates="filer")
    travel_records: List["TECTravel"] = Relationship(back_populates="filer")

# Guarantor Models
class TECLoanGuarantor(SQLModel, table=True):
    __tablename__ = "tx_loan_guarantors"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Foreign keys
    loan_id: int = Field(foreign_key="texas.tx_loans.id")
    loan: TECLoan = Relationship(back_populates="guarantors")
    
    guarantor_id: int = Field(foreign_key="texas.tx_persons.id")
    guarantor: TECPerson = Relationship(back_populates="guarantors")

class TECDebtGuarantor(SQLModel, table=True):
    __tablename__ = "tx_debt_guarantors"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Foreign keys
    debt_id: int = Field(foreign_key="texas.tx_debts.id")
    debt: "TECDebt" = Relationship(back_populates="guarantors")
    
    guarantor_id: int = Field(foreign_key="texas.tx_persons.id")
    guarantor: TECPerson = Relationship(back_populates="debt_guarantors")

# Additional Record Models
class TECDebt(SQLModel, table=True):
    __tablename__ = "tx_debts"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="DEBT", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Debt details
    loan_info_id: Optional[int] = Field(default=None)
    loan_guaranteed_flag: Optional[bool] = Field(default=None)
    
    # Foreign keys to normalized tables
    lender_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    lender: Optional[TECPerson] = Relationship(back_populates="debts")
    
    lender_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    lender_address: Optional[TECAddress] = Relationship(back_populates="debt_lenders")
    
    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="debts")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="debts")
    
    # Guarantors relationship
    guarantors: List[TECDebtGuarantor] = Relationship(back_populates="debt")

class TECCredit(SQLModel, table=True):
    __tablename__ = "tx_credits"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="CRED", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Credit details
    credit_info_id: Optional[int] = Field(default=None)
    credit_dt: Optional[date] = Field(default=None)
    credit_amount: Optional[Decimal] = Field(default=None)
    credit_descr: Optional[str] = Field(default=None, max_length=100)
    
    # Foreign keys to normalized tables
    payor_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    payor: Optional[TECPerson] = Relationship(back_populates="credits")
    
    payor_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    payor_address: Optional[TECAddress] = Relationship(back_populates="credit_payors")

    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="credits")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="credits")

class TECAsset(SQLModel, table=True):
    __tablename__ = "tx_assets"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="ASSET", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Asset details
    asset_info_id: Optional[int] = Field(default=None)
    asset_descr: Optional[str] = Field(default=None, max_length=100)

    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="assets")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="assets")

class TECCandidate(SQLModel, table=True):
    __tablename__ = "tx_candidates"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="CAND", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Expenditure information
    expend_info_id: Optional[int] = Field(default=None)
    expend_persent_id: Optional[int] = Field(default=None)
    expend_dt: Optional[date] = Field(default=None)
    expend_amount: Optional[Decimal] = Field(default=None)
    expend_descr: Optional[str] = Field(default=None, max_length=100)
    expend_cat_cd: Optional[str] = Field(default=None, max_length=30)
    expend_cat_descr: Optional[str] = Field(default=None, max_length=100)
    itemize_flag: Optional[bool] = Field(default=None)
    political_expend_cd: Optional[str] = Field(default=None, max_length=30)
    reimburse_intended_flag: Optional[bool] = Field(default=None)
    src_corp_contrib_flag: Optional[bool] = Field(default=None)
    capital_living_exp_flag: Optional[bool] = Field(default=None)
    
    # Candidate information
    candidate_persent_type_cd: Optional[str] = Field(default=None, max_length=30)
    candidate_name_organization: Optional[str] = Field(default=None, max_length=100)
    candidate_name_last: Optional[str] = Field(default=None, max_length=100)
    candidate_name_suffix_cd: Optional[str] = Field(default=None, max_length=30)
    candidate_name_first: Optional[str] = Field(default=None, max_length=45)
    candidate_name_prefix_cd: Optional[str] = Field(default=None, max_length=30)
    candidate_name_short: Optional[str] = Field(default=None, max_length=25)
    
    # Office information
    candidate_hold_office_cd: Optional[str] = Field(default=None, max_length=30)
    candidate_hold_office_district: Optional[str] = Field(default=None, max_length=11)
    candidate_hold_office_place: Optional[str] = Field(default=None, max_length=11)
    candidate_hold_office_descr: Optional[str] = Field(default=None, max_length=100)
    candidate_hold_office_county_cd: Optional[str] = Field(default=None, max_length=5)
    candidate_hold_office_county_descr: Optional[str] = Field(default=None, max_length=100)
    
    candidate_seek_office_cd: Optional[str] = Field(default=None, max_length=30)
    candidate_seek_office_district: Optional[str] = Field(default=None, max_length=11)
    candidate_seek_office_place: Optional[str] = Field(default=None, max_length=11)
    candidate_seek_office_descr: Optional[str] = Field(default=None, max_length=100)
    candidate_seek_office_county_cd: Optional[str] = Field(default=None, max_length=5)
    candidate_seek_office_county_descr: Optional[str] = Field(default=None, max_length=100)

    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="candidates")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="candidates")

class TECTravel(SQLModel, table=True):
    __tablename__ = "tx_travel"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="TRVL", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    sched_form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100)
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Travel details
    travel_info_id: Optional[int] = Field(default=None)
    parent_type: Optional[str] = Field(default=None, max_length=20)
    parent_id: Optional[int] = Field(default=None)
    parent_dt: Optional[date] = Field(default=None)
    parent_amount: Optional[Decimal] = Field(default=None)
    parent_full_name: Optional[str] = Field(default=None, max_length=100)
    
    # Transportation details
    transportation_type_cd: Optional[str] = Field(default=None, max_length=30)
    transportation_type_descr: Optional[str] = Field(default=None, max_length=100)
    departure_city: Optional[str] = Field(default=None, max_length=50)
    arrival_city: Optional[str] = Field(default=None, max_length=50)
    departure_dt: Optional[date] = Field(default=None)
    arrival_dt: Optional[date] = Field(default=None)
    travel_purpose: Optional[str] = Field(default=None, max_length=255)
    
    # Foreign keys to normalized tables
    traveller_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    traveller: Optional[TECPerson] = Relationship(back_populates="travel")

    # Foreign keys to Filer and Cover Sheet
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer: Optional["TECFiler"] = Relationship(back_populates="travel_records")

    report_info_ident: Optional[int] = Field(default=None, foreign_key="texas.tx_cover_sheet1.report_info_ident")
    cover_sheet: Optional["TECCoverSheet1"] = Relationship(back_populates="travel_records")

# Cover Sheet Models
class TECCoverSheet1(SQLModel, table=True):
    __tablename__ = "tx_cover_sheet1"
    __table_args__ = {"schema": "texas", "extend_existing": True}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Record metadata
    record_type: str = Field(default="CVR1", description="Record type code")
    form_type_cd: Optional[str] = Field(default=None, max_length=20)
    report_info_ident: Optional[int] = Field(default=None)
    received_dt: Optional[date] = Field(default=None)
    info_only_flag: Optional[bool] = Field(default=None)
    
    # Filer information
    # Filer information
    filer_ident: Optional[str] = Field(default=None, max_length=100, foreign_key="texas.tx_filers.filer_ident")
    filer_type_cd: Optional[str] = Field(default=None, max_length=30)
    filer_name: Optional[str] = Field(default=None, max_length=200)
    
    # Report information
    report_type_cd: Optional[str] = Field(default=None, max_length=30)
    source_category_cd: Optional[str] = Field(default=None, max_length=30)
    report_info_ident: Optional[int] = Field(default=None, sa_column_kwargs={"unique": True})
    due_dt: Optional[date] = Field(default=None)
    filed_dt: Optional[date] = Field(default=None)
    period_start_dt: Optional[date] = Field(default=None)
    period_end_dt: Optional[date] = Field(default=None)
    
    # Financial totals
    unitemized_contrib_amount: Optional[Decimal] = Field(default=None)
    total_contrib_amount: Optional[Decimal] = Field(default=None)
    unitemized_expend_amount: Optional[Decimal] = Field(default=None)
    total_expend_amount: Optional[Decimal] = Field(default=None)
    loan_balance_amount: Optional[Decimal] = Field(default=None)
    contribs_maintained_amount: Optional[Decimal] = Field(default=None)
    unitemized_pledge_amount: Optional[Decimal] = Field(default=None)
    unitemized_loan_amount: Optional[Decimal] = Field(default=None)
    total_interest_earned_amount: Optional[Decimal] = Field(default=None)
    
    # Election information
    election_dt: Optional[date] = Field(default=None)
    election_type_cd: Optional[str] = Field(default=None, max_length=30)
    election_type_descr: Optional[str] = Field(default=None, max_length=100)
    no_activity_flag: Optional[bool] = Field(default=None)
    political_party_cd: Optional[str] = Field(default=None, max_length=30)
    political_division_cd: Optional[str] = Field(default=None, max_length=30)
    political_party_other_descr: Optional[str] = Field(default=None, max_length=100)
    political_party_county_cd: Optional[str] = Field(default=None, max_length=30)
    
    # Other flags
    timely_correction_flag: Optional[bool] = Field(default=None)
    semiannual_checkbox_flag: Optional[bool] = Field(default=None)
    high_contrib_threshold_cd: Optional[str] = Field(default=None, max_length=30)
    software_release: Optional[str] = Field(default=None, max_length=20)
    internet_visible_flag: Optional[bool] = Field(default=None)
    signer_printed_name: Optional[str] = Field(default=None, max_length=100)
    addr_change_filer_flag: Optional[bool] = Field(default=None)
    addr_change_treas_flag: Optional[bool] = Field(default=None)
    addr_change_chair_flag: Optional[bool] = Field(default=None)
    
    # Foreign keys to normalized tables
    filer_person_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    filer_person: Optional[TECPerson] = Relationship(back_populates="cover_sheet1_filer")
    
    filer_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    filer_address: Optional[TECAddress] = Relationship(back_populates="cover_sheet1_filer")
    
    treasurer_person_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    treasurer_person: Optional[TECPerson] = Relationship(back_populates="cover_sheet1_treasurer")
    
    treasurer_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    treasurer_address: Optional[TECAddress] = Relationship(back_populates="cover_sheet1_treasurer")
    
    chair_person_id: Optional[int] = Field(default=None, foreign_key="texas.tx_persons.id")
    chair_person: Optional[TECPerson] = Relationship(back_populates="cover_sheet1_chair")
    
    chair_address_id: Optional[int] = Field(default=None, foreign_key="texas.tx_addresses.id")
    chair_address: Optional[TECAddress] = Relationship(back_populates="cover_sheet1_chair")

    # Relationship to Filer
    filer: Optional["TECFiler"] = Relationship(back_populates="cover_sheets")

    # Reverse relationships
    contributions: List["TECContribution"] = Relationship(back_populates="cover_sheet")
    expenditures: List["TECExpenditure"] = Relationship(back_populates="cover_sheet")
    loans: List["TECLoan"] = Relationship(back_populates="cover_sheet")
    pledges: List["TECPledge"] = Relationship(back_populates="cover_sheet")
    debts: List["TECDebt"] = Relationship(back_populates="cover_sheet")
    credits: List["TECCredit"] = Relationship(back_populates="cover_sheet")
    assets: List["TECAsset"] = Relationship(back_populates="cover_sheet")
    candidates: List["TECCandidate"] = Relationship(back_populates="cover_sheet")
    travel_records: List["TECTravel"] = Relationship(back_populates="cover_sheet")

# Update relationships in base models
TECPerson.model_rebuild()
TECAddress.model_rebuild() 