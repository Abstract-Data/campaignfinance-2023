"""Texas TEC DEBT record validator.

The TEC debt file (debts_YYYYMMDD.csv) carries 89 columns: a report header,
lender identity (INDIVIDUAL or ENTITY), lender address, and five optional
guarantor blocks (13 fields each).

There are NO amount, interest-rate, or balance fields in the TEC debt CSV —
those are intentionally absent from this model.
"""

from datetime import date
from typing import Optional

from pydantic import model_validator
from pydantic_core import PydanticCustomError
from sqlmodel import Field

from .texas_settings import TECSettings


class DebtData(TECSettings, table=True):
    __tablename__ = "tx_debt_data"
    __table_args__ = {"schema": "texas"}

    # ── Ingestion metadata (snake_case, kept exactly as before) ──────────
    id: Optional[str] = Field(default=None, description="Unique record ID")

    # ── Report header ─────────────────────────────────────────────────────
    recordType: str = Field(..., max_length=20, description="Record type code - always DEBT")
    formTypeCd: str = Field(..., max_length=20, description="TEC form used")
    schedFormTypeCd: str = Field(..., max_length=20, description="TEC Schedule Used")
    reportInfoIdent: int = Field(..., description="Unique report #")
    receivedDt: date = Field(..., description="Date report received by TEC")
    # Optional because some rows omit the flag entirely
    infoOnlyFlag: Optional[bool] = Field(default=None, description="Superseded by other report")

    # ── Filer ─────────────────────────────────────────────────────────────
    filerIdent: str = Field(..., max_length=100, description="Filer account #")
    filerTypeCd: str = Field(..., max_length=30, description="Type of filer")
    filerName: str = Field(..., max_length=200, description="Filer name")

    # ── Loan record ───────────────────────────────────────────────────────
    loanInfoId: int = Field(
        ..., description="Loan unique identifier", primary_key=True
    )
    # Optional because rows may omit the guarantee flag
    loanGuaranteedFlag: Optional[bool] = Field(
        default=None, description="Loan guaranteed indicator"
    )

    # ── Lender identity ───────────────────────────────────────────────────
    lenderPersentTypeCd: str = Field(
        ..., max_length=30, description="Type of lender name data - INDIVIDUAL or ENTITY"
    )

    # ENTITY rows: lenderNameOrganization required; individual fields optional
    lenderNameOrganization: Optional[str] = Field(
        default=None, max_length=100, description="For ENTITY, the lender organization name"
    )
    # INDIVIDUAL rows: last + first required; rest optional
    lenderNameLast: Optional[str] = Field(
        default=None, max_length=100, description="For INDIVIDUAL, the lender last name"
    )
    lenderNameSuffixCd: Optional[str] = Field(
        default=None, max_length=30, description="For INDIVIDUAL, the lender name suffix"
    )
    lenderNameFirst: Optional[str] = Field(
        default=None, max_length=45, description="For INDIVIDUAL, the lender first name"
    )
    lenderNamePrefixCd: Optional[str] = Field(
        default=None, max_length=30, description="For INDIVIDUAL, the lender name prefix"
    )
    lenderNameShort: Optional[str] = Field(
        default=None, max_length=25, description="For INDIVIDUAL, the lender short name"
    )

    # ── Lender address ────────────────────────────────────────────────────
    lenderStreetCity: Optional[str] = Field(
        default=None, max_length=30, description="Lender street address - city"
    )
    lenderStreetStateCd: Optional[str] = Field(
        default=None,
        max_length=2,
        description="Lender street address - state code (for country=USA/UMI only)",
    )
    lenderStreetCountyCd: Optional[str] = Field(
        default=None, max_length=5, description="Lender street address - Texas county"
    )
    lenderStreetCountryCd: Optional[str] = Field(
        default=None,
        max_length=3,
        description="Lender street address - country (e.g. USA, UMI, MEX, CAN)",
    )
    lenderStreetPostalCode: Optional[str] = Field(
        default=None, max_length=20, description="Lender street address - postal code"
    )
    lenderStreetRegion: Optional[str] = Field(
        default=None,
        max_length=30,
        description="Lender street address - region for country other than USA",
    )

    # ── Guarantor block 1 (13 fields) ─────────────────────────────────────
    guarantorPersentTypeCd1: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 1: type of name data"
    )
    guarantorNameOrganization1: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 1: organization name"
    )
    guarantorNameLast1: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 1: last name"
    )
    guarantorNameSuffixCd1: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 1: name suffix"
    )
    guarantorNameFirst1: Optional[str] = Field(
        default=None, max_length=45, description="Guarantor 1: first name"
    )
    guarantorNamePrefixCd1: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 1: name prefix"
    )
    guarantorNameShort1: Optional[str] = Field(
        default=None, max_length=25, description="Guarantor 1: short name"
    )
    guarantorStreetCity1: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 1: street city"
    )
    guarantorStreetStateCd1: Optional[str] = Field(
        default=None, max_length=2, description="Guarantor 1: state code"
    )
    guarantorStreetCountyCd1: Optional[str] = Field(
        default=None, max_length=5, description="Guarantor 1: county code"
    )
    guarantorStreetCountryCd1: Optional[str] = Field(
        default=None, max_length=3, description="Guarantor 1: country code"
    )
    guarantorStreetPostalCode1: Optional[str] = Field(
        default=None, max_length=20, description="Guarantor 1: postal code"
    )
    guarantorStreetRegion1: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 1: region"
    )

    # ── Guarantor block 2 (13 fields) ─────────────────────────────────────
    guarantorPersentTypeCd2: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 2: type of name data"
    )
    guarantorNameOrganization2: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 2: organization name"
    )
    guarantorNameLast2: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 2: last name"
    )
    guarantorNameSuffixCd2: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 2: name suffix"
    )
    guarantorNameFirst2: Optional[str] = Field(
        default=None, max_length=45, description="Guarantor 2: first name"
    )
    guarantorNamePrefixCd2: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 2: name prefix"
    )
    guarantorNameShort2: Optional[str] = Field(
        default=None, max_length=25, description="Guarantor 2: short name"
    )
    guarantorStreetCity2: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 2: street city"
    )
    guarantorStreetStateCd2: Optional[str] = Field(
        default=None, max_length=2, description="Guarantor 2: state code"
    )
    guarantorStreetCountyCd2: Optional[str] = Field(
        default=None, max_length=5, description="Guarantor 2: county code"
    )
    guarantorStreetCountryCd2: Optional[str] = Field(
        default=None, max_length=3, description="Guarantor 2: country code"
    )
    guarantorStreetPostalCode2: Optional[str] = Field(
        default=None, max_length=20, description="Guarantor 2: postal code"
    )
    guarantorStreetRegion2: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 2: region"
    )

    # ── Guarantor block 3 (13 fields) ─────────────────────────────────────
    guarantorPersentTypeCd3: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 3: type of name data"
    )
    guarantorNameOrganization3: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 3: organization name"
    )
    guarantorNameLast3: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 3: last name"
    )
    guarantorNameSuffixCd3: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 3: name suffix"
    )
    guarantorNameFirst3: Optional[str] = Field(
        default=None, max_length=45, description="Guarantor 3: first name"
    )
    guarantorNamePrefixCd3: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 3: name prefix"
    )
    guarantorNameShort3: Optional[str] = Field(
        default=None, max_length=25, description="Guarantor 3: short name"
    )
    guarantorStreetCity3: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 3: street city"
    )
    guarantorStreetStateCd3: Optional[str] = Field(
        default=None, max_length=2, description="Guarantor 3: state code"
    )
    guarantorStreetCountyCd3: Optional[str] = Field(
        default=None, max_length=5, description="Guarantor 3: county code"
    )
    guarantorStreetCountryCd3: Optional[str] = Field(
        default=None, max_length=3, description="Guarantor 3: country code"
    )
    guarantorStreetPostalCode3: Optional[str] = Field(
        default=None, max_length=20, description="Guarantor 3: postal code"
    )
    guarantorStreetRegion3: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 3: region"
    )

    # ── Guarantor block 4 (13 fields) ─────────────────────────────────────
    guarantorPersentTypeCd4: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 4: type of name data"
    )
    guarantorNameOrganization4: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 4: organization name"
    )
    guarantorNameLast4: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 4: last name"
    )
    guarantorNameSuffixCd4: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 4: name suffix"
    )
    guarantorNameFirst4: Optional[str] = Field(
        default=None, max_length=45, description="Guarantor 4: first name"
    )
    guarantorNamePrefixCd4: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 4: name prefix"
    )
    guarantorNameShort4: Optional[str] = Field(
        default=None, max_length=25, description="Guarantor 4: short name"
    )
    guarantorStreetCity4: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 4: street city"
    )
    guarantorStreetStateCd4: Optional[str] = Field(
        default=None, max_length=2, description="Guarantor 4: state code"
    )
    guarantorStreetCountyCd4: Optional[str] = Field(
        default=None, max_length=5, description="Guarantor 4: county code"
    )
    guarantorStreetCountryCd4: Optional[str] = Field(
        default=None, max_length=3, description="Guarantor 4: country code"
    )
    guarantorStreetPostalCode4: Optional[str] = Field(
        default=None, max_length=20, description="Guarantor 4: postal code"
    )
    guarantorStreetRegion4: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 4: region"
    )

    # ── Guarantor block 5 (13 fields) ─────────────────────────────────────
    guarantorPersentTypeCd5: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 5: type of name data"
    )
    guarantorNameOrganization5: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 5: organization name"
    )
    guarantorNameLast5: Optional[str] = Field(
        default=None, max_length=100, description="Guarantor 5: last name"
    )
    guarantorNameSuffixCd5: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 5: name suffix"
    )
    guarantorNameFirst5: Optional[str] = Field(
        default=None, max_length=45, description="Guarantor 5: first name"
    )
    guarantorNamePrefixCd5: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 5: name prefix"
    )
    guarantorNameShort5: Optional[str] = Field(
        default=None, max_length=25, description="Guarantor 5: short name"
    )
    guarantorStreetCity5: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 5: street city"
    )
    guarantorStreetStateCd5: Optional[str] = Field(
        default=None, max_length=2, description="Guarantor 5: state code"
    )
    guarantorStreetCountyCd5: Optional[str] = Field(
        default=None, max_length=5, description="Guarantor 5: county code"
    )
    guarantorStreetCountryCd5: Optional[str] = Field(
        default=None, max_length=3, description="Guarantor 5: country code"
    )
    guarantorStreetPostalCode5: Optional[str] = Field(
        default=None, max_length=20, description="Guarantor 5: postal code"
    )
    guarantorStreetRegion5: Optional[str] = Field(
        default=None, max_length=30, description="Guarantor 5: region"
    )

    # ── Ingestion metadata (snake_case, kept exactly as before) ──────────
    file_origin: str = Field(..., description="File origin", max_length=64)
    download_date: date = Field(..., description="Date file downloaded")

    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values: dict) -> dict:
        return {k: (None if v in ("", '"', "null") else v) for k, v in values.items()}

    @model_validator(mode="before")
    @classmethod
    def check_lender_type(cls, values: dict) -> dict:
        lender_type = values.get("lenderPersentTypeCd")
        if lender_type not in ("INDIVIDUAL", "ENTITY"):
            raise PydanticCustomError(
                "lender_type",
                "Lender type must be INDIVIDUAL or ENTITY",
                {
                    "column": "lenderPersentTypeCd",
                    "value": lender_type,
                },
            )
        return values

    @model_validator(mode="before")
    @classmethod
    def check_individual_lender_info_filled(cls, values: dict) -> dict:
        if values.get("lenderPersentTypeCd") == "INDIVIDUAL":
            if not values.get("lenderNameLast") or not values.get("lenderNameFirst"):
                raise PydanticCustomError(
                    "individual_lender_info",
                    "For INDIVIDUAL lender, last name and first name must be provided",
                    {"column": "lenderPersentTypeCd", "value": "INDIVIDUAL"},
                )
        return values

    @model_validator(mode="before")
    @classmethod
    def check_entity_lender_info_filled(cls, values: dict) -> dict:
        if values.get("lenderPersentTypeCd") == "ENTITY":
            if not values.get("lenderNameOrganization"):
                raise PydanticCustomError(
                    "entity_lender_info",
                    "For ENTITY lender, organization name must be provided",
                    {"column": "lenderPersentTypeCd", "value": "ENTITY"},
                )
        return values
