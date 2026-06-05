from datetime import date
from typing import Optional

from pydantic import model_validator
from pydantic_core import PydanticCustomError
from sqlmodel import Field

from .texas_settings import TECSettings


class DebtData(TECSettings, table=True):
    __tablename__ = "tx_debt_data"
    __table_args__ = {"schema": "texas"}

    id: Optional[str] = Field(default=None, description="Unique record ID")
    record_type: str = Field(..., max_length=20, description="Record type code - always DEBT")
    form_type_cd: str = Field(..., max_length=20, description="TEC form used")
    sched_form_type_cd: str = Field(..., max_length=20, description="TEC Schedule Used")
    report_info_ident: int = Field(..., description="Unique report #")
    received_dt: date = Field(..., description="Date report received by TEC")
    # Optional because some rows omit the flag entirely
    info_only_flag: Optional[bool] = Field(default=None, description="Superseded by other report")
    filer_ident: str = Field(..., max_length=100, description="Filer account #")
    filer_type_cd: str = Field(..., max_length=30, description="Type of filer")
    filer_name: str = Field(..., max_length=200, description="Filer name")
    loan_info_id: int = Field(..., description="Loan unique identifier", primary_key=True)
    # Optional because rows may omit the guarantee flag
    loan_guaranteed_flag: Optional[bool] = Field(
        default=None, description="Loan guaranteed indicator"
    )
    lender_persent_type_cd: str = Field(
        ..., max_length=30, description="Type of lender name data - INDIVIDUAL or ENTITY"
    )

    # ── Name fields: only one group is required, depending on lender type ─────
    # ENTITY rows: lender_name_organization required; individual fields optional
    lender_name_organization: Optional[str] = Field(
        default=None, max_length=100, description="For ENTITY, the lender organization name"
    )
    # INDIVIDUAL rows: last + first required; rest optional
    lender_name_last: Optional[str] = Field(
        default=None, max_length=100, description="For INDIVIDUAL, the lender last name"
    )
    lender_name_suffix_cd: Optional[str] = Field(
        default=None, max_length=30, description="For INDIVIDUAL, the lender name suffix"
    )
    lender_name_first: Optional[str] = Field(
        default=None, max_length=45, description="For INDIVIDUAL, the lender first name"
    )
    lender_name_prefix_cd: Optional[str] = Field(
        default=None, max_length=30, description="For INDIVIDUAL, the lender name prefix"
    )
    lender_name_short: Optional[str] = Field(
        default=None, max_length=25, description="For INDIVIDUAL, the lender short name"
    )

    # ── Address fields: city and country are the minimum required ─────────────
    lender_street_city: Optional[str] = Field(
        default=None, max_length=30, description="Lender street address - city"
    )
    lender_street_state_cd: Optional[str] = Field(
        default=None,
        max_length=2,
        description="Lender street address - state code (for country=USA/UMI only)",
    )
    lender_street_county_cd: Optional[str] = Field(
        default=None, max_length=5, description="Lender street address - Texas county"
    )
    lender_street_country_cd: Optional[str] = Field(
        default=None,
        max_length=3,
        description="Lender street address - country (e.g. USA, UMI, MEX, CAN)",
    )
    lender_street_postal_code: Optional[str] = Field(
        default=None, max_length=20, description="Lender street address - postal code"
    )
    lender_street_region: Optional[str] = Field(
        default=None,
        max_length=30,
        description="Lender street address - region for country other than USA",
    )

    file_origin: str = Field(..., description="File origin", max_length=64)
    download_date: date = Field(..., description="Date file downloaded")

    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values: dict) -> dict:
        return {k: (None if v in ("", '"', "null") else v) for k, v in values.items()}

    @model_validator(mode="before")
    @classmethod
    def check_lender_type(cls, values: dict) -> dict:
        if values.get("lender_persent_type_cd") not in ("INDIVIDUAL", "ENTITY"):
            raise PydanticCustomError(
                "lender_type",
                "Lender type must be INDIVIDUAL or ENTITY",
                {
                    "column": "lender_persent_type_cd",
                    "value": values.get("lender_persent_type_cd"),
                },
            )
        return values

    @model_validator(mode="before")
    @classmethod
    def check_individual_lender_info_filled(cls, values: dict) -> dict:
        if values.get("lender_persent_type_cd") == "INDIVIDUAL":
            if not values.get("lender_name_last") or not values.get("lender_name_first"):
                raise PydanticCustomError(
                    "individual_lender_info",
                    "For INDIVIDUAL lender, last name and first name must be provided",
                    {"column": "lender_persent_type_cd", "value": "INDIVIDUAL"},
                )
        return values

    @model_validator(mode="before")
    @classmethod
    def check_entity_lender_info_filled(cls, values: dict) -> dict:
        if values.get("lender_persent_type_cd") == "ENTITY":
            if not values.get("lender_name_organization"):
                raise PydanticCustomError(
                    "entity_lender_info",
                    "For ENTITY lender, organization name must be provided",
                    {"column": "lender_persent_type_cd", "value": "ENTITY"},
                )
        return values
