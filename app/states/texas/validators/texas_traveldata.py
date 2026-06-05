from datetime import date, datetime
from typing import Optional

from pydantic import Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from .texas_settings import TECSettings


class TECTravelData(TECSettings):
    __tablename__ = "tx_travel_data"
    __table_args__ = {"schema": "texas"}

    id: Optional[str] = Field(default=None, description="Unique record ID")

    # ── Report header ─────────────────────────────────────────────────────
    recordType: str = Field(..., description="Record type code - always TRVL", max_length=20)
    formTypeCd: str = Field(..., description="TEC form used", max_length=20)
    schedFormTypeCd: str = Field(..., description="TEC Schedule Used", max_length=20)
    reportInfoIdent: int = Field(..., description="Unique report #")
    receivedDt: date = Field(..., description="Date report received by TEC")
    # Optional because some rows omit the flag; was incorrectly marked required.
    infoOnlyFlag: Optional[bool] = Field(default=None, description="Superseded by other report")

    # ── Filer ─────────────────────────────────────────────────────────────
    filerIdent: str = Field(..., description="Filer account #", max_length=100)
    filerTypeCd: str = Field(..., description="Type of filer", max_length=30)
    filerName: str = Field(..., description="Filer name", max_length=200)

    # ── Travel record ─────────────────────────────────────────────────────
    travelInfoId: int = Field(..., description="Travel unique identifier", primary_key=True)
    parentType: str = Field(
        ...,
        description="Parent record type (CONTRIB, EXPEND, PLEDGE)",
        max_length=20,
    )
    parentId: int = Field(..., description="Parent unique identifier")
    parentDt: datetime = Field(..., description="Date of parent transaction")
    parentAmount: float = Field(..., description="Amount of parent transaction")
    # Optional — absent when the parent has no associated full name
    parentFullName: Optional[str] = Field(
        default=None, description="Full name associated with parent", max_length=100
    )
    transportationTypeCd: str = Field(
        ...,
        description="Type of transportation (COMMAIR, PRIVAIR, etc)",
        max_length=30,
    )
    transportationTypeDescr: str = Field(
        ..., description="Transportation type description", max_length=100
    )
    departureCity: str = Field(..., description="Departure city", max_length=50)
    arrivalCity: str = Field(..., description="Arrival city", max_length=50)
    departureDt: date = Field(..., description="Departure date")
    arrivalDt: date = Field(..., description="Arrival date")
    travelPurpose: str = Field(..., description="Purpose of travel", max_length=255)

    # ── Traveller identity ────────────────────────────────────────────────
    travellerPersentTypeCd: str = Field(
        ...,
        description="Type of traveller name data - INDIVIDUAL or ENTITY",
        max_length=30,
    )
    # ENTITY only
    travellerNameOrganization: Optional[str] = Field(
        default=None, description="For ENTITY, the traveller organization name", max_length=100
    )
    # INDIVIDUAL only
    travellerNameLast: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the traveller last name", max_length=100
    )
    travellerNameSuffixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the traveller name suffix", max_length=30
    )
    travellerNameFirst: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the traveller first name", max_length=45
    )
    travellerNamePrefixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the traveller name prefix", max_length=30
    )
    travellerNameShort: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the traveller short name", max_length=25
    )

    # ── Ingestion metadata ────────────────────────────────────────────────
    file_origin: str = Field(..., description="File origin", max_length=64)
    download_date: date = Field(..., description="Date file downloaded")

    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values: dict) -> dict:
        return {k: (None if v in ("", '"', "null") else v) for k, v in values.items()}

    @field_validator("travellerPersentTypeCd", mode="before")
    @classmethod
    def check_traveller_type(cls, v: str) -> str:
        if v not in ("INDIVIDUAL", "ENTITY"):
            raise PydanticCustomError(
                "invalid_traveller_type",
                "Traveller type must be INDIVIDUAL or ENTITY",
                {"value": v},
            )
        return v

    @model_validator(mode="before")
    @classmethod
    def check_name_fields(cls, values: dict) -> dict:
        ttype = values.get("travellerPersentTypeCd")
        if ttype == "INDIVIDUAL" and not values.get("travellerNameLast"):
            raise PydanticCustomError(
                "missing_required_value",
                "travellerNameLast is required for INDIVIDUAL travellerPersentTypeCd",
                {"column": "travellerNameLast"},
            )
        if ttype == "ENTITY" and not values.get("travellerNameOrganization"):
            raise PydanticCustomError(
                "missing_required_value",
                "travellerNameOrganization is required for ENTITY travellerPersentTypeCd",
                {"column": "travellerNameOrganization"},
            )
        return values
