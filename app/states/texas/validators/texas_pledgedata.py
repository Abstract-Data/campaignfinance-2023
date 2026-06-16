from datetime import date, datetime
from typing import Optional

from pydantic import Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from .texas_settings import TECSettings


class PledgeData(TECSettings):
    id: Optional[str] = Field(default=None, description="Unique record ID")

    # ── Report header ─────────────────────────────────────────────────────
    recordType: str = Field(..., description="Record type code - always PLDG", max_length=20)
    formTypeCd: str = Field(..., description="TEC form used", max_length=20)
    schedFormTypeCd: str = Field(..., description="TEC Schedule Used", max_length=20)
    reportInfoIdent: int = Field(..., description="Unique report #")
    receivedDt: datetime = Field(..., description="Date report received by TEC")
    infoOnlyFlag: str = Field(..., description="Superseded by other report (Y/N)", max_length=1)

    # ── Filer ─────────────────────────────────────────────────────────────
    filerIdent: str = Field(..., description="Filer account #", max_length=100)
    filerTypeCd: str = Field(..., description="Type of filer", max_length=30)
    filerName: str = Field(..., description="Filer name", max_length=200)

    # ── Pledge core ───────────────────────────────────────────────────────
    pledgeInfoId: int = Field(..., description="Pledge unique identifier")
    pledgeDt: datetime = Field(..., description="Pledge date")
    pledgeAmount: float = Field(..., description="Pledge amount")
    pledgeDescr: Optional[str] = Field(
        default=None, description="Pledge description", max_length=100
    )
    itemizeFlag: str = Field(..., description="Y indicates the pledge is itemized", max_length=1)
    travelFlag: str = Field(
        ..., description="Y indicates the pledge has associated travel", max_length=1
    )

    # ── Pledger identity ──────────────────────────────────────────────────
    pledgerPersentTypeCd: str = Field(
        ...,
        description="Type of pledger name data - INDIVIDUAL or ENTITY",
        max_length=30,
    )
    # ENTITY only
    pledgerNameOrganization: Optional[str] = Field(
        default=None,
        description="For ENTITY, the pledger organization name",
        max_length=100,
    )
    # INDIVIDUAL only
    pledgerNameLast: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the pledger last name", max_length=100
    )
    pledgerNameSuffixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the pledger name suffix", max_length=30
    )
    pledgerNameFirst: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the pledger first name", max_length=45
    )
    pledgerNamePrefixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the pledger name prefix", max_length=30
    )
    pledgerNameShort: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the pledger short name", max_length=25
    )

    # ── Pledger address ───────────────────────────────────────────────────
    pledgerStreetCity: Optional[str] = Field(
        default=None, description="Pledger street address - city", max_length=30
    )
    pledgerStreetStateCd: Optional[str] = Field(
        default=None,
        description="Pledger street address - state code (for country=USA/UMI only)",
        max_length=2,
    )
    pledgerStreetCountyCd: Optional[str] = Field(
        default=None, description="Pledger street address - Texas county", max_length=5
    )
    pledgerStreetCountryCd: Optional[str] = Field(
        default=None,
        description="Pledger street address - country (e.g. USA, UMI, MEX, CAN)",
        max_length=3,
    )
    pledgerStreetPostalCode: Optional[str] = Field(
        default=None,
        description="Pledger street address - postal code (USA only)",
        max_length=20,
    )
    pledgerStreetRegion: Optional[str] = Field(
        default=None,
        description="Pledger street address - region for non-USA country",
        max_length=30,
    )

    # ── Employment / law firm — sparse; many rows will be NULL ────────────
    pledgerEmployer: Optional[str] = Field(
        default=None, description="Pledger employer", max_length=60
    )
    pledgerOccupation: Optional[str] = Field(
        default=None, description="Pledger occupation", max_length=60
    )
    pledgerJobTitle: Optional[str] = Field(
        default=None, description="Pledger job title", max_length=60
    )
    pledgerPacFein: Optional[str] = Field(
        default=None, description="For PAC pledger, the FEIN", max_length=12
    )
    pledgerOosPacFlag: Optional[str] = Field(
        default=None, description="Indicates if pledger is an out-of-state PAC (Y/N)", max_length=1
    )
    pledgerLawFirmName: Optional[str] = Field(
        default=None, description="Pledger law firm name", max_length=60
    )
    pledgerSpouseLawFirmName: Optional[str] = Field(
        default=None, description="Pledger spouse law firm name", max_length=60
    )
    pledgerParent1LawFirmName: Optional[str] = Field(
        default=None, description="Pledger parent #1 law firm name", max_length=60
    )
    pledgerParent2LawFirmName: Optional[str] = Field(
        default=None, description="Pledger parent #2 law firm name", max_length=60
    )

    # ── Ingestion metadata ────────────────────────────────────────────────
    file_origin: str = Field(..., description="File origin", max_length=64)
    download_date: date = Field(..., description="Date file downloaded")

    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values: dict) -> dict:
        return {k: (None if v in ("", '"', "null") else v) for k, v in values.items()}

    @field_validator("receivedDt", "pledgeDt", mode="before")
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, "%Y%m%d")
        return v

    @field_validator("infoOnlyFlag", "itemizeFlag", "travelFlag", mode="before")
    @classmethod
    def check_flags(cls, v: str) -> str:
        if str(v).upper() not in ("Y", "N"):
            raise PydanticCustomError("invalid_flag", "Flag must be Y or N", {"value": v})
        return str(v).upper()

    @field_validator("pledgerOosPacFlag", mode="before")
    @classmethod
    def check_oos_pac_flag(cls, v) -> Optional[str]:
        if v is None:
            return None
        if str(v).upper() not in ("Y", "N"):
            raise PydanticCustomError(
                "invalid_flag", "pledgerOosPacFlag must be Y or N", {"value": v}
            )
        return str(v).upper()

    @field_validator("pledgerPersentTypeCd", mode="before")
    @classmethod
    def check_pledger_type(cls, v: str) -> str:
        if v not in ("INDIVIDUAL", "ENTITY"):
            raise PydanticCustomError(
                "invalid_pledger_type",
                "Pledger type must be INDIVIDUAL or ENTITY",
                {"value": v},
            )
        return v

    @model_validator(mode="before")
    @classmethod
    def check_name_fields(cls, values: dict) -> dict:
        ptype = values.get("pledgerPersentTypeCd")
        if ptype == "INDIVIDUAL" and not values.get("pledgerNameLast"):
            raise PydanticCustomError(
                "missing_required_value",
                "pledgerNameLast is required for INDIVIDUAL pledgerPersentTypeCd",
                {"column": "pledgerNameLast"},
            )
        if ptype == "ENTITY" and not values.get("pledgerNameOrganization"):
            raise PydanticCustomError(
                "missing_required_value",
                "pledgerNameOrganization is required for ENTITY pledgerPersentTypeCd",
                {"column": "pledgerNameOrganization"},
            )
        return values
