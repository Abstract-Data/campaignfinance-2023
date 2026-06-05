"""Texas TEC ASSET record validator.

The TEC asset file (assets_YYYYMMDD.csv) only carries a free-text description.
There are NO financial or date fields — asset_type, acquisition_date/cost,
current_value, valuation_date, disposition_date, and disposition_amount will
always be NULL in unified_assets for TEC rows.  That is correct behaviour.
"""

from datetime import date, datetime
from typing import Optional

from pydantic import Field, field_validator
from pydantic_core import PydanticCustomError

from .texas_settings import TECSettings


class AssetRecord(TECSettings):
    """Pydantic model for a single TEC ASSET source row.

    Required fields reflect every column present in the TEC asset CSV.
    Fields that TEC does not provide are intentionally absent — they are
    not mapped and will remain NULL in unified_assets.
    """

    id: Optional[str] = Field(default=None, description="Unique record ID")

    # ── Report header (always present in TEC ASSET rows) ──────────────────
    recordType: str = Field(..., max_length=20, description="Record type — always ASSET")
    formTypeCd: str = Field(..., max_length=20, description="TEC form type code")
    schedFormTypeCd: str = Field(..., max_length=20, description="TEC schedule form type code")
    reportInfoIdent: int = Field(..., ge=0, description="Unique report identifier")
    receivedDt: datetime = Field(..., description="Date report received by TEC")
    infoOnlyFlag: str = Field(
        ..., max_length=1, description="Superseded-by-other-report flag (Y/N)"
    )

    # ── Filer (maps to committee_id / committee record) ───────────────────
    filerIdent: str = Field(..., max_length=100, description="Filer account # → committee_id")
    filerTypeCd: str = Field(..., max_length=30, description="Filer type code → committee_type FK")
    filerName: str = Field(..., max_length=200, description="Filer/committee name")

    # ── Asset detail — description is the ONLY substantive field TEC gives ─
    assetInfoId: int = Field(
        ..., ge=0, description="Asset record unique identifier → transaction_id"
    )
    # TEC's spec says max 100 chars but real data exceeds that.
    # 500 chars covers the longest observed value with headroom.
    assetDescr: str = Field(..., max_length=500, description="Free-text asset description")

    # ── Ingestion metadata ────────────────────────────────────────────────
    file_origin: str = Field(..., max_length=64, description="SHA-256 file origin key")
    download_date: date = Field(..., description="Date the source file was downloaded")

    @field_validator("infoOnlyFlag", mode="before")
    @classmethod
    def validate_info_only_flag(cls, v: str) -> str:
        if str(v).upper() not in ("Y", "N"):
            raise PydanticCustomError(
                "invalid_flag",
                "infoOnlyFlag must be Y or N, got {value!r}",
                {"value": v},
            )
        return str(v).upper()

    @field_validator("assetDescr", mode="before")
    @classmethod
    def validate_asset_descr(cls, v: str) -> str:
        if not v or not str(v).strip():
            raise PydanticCustomError(
                "missing_required_value",
                "assetDescr is required and cannot be blank",
                {"column": "assetDescr", "value": v},
            )
        return str(v).strip()

    @field_validator("filerIdent", "filerName", "recordType", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str) -> str:
        if not v or not str(v).strip():
            raise PydanticCustomError(
                "missing_required_value",
                "Field is required and cannot be blank",
                {"value": v},
            )
        return str(v).strip()
