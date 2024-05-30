from datetime import datetime, date
from pydantic import Field
from .texas_settings import TECSettings
from typing import Optional


class AssetRecord(TECSettings):
    id: Optional[str] = Field(default=None, description="Unique record ID")
    recordType: str = Field(..., max_length=20)
    formTypeCd: str = Field(..., max_length=20)
    schedFormTypeCd: str = Field(..., max_length=20)
    reportInfoIdent: int = Field(..., ge=0)
    receivedDt: datetime
    infoOnlyFlag: str = Field(..., max_length=1)
    filerIdent: str = Field(..., max_length=100)
    filerTypeCd: str = Field(..., max_length=30)
    filerName: str = Field(..., max_length=200)
    assetInfoId: int = Field(..., ge=0)
    assetDescr: str = Field(..., max_length=100)
    file_origin: str = Field(..., description="File origin", max_length=20)
    download_date: date = Field(..., description="Date file downloaded")