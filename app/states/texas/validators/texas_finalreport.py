from pydantic import field_validator
from datetime import datetime, date
from sqlmodel import Field, Relationship
from typing import Optional, List
from .texas_settings import TECSettings


class TECFinalReport(TECSettings, table=True):
    __tablename__ = "tx_finalreports"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, description="Unique record ID")
    recordType: str = Field(..., description="Record type code - always FINL", max_length=20)
    formTypeCd: str = Field(..., description="TEC form used", max_length=20)
    reportInfoIdent: int = Field(..., description="Unique report #", primary_key=True)
    receivedDt: datetime = Field(..., description="Date report received by TEC")
    infoOnlyFlag: Optional[bool] = Field(default=None, description="Superseded by other report")
    filerIdent: str = Field(..., description="Filer account #", max_length=100)
    filerTypeCd: str = Field(..., description="Type of filer", max_length=30)
    filerName: str = Field(..., description="Filer name", max_length=200)
    finalUnexpendContribFlag: Optional[bool] = Field(default=None, description="Unexpended contributions indicator")
    finalRetainedAssetsFlag: Optional[bool] = Field(default=None, description="Retained assets indicator")
    finalOfficeholderAckFlag: Optional[bool] = Field(default=None, description="Office holder ack indicator")
    file_origin: str = Field(..., description="File origin", max_length=20)
    download_date: date = Field(..., description="Date file downloaded")

    @field_validator('recordType')
    def check_record_type(cls, v):
        assert v == 'FINL', 'Record type must be FINL'
        return v
    # @field_validator('receivedDt', mode='before')
    # def parse_date(cls, v):
    #     return datetime.strptime(v, '%Y%m%d')

    # @field_validator('infoOnlyFlag', 'finalUnexpendContribFlag', 'finalRetainedAssetsFlag', 'finalOfficeholderAckFlag')
    # def check_flags(cls, v):
    #     assert v in ('Y', 'N'), 'Flag must be Y or N'
    #     return v