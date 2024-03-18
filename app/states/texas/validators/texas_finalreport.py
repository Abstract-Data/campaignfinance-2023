from pydantic import field_validator
from datetime import datetime
from sqlmodel import Field
from .texas_settings import TECSettings


class TECFinalReport(TECSettings):
    __tablename__ = "tx_finalreports"
    __table_args__ = {"schema": "texas"}
    recordType: str = Field(..., description="Record type code - always FINL", max_length=20)
    formTypeCd: str = Field(..., description="TEC form used", max_length=20)
    reportInfoIdent: int = Field(..., description="Unique report #", primary_key=True)
    receivedDt: datetime = Field(..., description="Date report received by TEC")
    infoOnlyFlag: str = Field(..., description="Superseded by other report", max_length=1)
    filerIdent: str = Field(..., description="Filer account #", max_length=100, foreign_key="tx_filers.filerIdent")
    filerTypeCd: str = Field(..., description="Type of filer", max_length=30)
    filerName: str = Field(..., description="Filer name", max_length=200)
    finalUnexpendContribFlag: str = Field(..., description="Unexpended contributions indicator", max_length=1)
    finalRetainedAssetsFlag: str = Field(..., description="Retained assets indicator", max_length=1)
    finalOfficeholderAckFlag: str = Field(..., description="Office holder ack indicator", max_length=1)

    @field_validator('receivedDt', mode='before')
    def parse_date(cls, v):
        return datetime.strptime(v, '%Y%m%d')

    @field_validator('infoOnlyFlag', 'finalUnexpendContribFlag', 'finalRetainedAssetsFlag', 'finalOfficeholderAckFlag')
    def check_flags(cls, v):
        assert v in ('Y', 'N'), 'Flag must be Y or N'
        return v