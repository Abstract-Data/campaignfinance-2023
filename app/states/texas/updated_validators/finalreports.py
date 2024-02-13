from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Annotated
from datetime import date
from funcs.validator_functions import validate_date
import hashlib


class FinalReport(BaseModel):
    recordType: Annotated[str, Field(..., max_length=20)]
    formTypeCd: Annotated[str, Field(..., max_length=20)]
    reportInfoIdent: int
    receivedDt: date
    infoOnlyFlag: Annotated[Optional[str], Field(max_length=1)] = None
    filerIdent: Annotated[str, Field(max_length=100)]
    filerTypeCd: Annotated[str, Field(max_length=30)]
    filerName: Annotated[str, Field(max_length=200)]
    finalUnexpendContribFlag: Annotated[Optional[str], Field(max_length=1)] = None
    finalRetainedAssetsFlag: Annotated[Optional[str], Field(max_length=1)] = None
    finalOfficeholderAckFlag: Annotated[Optional[str], Field(max_length=1)] = None
    filerReportKey: Optional[str]

    _validate_date = field_validator('receivedDt', mode='before')(validate_date)

    @model_validator(mode='after')
    @classmethod
    def create_filer_reportkey(cls, values):
        values['filerReportKey'] = hashlib.sha256(
            f"{values['filerName']}".encode()
        ).hexdigest()
        return values
