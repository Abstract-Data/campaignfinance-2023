from __future__ import annotations
from pydantic import field_validator, model_validator
from sqlmodel import Field, Relationship
from typing import Optional, Annotated, List
from datetime import date
from funcs.validator_functions import validate_date
import hashlib
from states.texas.validators.texas_settings import TECSettings


class TECFinalReport(TECSettings, table=True):
    __tablename__ = "tx_finalreports"
    __table_args__ = {"schema": "texas"}
    recordType: Annotated[str, Field(..., max_length=20)]
    formTypeCd: Annotated[str, Field(..., max_length=20)]
    reportInfoIdent: int = Field(..., primary_key=True)
    receivedDt: date
    infoOnlyFlag: Annotated[Optional[str], Field(max_length=1)] = None
    filerIdent: Annotated[str, Field(max_length=100, foreign_key="tx_filers.filerIdent")]
    filerTypeCd: Annotated[str, Field(max_length=30)]
    filerName: Annotated[str, Field(max_length=200)]
    finalUnexpendContribFlag: Annotated[Optional[str], Field(max_length=1)] = None
    finalRetainedAssetsFlag: Annotated[Optional[str], Field(max_length=1)] = None
    finalOfficeholderAckFlag: Annotated[Optional[str], Field(max_length=1)] = None
    filerReportKey: Optional[str]
    filer_id: Optional[int] = Field(default=None, foreign_key="tx_filers.filerIdent")
    expense_id: Optional[int] = Field(default=None, foreign_key="tx_expenses.reportInfoIdent")
    contribution_id: Optional[int] = Field(default=None, foreign_key="tx_contributions.reportInfoIdent")
    contributions: Optional['TECContribution'] = Relationship(back_populates="finalreport")
    expenses: Optional[List['TECExpense']] = Relationship(back_populates="finalreport")
    filers: Optional[List['TECFiler']] = Relationship(back_populates="finalreport")


    _validate_date = field_validator('receivedDt', mode='before')(validate_date)

    @model_validator(mode='before')
    @classmethod
    def add_filer_id(cls, values):
        values['contribution_id'] = values['reportInfoIdent']
        return values

    @model_validator(mode='after')
    @classmethod
    def create_filer_reportkey(cls, values):
        values['filerReportKey'] = hashlib.sha256(
            f"{values['filerName']}".encode()
        ).hexdigest()
        return values


