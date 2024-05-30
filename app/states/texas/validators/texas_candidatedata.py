from datetime import datetime, date
from sqlmodel import Field
from pydantic import ValidationError, model_validator, field_validator
from pydantic_core import PydanticCustomError
from typing import Optional
from .texas_settings import TECSettings


class CandidateData(TECSettings, table=True):
    __tablename__ = "tx_candidate_data"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, description="Unique record ID", primary_key=True)
    recordType: str = Field(..., description="Record type code - always CAND")
    formTypeCd: str = Field(..., description="TEC form used")
    schedFormTypeCd: str = Field(..., description="TEC Schedule Used")
    reportInfoIdent: int = Field(..., description="Unique report #")
    receivedDt: Optional[date] = Field(..., description="Date report received by TEC")
    infoOnlyFlag: str = Field(...,  description="Superseded by other report")
    filerIdent: int = Field(..., description="Filer account #")
    filerTypeCd: str = Field(...,  description="Type of filer")
    filerName: Optional[str] = Field(...,  description="Filer name")
    expendInfoId: int = Field(..., description="Expenditure unique identifier")
    expendPersentId: int = Field(..., description="Candidate unique identifier")
    expendDt: Optional[date] = Field(..., description="Expenditure date")
    expendAmount: Optional[float] = Field(..., description="Expenditure amount")
    expendDescr: Optional[str] = Field(...,  description="Expenditure description")
    expendCatCd: Optional[str] = Field(..., description="Expenditure category code")
    expendCatDescr: Optional[str] = Field(..., description="Expenditure category description")
    itemizeFlag: Optional[bool] = Field(..., description="Y indicates that the expenditure is itemized")
    politicalExpendCd: Optional[str] = Field(..., description="Political expenditure indicator")
    reimburseIntendedFlag: Optional[bool] = Field(..., description="Reimbursement intended indicator")
    srcCorpContribFlag: Optional[bool] = Field(..., description="Expenditure from corporate funds indicator")
    capitalLivingexpFlag: Optional[bool] = Field(..., description="Austin living expense indicator")
    candidatePersentTypeCd: str = Field(...,  description="Type of candidate name data - INDIVIDUAL or ENTITY")
    candidateNameOrganization: Optional[str] = Field(..., description="For ENTITY, the candidate organization name")
    candidateNameLast: Optional[str] = Field(...,  description="For INDIVIDUAL, the candidate last name")
    candidateNameSuffixCd: Optional[str] = Field(...,  description="For INDIVIDUAL, the candidate name suffix (e.g. JR, MD, II)")
    candidateNameFirst: Optional[str] = Field(...,  description="For INDIVIDUAL, the candidate first name")
    candidateNamePrefixCd: Optional[str] = Field(...,  description="For INDIVIDUAL, the candidate name prefix (e.g. MR, MRS, MS)")
    candidateNameShort: Optional[str] = Field(...,  description="For INDIVIDUAL, the candidate short name (nickname)")
    candidateHoldOfficeCd: Optional[str] = Field(...,  description="Candidate office held")
    candidateHoldOfficeDistrict: Optional[str] = Field(...,  description="Candidate office held district")
    candidateHoldOfficePlace: Optional[str] = Field(...,  description="Candidate office held place")
    candidateHoldOfficeDescr: Optional[str] = Field(...,  description="Candidate office held description")
    candidateHoldOfficeCountyCd: Optional[str] = Field(...,  description="Candidate office held country code")
    candidateHoldOfficeCountyDescr: Optional[str] = Field(...,  description="Candidate office help county description")
    candidateSeekOfficeCd: Optional[str] = Field(...,  description="Candidate office sought")
    candidateSeekOfficeDistrict: Optional[str] = Field(...,  description="Candidate office sought district")
    candidateSeekOfficePlace: Optional[str] = Field(...,  description="Candidate office sought place")
    candidateSeekOfficeDescr: Optional[str] = Field(...,  description="Candidate office sought description")
    candidateSeekOfficeCountyCd: Optional[str] = Field(..., description="Candidate office sought county code")
    candidateSeekOfficeCountyDescr: Optional[str] = Field(..., description="Candidate office sought county description")
    file_origin: str = Field(..., description="File origin")
    download_date: date = Field(..., description="Date file downloaded")


    @field_validator('capitalLivingexpFlag', mode='before')
    @classmethod
    def check_capital_living_expense(cls, value):
        if value == ',':
            return None

    @model_validator(mode='before')
    @classmethod
    def check_candidate_type(cls, values):
        if values['candidatePersentTypeCd'] not in ('INDIVIDUAL', 'ENTITY'):
            raise PydanticCustomError(
                'candidate_type',
                'Candidate type must be INDIVIDUAL or ENTITY',
                {
                    'column': 'candidatePersentTypeCd',
                    'value': values['candidatePersentTypeCd']
                }
            )
        return values

    # @model_validator(mode='before')
    # @classmethod
    # def check_individual_name_filled(cls, values):
    #     if values['candidatePersentTypeCd'] == 'INDIVIDUAL':
    #         if not values['candidateNameLast'] or not values['candidateNameFirst']:
    #             raise PydanticCustomError(
    #                 'candidate_name',
    #                 'Individual candidate name must have both first and last name',
    #                 {
    #                     'column': 'candidatePersentTypeCd',
    #                     'value': values['candidatePersentTypeCd']
    #                 }
    #             )
    #     return values

    @model_validator(mode='before')
    @classmethod
    def check_entity_name_filled(cls, values):
        if values['candidatePersentTypeCd'] == 'ENTITY':
            if not values['candidateNameOrganization']:
                raise PydanticCustomError(
                    'candidate_name',
                    'Entity candidate name must have an organization name',
                    {
                        'column': 'candidatePersentTypeCd',
                        'value': values['candidatePersentTypeCd']
                    }
                )
        return values
