from datetime import datetime
from pydantic import Field
from decimal import Decimal
from .texas_settings import TECSettings


class CandidateRecord(TECSettings):
    recordType: str = Field(..., max_length=20, description="Record type code - always CAND")
    formTypeCd: str = Field(..., max_length=20, description="TEC form used")
    schedFormTypeCd: str = Field(..., max_length=20, description="TEC Schedule Used")
    reportInfoIdent: int = Field(..., ge=0, description="Unique report #")
    receivedDt: datetime = Field(..., description="Date report received by TEC")
    infoOnlyFlag: str = Field(..., max_length=1, description="Superseded by other report")
    filerIdent: str = Field(..., max_length=100, description="Filer account #")
    filerTypeCd: str = Field(..., max_length=30, description="Type of filer")
    filerName: str = Field(..., max_length=200, description="Filer name")
    expendInfoId: int = Field(..., ge=0, description="Expenditure unique identifier")
    expendPersentId: int = Field(..., ge=0, description="Candidate unique identifier")
    expendDt: datetime = Field(..., description="Expenditure date")
    expendAmount: Decimal = Field(..., description="Expenditure amount")
    expendDescr: str = Field(..., max_length=100, description="Expenditure description")
    expendCatCd: str = Field(..., max_length=30, description="Expenditure category code")
    expendCatDescr: str = Field(..., max_length=100, description="Expenditure category description")
    itemizeFlag: str = Field(..., max_length=1, description="Y indicates that the expenditure is itemized")
    politicalExpendCd: str = Field(..., max_length=30, description="Political expenditure indicator")
    reimburseIntendedFlag: str = Field(..., max_length=1, description="Reimbursement intended indicator")
    srcCorpContribFlag: str = Field(..., max_length=1, description="Expenditure from corporate funds indicator")
    capitalLivingexpFlag: str = Field(..., max_length=1, description="Austin living expense indicator")
    candidatePersentTypeCd: str = Field(..., max_length=30, description="Type of candidate name data - INDIVIDUAL or ENTITY")
    candidateNameOrganization: str = Field(..., max_length=100, description="For ENTITY, the candidate organization name")
    candidateNameLast: str = Field(..., max_length=100, description="For INDIVIDUAL, the candidate last name")
    candidateNameSuffixCd: str = Field(..., max_length=30, description="For INDIVIDUAL, the candidate name suffix (e.g. JR, MD, II)")
    candidateNameFirst: str = Field(..., max_length=45, description="For INDIVIDUAL, the candidate first name")
    candidateNamePrefixCd: str = Field(..., max_length=30, description="For INDIVIDUAL, the candidate name prefix (e.g. MR, MRS, MS)")
    candidateNameShort: str = Field(..., max_length=25, description="For INDIVIDUAL, the candidate short name (nickname)")
    candidateHoldOfficeCd: str = Field(..., max_length=30, description="Candidate office held")
    candidateHoldOfficeDistrict: str = Field(..., max_length=11, description="Candidate office held district")
    candidateHoldOfficePlace: str = Field(..., max_length=11, description="Candidate office held place")
    candidateHoldOfficeDescr: str = Field(..., max_length=100, description="Candidate office held description")
    candidateHoldOfficeCountyCd: str = Field(..., max_length=5, description="Candidate office held country code")
    candidateHoldOfficeCountyDescr: str = Field(..., max_length=100, description="Candidate office help county description")
    candidateSeekOfficeCd: str = Field(..., max_length=30, description="Candidate office sought")
    candidateSeekOfficeDistrict: str = Field(..., max_length=11, description="Candidate office sought district")
    candidateSeekOfficePlace: str = Field(..., max_length=11, description="Candidate office sought place")
    candidateSeekOfficeDescr: str = Field(..., max_length=100, description="Candidate office sought description")
    candidateSeekOfficeCountyCd: str = Field(..., max_length=5, description="Candidate office sought county code")
    candidateSeekOfficeCountyDescr: str = Field(..., max_length=100, description="Candidate office sought county description")