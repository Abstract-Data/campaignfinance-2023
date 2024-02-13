from pydantic import Field, field_validator
from .texas_settings import TECSettings
from datetime import datetime


class TravelData(TECSettings):
    recordType: str = Field(..., description="Record type code - always TRVL", max_length=20)
    formTypeCd: str = Field(..., description="TEC form used", max_length=20)
    schedFormTypeCd: str = Field(..., description="TEC Schedule Used", max_length=20)
    reportInfoIdent: int = Field(..., description="Unique report #")
    receivedDt: datetime = Field(..., description="Date report received by TEC")
    infoOnlyFlag: str = Field(..., description="Superseded by other report", max_length=1)
    filerIdent: str = Field(..., description="Filer account #", max_length=100)
    filerTypeCd: str = Field(..., description="Type of filer", max_length=30)
    filerName: str = Field(..., description="Filer name", max_length=200)
    travelInfoId: int = Field(..., description="Travel unique identifier")
    parentType: str = Field(..., description="Parent record type (CONTRIB, EXPEND, PLEDGE)", max_length=20)
    parentId: int = Field(..., description="Parent unique identifier")
    parentDt: datetime = Field(..., description="Date of parent transaction")
    parentAmount: float = Field(..., description="Amount of parent transaction")
    parentFullName: str = Field(..., description="Full name associated with parent", max_length=100)
    transportationTypeCd: str = Field(..., description="Type of transportation (COMMAIR, PRIVAIR, etc)", max_length=30)
    transportationTypeDescr: str = Field(..., description="Transporation type description", max_length=100)
    departureCity: str = Field(..., description="Departure city", max_length=50)
    arrivalCity: str = Field(..., description="Arrival city", max_length=50)
    departureDt: datetime = Field(..., description="Departure date")
    arrivalDt: datetime = Field(..., description="Arrival date")
    travelPurpose: str = Field(..., description="Purpose of travel", max_length=255)
    travellerPersentTypeCd: str = Field(..., description="Type of traveller name data - INDIVIDUAL or ENTITY", max_length=30)
    travellerNameOrganization: str = Field(..., description="For ENTITY, the traveller organization name", max_length=100)
    travellerNameLast: str = Field(..., description="For INDIVIDUAL, the traveller last name", max_length=100)
    travellerNameSuffixCd: str = Field(..., description="For INDIVIDUAL, the traveller name suffix (e.g. JR, MD, II)", max_length=30)
    travellerNameFirst: str = Field(..., description="For INDIVIDUAL, the traveller first name", max_length=45)
    travellerNamePrefixCd: str = Field(..., description="For INDIVIDUAL, the traveller name prefix (e.g. MR, MRS, MS)", max_length=30)
    travellerNameShort: str = Field(..., description="For INDIVIDUAL, the traveller short name (nickname)", max_length=25)

    @field_validator('receivedDt', 'parentDt', 'departureDt', 'arrivalDt', mode='before')
    def parse_date(cls, v):
        return datetime.strptime(v, '%Y%m%d')

    @field_validator('infoOnlyFlag')
    def check_flag(cls, v):
        assert v in ('Y', 'N'), 'Flag must be Y or N'
        return v

    @field_validator('travellerPersentTypeCd')
    def check_traveller_type(cls, v):
        assert v in ('INDIVIDUAL', 'ENTITY'), 'Traveller type must be INDIVIDUAL or ENTITY'
        return v
