from pydantic import Field, field_validator
from datetime import datetime
from .texas_settings import TECSettings


class PledgeData(TECSettings):
    recordType: str = Field(..., description="Record type code - always PLDG", max_length=20)
    formTypeCd: str = Field(..., description="TEC form used", max_length=20)
    schedFormTypeCd: str = Field(..., description="TEC Schedule Used", max_length=20)
    reportInfoIdent: int = Field(..., description="Unique report #")
    receivedDt: datetime = Field(..., description="Date report received by TEC")
    infoOnlyFlag: str = Field(..., description="Superseded by other report", max_length=1)
    filerIdent: str = Field(..., description="Filer account #", max_length=100)
    filerTypeCd: str = Field(..., description="Type of filer", max_length=30)
    filerName: str = Field(..., description="Filer name", max_length=200)
    pledgeInfoId: int = Field(..., description="Pledge unique identifier")
    pledgeDt: datetime = Field(..., description="Pledge data")
    pledgeAmount: float = Field(..., description="Pledge amount")
    pledgeDescr: str = Field(..., description="Pledge description", max_length=100)
    itemizeFlag: str = Field(..., description="Y indicates that the pledge is itemized", max_length=1)
    travelFlag: str = Field(..., description="Y indicates that the pledge has associated travel", max_length=1)
    pledgerPersentTypeCd: str = Field(..., description="Type of pledger name data - INDIVIDUAL or ENTITY", max_length=30)
    pledgerNameOrganization: str = Field(..., description="For ENTITY, the pledger organization name", max_length=100)
    pledgerNameLast: str = Field(..., description="For INDIVIDUAL, the pledger last name", max_length=100)
    pledgerNameSuffixCd: str = Field(..., description="For INDIVIDUAL, the pledger name suffix (e.g. JR, MD, II)", max_length=30)
    pledgerNameFirst: str = Field(..., description="For INDIVIDUAL, the pledger first name", max_length=45)
    pledgerNamePrefixCd: str = Field(..., description="For INDIVIDUAL, the pledger name prefix (e.g. MR, MRS, MS)", max_length=30)
    pledgerNameShort: str = Field(..., description="For INDIVIDUAL, the pledger short name (nickname)", max_length=25)
    pledgerStreetCity: str = Field(..., description="Pledger street address - city", max_length=30)
    pledgerStreetStateCd: str = Field(..., description="Pledger street address - state code (e.g. TX, CA) - for country=USA/UMI only", max_length=2)
    pledgerStreetCountyCd: str = Field(..., description="Pledger street address - Texas county", max_length=5)
    pledgerStreetCountryCd: str = Field(..., description="Pledger street address - country (e.g. USA, UMI, MEX, CAN)", max_length=3)
    pledgerStreetPostalCode: str = Field(..., description="Pledger street address - postal code - for USA addresses only", max_length=20)
    pledgerStreetRegion: str = Field(..., description="Pledger street address - region for country other than USA", max_length=30)
    pledgerEmployer: str = Field(..., description="Pledger employer", max_length=60)
    pledgerOccupation: str = Field(..., description="Pledger occupation", max_length=60)
    pledgerJobTitle: str = Field(..., description="Pledger job title", max_length=60)
    pledgerPacFein: str = Field(..., description="For PAC pledger the FEIN", max_length=12)
    pledgerOosPacFlag: str = Field(..., description="Indicates if pledger is an out-of-state PAC", max_length=1)
    pledgerLawFirmName: str = Field(..., description="Pledger law firm name", max_length=60)
    pledgerSpouseLawFirmName: str = Field(..., description="Pledger spouse law firm name", max_length=60)
    pledgerParent1LawFirmName: str = Field(..., description="Pledger parent #1 law firm name", max_length=60)
    pledgerParent2LawFirmName: str = Field(..., description="Pledger parent #2 law firm name", max_length=60)

    @field_validator('receivedDt', 'pledgeDt', mode='before')
    def parse_date(cls, v):
        return datetime.strptime(v, '%Y%m%d')

    @field_validator('infoOnlyFlag', 'itemizeFlag', 'travelFlag', 'pledgerOosPacFlag')
    def check_flags(cls, v):
        assert v in ('Y', 'N'), 'Flag must be Y or N'
        return v

    @field_validator('pledgerPersentTypeCd')
    def check_pledger_type(cls, v):
        assert v in ('INDIVIDUAL', 'ENTITY'), 'Pledger type must be INDIVIDUAL or ENTITY'
        return v
