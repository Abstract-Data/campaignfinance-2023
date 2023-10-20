from states.texas.validators.texas_settings import TECSettings
from datetime import date

class TECDirectExpenses(TECSettings):
    recordType: str
    formTypeCd: str
    schedFormTypeCd: str
    reportInfoIdent: int
    receivedDt: date
    infoOnlyFlag
    filerIdent
    filerTypeCd
    filerName
    expendInfoId
    expendPersentId
    expendDt
    expendAmount
    expendDescr
    expendCatCd
    expendCatDescr
    itemizeFlag
    politicalExpendCd
    reimburseIntendedFlag
    srcCorpContribFlag
    capitalLivingexpFlag
    candidatePersentTypeCd
    candidateNameOrganization
    candidateNameLast
    candidateNameSuffixCd
    candidateNameFirst
    candidateNamePrefixCd
    candidateNameShort
    candidateHoldOfficeCd
    candidateHoldOfficeDistrict
    candidateHoldOfficePlace
    candidateHoldOfficeDescr
    candidateHoldOfficeCountyCd
    candidateHoldOfficeCountyDescr
    candidateSeekOfficeCd
    candidateSeekOfficeDistrict
    candidateSeekOfficePlace
    candidateSeekOfficeDescr
    candidateSeekOfficeCountyCd
    candidateSeekOfficeCountyDescr