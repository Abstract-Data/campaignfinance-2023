from sqlalchemy import Column, Integer, String, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from ..database import Base


class CandidateData(Base):
    __tablename__ = 'candidate_data'
    __table_args__ = {"schema": 'texas'}
    id = Column(Integer, primary_key=True)
    recordType = Column(String(20))
    formTypeCd = Column(String(20))
    schedFormTypeCd = Column(String(20))
    reportInfoIdent = Column(Integer)
    receivedDt = Column(Date)
    infoOnlyFlag = Column(String(1))
    filerIdent = Column(String(100), ForeignKey('filers.filerIdent'))
    filerTypeCd = Column(String(30))
    filerName = Column(String(200))
    expendInfoId = Column(Integer)
    expendPersentId = Column(Integer)
    expendDt = Column(Date)
    expendAmount = Column(Numeric(12, 2))
    expendDescr = Column(String(100))
    expendCatCd = Column(String(30))
    expendCatDescr = Column(String(100))
    itemizeFlag = Column(String(1))
    politicalExpendCd = Column(String(30))
    reimburseIntendedFlag = Column(String(1))
    srcCorpContribFlag = Column(String(1))
    capitalLivingexpFlag = Column(String(1))
    candidatePersentTypeCd = Column(String(30))
    candidateNameOrganization = Column(String(100))
    candidateNameLast = Column(String(100))
    candidateNameSuffixCd = Column(String(30))
    candidateNameFirst = Column(String(45))
    candidateNamePrefixCd = Column(String(30))
    candidateNameShort = Column(String(25))
    candidateHoldOfficeCd = Column(String(30))
    candidateHoldOfficeDistrict = Column(String(11))
    candidateHoldOfficePlace = Column(String(11))
    candidateHoldOfficeDescr = Column(String(100))
    candidateHoldOfficeCountyCd = Column(String(5))
    candidateHoldOfficeCountyDescr = Column(String(100))
    candidateSeekOfficeCd = Column(String(30))
    candidateSeekOfficeDistrict = Column(String(11))
    candidateSeekOfficePlace = Column(String(11))
    candidateSeekOfficeDescr = Column(String(100))
    candidateSeekOfficeCountyCd = Column(String(5))
    candidateSeekOfficeCountyDescr = Column(String(100))

    filer = relationship('FilerModel', back_populates='candidates')