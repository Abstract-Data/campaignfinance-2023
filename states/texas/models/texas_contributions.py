from states.texas.database import Base
from sqlalchemy import Column, ForeignKey, Integer, String, text, Float, Boolean, Date

class TECContributionRecord(Base):
    __tablename__ = "texas.contributions"
    recordType = Column(String, nullable=False)
    formTypeCd = Column(String, nullable=False)
    schedFormTypeCd = Column(String, nullable=False)
    reportInfoIdent = Column(Integer, nullable=False)
    receivedDt = Column(Date, nullable=True)
    infoOnlyFlag = Column(Boolean, nullable=True)
    filerIdent = Column(Integer, nullable=False)
    filerTypeCd = Column(String, nullable=False)
    filerName = Column(String, nullable=False)
    contributionInfoId = Column(Integer, nullable=False, primary_key=True)
    contributionDt = Column(Date, nullable=True)
    contributionAmount = Column(Float, nullable=False)
    contributionDescr = Column(String, nullable=False)
    itemizeFlag = Column(Boolean, nullable=True)
    travelFlag = Column(Boolean, nullable=True)
    contributorPersentTypeCd = Column(String, nullable=False)
    contributorNameOrganization = Column(String, nullable=True)
    contributorNameLast = Column(String, nullable=True)
    contributorNameSuffixCd = Column(String, nullable=True)
    contributorNameFirst = Column(String, nullable=True)
    contributorNamePrefixCd = Column(String, nullable=True)
    contributorNameShort = Column(String, nullable=True)
    contributorStreetCity = Column(String, nullable=True)
    contributorStreetStateCd = Column(String, nullable=True)
    
    contributorStreetCountyCd = Column(String, nullable=True)
    contributorStreetCountryCd = Column(String, nullable=True)
    contributorStreetPostalCode = Column(String, nullable=True)
    contributorStreetRegion = Column(String, nullable=True)
    contributorEmployer = Column(String, nullable=True)
    contributorOccupation = Column(String, nullable=True)
    contributorJobTitle = Column(String, nullable=True)
    contributorPacFein = Column(Integer, nullable=True)
    contributorOosPacFlag = Column(Boolean, nullable=True)
    contributorLawFirmName = Column(String, nullable=True)
    contributorSpouseLawFirmName = Column(String, nullable=True)
    contributorParent1LawFirmName = Column(String, nullable=True)
    contributorParent2LawFirmName = Column(String, nullable=True)
    filers_filderId = Column(Integer, nullable=False)
    filer_id = Column(Integer, nullable=False)
    expenses_id = Column(Integer, nullable=False)
