from sqlalchemy import Column, Integer, String, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from ..database import Base


class ContributorDetailModel(Base):
    __tablename__ = 'contributors'
    __table_args__ = {"schema": 'texas'}

    contributorNameOrganization = Column(String)
    contributorNameLast = Column(String)
    contributorNameSuffixCd = Column(String)
    contributorNameFirst = Column(String)
    contributorNamePrefixCd = Column(String)
    contributorNameShort = Column(String)
    contributorStreetCity = Column(String)
    contributorStreetStateCd = Column(String)
    contributorStreetCountyCd = Column(String)
    contributorStreetCountryCd = Column(String)
    contributorStreetPostalCode = Column(String)
    contributorStreetRegion = Column(String)
    contributionAddressStandardized = Column(String)
    contributorEmployer = Column(String)
    contributorOccupation = Column(String)
    contributorJobTitle = Column(String)
    contributorPacFein = Column(String)
    contributorOosPacFlag = Column(String)
    contributorLawFirmName = Column(String)
    contributorSpouseLawFirmName = Column(String)
    contributorParent1LawFirmName = Column(String)
    contributorParent2LawFirmName = Column(String)
    contributorNameKey = Column(String)
    contributorOrgKey = Column(String)
    contributorAddressKey = Column(String)
    contributorNameAddressKey = Column(String, primary_key=True)
    # contributions = relationship('ContributionData', back_populates='contributor')


class ContributionDataModel(Base):
    __tablename__ = 'contribution_data'
    __table_args__ = {"schema": 'texas'}

    recordType = Column(String)
    formTypeCd = Column(String)
    schedFormTypeCd = Column(String)
    reportInfoIdent = Column(Integer, ForeignKey('final_reports.reportInfoIdent', use_alter=True), nullable=False)
    receivedDt = Column(Date)
    infoOnlyFlag = Column(String)
    filerIdent = Column(String)
    filerTypeCd = Column(String)
    filerName = Column(String)
    contributionInfoId = Column(Integer, primary_key=True)
    contributionDt = Column(Date)
    contributionAmount = Column(Numeric)
    contributionDescr = Column(String)
    itemizeFlag = Column(String)
    travelFlag = Column(String)
    contributorNameAddressKey = Column(String, ForeignKey('contributors.contributorNameAddressKey', use_alter=True), nullable=False)
    contributorOrgKey = Column(String, ForeignKey('contributors.contributorOrgKey', use_alter=True), nullable=False)
    # filer = relationship('FilerModel', back_populates='contributions')
