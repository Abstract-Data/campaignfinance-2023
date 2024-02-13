from sqlalchemy import Column, Integer, String, ForeignKey, Date
from sqlalchemy.orm import relationship
from ..database import Base


class FilerModel(Base):
    __tablename__ = 'filers'
    filerIdent = Column(String, primary_key=True)
    filerTypeCd = Column(String)
    filerName = Column(ForeignKey('FilerNameModel.filerIdent'))
    campaigns = relationship('FilerNameModel', back_populates='filer_names')


class TreasurerModel(Base):
    __tablename__ = 'treasurers'
    filerIdent = Column(String, primary_key=True)
    treasPersentTypeCd = Column(String)
    treasNameOrganization = Column(String)
    treasNameLast = Column(String)
    treasNameSuffixCd = Column(String)
    treasNameFirst = Column(String)
    treasNamePrefixCd = Column(String)
    treasNameShort = Column(String)
    treasStreetAddr1 = Column(String)
    treasStreetAddr2 = Column(String)
    treasStreetCity = Column(String)
    treasStreetStateCd = Column(String)
    treasStreetCountyCd = Column(String)
    treasStreetCountryCd = Column(String)
    treasStreetPostalCode = Column(String)
    treasStreetRegion = Column(String)
    treasStreetAddrStandardized = Column(String)
    treasMailingAddr1 = Column(String)
    treasMailingAddr2 = Column(String)
    treasMailingCity = Column(String)
    treasMailingStateCd = Column(String)
    treasMailingCountyCd = Column(String)
    treasMailingCountryCd = Column(String)
    treasMailingPostalCode = Column(String)
    treasMailingRegion = Column(String)
    treasMailingAddrStandardized = Column(String)
    treasPrimaryUsaPhoneFlag = Column(String)
    treasPrimaryPhoneNumber = Column(String)
    treasPrimaryPhoneExt = Column(String)
    treasAppointorNameLast = Column(String)
    treasAppointorNameFirst = Column(String)
    treasFilerpersStatusCd = Column(String)
    treasEffStartDt = Column(Date)
    treasEffStopDt = Column(Date)
    treasurerNameKey = Column(String, nullable=True)
    treasurerAddressKey = Column(String, nullable=True)
    filer = relationship('FilerModel', back_populates='treasurers')


class AssistantTreasurerModel(Base):
    __tablename__ = 'assistant_treasurers'
    __table_args__ = {"schema": 'texas'}

    filerIdent = Column(String, primary_key=True)
    assttreasPersentTypeCd = Column(String)
    assttreasNameOrganization = Column(String)
    assttreasNameLast = Column(String)
    assttreasNameSuffixCd = Column(String)
    assttreasNameFirst = Column(String)
    assttreasNamePrefixCd = Column(String)
    assttreasNameShort = Column(String)
    assttreasStreetAddr1 = Column(String)
    assttreasStreetAddr2 = Column(String)
    assttreasStreetCity = Column(String)
    assttreasStreetStateCd = Column(String)
    assttreasStreetCountyCd = Column(String)
    assttreasStreetCountryCd = Column(String)
    assttreasStreetPostalCode = Column(String)
    assttreasStreetRegion = Column(String)
    assttreasStreetAddrStandardized = Column(String)
    assttreasPrimaryUsaPhoneFlag = Column(String)
    assttreasPrimaryPhoneNumber = Column(String)
    assttreasPrimaryPhoneExt = Column(String)
    assttreasAppointorNameLast = Column(String)
    assttreasAppointorNameFirst = Column(String)
    assistantTreasurerNameKey = Column(String)
    assistantTreasurerAddressKey = Column(String)
    filer = relationship('FilerModel', back_populates='assistant_treasurers')


class ChairModel(Base):
    __tablename__ = 'chairs'
    __table_args__ = {"schema": 'texas'}

    filerIdent = Column(String, primary_key=True)
    chairPersentTypeCd = Column(String)
    chairNameOrganization = Column(String)
    chairNameLast = Column(String)
    chairNameSuffixCd = Column(String)
    chairNameFirst = Column(String)
    chairNamePrefixCd = Column(String)
    chairNameShort = Column(String)
    chairStreetAddr1 = Column(String)
    chairStreetAddr2 = Column(String)
    chairStreetCity = Column(String)
    chairStreetStateCd = Column(String)
    chairStreetCountyCd = Column(String)
    chairStreetCountryCd = Column(String)
    chairStreetPostalCode = Column(String)
    chairStreetRegion = Column(String)
    chairStreetAddrStandardized = Column(String)
    chairMailingAddr1 = Column(String)
    chairMailingAddr2 = Column(String)
    chairMailingCity = Column(String)
    chairMailingStateCd = Column(String)
    chairMailingCountyCd = Column(String)
    chairMailingCountryCd = Column(String)
    chairMailingPostalCode = Column(String)
    chairMailingRegion = Column(String)
    chairMailingAddrStandardized = Column(String)
    chairPrimaryUsaPhoneFlag = Column(String)
    chairPrimaryPhoneNumber = Column(String)
    chairPrimaryPhoneExt = Column(String)
    chairNameKey = Column(String, nullable=True)
    chairAddressKey = Column(String, nullable=True)
    filer = relationship('FilerModel', back_populates='chairs')


class FilerNameModel(Base):
    __tablename__ = 'filer_names'
    __table_args__ = {"schema": 'texas'}

    filerName = Column(String)
    filerIdent = Column(String, primary_key=True)
    committeeStatusCd = Column(String)
    ctaSeekOfficeCd = Column(String)
    ctaSeekOfficeDistrict = Column(String)
    ctaSeekOfficePlace = Column(String)
    ctaSeekOfficeDescr = Column(String)
    ctaSeekOfficeCountyCd = Column(String)
    ctaSeekOfficeCountyDescr = Column(String)
    filerPersentTypeCd = Column(String)
    filerNameOrganization = Column(String)
    filerNameLast = Column(String)
    filerNameSuffixCd = Column(String)
    filerNameFirst = Column(String)
    filerNamePrefixCd = Column(String)
    filerNameShort = Column(String)
    filerStreetAddr1 = Column(String)
    filerStreetAddr2 = Column(String)
    filerStreetCity = Column(String)
    filerStreetStateCd = Column(String)
    filerStreetCountyCd = Column(String)
    filerStreetCountryCd = Column(String)
    filerStreetPostalCode = Column(String)
    filerStreetRegion = Column(String)
    filerMailingAddr1 = Column(String)
    filerMailingAddr2 = Column(String)
    filerMailingCity = Column(String)
    filerMailingStateCd = Column(String)
    filerMailingCountyCd = Column(String)
    filerMailingCountryCd = Column(String)
    filerMailingPostalCode = Column(String)
    filerMailingRegion = Column(String)
    filerPrimaryUsaPhoneFlag = Column(String)
    filerPrimaryPhoneNumber = Column(String)
    filerPrimaryPhoneExt = Column(String)
    filerHoldOfficeCd = Column(String)
    filerHoldOfficeDistrict = Column(String)
    filerHoldOfficePlace = Column(String)
    filerHoldOfficeDescr = Column(String)
    filerHoldOfficeCountyCd = Column(String)
    filerHoldOfficeCountyDescr = Column(String)
    filerFilerpersStatusCd = Column(String)
    filerEffStartDt = Column(Date)
    filerEffStopDt = Column(Date)
    contestSeekOfficeCd = Column(String)
    contestSeekOfficeDistrict = Column(String)
    contestSeekOfficePlace = Column(String)
    contestSeekOfficeDescr = Column(String)
    contestSeekOfficeCountyCd = Column(String)
    contestSeekOfficeCountyDescr = Column(String)
    treasurerKey = Column(ForeignKey('treasurers.treasurerNameKey'))
    asstTreasurerKey = Column(ForeignKey('assistant_treasurers.assistantTreasurerNameKey'))
    chairKey = Column(ForeignKey('chairs.chairNameKey'))
    contributionKey = Column(ForeignKey('ContributionDataModel.filerIdent'))

    filer = relationship('FilerModel', back_populates='filer_names')
    treasurer = relationship('TreasurerModel', back_populates='filer_names')
    assistant_treasurer = relationship('AssistantTreasurerModel', back_populates='filer_names')
    chair = relationship('ChairModel', back_populates='filer_names')
    contribution = relationship('ContributionData', back_populates='filer_names')
    reports = relationship('FinalReportModel', back_populates='filer_names')
