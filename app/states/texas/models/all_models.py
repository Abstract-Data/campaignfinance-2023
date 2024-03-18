from states.texas.database import Base
from sqlalchemy import Column, ForeignKey, Integer, String, text, Float, Boolean
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import relationship
from datetime import date
from typing import List, Set


class TECFilerRecord(Base):
    __tablename__ = "filers"
    __table_args__ = {"schema": "texas"}
    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    recordType: Mapped[str] = mapped_column(nullable=False)
    filerIdent: Mapped[int] = mapped_column(nullable=False)
    filerTypeCd: Mapped[str] = mapped_column(nullable=False)
    filerName: Mapped[str] = mapped_column(nullable=False)
    unexpendContribFilerFlag: Mapped[str] = mapped_column(nullable=True)
    modifiedElectCycleFlag: Mapped[str] = mapped_column(nullable=True)
    filerJdiCd: Mapped[str] = mapped_column(nullable=True)
    committeeStatusCd: Mapped[str] = mapped_column(nullable=True)
    ctaSeekOfficeCd: Mapped[str] = mapped_column(nullable=True)
    ctaSeekOfficeDistrict: Mapped[str] = mapped_column(nullable=True)
    ctaSeekOfficePlace: Mapped[str] = mapped_column(nullable=True)
    ctaSeekOfficeDescr: Mapped[str] = mapped_column(nullable=True)
    ctaSeekOfficeCountyCd: Mapped[str] = mapped_column(nullable=True)
    ctaSeekOfficeCountyDescr: Mapped[str] = mapped_column(nullable=True)
    filerPersentTypeCd: Mapped[str] = mapped_column(nullable=False)
    filerNameOrganization: Mapped[str] = mapped_column(nullable=True)
    filerNameLast: Mapped[str] = mapped_column(nullable=True)
    filerNameSuffixCd: Mapped[str] = mapped_column(nullable=True)
    filerNameFirst: Mapped[str] = mapped_column(nullable=True)
    filerNamePrefixCd: Mapped[str] = mapped_column(nullable=True)
    filerNameShort: Mapped[str] = mapped_column(nullable=True)
    filerStreetAddr1: Mapped[str] = mapped_column(nullable=True)
    filerStreetAddr2: Mapped[str] = mapped_column(nullable=True)
    filerStreetCity: Mapped[str] = mapped_column(nullable=True)
    filerStreetStateCd: Mapped[str] = mapped_column(nullable=True)

    filerStreetCountyCd: Mapped[str] = mapped_column(nullable=True)
    filerStreetCountryCd: Mapped[str] = mapped_column(nullable=True)
    filerStreetPostalCode: Mapped[str] = mapped_column(nullable=True)
    filerStreetRegion: Mapped[str] = mapped_column(nullable=True)
    filerMailingAddr1: Mapped[str] = mapped_column(nullable=True)
    filerMailingAddr2: Mapped[str] = mapped_column(nullable=True)
    filerMailingCity: Mapped[str] = mapped_column(nullable=True)
    filerMailingStateCd: Mapped[str] = mapped_column(nullable=True)

    filerMailingCountyCd: Mapped[str] = mapped_column(nullable=True)
    filerMailingCountryCd: Mapped[str] = mapped_column(nullable=True)
    filerMailingPostalCode: Mapped[str] = mapped_column(nullable=True)
    filerMailingRegion: Mapped[str] = mapped_column(nullable=True)
    filerPrimaryUsaPhoneFlag: Mapped[str] = mapped_column(nullable=True)

    filerPrimaryPhoneNumber: Mapped[str] = mapped_column(nullable=True)
    filerPrimaryPhoneExt: Mapped[str] = mapped_column(nullable=True)
    filerHoldOfficeCd: Mapped[str] = mapped_column(nullable=True)
    filerHoldOfficeDistrict: Mapped[str] = mapped_column(nullable=True)
    filerHoldOfficePlace: Mapped[str] = mapped_column(nullable=True)
    filerHoldOfficeDescr: Mapped[str] = mapped_column(nullable=True)
    filerHoldOfficeCountyCd: Mapped[str] = mapped_column(nullable=True)
    filerHoldOfficeCountyDescr: Mapped[str] = mapped_column(nullable=True)
    filerFilerpersStatusCd: Mapped[str] = mapped_column(nullable=True)
    filerEffStartDt: Mapped[date] = mapped_column(nullable=True)
    filerEffStopDt: Mapped[date] = mapped_column(nullable=True)
    contestSeekOfficeCd: Mapped[str] = mapped_column(nullable=True)
    contestSeekOfficeDistrict: Mapped[str] = mapped_column(nullable=True)
    contestSeekOfficePlace: Mapped[str] = mapped_column(nullable=True)
    contestSeekOfficeDescr: Mapped[str] = mapped_column(nullable=True)
    contestSeekOfficeCountyCd: Mapped[str] = mapped_column(nullable=True)
    contestSeekOfficeCountyDescr: Mapped[str] = mapped_column(nullable=True)
    treasPersentTypeCd: Mapped[str] = mapped_column(nullable=True)
    treasNameOrganization: Mapped[str] = mapped_column(nullable=True)
    treasNameLast: Mapped[str] = mapped_column(nullable=True)
    treasNameSuffixCd: Mapped[str] = mapped_column(nullable=True)
    treasNameFirst: Mapped[str] = mapped_column(nullable=True)
    treasNamePrefixCd: Mapped[str] = mapped_column(nullable=True)
    treasNameShort: Mapped[str] = mapped_column(nullable=True)
    treasStreetAddr1: Mapped[str] = mapped_column(nullable=True)
    treasStreetAddr2: Mapped[str] = mapped_column(nullable=True)
    treasStreetCity: Mapped[str] = mapped_column(nullable=True)
    treasStreetStateCd: Mapped[str] = mapped_column(nullable=True)

    treasStreetCountyCd: Mapped[str] = mapped_column(nullable=True)
    treasStreetCountryCd: Mapped[str] = mapped_column(nullable=True)
    treasStreetPostalCode: Mapped[str] = mapped_column(nullable=True)
    treasStreetRegion: Mapped[str] = mapped_column(nullable=True)
    treasMailingAddr1: Mapped[str] = mapped_column(nullable=True)
    treasMailingAddr2: Mapped[str] = mapped_column(nullable=True)
    treasMailingCity: Mapped[str] = mapped_column(nullable=True)
    treasMailingStateCd: Mapped[str] = mapped_column(nullable=True)

    treasMailingCountyCd: Mapped[str] = mapped_column(nullable=True)
    treasMailingCountryCd: Mapped[str] = mapped_column(nullable=True)
    treasMailingPostalCode: Mapped[str] = mapped_column(nullable=True)
    treasMailingRegion: Mapped[str] = mapped_column(nullable=True)
    treasPrimaryUsaPhoneFlag: Mapped[str] = mapped_column(nullable=True)

    treasPrimaryPhoneNumber: Mapped[str] = mapped_column(nullable=True)
    treasPrimaryPhoneExt: Mapped[str] = mapped_column(nullable=True)
    treasAppointorNameLast: Mapped[str] = mapped_column(nullable=True)
    treasAppointorNameFirst: Mapped[str] = mapped_column(nullable=True)
    treasFilerpersStatusCd: Mapped[str] = mapped_column(nullable=True)
    treasEffStartDt: Mapped[date] = mapped_column(nullable=True)
    treasEffStopDt: Mapped[date] = mapped_column(nullable=True)
    assttreasPersentTypeCd: Mapped[str] = mapped_column(nullable=True)
    assttreasNameOrganization: Mapped[str] = mapped_column(nullable=True)
    assttreasNameLast: Mapped[str] = mapped_column(nullable=True)
    assttreasNameSuffixCd: Mapped[str] = mapped_column(nullable=True)
    assttreasNameFirst: Mapped[str] = mapped_column(nullable=True)
    assttreasNamePrefixCd: Mapped[str] = mapped_column(nullable=True)
    assttreasNameShort: Mapped[str] = mapped_column(nullable=True)
    assttreasStreetAddr1: Mapped[str] = mapped_column(nullable=True)
    assttreasStreetAddr2: Mapped[str] = mapped_column(nullable=True)
    assttreasStreetCity: Mapped[str] = mapped_column(nullable=True)
    assttreasStreetStateCd: Mapped[str] = mapped_column(nullable=True)

    assttreasStreetCountyCd: Mapped[str] = mapped_column(nullable=True)
    assttreasStreetCountryCd: Mapped[str] = mapped_column(nullable=True)
    assttreasStreetPostalCode: Mapped[str] = mapped_column(nullable=True)

    assttreasStreetRegion: Mapped[str] = mapped_column(nullable=True)
    assttreasPrimaryUsaPhoneFlag: Mapped[str] = mapped_column(nullable=True)

    assttreasPrimaryPhoneNumber: Mapped[str] = mapped_column(nullable=True)
    assttreasPrimaryPhoneExt: Mapped[str] = mapped_column(nullable=True)
    assttreasAppointorNameLast: Mapped[str] = mapped_column(nullable=True)
    assttreasAppointorNameFirst: Mapped[str] = mapped_column(nullable=True)
    chairPersentTypeCd: Mapped[str] = mapped_column(nullable=True)
    chairNameOrganization: Mapped[str] = mapped_column(nullable=True)
    chairNameLast: Mapped[str] = mapped_column(nullable=True)
    chairNameSuffixCd: Mapped[str] = mapped_column(nullable=True)
    chairNameFirst: Mapped[str] = mapped_column(nullable=True)
    chairNamePrefixCd: Mapped[str] = mapped_column(nullable=True)
    chairNameShort: Mapped[str] = mapped_column(nullable=True)
    chairStreetAddr1: Mapped[str] = mapped_column(nullable=True)
    chairStreetAddr2: Mapped[str] = mapped_column(nullable=True)
    chairStreetCity: Mapped[str] = mapped_column(nullable=True)
    chairStreetStateCd: Mapped[str] = mapped_column(nullable=True)

    chairStreetCountyCd: Mapped[str] = mapped_column(nullable=True)
    chairStreetCountryCd: Mapped[str] = mapped_column(nullable=True)
    chairStreetPostalCode: Mapped[str] = mapped_column(nullable=True)
    chairStreetRegion: Mapped[str] = mapped_column(nullable=True)
    chairMailingAddr1: Mapped[str] = mapped_column(nullable=True)
    chairMailingAddr2: Mapped[str] = mapped_column(nullable=True)
    chairMailingCity: Mapped[str] = mapped_column(nullable=True)
    chairMailingStateCd: Mapped[str] = mapped_column(nullable=True)

    chairMailingCountyCd: Mapped[str] = mapped_column(nullable=True)
    chairMailingCountryCd: Mapped[str] = mapped_column(nullable=True)
    chairMailingPostalCode: Mapped[str] = mapped_column(nullable=True)
    chairMailingRegion: Mapped[str] = mapped_column(nullable=True)
    chairPrimaryUsaPhoneFlag: Mapped[str] = mapped_column(nullable=True)

    chairPrimaryPhoneNumber: Mapped[str] = mapped_column(nullable=True)
    chairPrimaryPhoneExt: Mapped[str] = mapped_column(nullable=True)
    org_names: Mapped[str] = mapped_column(nullable=False)
    file_origin: Mapped[str] = mapped_column(nullable=False)

    expenses: Mapped[List["TECExpenseRecord"]] = relationship(back_populates="filers")
    contributions: Mapped[List["TECContributionRecord"]] = relationship(back_populates="filers")


class TECExpenseRecord(Base):
    __tablename__ = "expenses"
    __table_args__ = {"schema": "texas"}
    recordType: Mapped[str] = mapped_column(nullable=False)
    formTypeCd: Mapped[str] = mapped_column(nullable=False)
    schedFormTypeCd: Mapped[str] = mapped_column(nullable=False)
    reportInfoIdent: Mapped[int] = mapped_column(nullable=False)
    receivedDt: Mapped[date] = mapped_column(nullable=False)
    infoOnlyFlag: Mapped[bool] = mapped_column(nullable=True)
    filerIdent: Mapped[int] = mapped_column(nullable=False)
    filerTypeCd: Mapped[str] = mapped_column(nullable=False)
    filerName: Mapped[str] = mapped_column(nullable=False)
    expendInfoId: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    expendDt: Mapped[date] = mapped_column(nullable=False)
    expendAmount: Mapped[float] = mapped_column(nullable=False)
    expendDescr: Mapped[str] = mapped_column(nullable=False)
    expendCatCd: Mapped[str] = mapped_column(nullable=False)
    expendCatDescr: Mapped[str] = mapped_column(nullable=False)
    itemizeFlag: Mapped[bool] = mapped_column(nullable=True)
    travelFlag: Mapped[bool] = mapped_column(nullable=True)
    politicalExpendCd: Mapped[str] = mapped_column(nullable=True)
    reimburseIntendedFlag: Mapped[str] = mapped_column(nullable=True)
    srcCorpContribFlag: Mapped[str] = mapped_column(nullable=True)
    capitalLivingexpFlag: Mapped[str] = mapped_column(nullable=True)
    payeePersentTypeCd: Mapped[str] = mapped_column(nullable=False)
    payeeNameOrganization: Mapped[str] = mapped_column(nullable=True)
    payeeNameLast: Mapped[str] = mapped_column(nullable=True)
    payeeNameSuffixCd: Mapped[str] = mapped_column(nullable=True)
    payeeNameFirst: Mapped[str] = mapped_column(nullable=True)
    payeeNamePrefixCd: Mapped[str] = mapped_column(nullable=True)
    payeeNameShort: Mapped[str] = mapped_column(nullable=True)
    payeeStreetAddr1: Mapped[str] = mapped_column(nullable=False)
    payeeStreetAddr2: Mapped[str] = mapped_column(nullable=True)
    payeeStreetCity: Mapped[str] = mapped_column(nullable=False)
    payeeStreetStateCd: Mapped[str] = mapped_column(nullable=False)
    payeeStreetCountyCd: Mapped[str] = mapped_column(nullable=False)
    payeeStreetCountryCd: Mapped[str] = mapped_column(nullable=False)
    payeeStreetPostalCode: Mapped[str] = mapped_column(nullable=True)
    payeeStreetRegion: Mapped[str] = mapped_column(nullable=True)
    creditCardIssuer: Mapped[str] = mapped_column(nullable=True)
    repaymentDt: Mapped[date] = mapped_column(nullable=True)
    file_origin: Mapped[str] = mapped_column(nullable=False)
    filer_id: Mapped[int] = mapped_column(
        ForeignKey("texas.filers.filerIdent"), nullable=False, unique=False
    )
    filers: Mapped[Set["TECFilerRecord"]] = relationship(
        back_populates="expenses",
        foreign_keys=[filer_id],
        primaryjoin="and_(TECExpenseRecord.filer_id == TECFilerRecord.filerIdent)",
    )


class TECContributionRecord(Base):
    __tablename__ = "contributions"
    __table_args__ = {"schema": "texas"}
    recordType: Mapped[str] = mapped_column(nullable=False)
    formTypeCd: Mapped[str] = mapped_column(nullable=False)
    schedFormTypeCd: Mapped[str] = mapped_column(nullable=False)
    reportInfoIdent: Mapped[int] = mapped_column(nullable=False)
    receivedDt: Mapped[date] = mapped_column(nullable=False)
    infoOnlyFlag: Mapped[bool] = mapped_column(nullable=True)
    filerIdent: Mapped[int] = mapped_column(nullable=False)
    filerTypeCd: Mapped[str] = mapped_column(nullable=False)
    filerName: Mapped[str] = mapped_column(nullable=False)
    contributionInfoId = Column(Integer, nullable=False, primary_key=True)
    contributionDt: Mapped[date] = mapped_column(nullable=False)
    contributionAmount: Mapped[float] = mapped_column(nullable=False)
    contributionDescr: Mapped[str] = mapped_column(nullable=True)
    itemizeFlag: Mapped[bool] = mapped_column(nullable=True)
    travelFlag: Mapped[bool] = mapped_column(nullable=True)
    contributorPersentTypeCd: Mapped[str] = mapped_column(nullable=False)
    contributorNameOrganization: Mapped[str] = mapped_column(nullable=True)
    contributorNameLast: Mapped[str] = mapped_column(nullable=True)
    contributorNameSuffixCd: Mapped[str] = mapped_column(nullable=True)
    contributorNameFirst: Mapped[str] = mapped_column(nullable=True)
    contributorNamePrefixCd: Mapped[str] = mapped_column(nullable=True)
    contributorNameShort: Mapped[str] = mapped_column(nullable=True)
    contributorStreetCity: Mapped[str] = mapped_column(nullable=True)
    contributorStreetStateCd: Mapped[str] = mapped_column(nullable=True)

    contributorStreetCountyCd: Mapped[str] = mapped_column(nullable=True)
    contributorStreetCountryCd: Mapped[str] = mapped_column(nullable=True)
    contributorStreetPostalCode: Mapped[str] = mapped_column(nullable=True)
    contributorStreetRegion: Mapped[str] = mapped_column(nullable=True)
    contributorEmployer: Mapped[str] = mapped_column(nullable=True)
    contributorOccupation: Mapped[str] = mapped_column(nullable=True)
    contributorJobTitle: Mapped[str] = mapped_column(nullable=True)
    contributorPacFein: Mapped[str] = mapped_column(nullable=True)
    contributorOosPacFlag = Column(Boolean, nullable=True)
    contributorLawFirmName: Mapped[str] = mapped_column(nullable=True)
    contributorSpouseLawFirmName: Mapped[str] = mapped_column(nullable=True)
    contributorParent1LawFirmName: Mapped[str] = mapped_column(nullable=True)
    contributorParent2LawFirmName: Mapped[str] = mapped_column(nullable=True)
    file_origin: Mapped[str] = mapped_column(nullable=False)


    filer_id: Mapped[int] = mapped_column(
        ForeignKey("texas.filers.filerIdent"), nullable=False, unique=False
    )
    filers: Mapped[Set["TECFilerRecord"]] = relationship(
        back_populates="contributions",
        foreign_keys=[filer_id],
        primaryjoin="and_(TECContributionRecord.filer_id == TECFilerRecord.filerIdent)",
    )
