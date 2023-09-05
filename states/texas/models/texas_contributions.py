from states.texas.database import Base
from sqlalchemy import Column, ForeignKey, Integer, String, text, Float, Boolean, Date
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import relationship
from datetime import date
from typing import List

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
    contributionDescr: Mapped[str] = mapped_column(nullable=False)
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
    contributorPacFein: Mapped[int] = mapped_column(nullable=True)
    contributorOosPacFlag = Column(Boolean, nullable=True)
    contributorLawFirmName: Mapped[str] = mapped_column(nullable=True)
    contributorSpouseLawFirmName: Mapped[str] = mapped_column(nullable=True)
    contributorParent1LawFirmName: Mapped[str] = mapped_column(nullable=True)
    contributorParent2LawFirmName: Mapped[str] = mapped_column(nullable=True)
    filer_id: Mapped[int] = mapped_column(ForeignKey("texas.filers.filerIdent"), nullable=False)
    filers: Mapped["TECFilerRecord"] = relationship(back_populates="contributions")