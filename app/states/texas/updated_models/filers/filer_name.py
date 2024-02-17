from typing import Optional, List, Annotated
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import date
from ...database import Base


class FilerNameModel(Base):
    __tablename__ = 'filer_names'
    __table_args__ = {"schema": 'texas'}

    filerName: Mapped[str]
    filerIdent: Mapped[int] = mapped_column(primary_key=True)
    committeeStatusCd: Mapped[str]
    ctaSeekOfficeCd: Mapped[str]
    ctaSeekOfficeDistrict: Mapped[str]
    ctaSeekOfficePlace: Mapped[str]
    ctaSeekOfficeDescr: Mapped[str]
    ctaSeekOfficeCountyCd: Mapped[str]
    ctaSeekOfficeCountyDescr: Mapped[str]
    filerPersentTypeCd: Mapped[str]
    filerNameOrganization: Mapped[str]
    filerNameLast: Mapped[str]
    filerNameSuffixCd: Mapped[str]
    filerNameFirst: Mapped[str]
    filerNamePrefixCd: Mapped[str]
    filerNameShort: Mapped[str]
    filerStreetAddr1: Mapped[str]
    filerStreetAddr2: Mapped[str]
    filerStreetCity: Mapped[str]
    filerStreetStateCd: Mapped[str]
    filerStreetCountyCd: Mapped[str]
    filerStreetCountryCd: Mapped[str]
    filerStreetPostalCode: Mapped[str]
    filerStreetRegion: Mapped[str]
    filerMailingAddr1: Mapped[str]
    filerMailingAddr2: Mapped[str]
    filerMailingCity: Mapped[str]
    filerMailingStateCd: Mapped[str]
    filerMailingCountyCd: Mapped[str]
    filerMailingCountryCd: Mapped[str]
    filerMailingPostalCode: Mapped[str]
    filerMailingRegion: Mapped[str]
    filerPrimaryUsaPhoneFlag: Mapped[str]
    filerPrimaryPhoneNumber: Mapped[str]
    filerPrimaryPhoneExt: Mapped[str]
    filerHoldOfficeCd: Mapped[str]
    filerHoldOfficeDistrict: Mapped[str]
    filerHoldOfficePlace: Mapped[str]
    filerHoldOfficeDescr: Mapped[str]
    filerHoldOfficeCountyCd: Mapped[str]
    filerHoldOfficeCountyDescr: Mapped[str]
    filerFilerpersStatusCd: Mapped[str]
    filerEffStartDt: Mapped[date]
    filerEffStopDt: Mapped[date]
    contestSeekOfficeCd: Mapped[str]
    contestSeekOfficeDistrict: Mapped[str]
    contestSeekOfficePlace: Mapped[str]
    contestSeekOfficeDescr: Mapped[str]
    contestSeekOfficeCountyCd: Mapped[str]
    contestSeekOfficeCountyDescr: Mapped[str]
    treasurerKey = Annotated[str, mapped_column(ForeignKey('treasurers.treasurerNameKey', use_alter=True))]
    asstTreasurerKey = Annotated[Optional[str], mapped_column(ForeignKey('assistant_treasurers.assistantTreasurerNameKey', use_alter=True))]
    chairKey = Annotated[Optional[str], mapped_column(ForeignKey('chairs.chairNameKey', use_alter=True))]
    contributionKey = Annotated[Optional[str], mapped_column(ForeignKey('contribution_data.contributorNameAddressKey', use_alter=True))]
    reportKey = Annotated[Optional[str], mapped_column(ForeignKey('final_reports.reportKey', use_alter=True))]

    filer: Mapped["FilerModel"] = relationship(back_populates="filer_names")
    treasurer: Mapped["TreasurerModel"] = relationship(back_populates="filer_names")
    assistant_treasurer: Mapped["AssistantTreasurerModel"] = relationship(back_populates="filer_names")
    chair: Mapped["ChairModel"] = relationship(back_populates="filer_names")
    contribution: Mapped["ContributionDataModel"] = relationship(back_populates="filer_names")
    reports: Mapped[List["FinalReportModel"]] = relationship(back_populates="filer_names")
