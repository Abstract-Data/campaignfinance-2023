from typing import Optional, List
from sqlalchemy import ForeignKey, String, Date
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ...database import Base


class ContributorDetailModel(Base):
    __tablename__ = 'contributors'
    __table_args__ = {"schema": 'texas'}

    contributorNameOrganization: Mapped[str]
    contributorNameLast: Mapped[str]
    contributorNameSuffixCd: Mapped[str]
    contributorNameFirst: Mapped[str]
    contributorNamePrefixCd: Mapped[str]
    contributorNameShort: Mapped[str]
    contributorStreetCity: Mapped[str]
    contributorStreetStateCd: Mapped[str]
    contributorStreetCountyCd: Mapped[str]
    contributorStreetCountryCd: Mapped[str]
    contributorStreetPostalCode: Mapped[str]
    contributorStreetRegion: Mapped[str]
    contributionAddressStandardized: Mapped[str]
    contributorEmployer: Mapped[str]
    contributorOccupation: Mapped[str]
    contributorJobTitle: Mapped[str]
    contributorPacFein: Mapped[str]
    contributorOosPacFlag: Mapped[str]
    contributorLawFirmName: Mapped[str]
    contributorSpouseLawFirmName: Mapped[str]
    contributorParent1LawFirmName: Mapped[str]
    contributorParent2LawFirmName: Mapped[str]
    contributorNameKey: Mapped[str]
    contributorOrgKey: Mapped[str]
    contributorAddressKey: Mapped[str]
    contributorNameAddressKey: Mapped[str] = mapped_column(primary_key=True)