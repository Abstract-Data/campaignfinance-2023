from typing import Optional, List
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import date
from ...database import Base


class TreasurerModel(Base):
    __tablename__ = 'treasurers'
    __table_args__ = {"schema": 'texas'}
    filerIdent: Mapped[int] = mapped_column(primary_key=True)
    treasPersentTypeCd: Mapped[str]
    treasNameOrganization: Mapped[str]
    treasNameLast: Mapped[str]
    treasNameSuffixCd: Mapped[str]
    treasNameFirst: Mapped[str]
    treasNamePrefixCd: Mapped[str]
    treasNameShort: Mapped[str]
    treasStreetAddr1: Mapped[str]
    treasStreetAddr2: Mapped[str]
    treasStreetCity: Mapped[str]
    treasStreetStateCd: Mapped[str]
    treasStreetCountyCd: Mapped[str]
    treasStreetCountryCd: Mapped[str]
    treasStreetPostalCode: Mapped[str]
    treasStreetRegion: Mapped[str]
    treasStreetAddrStandardized: Mapped[str]
    treasMailingAddr1: Mapped[str]
    treasMailingAddr2: Mapped[str]
    treasMailingCity: Mapped[str]
    treasMailingStateCd: Mapped[str]
    treasMailingCountyCd: Mapped[str]
    treasMailingCountryCd: Mapped[str]
    treasMailingPostalCode: Mapped[str]
    treasMailingRegion: Mapped[str]
    treasMailingAddrStandardized: Mapped[str]
    treasPrimaryUsaPhoneFlag: Mapped[str]
    treasPrimaryPhoneNumber: Mapped[str]
    treasPrimaryPhoneExt: Mapped[str]
    treasAppointorNameLast: Mapped[str]
    treasAppointorNameFirst: Mapped[str]
    treasFilerpersStatusCd: Mapped[str]
    treasEffStartDt: Mapped[date]
    treasEffStopDt: Mapped[date]
    treasurerNameKey: Mapped[Optional[str]]
    treasurerAddressKey: Mapped[Optional[str]]
    filer: Mapped["FilerModel"] = relationship(back_populates="treasurer")