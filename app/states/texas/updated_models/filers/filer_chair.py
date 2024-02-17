from typing import Optional, List
from sqlalchemy import ForeignKey, String, Date
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ...database import Base


class ChairModel(Base):
    __tablename__ = 'chairs'
    __table_args__ = {"schema": 'texas'}

    filerIdent: Mapped[int] = mapped_column(primary_key=True)
    chairPersentTypeCd: Mapped[str]
    chairNameOrganization: Mapped[str]
    chairNameLast: Mapped[str]
    chairNameSuffixCd: Mapped[str]
    chairNameFirst: Mapped[str]
    chairNamePrefixCd: Mapped[str]
    chairNameShort: Mapped[str]
    chairStreetAddr1: Mapped[str]
    chairStreetAddr2: Mapped[str]
    chairStreetCity: Mapped[str]
    chairStreetStateCd: Mapped[str]
    chairStreetCountyCd: Mapped[str]
    chairStreetCountryCd: Mapped[str]
    chairStreetPostalCode: Mapped[str]
    chairStreetRegion: Mapped[str]
    chairStreetAddrStandardized: Mapped[str]
    chairMailingAddr1: Mapped[str]
    chairMailingAddr2: Mapped[str]
    chairMailingCity: Mapped[str]
    chairMailingStateCd: Mapped[str]
    chairMailingCountyCd: Mapped[str]
    chairMailingCountryCd: Mapped[str]
    chairMailingPostalCode: Mapped[str]
    chairMailingRegion: Mapped[str]
    chairMailingAddrStandardized: Mapped[str]
    chairPrimaryUsaPhoneFlag: Mapped[str]
    chairPrimaryPhoneNumber: Mapped[str]
    chairPrimaryPhoneExt: Mapped[str]
    chairNameKey: Mapped[Optional[str]]
    chairAddressKey: Mapped[Optional[str]]
