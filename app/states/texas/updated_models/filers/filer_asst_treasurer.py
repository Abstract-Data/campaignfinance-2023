from typing import Optional, List
from sqlalchemy import ForeignKey, String, Date
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ...database import Base


class AssistantTreasurerModel(Base):
    __tablename__ = 'assistant_treasurers'
    __table_args__ = {"schema": 'texas'}

    filerIdent: Mapped[int] = mapped_column(primary_key=True)
    assttreasPersentTypeCd: Mapped[str]
    assttreasNameOrganization: Mapped[str]
    assttreasNameLast: Mapped[str]
    assttreasNameSuffixCd: Mapped[str]
    assttreasNameFirst: Mapped[str]
    assttreasNamePrefixCd: Mapped[str]
    assttreasNameShort: Mapped[str]
    assttreasStreetAddr1: Mapped[str]
    assttreasStreetAddr2: Mapped[str]
    assttreasStreetCity: Mapped[str]
    assttreasStreetStateCd: Mapped[str]
    assttreasStreetCountyCd: Mapped[str]
    assttreasStreetCountryCd: Mapped[str]
    assttreasStreetPostalCode: Mapped[str]
    assttreasStreetRegion: Mapped[str]
    assttreasStreetAddrStandardized: Mapped[str]
    assttreasPrimaryUsaPhoneFlag: Mapped[str]
    assttreasPrimaryPhoneNumber: Mapped[str]
    assttreasPrimaryPhoneExt: Mapped[str]
    assttreasAppointorNameLast: Mapped[str]
    assttreasAppointorNameFirst: Mapped[str]
    assistantTreasurerNameKey: Mapped[str]
    assistantTreasurerAddressKey: Mapped[str]