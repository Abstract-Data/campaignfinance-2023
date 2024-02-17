from typing import Optional, List
from sqlalchemy import ForeignKey, String, Date
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ...database import Base


class PayeeModel(Base):
    __tablename__ = 'payees'
    __table_args__ = {"schema": 'texas'}

    id: Mapped[int] = mapped_column(primary_key=True)
    payeePersentTypeCd: Mapped[Optional[str]]
    payeeNameOrganization: Mapped[Optional[str]]
    payeeNameLast: Mapped[Optional[str]]
    payeeNameSuffixCd: Mapped[Optional[str]]
    payeeNameFirst: Mapped[Optional[str]]
    payeeNamePrefixCd: Mapped[Optional[str]]
    payeeNameShort: Mapped[Optional[str]]
    payeeStreetAddr1: Mapped[Optional[str]]
    payeeStreetAddr2: Mapped[Optional[str]]
    payeeStreetCity: Mapped[Optional[str]]
    payeeStreetStateCd: Mapped[Optional[str]]
    payeeStreetCountyCd: Mapped[Optional[str]]
    payeeStreetCountryCd: Mapped[Optional[str]]
    payeeStreetPostalCode: Mapped[Optional[str]]
    payeeStreetRegion: Mapped[Optional[str]]
    AddressNumber: Mapped[Optional[str]]
    StreetNamePreDirectional: Mapped[Optional[str]]
    StreetNamePreType: Mapped[Optional[str]]
    StreetNamePreModifier: Mapped[Optional[str]]
    StreetName: Mapped[Optional[str]]
    StreetNamePostDirectional: Mapped[Optional[str]]
    StreetNamePostModifier: Mapped[Optional[str]]
    StreetNamePostType: Mapped[Optional[str]]
    OccupancyIdentifier: Mapped[Optional[str]]
    OccupancyType: Mapped[Optional[str]]
    payeeId: Mapped[str]
    payeeAddressKey: Mapped[Optional[str]]
    payeeNameKey: Mapped[Optional[str]]
    expenditures: Mapped[List["ExpenditureModel"]] = relationship(back_populates="payee")