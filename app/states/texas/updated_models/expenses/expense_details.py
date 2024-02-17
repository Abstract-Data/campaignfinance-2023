from typing import Optional, Annotated
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import date
from ...database import Base


class ExpenditureModel(Base):
    __tablename__ = 'expenditures'
    __table_args__ = {"schema": 'texas'}

    id: Mapped[int] = mapped_column(primary_key=True)
    recordType: Mapped[Optional[str]]
    formTypeCd: Mapped[Optional[str]]
    schedFormTypeCd: Mapped[Optional[str]]
    reportInfoIdent: Mapped[int]
    receivedDt: Mapped[date]
    infoOnlyFlag: Mapped[Optional[str]]
    filerIdent: Mapped[Optional[str]]
    filerTypeCd: Mapped[Optional[str]]
    filerName: Mapped[Optional[str]]
    expendInfoId: Mapped[Optional[str]]
    expendDt: Mapped[date]
    expendAmount: Mapped[float]
    expendDescr: Mapped[Optional[str]]
    expendCatCd: Mapped[Optional[str]]
    expendCatDescr: Mapped[Optional[str]]
    itemizeFlag: Mapped[Optional[str]]
    travelFlag: Mapped[Optional[str]]
    politicalExpendCd: Mapped[Optional[str]]
    reimburseIntendedFlag: Mapped[Optional[str]]
    srcCorpContribFlag: Mapped[Optional[str]]
    capitalLivingexpFlag: Mapped[Optional[str]]
    creditCardIssuer: Mapped[Optional[str]]
    payeeId = Annotated[str, mapped_column(ForeignKey('payees.payeeId'))]
    payee: Mapped["PayeeModel"] = relationship(back_populates="expenditures")