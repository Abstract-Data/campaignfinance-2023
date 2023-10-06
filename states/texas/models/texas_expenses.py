""" DO NOT USE THIS MODEL. USE ALL_MODELS MODULE INSTEAD"""

# from states.texas.database import Base
# from sqlalchemy import Column, ForeignKey, Integer, String, text, Float, Boolean
# from sqlalchemy.orm import relationship
# from sqlalchemy.orm import Mapped
# from sqlalchemy.orm import mapped_column
# from sqlalchemy.orm import DeclarativeBase
# from sqlalchemy.orm import relationship
# from datetime import date
# from typing import List
#
#
# class TECExpenseRecord(Base):
#     __tablename__ = "expenses"
#     __table_args__ = {"schema": "texas"}
#     recordType: Mapped[str] = mapped_column(nullable=False)
#     formTypeCd: Mapped[str] = mapped_column(nullable=False)
#     schedFormTypeCd: Mapped[str] = mapped_column(nullable=False)
#     reportInfoIdent: Mapped[int] = mapped_column(nullable=False)
#     receivedDt: Mapped[date] = mapped_column(nullable=False)
#     infoOnlyFlag: Mapped[bool] = mapped_column(nullable=True)
#     filerIdent: Mapped[int] = mapped_column(nullable=False)
#     filerTypeCd: Mapped[str] = mapped_column(nullable=False)
#     filerName: Mapped[str] = mapped_column(nullable=False)
#     expendInfoId: Mapped[int] = mapped_column(primary_key=True, nullable=False)
#     expendDt: Mapped[date] = mapped_column(nullable=False)
#     expendAmount: Mapped[float] = mapped_column(nullable=False)
#     expendDescr: Mapped[str] = mapped_column(nullable=False)
#     expendCatCd: Mapped[str] = mapped_column(nullable=False)
#     expendCatDescr: Mapped[str] = mapped_column(nullable=False)
#     itemizeFlag: Mapped[bool] = mapped_column(nullable=True)
#     travelFlag: Mapped[bool] = mapped_column(nullable=True)
#     politicalExpendCd: Mapped[str] = mapped_column(nullable=True)
#     reimburseIntendedFlag: Mapped[str] = mapped_column(nullable=True)
#     srcCorpContribFlag: Mapped[str] = mapped_column(nullable=True)
#     capitalLivingexpFlag: Mapped[str] = mapped_column(nullable=True)
#     payeePersentTypeCd: Mapped[str] = mapped_column(nullable=False)
#     payeeNameOrganization: Mapped[str] = mapped_column(nullable=True)
#     payeeNameLast: Mapped[str] = mapped_column(nullable=True)
#     payeeNameSuffixCd: Mapped[str] = mapped_column(nullable=True)
#     payeeNameFirst: Mapped[str] = mapped_column(nullable=True)
#     payeeNamePrefixCd: Mapped[str] = mapped_column(nullable=True)
#     payeeNameShort: Mapped[str] = mapped_column(nullable=True)
#     payeeStreetAddr1: Mapped[str] = mapped_column(nullable=False)
#     payeeStreetAddr2: Mapped[str] = mapped_column(nullable=True)
#     payeeStreetCity: Mapped[str] = mapped_column(nullable=False)
#     payeeStreetStateCd: Mapped[str] = mapped_column(nullable=False)
#     payeeStreetCountyCd: Mapped[str] = mapped_column(nullable=False)
#     payeeStreetCountryCd: Mapped[str] = mapped_column(nullable=False)
#     payeeStreetPostalCode: Mapped[str] = mapped_column(nullable=True)
#     payeeStreetRegion: Mapped[str] = mapped_column(nullable=True)
#     creditCardIssuer: Mapped[str] = mapped_column(nullable=True)
#     repaymentDt: Mapped[date] = mapped_column(nullable=True)
#     file_origin: Mapped[str] = mapped_column(nullable=False)
#     filer_id: Mapped[int] = mapped_column(ForeignKey("texas.filers.filerIdent"), nullable=False)
#     filers: Mapped["TECFilerRecord"] = relationship(back_populates="expenses")
