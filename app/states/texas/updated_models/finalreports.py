from typing import Optional, List, Annotated
from sqlalchemy import ForeignKey, String
from datetime import date
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ..database import Base


class FinalReportModel(Base):
    __tablename__ = 'final_reports'
    __table_args__ = {"schema": 'texas'}

    recordType: Mapped[Optional[str]]
    formTypeCd: Mapped[Optional[str]]
    reportInfoIdent: Mapped[int] = mapped_column(primary_key=True)
    receivedDt: Mapped[date]
    infoOnlyFlag: Mapped[Optional[str]]
    filerIdent = Annotated[int, mapped_column(ForeignKey('filers.filerIdent', use_alter=True))]
    filerTypeCd: Mapped[Optional[str]]
    filerName: Mapped[Optional[str]]
    finalUnexpendContribFlag: Mapped[Optional[str]]
    finalRetainedAssetsFlag: Mapped[Optional[str]]
    finalOfficeholderAckFlag: Mapped[Optional[str]]
    filerReportKey: Mapped[Optional[str]]