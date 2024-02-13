from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import text
from ..database import Base


class FinalReportModel(Base):
    __tablename__ = 'final_reports'
    __table_args__ = {"schema": 'texas'}

    recordType = Column(String, nullable=False)
    formTypeCd = Column(String, nullable=False)
    reportInfoIdent = Column(Integer, primary_key=True)
    receivedDt = Column(Date, nullable=False)
    infoOnlyFlag = Column(String, nullable=False)
    filerIdent = Column(String, ForeignKey('filers.filerIdent'), nullable=False)
    filerTypeCd = Column(String, nullable=False)
    filerName = Column(String, nullable=False)
    finalUnexpendContribFlag = Column(String, nullable=False)
    finalRetainedAssetsFlag = Column(String, nullable=False)
    finalOfficeholderAckFlag = Column(String, nullable=False)
    filerReportKey = Column(String, nullable=False)