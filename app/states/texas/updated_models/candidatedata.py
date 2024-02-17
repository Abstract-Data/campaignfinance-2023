from typing import Optional, List, Annotated
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import date
from ..database import Base


class CandidateDataModel(Base):
    __tablename__ = 'candidate_data'
    __table_args__ = {"schema": 'texas'}
    id: Mapped[int] = mapped_column(primary_key=True)
    recordType: Mapped[str]
    formTypeCd: Mapped[str]
    schedFormTypeCd: Mapped[str]
    reportInfoIdent: Mapped[int]
    receivedDt: Mapped[date]
    infoOnlyFlag: Mapped[str]
    filerIdent = Annotated[int, mapped_column(ForeignKey('filers.filerIdent', use_alter=True))]
    filerTypeCd: Mapped[str]
    filerName: Mapped[str]
    expendInfoId: Mapped[int]
    expendPersentId: Mapped[int]
    expendDt: Mapped[date]
    expendAmount: Mapped[float]
    expendDescr: Mapped[str]
    expendCatCd: Mapped[str]
    expendCatDescr: Mapped[str]
    itemizeFlag: Mapped[str]
    politicalExpendCd: Mapped[str]
    reimburseIntendedFlag: Mapped[str]
    srcCorpContribFlag: Mapped[str]
    capitalLivingexpFlag: Mapped[str]
    candidatePersentTypeCd: Mapped[str]
    candidateNameOrganization: Mapped[str]
    candidateNameLast: Mapped[str]
    candidateNameSuffixCd: Mapped[str]
    candidateNameFirst: Mapped[str]
    candidateNamePrefixCd: Mapped[str]
    candidateNameShort: Mapped[str]
    candidateHoldOfficeCd: Mapped[str]
    candidateHoldOfficeDistrict: Mapped[str]
    candidateHoldOfficePlace: Mapped[str]
    candidateHoldOfficeDescr: Mapped[str]
    candidateHoldOfficeCountyCd: Mapped[str]
    candidateHoldOfficeCountyDescr: Mapped[str]
    candidateSeekOfficeCd: Mapped[str]
    candidateSeekOfficeDistrict: Mapped[str]
    candidateSeekOfficePlace: Mapped[str]
    candidateSeekOfficeDescr: Mapped[str]
    candidateSeekOfficeCountyCd: Mapped[str]
    candidateSeekOfficeCountyDescr: Mapped[str]

    filer: Mapped["FilerModel"] = relationship(back_populates="candidates")