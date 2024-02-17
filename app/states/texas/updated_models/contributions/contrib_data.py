from typing import Optional, List, Annotated
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import date
from ...database import Base


class ContributionDataModel(Base):
    __tablename__ = 'contribution_data'
    __table_args__ = {"schema": 'texas'}

    recordType: Mapped[str]
    formTypeCd: Mapped[str]
    schedFormTypeCd: Mapped[str]
    reportInfoIdent: Mapped[int]
    receivedDt: Mapped[date]
    infoOnlyFlag: Mapped[str]
    filerIdent: Mapped[int]
    filerTypeCd: Mapped[str]
    filerName: Mapped[str]
    contributionInfoId: Mapped[str] = mapped_column(primary_key=True)
    contributionDt: Mapped[date]
    contributionAmount: Mapped[float]
    contributionDescr: Mapped[str]
    itemizeFlag: Mapped[str]
    travelFlag: Mapped[str]
    contributorNameAddressKey = Annotated[str, mapped_column(ForeignKey('contributor_names.contributorNameAddressKey'))]
    contributorOrgKey = Annotated[str, mapped_column(ForeignKey('contributor_organizations.contributorOrgKey'))]
