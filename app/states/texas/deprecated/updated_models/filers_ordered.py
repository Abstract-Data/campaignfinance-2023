# from typing import Optional, List
# from sqlmodel import Field, SQLModel, Relationship, Table, Integer, ForeignKey, Column
# from datetime import date
#
#
# # expenditure_data_link = Table(
# #     "expenditure_data_link",
# #     SQLModel.metadata,
# #     Column("filer_ident", Integer, ForeignKey("filer_names.filerIdent")),
# #     Column("expenditure_id", Integer, ForeignKey("expenditures.id")),
# #     schema="texas"
# # )
# #
# # contribution_data_link = Table(
# #     "contribution_data_link",
# #     SQLModel.metadata,
# #     Column("filer_ident", Integer, ForeignKey("filer_names.filerIdent")),
# #     Column("contribution_id", Integer, ForeignKey("contribution_data.id")),
# #     schema="texas"
# # )
# #
# # candidate_data_link = Table(
# #     "candidate_data_link",
# #     SQLModel.metadata,
# #     Column("filer_ident", Integer, ForeignKey("filer_names.filerIdent")),
# #     Column("candidate_id", Integer, ForeignKey("candidate_data.id")),
# #     schema="texas"
# # )
# #
# # filer_treasurer_link = Table(
# #     "filer_treasurer_link",
# #     SQLModel.metadata,
# #     Column("filer_ident", Integer, ForeignKey("filer_names.filerIdent")),
# #     Column("treasurer_id", Integer, ForeignKey("treasurers.id")),
# #     schema="texas"
# # )
# #
# # filer_asst_treasurer_link = Table(
# #     "filer_asst_treasurer_link",
# #     SQLModel.metadata,
# #     Column("filer_ident", Integer, ForeignKey("filer_names.filerIdent")),
# #     Column("asst_treasurer_id", Integer, ForeignKey("assistant_treasurers.id")),
# #     schema="texas"
# # )
# #
# # filer_chair_link = Table(
# #     "filer_chair_link",
# #     SQLModel.metadata,
# #     Column("filer_ident", Integer, ForeignKey("filer_names.filerIdent")),
# #     Column("chair_id", Integer, ForeignKey("chairs.id")),
# #     schema="texas"
# # )
# #
# # reports_filer_link = Table(
# #     "reports_filer_link",
# #     SQLModel.metadata,
# #     Column("filer_ident", Integer, ForeignKey("filer_names.filerIdent")),
# #     Column("report_id", Integer, ForeignKey("final_reports.reportInfoIdent")),  # Corrected ForeignKey
# #     schema="texas"
# # )
#
#
# class ExpenditureModel(SQLModel, table=True):
#     __tablename__ = 'expenditures'
#     __table_args__ = {"schema": 'texas'}
#
#     recordType: Optional[str]
#     formTypeCd: Optional[str]
#     schedFormTypeCd: Optional[str]
#     reportInfoIdent: int
#     receivedDt: date
#     infoOnlyFlag: Optional[str]
#     filerIdent: Optional[int] = Field(foreign_key="texas.filers.filerIdent")
#     filerTypeCd: Optional[str]
#     filerName: Optional[str]
#     expendInfoId: int = Field(primary_key=True)
#     expendDt: date
#     expendAmount: float
#     expendDescr: Optional[str]
#     expendCatCd: Optional[str]
#     expendCatDescr: Optional[str]
#     itemizeFlag: Optional[str]
#     travelFlag: Optional[str]
#     politicalExpendCd: Optional[str]
#     reimburseIntendedFlag: Optional[str]
#     srcCorpContribFlag: Optional[str]
#     capitalLivingexpFlag: Optional[str]
#     creditCardIssuer: Optional[str]
#     expendPayee: Optional[str]
#     payeeId: Optional[str]
#     # payee: "PayeeModel" = Relationship(back_populates="expenditure")
#     filer: "FilerModel" = Relationship(back_populates="expenses")
#
#
# class PayeeModel(SQLModel, table=True):
#     __tablename__ = 'payees'
#     __table_args__ = {"schema": 'texas'}
#
#     id: int = Field(default=None, primary_key=True)
#     payeePersentTypeCd: Optional[str]
#     payeeNameOrganization: Optional[str]
#     payeeNameLast: Optional[str]
#     payeeNameSuffixCd: Optional[str]
#     payeeNameFirst: Optional[str]
#     payeeNamePrefixCd: Optional[str]
#     payeeNameShort: Optional[str]
#     payeeStreetAddr1: Optional[str]
#     payeeStreetAddr2: Optional[str]
#     payeeStreetCity: Optional[str]
#     payeeStreetStateCd: Optional[str]
#     payeeStreetCountyCd: Optional[str]
#     payeeStreetCountryCd: Optional[str]
#     payeeStreetPostalCode: Optional[str]
#     payeeStreetRegion: Optional[str]
#     AddressNumber: Optional[str]
#     StreetNamePreDirectional: Optional[str]
#     StreetNamePreType: Optional[str]
#     StreetNamePreModifier: Optional[str]
#     StreetName: Optional[str]
#     StreetNamePostDirectional: Optional[str]
#     StreetNamePostModifier: Optional[str]
#     StreetNamePostType: Optional[str]
#     OccupancyIdentifier: Optional[str]
#     OccupancyType: Optional[str]
#     payeeId: Optional[str]
#     payeeAddressKey: Optional[str]
#     payeeNameKey: Optional[str]
#     expendInfoId: Optional[int] = Field(default=None, foreign_key="texas.expenditures.expendInfoId")
#     # expenditure: List["ExpenditureModel"] = Relationship(back_populates="payee")
#
#
# class ContributorDetailModel(SQLModel, table=True):
#     __tablename__ = 'contributor_name'
#     __table_args__ = {"schema": 'texas'}
#     contributionInfoId: int = Field(primary_key=True)
#     contributorNameOrganization: str
#     contributorNameLast: str
#     contributorNameSuffixCd: Optional[str]
#     contributorNameFirst: str
#     contributorNamePrefixCd: Optional[str]
#     contributorNameShort: Optional[str]
#     contributorStreetCity: str
#     contributorStreetStateCd: str
#     contributorStreetCountyCd: str
#     contributorStreetCountryCd: str
#     contributorStreetPostalCode: str
#     contributorStreetRegion: str
#     contributionAddressStandardized: str
#     contributorEmployer: Optional[str]
#     contributorOccupation: Optional[str]
#     contributorJobTitle: Optional[str]
#     contributorPacFein: Optional[str]
#     contributorOosPacFlag: Optional[bool]
#     contributorLawFirmName: Optional[str]
#     contributorSpouseLawFirmName: Optional[str]
#     contributorParent1LawFirmName: Optional[str]
#     contributorParent2LawFirmName: Optional[str]
#     contributorNameKey: Optional[str]
#     contributorOrgKey: Optional[str] = Field(unique=True)
#     contributorAddressKey: Optional[str]
#     contributorNameAddressKey: Optional[str] = Field(unique=True)
#
#
# class ContributionDataModel(SQLModel, table=True):
#     __tablename__ = 'contribution_data'
#     __table_args__ = {"schema": 'texas'}
#
#     recordType: str
#     formTypeCd: str
#     schedFormTypeCd: str
#     reportInfoIdent: int
#     receivedDt: date
#     infoOnlyFlag: Optional[str]
#     filerIdent: int = Field(foreign_key='texas.filers.filerIdent')
#     filerTypeCd: str
#     filerName: str
#     contributionInfoId: str = Field(primary_key=True, unique=True)
#     contributionDt: date
#     contributionAmount: float
#     contributionDescr: str
#     itemizeFlag: Optional[str]
#     travelFlag: Optional[str]
#     contributorNameAddressKey: Optional[str] = Field(foreign_key='texas.contributor_name.contributorNameAddressKey', nullable=True, unique=True)
#     contributorOrgKey: Optional[str] = Field(foreign_key='texas.contributor_name.contributorOrgKey', nullable=True, unique=True)
#     filer: "FilerModel" = Relationship(back_populates="contributions")
#
#
# class FilerModel(SQLModel, table=True):
#     __tablename__ = 'filers'
#     __table_args__ = {"schema": 'texas'}
#     filerIdent: int = Field(primary_key=True)
#     filerTypeCd: str
#     filer_name: List["FilerNameModel"] = Relationship(back_populates="filer")
#     candidates: List["CandidateDataModel"] = Relationship(back_populates="filer")
#     reports: List["FinalReportModel"] = Relationship(back_populates="filer")
#     treasurer: List["TreasurerModel"] = Relationship(back_populates="filer")
#     assistant_treasurer: List["AssistantTreasurerModel"] = Relationship(back_populates="filer")
#     chair: List["ChairModel"] = Relationship(back_populates="filer")
#     expenses: List["ExpenditureModel"] = Relationship(back_populates="filer")
#     contributions: List["ContributionDataModel"] = Relationship(back_populates="filer")
#
#
# class FinalReportModel(SQLModel, table=True):
#     __tablename__ = 'final_reports'
#     __table_args__ = {"schema": 'texas'}
#
#     recordType: Optional[str]
#     formTypeCd: Optional[str]
#     reportInfoIdent: int = Field(primary_key=True)
#     receivedDt: date
#     infoOnlyFlag: Optional[str]
#     filerIdent: Optional[int] = Field(foreign_key='texas.filers.filerIdent')
#     filerTypeCd: Optional[str]
#     filerName: Optional[str]
#     finalUnexpendContribFlag: Optional[str]
#     finalRetainedAssetsFlag: Optional[str]
#     finalOfficeholderAckFlag: Optional[str]
#     filerReportKey: Optional[str]
#     filer: List[FilerModel] = Relationship(back_populates="reports")
#
#
# class CandidateDataModel(SQLModel, table=True):
#     __tablename__ = 'candidate_data'
#     __table_args__ = {"schema": 'texas'}
#     id: Optional[int] = Field(primary_key=True)
#     recordType: Optional[str]
#     formTypeCd: Optional[str]
#     schedFormTypeCd: Optional[str]
#     reportInfoIdent: int
#     receivedDt: date
#     infoOnlyFlag: Optional[str]
#     filerIdent: int = Field(foreign_key='texas.filers.filerIdent')
#     filerTypeCd: Optional[str]
#     filerName: Optional[str]
#     expendInfoId: int
#     expendPersentId: int
#     expendDt: date
#     expendAmount: float
#     expendDescr: str
#     expendCatCd: Optional[str]
#     expendCatDescr: Optional[str]
#     itemizeFlag: Optional[str]
#     politicalExpendCd: Optional[str]
#     reimburseIntendedFlag: Optional[str]
#     srcCorpContribFlag: Optional[str]
#     capitalLivingexpFlag: Optional[str]
#     candidatePersentTypeCd: Optional[str]
#     candidateNameOrganization: Optional[str]
#     candidateNameLast: Optional[str]
#     candidateNameSuffixCd: Optional[str]
#     candidateNameFirst: Optional[str]
#     candidateNamePrefixCd: Optional[str]
#     candidateNameShort: Optional[str]
#     candidateHoldOfficeCd: Optional[str]
#     candidateHoldOfficeDistrict: Optional[str]
#     candidateHoldOfficePlace: Optional[str]
#     candidateHoldOfficeDescr: Optional[str]
#     candidateHoldOfficeCountyCd: Optional[str]
#     candidateHoldOfficeCountyDescr: Optional[str]
#     candidateSeekOfficeCd: Optional[str]
#     candidateSeekOfficeDistrict: Optional[str]
#     candidateSeekOfficePlace: Optional[str]
#     candidateSeekOfficeDescr: Optional[str]
#     candidateSeekOfficeCountyCd: Optional[str]
#     candidateSeekOfficeCountyDescr: Optional[str]
#     filer: List[FilerModel] = Relationship(back_populates="candidates")
#
#
# class TreasurerModel(SQLModel, table=True):
#     __tablename__ = 'treasurers'
#     __table_args__ = {"schema": 'texas'}
#     id: Optional[int] = Field(default=None, primary_key=True)
#     filerIdent: int = Field(foreign_key='texas.filers.filerIdent')
#     treasPersentTypeCd: str
#     treasNameOrganization: str
#     treasNameLast: str
#     treasNameSuffixCd: str
#     treasNameFirst: str
#     treasNamePrefixCd: str
#     treasNameShort: str
#     treasStreetAddr1: str
#     treasStreetAddr2: Optional[str]
#     treasStreetCity: str
#     treasStreetStateCd: str
#     treasStreetCountyCd: Optional[str]
#     treasStreetCountryCd: str
#     treasStreetPostalCode: str
#     treasStreetRegion: Optional[str]
#     treasStreetAddrStandardized: str
#     treasMailingAddr1: Optional[str]
#     treasMailingAddr2: Optional[str]
#     treasMailingCity: Optional[str]
#     treasMailingStateCd: Optional[str]
#     treasMailingCountyCd: Optional[str]
#     treasMailingCountryCd: Optional[str]
#     treasMailingPostalCode: Optional[str]
#     treasMailingRegion: Optional[str]
#     treasMailingAddrStandardized: Optional[str]
#     treasPrimaryUsaPhoneFlag: str
#     treasPrimaryPhoneNumber: str
#     treasPrimaryPhoneExt: str
#     treasAppointorNameLast: Optional[str]
#     treasAppointorNameFirst: Optional[str]
#     treasFilerpersStatusCd: str
#     treasEffStartDt: date
#     treasEffStopDt: date
#     treasurerNameKey: Optional[str]
#     treasurerAddressKey: Optional[str]
#     treasurerNameAddressKey: Optional[str] = Field(default=None, unique=True)
#     filer: "FilerModel" = Relationship(back_populates="treasurer")
#
#
# class AssistantTreasurerModel(SQLModel, table=True):
#     __tablename__ = 'assistant_treasurers'
#     __table_args__ = {"schema": 'texas'}
#
#     id: Optional[int] = Field(default=None, primary_key=True)
#     filerIdent: int = Field(foreign_key='texas.filers.filerIdent')
#     assttreasPersentTypeCd: Optional[str]
#     assttreasNameOrganization: Optional[str]
#     assttreasNameLast: Optional[str]
#     assttreasNameSuffixCd: Optional[str]
#     assttreasNameFirst: Optional[str]
#     assttreasNamePrefixCd: Optional[str]
#     assttreasNameShort: Optional[str]
#     assttreasStreetAddr1: Optional[str]
#     assttreasStreetAddr2: Optional[str]
#     assttreasStreetCity: Optional[str]
#     assttreasStreetStateCd: Optional[str]
#     assttreasStreetCountyCd: Optional[str]
#     assttreasStreetCountryCd: Optional[str]
#     assttreasStreetPostalCode: Optional[str]
#     assttreasStreetRegion: Optional[str]
#     assttreasStreetAddrStandardized: Optional[str]
#     assttreasPrimaryUsaPhoneFlag: Optional[str]
#     assttreasPrimaryPhoneNumber: Optional[str]
#     assttreasPrimaryPhoneExt: Optional[str]
#     assttreasAppointorNameLast: Optional[str]
#     assttreasAppointorNameFirst: Optional[str]
#     assistantTreasurerNameKey: Optional[str]
#     assistantTreasurerAddressKey: Optional[str]
#     assistantTreasurerNameAddressKey: Optional[str] = Field(default=None, unique=True)
#     filer: "FilerModel" = Relationship(back_populates="assistant_treasurer")
#
#
# class ChairModel(SQLModel, table=True):
#     __tablename__ = 'chairs'
#     __table_args__ = {"schema": 'texas'}
#
#     id: Optional[int] = Field(default=None, primary_key=True)
#     filerIdent: int = Field(foreign_key='texas.filers.filerIdent')
#     chairPersentTypeCd: Optional[str]
#     chairNameOrganization: Optional[str]
#     chairNameLast: Optional[str]
#     chairNameSuffixCd: Optional[str]
#     chairNameFirst: Optional[str]
#     chairNamePrefixCd: Optional[str]
#     chairNameShort: Optional[str]
#     chairStreetAddr1: Optional[str]
#     chairStreetAddr2: Optional[str]
#     chairStreetCity: Optional[str]
#     chairStreetStateCd: Optional[str]
#     chairStreetCountyCd: Optional[str]
#     chairStreetCountryCd: Optional[str]
#     chairStreetPostalCode: Optional[str]
#     chairStreetRegion: Optional[str]
#     chairStreetAddrStandardized: Optional[str]
#     chairMailingAddr1: Optional[str]
#     chairMailingAddr2: Optional[str]
#     chairMailingCity: Optional[str]
#     chairMailingStateCd: Optional[str]
#     chairMailingCountyCd: Optional[str]
#     chairMailingCountryCd: Optional[str]
#     chairMailingPostalCode: Optional[str]
#     chairMailingRegion: Optional[str]
#     chairMailingAddrStandardized: Optional[str]
#     chairPrimaryUsaPhoneFlag: Optional[str]
#     chairPrimaryPhoneNumber: Optional[str]
#     chairPrimaryPhoneExt: Optional[str]
#     chairNameKey: Optional[str]
#     chairAddressKey: Optional[str]
#     filer: "FilerModel" = Relationship(back_populates="chair")
#
#
# class FilerNameModel(SQLModel, table=True):
#     __tablename__ = 'filer_names'
#     __table_args__ = {"schema": 'texas'}
#
#     id: Optional[int] = Field(default=None, primary_key=True)
#     filerName: str
#     filerIdent: int = Field(foreign_key='texas.filers.filerIdent')
#     committeeStatusCd: str
#     ctaSeekOfficeCd: str
#     ctaSeekOfficeDistrict: str
#     ctaSeekOfficePlace: str
#     ctaSeekOfficeDescr: str
#     ctaSeekOfficeCountyCd: str
#     ctaSeekOfficeCountyDescr: str
#     filerPersentTypeCd: str
#     filerNameOrganization: str
#     filerNameLast: str
#     filerNameSuffixCd: str
#     filerNameFirst: str
#     filerNamePrefixCd: str
#     filerNameShort: str
#     filerStreetAddr1: str
#     filerStreetAddr2: str
#     filerStreetCity: str
#     filerStreetStateCd: str
#     filerStreetCountyCd: str
#     filerStreetCountryCd: str
#     filerStreetPostalCode: str
#     filerStreetRegion: str
#     filerMailingAddr1: str
#     filerMailingAddr2: str
#     filerMailingCity: str
#     filerMailingStateCd: str
#     filerMailingCountyCd: str
#     filerMailingCountryCd: str
#     filerMailingPostalCode: str
#     filerMailingRegion: str
#     filerPrimaryUsaPhoneFlag: str
#     filerPrimaryPhoneNumber: str
#     filerPrimaryPhoneExt: str
#     filerHoldOfficeCd: str
#     filerHoldOfficeDistrict: str
#     filerHoldOfficePlace: str
#     filerHoldOfficeDescr: str
#     filerHoldOfficeCountyCd: str
#     filerHoldOfficeCountyDescr: str
#     filerFilerpersStatusCd: str
#     filerEffStartDt: date
#     filerEffStopDt: date
#     contestSeekOfficeCd: str
#     contestSeekOfficeDistrict: str
#     contestSeekOfficePlace: str
#     contestSeekOfficeDescr: str
#     contestSeekOfficeCountyCd: str
#     contestSeekOfficeCountyDescr: str
#     # treasurerKey: str = Field(foreign_key='texas.treasurers.treasurerNameAddressKey')
#     # asstTreasurerKey: Optional[str] = Field(foreign_key='texas.assistant_treasurers.filerIdent')
#     # chairKey: Optional[str] = Field(foreign_key='texas.chairs.filerIdent')
#     # contributionKey: Optional[str] = Field(foreign_key='texas.contribution_data.filerIdent')
#     # reportKey: Optional[str] = Field('texas.final_reports.filerIdent')
#     # expenseKey: Optional[str] = Field('texas.expenditures.filerIdent')
#     filer: List["FilerModel"] = Relationship(back_populates="filer_name")
#     # treasurer: List["TreasurerModel"] = Relationship(back_populates="filer_name")
#     # assistant_treasurer: List["AssistantTreasurerModel"] = Relationship(back_populates="filer_name")
#     # chair: List["ChairModel"] = Relationship(back_populates="filer_name")
#     # reports: List["FinalReportModel"] = Relationship(back_populates="filer_name")
#     # expenses: List["ExpenditureModel"] = Relationship(back_populates="filer_name")
#     # contributions: List["ContributionDataModel"] = Relationship(back_populates="filer_name")
