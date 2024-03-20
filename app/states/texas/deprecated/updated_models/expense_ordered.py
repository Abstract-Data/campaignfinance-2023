# from typing import Optional, Annotated, List
# from sqlmodel import Field, SQLModel, Relationship
# from datetime import date
#
#
# class ExpenditureModel(SQLModel, table=True):
#     __tablename__ = 'expenditures'
#     __table_args__ = {"schema": 'texas'}
#
#     id: Optional[int] = Field(default=None, primary_key=True)
#     recordType: Optional[str]
#     formTypeCd: Optional[str]
#     schedFormTypeCd: Optional[str]
#     reportInfoIdent: int
#     receivedDt: date
#     infoOnlyFlag: Optional[str]
#     filerIdent: Optional[int]
#     filerTypeCd: Optional[str]
#     filerName: Optional[str]
#     expendInfoId: int = Field(unique=True)
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
#     payee: "PayeeModel" = Relationship(back_populates="expenditures")
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
#     expenditures: List[ExpenditureModel] = Relationship(back_populates="payee")
