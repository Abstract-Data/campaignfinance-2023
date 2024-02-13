from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import relationship
from ..database import Base


class PayeeModel(Base):
    __tablename__ = 'payees'
    __tableargs__ = {"schema": 'texas'}

    id = Column(Integer, primary_key=True)
    payeePersentTypeCd = Column(String, nullable=True)
    payeeNameOrganization = Column(String, nullable=True)
    payeeNameLast = Column(String, nullable=True)
    payeeNameSuffixCd = Column(String, nullable=True)
    payeeNameFirst = Column(String, nullable=True)
    payeeNamePrefixCd = Column(String, nullable=True)
    payeeNameShort = Column(String, nullable=True)
    payeeStreetAddr1 = Column(String, nullable=True)
    payeeStreetAddr2 = Column(String, nullable=True)
    payeeStreetCity = Column(String, nullable=True)
    payeeStreetStateCd = Column(String, nullable=True)
    payeeStreetCountyCd = Column(String, nullable=True)
    payeeStreetCountryCd = Column(String, nullable=True)
    payeeStreetPostalCode = Column(String, nullable=True)
    payeeStreetRegion = Column(String, nullable=True)
    AddressNumber = Column(String, nullable=True)
    StreetNamePreDirectional = Column(String, nullable=True)
    StreetNamePreType = Column(String, nullable=True)
    StreetNamePreModifier = Column(String, nullable=True)
    StreetName = Column(String, nullable=True)
    StreetNamePostDirectional = Column(String, nullable=True)
    StreetNamePostModifier = Column(String, nullable=True)
    StreetNamePostType = Column(String, nullable=True)
    OccupancyIdentifier = Column(String, nullable=True)
    OccupancyType = Column(String, nullable=True)
    payeeID = Column(String, nullable=True)
    payeeAddressKey = Column(String, nullable=True)
    payeeNameKey = Column(String, nullable=True)

    expenditures = relationship("Expenditure", back_populates="payee")


class ExpenditureModel(Base):
    __tablename__ = 'expenditures'
    __tableargs__ = {"schema": 'texas'}

    id = Column(Integer, primary_key=True)
    recordType = Column(String, nullable=True)
    formTypeCd = Column(String, nullable=True)
    schedFormTypeCd = Column(String, nullable=True)
    reportInfoIdent = Column(ForeignKey('FinalReportModel.reportInfoIdent'), nullable=False)
    receivedDt = Column(Date, nullable=True)
    infoOnlyFlag = Column(String, nullable=True)
    filerIdent = Column(String, nullable=True)
    filerTypeCd = Column(String, nullable=True)
    filerName = Column(String, nullable=True)
    expendInfoId = Column(String, nullable=True)
    expendDt = Column(Date, nullable=True)
    expendAmount = Column(Float, nullable=False)
    expendDescr = Column(String, nullable=True)
    expendCatCd = Column(String, nullable=True)
    expendCatDescr = Column(String, nullable=True)
    itemizeFlag = Column(String, nullable=True)
    travelFlag = Column(String, nullable=True)
    politicalExpendCd = Column(String, nullable=True)
    reimburseIntendedFlag = Column(String, nullable=True)
    srcCorpContribFlag = Column(String, nullable=True)
    capitalLivingexpFlag = Column(String, nullable=True)
    creditCardIssuer = Column(String, nullable=True)

    payeeId = Column(Integer, ForeignKey('payees.payeeId'))
    payee = relationship("Payee", back_populates="expenditures")