from typing import Optional
from pydantic import BaseModel, field_validator, model_validator
from datetime import date
import hashlib
import usaddress
from funcs.validator_functions import validate_date, create_address_dict, clear_blank_strings


class Payee(BaseModel):
    payeePersentTypeCd: Optional[str] = None
    payeeNameOrganization: Optional[str] = None
    payeeNameLast: Optional[str] = None
    payeeNameSuffixCd: Optional[str] = None
    payeeNameFirst: Optional[str] = None
    payeeNamePrefixCd: Optional[str] = None
    payeeNameShort: Optional[str] = None
    payeeStreetAddr1: Optional[str] = None
    payeeStreetAddr2: Optional[str] = None
    payeeStreetCity: Optional[str] = None
    payeeStreetStateCd: Optional[str] = None
    payeeStreetCountyCd: Optional[str] = None
    payeeStreetCountryCd: Optional[str] = None
    payeeStreetPostalCode: Optional[str] = None
    payeeStreetRegion: Optional[str] = None

    AddressNumber: Optional[str] = None
    StreetNamePreDirectional: Optional[str] = None
    StreetNamePreType: Optional[str] = None
    StreetNamePreModifier: Optional[str] = None
    StreetName: Optional[str] = None
    StreetNamePostDirectional: Optional[str] = None
    StreetNamePostModifier: Optional[str] = None
    StreetNamePostType: Optional[str] = None
    OccupancyIdentifier: Optional[str] = None
    OccupancyType: Optional[str] = None

    @property
    def payeeNameKey(self):
        _fields = [
            self.payeeNameOrganization,
            self.payeeNameLast,
            self.payeeNameSuffixCd,
            self.payeeNameFirst,
            self.payeeNamePrefixCd,
            self.payeeNameShort
        ]
        key = ''.join(x[0] for x in _fields if x is not None)
        return hashlib.sha256(key.encode()).hexdigest()

    @property
    def payeeAddressKey(self):
        _fields = [
            self.AddressNumber,
            self.StreetNamePreDirectional,
            self.StreetNamePreType,
            self.StreetNamePreModifier,
            self.StreetName,
            self.StreetNamePostDirectional,
            self.StreetNamePostModifier,
            self.StreetNamePostType,
            self.OccupancyIdentifier,
            self.OccupancyType
        ]
        key = ''.join(x for x in _fields if x is not None)
        return hashlib.sha256(key.encode()).hexdigest()

    @property
    def payeeId(self):
        key = f"{self.payeeNameKey}_{self.payeeAddressKey}"
        return hashlib.sha256(key.encode()).hexdigest()

    _clear_blank_strings = model_validator(mode='before')(clear_blank_strings)

    @model_validator(mode='before')
    def parse_address(cls, values):
        _fields = [
            'payeeStreetAddr1',
            'payeeStreetAddr2',
            'payeeStreetCity',
            'payeeStreetStateCd',
            'payeeStreetPostalCode',
        ]
        for field in _fields:
            if values[field] is not None:
                _address_components = create_address_dict(usaddress.parse(values[field]))
                for k, v in _address_components.items():
                    values[k] = v
        return values


class Expenditure(BaseModel):
    recordType: Optional[str] = None
    formTypeCd: Optional[str] = None
    schedFormTypeCd: Optional[str] = None
    reportInfoIdent: int
    receivedDt: Optional[date] = None
    infoOnlyFlag: Optional[str] = None
    filerIdent: Optional[str] = None
    filerTypeCd: Optional[str] = None
    filerName: Optional[str] = None
    expendInfoId: Optional[str] = None
    expendDt: Optional[date] = None
    expendAmount: float
    expendDescr: Optional[str] = None
    expendCatCd: Optional[str] = None
    expendCatDescr: Optional[str] = None
    itemizeFlag: Optional[str] = None
    travelFlag: Optional[str] = None
    politicalExpendCd: Optional[str] = None
    reimburseIntendedFlag: Optional[str] = None
    srcCorpContribFlag: Optional[str] = None
    capitalLivingexpFlag: Optional[str] = None
    creditCardIssuer: Optional[str] = None
    payeeId: str

    _clear_blank_strings = model_validator(mode='before')(clear_blank_strings)
    _validate_dates = field_validator('expendDt', 'receivedDt', mode='before')(validate_date)
