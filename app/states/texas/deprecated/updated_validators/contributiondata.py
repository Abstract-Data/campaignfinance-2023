from typing import Optional, Annotated
from pydantic import BaseModel, field_validator, model_validator, Field
from datetime import date
from decimal import Decimal
from funcs.validator_functions import validate_date
import usaddress
import hashlib


class ContributorDetails(BaseModel):
    contributorNameOrganization: Optional[str] = None
    contributorNameLast: Optional[str] = None
    contributorNameSuffixCd: Optional[str] = None
    contributorNameFirst: Optional[str] = None
    contributorNamePrefixCd: Optional[str] = None
    contributorNameShort: Optional[str] = None
    contributorStreetCity: Optional[str] = None
    contributorStreetStateCd: Optional[str] = None
    contributorStreetCountyCd: Optional[str] = None
    contributorStreetCountryCd: Optional[str] = None
    contributorStreetPostalCode: Optional[str] = None
    contributorStreetRegion: Optional[str] = None
    contributorAddressStandardized: Optional[str] = None
    contributorEmployer: Optional[str] = None
    contributorOccupation: Optional[str] = None
    contributorJobTitle: Optional[str] = None
    contributorPacFein: Optional[str] = None
    contributorOosPacFlag: Optional[str] = None
    contributorLawFirmName: Optional[str] = None
    contributorSpouseLawFirmName: Optional[str] = None
    contributorParent1LawFirmName: Optional[str] = None
    contributorParent2LawFirmName: Optional[str] = None
    contributorNameKey: Optional[str] = None
    contributorOrgKey: Optional[str] = None
    contributorAddressKey: Optional[str] = None
    contributorNameAddressKey: Optional[str] = None

    @model_validator(mode='before')
    @classmethod
    def standardize_address(cls, values):
        _fields = [
            'contributorStreetCity',
            'contributorStreetStateCd',
            'contributorStreetCountyCd',
            'contributorStreetCountryCd',
            'contributorStreetPostalCode',
            'contributorStreetRegion',
        ]
        if any(_fields) in values:
            _address = ' '.join(values[x] for x in _fields if x is not None)
            _address = usaddress.parse(_address)
            values['contributorAddressStandardized'] = " ".join(x[0] for x in _address if x) if _address else None
        return values

    @model_validator(mode='before')
    @classmethod
    def create_contributor_name_key(cls, values):
        _fields = [
            'contributorNameLast',
            'contributorNameSuffixCd',
            'contributorNameFirst',
            'contributorNamePrefixCd',
            'contributorNameShort'
        ]
        if any(_fields) in values:
            key = ''.join(values[x] for x in _fields if x is not None)
            values['contributorNameKey'] = hashlib.sha256(key.encode()).hexdigest()
        return values

    @model_validator(mode='before')
    @classmethod
    def create_contributor_org_key(cls, values):
        _fields = [
            'contributorNameOrganization',
        ]
        if any(_fields) in values:
            key = ''.join(values[x] for x in _fields if x is not None)
            values['contributorOrgKey'] = hashlib.sha256(key.encode()).hexdigest()
        return values

    @model_validator(mode='before')
    @classmethod
    def create_contributor_address_key(cls, values):
        if 'contributorAddressStandardized' in values:
            values['contributorAddressKey'] = hashlib.sha256(
                values["contributorAddressStandardized"].encode()).hexdigest()
        return values

    @model_validator(mode='after')
    @classmethod
    def create_contributor_name_address_key(cls, values):
        if 'contributorNameKey' not in values and 'contributorOrgKey' not in values:
            raise ValueError('Must have either a contributorNameKey or contributorOrgKey')
        else:
            name_key = values['contributorNameKey'] \
                if values['contributorNameKey'] else values['contributorOrgKey']
            values['contributorNameAddressKey'] = hashlib.sha256(
                f"{name_key}{values['contributorAddressKey']}".encode()
            ).hexdigest()
        return values

    @model_validator(mode='after')
    def check_for_name_or_org_key(cls, values):
        if ['contributorNameKey', 'contributorOrgKey', 'contributorNameAddressKey'] not in values:
            raise ValueError('Must have either a contributorNameKey or contributorOrgKey or contributorNameAddressKey')
        return values


class ContributionData(BaseModel):
    recordType: Optional[str] = None
    formTypeCd: Optional[str] = None
    schedFormTypeCd: Optional[str] = None
    reportInfoIdent: Optional[int] = None
    receivedDt: Optional[date] = None
    infoOnlyFlag: Optional[str] = None
    filerIdent: Optional[str] = None
    filerTypeCd: Optional[str] = None
    filerName: Optional[str] = None
    contributionInfoId: Optional[int] = None
    contributionDt: Optional[date] = None
    contributionAmount: Optional[Decimal] = None
    contributionDescr: Optional[str] = None
    itemizeFlag: Optional[str] = None
    travelFlag: Optional[str] = None
    contributorNameAddressKey: Annotated[Optional[str], Field(description="ContributorDetail.contributorNameAddressKey")] = None
    contributorOrgKey: Annotated[Optional[str], Field(description="ContributorDetail.contributorOrgKey")] = None

    _validate_date = field_validator('contributionDt', 'receivedDt', mode='before')(validate_date)

    @field_validator('contributionAmount')
    def validate_contributionAmount(cls, v):
        if v is not None and v < 0:
            raise ValueError('contributionAmount must be a positive number')
        return v

    @model_validator(mode='after')
    @classmethod
    def check_for_name_or_org_key(cls, values):
        if ['contributorNameKey', 'contributorOrgKey', 'contributorNameAddressKey'] not in values:
            raise ValueError('Must have either a contributorNameKey or contributorOrgKey or contributorNameAddressKey')
        return values
