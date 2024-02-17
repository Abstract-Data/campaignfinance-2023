from pydantic import field_validator, Field, model_validator
from pydantic_extra_types.phone_numbers import PhoneNumber
from datetime import date, datetime
from typing import Optional, Annotated
from ..validators.texas_settings import TECSettings
from ..database import SessionLocal
from sqlalchemy.orm import Session
# from ..updated_models.filers import FilerModel
from funcs.validator_functions import clear_blank_strings, validate_phone_number, validate_date
import usaddress
import hashlib


class Filer(TECSettings):
    filerIdent: str
    filerTypeCd: str

    # @field_validator('filerIdent', 'filerTypeCd')
    # def check_string(cls, v):
    #     assert isinstance(v, str), 'value is not a string'
    #     return v

    @staticmethod
    def get(filer_ident: str):
        # create a new session
        db: Session = SessionLocal()

        # query the database for a Filer with the given identifier
        filer = db.query(Filer).filter(FilerModel.filerIdent == filer_ident).first()

        # close the session
        db.close()

        return filer

    _clear_blank_strings = model_validator(mode='before')(clear_blank_strings)


class Treasurer(TECSettings):
    filerIdent: str
    treasPersentTypeCd: Annotated[Optional[str],  Field(max_length=30)] = None
    treasNameOrganization: Annotated[Optional[str],  Field(max_length=100)] = None
    treasNameLast: Annotated[Optional[str],  Field(max_length=100)] = None
    treasNameSuffixCd: Annotated[Optional[str],  Field(max_length=30)] = None
    treasNameFirst: Annotated[Optional[str],  Field(max_length=45)] = None
    treasNamePrefixCd: Annotated[Optional[str],  Field(max_length=30)] = None
    treasNameShort: Annotated[Optional[str],  Field(max_length=25)] = None
    treasStreetAddr1: Annotated[Optional[str],  Field(max_length=55)] = None
    treasStreetAddr2: Annotated[Optional[str],  Field(max_length=55)] = None
    treasStreetCity: Annotated[Optional[str], Field(max_length=30)] = None
    treasStreetStateCd: Annotated[Optional[str], Field(max_length=2)] = None
    treasStreetCountyCd: Annotated[Optional[str], Field(max_length=5)] = None
    treasStreetCountryCd: Annotated[Optional[str], Field(max_length=3)] = None
    treasStreetPostalCode: Annotated[Optional[str], Field(max_length=20)] = None
    treasStreetRegion: Annotated[Optional[str],  Field(max_length=30)] = None
    treasStreetAddrStandardized: Annotated[Optional[str],  Field(max_length=255)] = None
    treasMailingAddr1: Annotated[Optional[str],  Field(max_length=55)] = None
    treasMailingAddr2: Annotated[Optional[str],  Field(max_length=55)] = None
    treasMailingCity: Annotated[Optional[str],  Field(max_length=30)] = None
    treasMailingStateCd: Annotated[Optional[str],  Field(max_length=2)] = None
    treasMailingCountyCd: Annotated[Optional[str],  Field(max_length=5)] = None
    treasMailingCountryCd: Annotated[Optional[str],  Field(max_length=3)] = None
    treasMailingPostalCode: Annotated[Optional[str],  Field(max_length=20)] = None
    treasMailingRegion: Annotated[Optional[str],  Field(max_length=30)] = None
    treasMailingAddrStandardized: Annotated[Optional[str],  Field(max_length=255)] = None
    treasPrimaryUsaPhoneFlag: Annotated[Optional[str],  Field(max_length=1)] = None
    treasPrimaryPhoneNumber: Annotated[Optional[str],  Field(max_length=20)] = None
    treasPrimaryPhoneExt: Annotated[Optional[str],  Field(max_length=10)] = None
    treasAppointorNameLast: Annotated[Optional[str],  Field(max_length=100)] = None
    treasAppointorNameFirst: Annotated[Optional[str],  Field(max_length=45)] = None
    treasFilerpersStatusCd: Annotated[Optional[str],  Field(max_length=30)] = None
    treasEffStartDt: Optional[date] = None
    treasEffStopDt: Optional[date] = None
    treasurerNameKey: str
    treasurerAddressKey: Optional[str] = None

    _clear_blank_strings = model_validator(mode='before')(clear_blank_strings)
    _validate_phone_number = field_validator('treasPrimaryPhoneNumber', mode='before')(validate_phone_number)
    _validate_date = field_validator('treasEffStartDt', 'treasEffStopDt', mode='before')(validate_date)

    @model_validator(mode='before')
    @classmethod
    def standardize_street_address(cls, values):
        _fields = [
            'treasStreetAddr1',
            'treasStreetAddr2',
            'treasStreetCity',
            'treasStreetStateCd',
            'treasStreetPostalCode',
        ]
        if any(_fields) in values:
            _address = ' '.join(values[x] for x in _fields if x is not None)
            _address = usaddress.parse(_address)
            values['treasStreetAddrStandardized'] = ' '.join(x[0] for x in _address if x)
        return values

    @model_validator(mode='before')
    @classmethod
    def standardize_mailing_address(cls, values):
        _fields = [
            'treasMailingAddr1',
            'treasMailingAddr2',
            'treasMailingCity',
            'treasMailingStateCd',
            'treasMailingPostalCode',
        ]
        if any(_fields) in values:
            _address = ' '.join(values[x] for x in _fields if x is not None)
            _address = usaddress.parse(_address)
            values['treasMailingAddrStandardized'] = ' '.join(x[0] for x in _address if x)
        return values

    @model_validator(mode='before')
    @classmethod
    def create_treasurer_name_key(cls, values):
        _fields = [
            'treasNameOrganization',
            'treasNameLast',
            'treasNameSuffixCd',
            'treasNameFirst',
            'treasNamePrefixCd',
            'treasNameShort',
        ]
        if any(_fields) in values:
            key = ''.join([values[x] for x in _fields if x is not None])
            values['treasurerNameKey'] = hashlib.sha256(key.encode()).hexdigest()
        else:
            values['treasurerNameKey'] = 'Not filed'
        return values

    @model_validator(mode='after')
    @classmethod
    def create_treasurer_address_key(cls, values):
        _fields = [
            'treasStreetAddrStandardized',
            'treasMailingAddrStandardized'
        ]
        if any(_fields) in values:
            _address = values['treasStreetAddrStandardized'] \
                if values['treasStreetAddrStandardized'] else values['treasMailingAddrStandardized']
            values['treasurerAddressKey'] = hashlib.sha256(_address.encode()).hexdigest()
        return values


    # @field_validator('*')
    # def check_string(cls, v):
    #     assert isinstance(v, str), 'value is not a string'
    #     return v

    # @field_validator('filerIdent')
    # def check_filer_exists(cls, v):
    #     assert Filer.get(v) is not None, 'Filer ID does not exist'
    #     return v


class AssistantTreasurer(TECSettings):
    filerIdent: str
    assttreasPersentTypeCd: Annotated[Optional[str], Field(max_length=30)] = None
    assttreasNameOrganization: Annotated[Optional[str], Field(max_length=100)] = None
    assttreasNameLast: Annotated[Optional[str], Field(max_length=100)] = None
    assttreasNameSuffixCd: Annotated[Optional[str], Field(max_length=30)] = None
    assttreasNameFirst: Annotated[Optional[str], Field(max_length=45)] = None
    assttreasNamePrefixCd: Annotated[Optional[str], Field(max_length=30)] = None
    assttreasNameShort: Annotated[Optional[str], Field(max_length=25)] = None
    assttreasStreetAddr1: Annotated[Optional[str], Field(max_length=55)] = None
    assttreasStreetAddr2: Annotated[Optional[str], Field(max_length=55)] = None
    assttreasStreetCity: Annotated[Optional[str], Field(max_length=30)] = None
    assttreasStreetStateCd: Annotated[Optional[str], Field(max_length=2)] = None
    assttreasStreetCountyCd: Annotated[Optional[str], Field(max_length=5)] = None
    assttreasStreetCountryCd: Annotated[Optional[str], Field(max_length=3)] = None
    assttreasStreetPostalCode: Annotated[Optional[str], Field(max_length=20)] = None
    assttreasStreetRegion: Annotated[Optional[str], Field(max_length=30)] = None
    assttreasStreetAddrStandardized: Annotated[Optional[str], Field(max_length=255)] = None
    assttreasPrimaryUsaPhoneFlag: Annotated[Optional[str], Field(max_length=1)] = None
    assttreasPrimaryPhoneNumber: Annotated[Optional[str], Field(max_length=20)] = None
    assttreasPrimaryPhoneExt: Annotated[Optional[str], Field(max_length=10)] = None
    assttreasAppointorNameLast: Annotated[Optional[str], Field(max_length=100)] = None
    assttreasAppointorNameFirst: Annotated[Optional[str], Field(max_length=45)] = None
    assistantTreasurerNameKey: Optional[str] = None
    assistantTreasurerAddressKey: Optional[str] = None

    _clear_blank_strings = model_validator(mode='before')(clear_blank_strings)
    _validate_phone_number = field_validator('assttreasPrimaryPhoneNumber', mode='before')(validate_phone_number)

    @model_validator(mode='before')
    @classmethod
    def create_assistant_treasurer_name_key(cls, values):
        _fields = [
            'assttreasNameOrganization',
            'assttreasNameLast',
            'assttreasNameSuffixCd',
            'assttreasNameFirst',
            'assttreasNamePrefixCd',
            'assttreasNameShort',
        ]
        if any(_fields) in values:
            key = ''.join(values[x] for x in _fields if x is not None)
            values['assistantTreasurerNameKey'] = hashlib.sha256(key.encode()).hexdigest() if key else None
        return values

    @model_validator(mode='before')
    @classmethod
    def standardize_address(cls, values):
        _fields = [
            'assttreasStreetAddr1',
            'assttreasStreetAddr2',
            'assttreasStreetCity',
            'assttreasStreetStateCd',
            'assttreasStreetPostalCode',
        ]
        if any(_fields) in values:
            _address = ' '.join(x for x in _fields if x is not None)
            _address = usaddress.parse(_address)
            values['assttreasStreetAddrStandardized'] = ' '.join(x[0] for x in _address if x)
        return values

    @model_validator(mode='after')
    @classmethod
    def create_assistant_treasurer_address_key(cls, values):
        if 'assttreasStreetAddrStandardized' in values:
            values['assistantTreasurerAddressKey'] = hashlib.sha256(
                values['assttreasStreetAddrStandardized'].encode()).hexdigest()
        return values


class Chair(TECSettings):
    filerIdent: str
    chairPersentTypeCd: Annotated[Optional[str], Field(max_length=30)] = None
    chairNameOrganization: Annotated[Optional[str], Field(max_length=100)] = None
    chairNameLast: Annotated[Optional[str], Field(max_length=100)] = None
    chairNameSuffixCd: Annotated[Optional[str], Field(max_length=30)] = None
    chairNameFirst: Annotated[Optional[str], Field(max_length=45)] = None
    chairNamePrefixCd: Annotated[Optional[str], Field(max_length=30)] = None
    chairNameShort: Annotated[Optional[str], Field(max_length=25)] = None
    chairStreetAddr1: Annotated[Optional[str], Field(max_length=55)] = None
    chairStreetAddr2: Annotated[Optional[str], Field(max_length=55)] = None
    chairStreetCity: Annotated[Optional[str], Field(max_length=30)] = None
    chairStreetStateCd: Annotated[Optional[str], Field(max_length=2)] = None
    chairStreetCountyCd: Annotated[Optional[str], Field(max_length=5)] = None
    chairStreetCountryCd: Annotated[Optional[str], Field(max_length=3)] = None
    chairStreetPostalCode: Annotated[Optional[str], Field(max_length=20)] = None
    chairStreetRegion: Annotated[Optional[str], Field(max_length=30)] = None
    chairStreetAddrStandardized: Annotated[Optional[str], Field(max_length=255)] = None
    chairMailingAddr1: Annotated[Optional[str], Field(max_length=55)] = None
    chairMailingAddr2: Annotated[Optional[str], Field(max_length=55)] = None
    chairMailingCity: Annotated[Optional[str], Field(max_length=30)] = None
    chairMailingStateCd: Annotated[Optional[str], Field(max_length=2)] = None
    chairMailingCountyCd: Annotated[Optional[str], Field(max_length=5)] = None
    chairMailingCountryCd: Annotated[Optional[str], Field(max_length=3)] = None
    chairMailingPostalCode: Annotated[Optional[str], Field(max_length=20)] = None
    chairMailingRegion: Annotated[Optional[str], Field(max_length=30)] = None
    chairMailingAddrStandardized: Annotated[Optional[str], Field(max_length=255)] = None
    chairPrimaryUsaPhoneFlag: Annotated[Optional[str], Field(max_length=1)] = None
    chairPrimaryPhoneNumber: Annotated[Optional[str], Field(max_length=20)] = None
    chairPrimaryPhoneExt: Annotated[Optional[str], Field(max_length=10)] = None
    chairNameKey: Optional[str] = None
    chairAddressKey: Optional[str] = None
    _clear_blank_strings = model_validator(mode='before')(clear_blank_strings)
    _validate_phone_number = field_validator('chairPrimaryPhoneNumber', mode='before')(validate_phone_number)

    @model_validator(mode='before')
    @classmethod
    def standardize_street_address(cls, values):
        _fields = [
            'chairStreetAddr1',
            'chairStreetAddr2',
            'chairStreetCity',
            'chairStreetStateCd',
            'chairStreetPostalCode',
        ]
        if any(_fields) in values:
            _address = ' '.join(values[x] for x in _fields if x is not None)
            _address = usaddress.parse(_address)
            values['chairStreetAddrStandardized'] = ' '.join(x[0] for x in _address if x)
        return values

    @model_validator(mode='before')
    @classmethod
    def standardize_mailing_address(cls, values):
        _fields = [
            'chairMailingAddr1',
            'chairMailingAddr2',
            'chairMailingCity',
            'chairMailingStateCd',
            'chairMailingPostalCode',
        ]
        if any(_fields) in values:
            _address = ' '.join(values[x] for x in _fields if x is not None)
            _address = usaddress.parse(_address)
            values['chairMailingAddrStandardized'] = ' '.join(x[0] for x in _address if x)
        return values

    @model_validator(mode='before')
    @classmethod
    def create_chair_name_key(cls, values):
        _fields = [
            'chairNameOrganization',
            'chairNameLast',
            'chairNameSuffixCd',
            'chairNameFirst',
            'chairNamePrefixCd',
            'chairNameShort',
        ]
        if any(_fields) in values:
            key = ''.join(values[x] for x in _fields if x is not None)
            values['chairNameKey'] = hashlib.sha256(key.encode()).hexdigest()
        return values

    @model_validator(mode='after')
    @classmethod
    def create_chair_address_key(cls, values):
        _fields = [
            'chairStreetAddrStandardized',
            'chairMailingAddrStandardized'
        ]
        if any(_fields) in values:
            _address = values['chairStreetAddrStandardized'] \
                if values['chairStreetAddrStandardized'] else values['chairMailingAddrStandardized']
            values['chairAddressKey'] = hashlib.sha256(_address.encode()).hexdigest()
        return values


class FilerName(TECSettings):
    filerName: str
    filerIdent: str
    committeeStatusCd: Optional[str] = None
    ctaSeekOfficeCd: Optional[str] = None
    ctaSeekOfficeDistrict: Optional[str] = None
    ctaSeekOfficePlace: Optional[str] = None
    ctaSeekOfficeDescr: Optional[str] = None
    ctaSeekOfficeCountyCd: Optional[str] = None
    ctaSeekOfficeCountyDescr: Optional[str] = None
    filerPersentTypeCd: Optional[str] = None
    filerNameOrganization: Optional[str] = None
    filerNameLast: Optional[str] = None
    filerNameSuffixCd: Optional[str] = None
    filerNameFirst: Optional[str] = None
    filerNamePrefixCd: Optional[str] = None
    filerNameShort: Optional[str] = None
    filerStreetAddr1: Optional[str] = None
    filerStreetAddr2: Optional[str] = None
    filerStreetCity: Optional[str] = None
    filerStreetStateCd: Optional[str] = None
    filerStreetCountyCd: Optional[str] = None
    filerStreetCountryCd: Optional[str] = None
    filerStreetPostalCode: Optional[str] = None
    filerStreetRegion: Optional[str] = None
    filerMailingAddr1: Optional[str] = None
    filerMailingAddr2: Optional[str] = None
    filerMailingCity: Optional[str] = None
    filerMailingStateCd: Optional[str] = None
    filerMailingCountyCd: Optional[str] = None
    filerMailingCountryCd: Optional[str] = None
    filerMailingPostalCode: Optional[str] = None
    filerMailingRegion: Optional[str] = None
    filerPrimaryUsaPhoneFlag: Optional[str] = None
    filerPrimaryPhoneNumber: Optional[PhoneNumber] = None
    filerPrimaryPhoneExt: Optional[str] = None
    filerHoldOfficeCd: Optional[str] = None
    filerHoldOfficeDistrict: Optional[str] = None
    filerHoldOfficePlace: Optional[str] = None
    filerHoldOfficeDescr: Optional[str] = None
    filerHoldOfficeCountyCd: Optional[str] = None
    filerHoldOfficeCountyDescr: Optional[str] = None
    filerFilerpersStatusCd: Optional[str] = None
    filerEffStartDt: Optional[date] = None
    filerEffStopDt: Optional[date] = None
    contestSeekOfficeCd: Optional[str] = None
    contestSeekOfficeDistrict: Optional[str] = None
    contestSeekOfficePlace: Optional[str] = None
    contestSeekOfficeDescr: Optional[str] = None
    contestSeekOfficeCountyCd: Optional[str] = None
    contestSeekOfficeCountyDescr: Optional[str] = None
    treasurerKey: str
    asstTreasurerKey: Annotated[Optional[str], Field(description="AssistantTreasurer.assistantTreasurerNameKey")] = None
    chairKey: Annotated[Optional[str], Field(description="Chairs.chairNameKey")] = None
    contributionKey: Annotated[Optional[str], Field(description="ContributionData.filerIdent")] = None

    _clear_blank_strings = model_validator(mode='before')(clear_blank_strings)
    _validate_phone_number = field_validator('filerPrimaryPhoneNumber', mode='before')(validate_phone_number)
    _validate_date = field_validator('filerEffStartDt', 'filerEffStopDt', mode='before')(validate_date)