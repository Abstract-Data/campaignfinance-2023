from datetime import date
from typing import Optional

from pydantic import model_validator
from pydantic_core import PydanticCustomError
from pydantic_extra_types.phone_numbers import PhoneNumber
from sqlmodel import JSON, Field, Relationship

import app.states.texas.funcs.tx_validation_funcs as tx_funcs

from ._mixins import (
    _tec_address,
    bind_street_address_if_present,
    bind_street_mailing_pair,
    extract_address,
    format_filer_check_name,
)
from .texas_address import TECAddress
from .texas_settings import TECSettings

# TODO: Create a 'Corrections' variable to hold the corrections for
#  the data as a list to add each correction to the list.

class TECFilerLink(TECSettings, table=True):
    __tablename__ = "tx_filerlink"
    __table_args__ = {"schema": "texas"}
    filer_identity_id: int = Field(foreign_key='texas.tx_fileridentity.filerIdent', primary_key=True)
    filer_details_id: int = Field(foreign_key='texas.tx_filername.filerIdent', primary_key=True)


class TECTreasurerLink(TECSettings, table=True):
    __tablename__ = "tx_treasurerlink"
    __table_args__ = {"schema": "texas"}
    filer_identity_id: int = Field(foreign_key='texas.tx_filer.filerIdent', primary_key=True)
    treasurer_id: str = Field(foreign_key='texas.tx_treasurer.treasId', primary_key=True)

class TECFilerIdentity(TECSettings, table= True):
    __tablename__ = "tx_fileridentity"
    __table_args__ = {"schema": "texas"}
    filerIdent: int = Field(..., description="Filer account #", primary_key=True)
    filerTypeCd: str = Field(..., description="Type of filer")
    filerName: str = Field(..., description="Filer name")

    filer_name: list["TECFilerName"] = Relationship(back_populates="filer_name", link_model=TECFilerLink)


class TECFilerName(TECSettings, table=True):
    __tablename__ = "tx_filername"
    __table_args__ = {"schema": "texas"}
    filerIdent: int = Field(..., description="Filer account #", primary_key=True)
    unexpendContribFilerFlag: Optional[bool] = Field(
        default=None, description="Unexpended contribution filer flag")
    modifiedElectCycleFlag: Optional[bool] = Field(
        default=None, description="Modified election cycle flag")
    filerJdiCd: Optional[str] = Field(
        default=None, description="Judicial declaration of intent code ")
    committeeStatusCd: Optional[str] = Field(
        default=None, description="PAC filing status code")
    ctaSeekOfficeCd: Optional[str] = Field(
        default=None, description="CTA office sought")
    ctaSeekOfficeDistrict: Optional[str] = Field(
        default=None, description="CTA office sought district")
    ctaSeekOfficePlace: Optional[str] = Field(
        default=None, description="CTA office sought place")
    ctaSeekOfficeDescr: Optional[str] = Field(
        default=None, description="CTA office sought description")
    ctaSeekOfficeCountyCd: Optional[str] = Field(
        default=None, description="CTA office sought county code")
    ctaSeekOfficeCountyDescr: Optional[str] = Field(
        default=None, description="CTA office sought county description")
    filerPersentTypeCd: Optional[str] = Field(
        default=None, description="Type of filer name data - INDIVIDUAL or ENTITY")
    filerNameOrganization: Optional[str] = Field(
        default=None, description="For ENTITY, the filer organization name")
    filerNameLast: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the filer last name")
    filerNameSuffixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the filer suffix")
    filerNameFirst: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the filer first name")
    filerNameMiddle: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the filer middle name")
    filerNamePrefixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the filer prefix")
    filerNameFull: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, Full name of the filer")
    filerNameShort: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, Short name of the filer")
    filerAddress: TECAddress = Field(
        default=None, description="Filer address", sa_type=JSON)
    filerMailing: TECAddress = Field(
        default=None, description="Filer mailing address", sa_type=JSON)

    filer_name: Optional["TECFilerIdentity"] = Relationship(back_populates="filer_name", link_model=TECFilerLink)

    @model_validator(mode="before")
    @classmethod
    def validate_addresses(cls, values):
        bind_street_mailing_pair(
            values,
            street_prefix="filerStreet",
            street_key="filerAddress",
            mailing_prefix="filerMailing",
            mailing_key="filerMailing",
        )
        return values

    @model_validator(mode="before")
    @classmethod
    def check_name(cls, values):
        return format_filer_check_name(values)




class TECTreasurer(TECSettings, table=True):
    __tablename__ = "tx_treasurer"
    __table_args__ = {"schema": "texas"}
    treasId: str = Field(..., description="Filer account #", primary_key=True)
    treasPersentTypeCd: Optional[str] = Field(
        default=None, description="Type of treasurer name data - INDIVIDUAL or ENTITY")
    treasNameOrganization: Optional[str] = Field(
        default=None, description="For ENTITY, the treasurer organization name")
    treasNameLast: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the treasurer last name")
    treasNameSuffixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the treasurer suffix")
    treasNameFirst: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the treasurer first name")
    treasNamePrefixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the treasurer prefix")
    treasNameShort: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the treasurer short name")
    treasPrimaryUsaPhoneFlag: Optional[bool] = Field(
        default=None, description="Primary phone number is in the USA")

    treasPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
        default=None, description="Treasurer primary phone number")
    treasPrimaryPhoneExt: Optional[str] = Field(
        default=None, description="Treasurer primary phone number extension")
    treasAppointorNameLast: Optional[str] = Field(
        default=None, description="Treasurer appointor last name")
    treasAppointorNameFirst: Optional[str] = Field(
        default=None, description="Treasurer appointor first name")
    treasFilerpersStatusCd: Optional[str] = Field(
        default=None, description="Treasurer status (CURRENT, etc)")
    treasEffStartDt: Optional[date] = Field(
        default=None, description="Treasurer effective start date")
    treasEffStopDt: Optional[date] = Field(
        default=None, description="Treasurer effective stop date")
    treasAddress: Optional[TECAddress] = Field(default=None, description="Treasurer address", sa_type=JSON)
    treasMailing: Optional[TECAddress] = Field(
        default=None, description="Treasurer mailing address", sa_type=JSON)

    treasurer: list["TECFiler"] = Relationship(back_populates="treasurer", link_model=TECTreasurerLink)


    @model_validator(mode="before")
    @classmethod
    def validate_addresses(cls, values):
        bind_street_mailing_pair(
            values,
            street_prefix="treasStreet",
            street_key="treasAddress",
            mailing_prefix="treasMailing",
            mailing_key="treasMailing",
            optional=True,
        )
        return values

    @model_validator(mode='after')
    def check_for_address(self):
        if not self.treasAddress and not self.treasMailing:
            raise PydanticCustomError(
                'missing_address',
                "Treasurer address is missing",
                {
                    'address': self.treasAddress,
                    'mailing': self.treasMailing
                }
            )
        return self

    @model_validator(mode='after')
    def create_key(self):
        _addresses = [self.treasAddress, self.treasMailing]
        _address = next((_address.standardized for _address in _addresses if _address), None)
        if not _address:
            raise PydanticCustomError(
                'missing_address',
                "Treasurer address is missing",
                {
                    'address': _address
                }
            )
        self.treasId = self.generate_key(self.treasNameFirst, self.treasNameLast, _address)
        return self


class TECAsstTreasurer(TECSettings):
    assttreasPersentTypeCd: Optional[str] = Field(
        default=None, description="Type of assistant treasurer name data - INDIVIDUAL or ENTITY")
    assttreasNameOrganization: Optional[str] = Field(
        default=None, description="For ENTITY, the assistant treasurer organization name")
    assttreasNameLast: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the assistant treasurer last name")
    assttreasNameSuffixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the assistant treasurer suffix")
    assttreasNameFirst: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the assistant treasurer first name")
    assttreasNamePrefixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the assistant treasurer prefix")
    assttreasNameShort: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the assistant treasurer short name")
    assttreasPrimaryUsaPhoneFlag: Optional[bool] = Field(
        default=None, description="Primary phone number is in the USA")

    assttreasPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
        default=None, description="Assistant treasurer primary phone number")
    assttreasPrimaryPhoneExt: Optional[str] = Field(
        default=None, description="Assistant treasurer primary phone number extension")
    assttreasAppointorNameLast: Optional[str] = Field(
        default=None, description="Assistant treasurer appointor last name")
    assttreasAppointorNameFirst: Optional[str] = Field(
        default=None, description="Assistant treasurer appointor first name")

    assttreasAddress: TECAddress = Field(
        default=None, description="Assistant treasurer address", sa_type=JSON)



    @model_validator(mode="before")
    @classmethod
    def validate_addresses(cls, values):
        bind_street_address_if_present(
            values,
            prefix="assttreasStreet",
            target_key="assttreasAddress",
        )
        return values


class TECChair(TECSettings):
    chairPersentTypeCd: Optional[str] = Field(
        default=None, description="Type of chair name data - INDIVIDUAL or ENTITY")
    chairNameOrganization: Optional[str] = Field(
        default=None, description="For ENTITY, the chair organization name")
    chairNameLast: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the chair last name")
    chairNameSuffixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the chair suffix")
    chairNameFirst: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the chair first name")
    chairNamePrefixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the chair prefix")
    chairNameShort: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the chair short name")
    chairAddress: TECAddress = Field(..., description="Chair address", sa_type=JSON)
    chairPrimaryUsaPhoneFlag: Optional[bool] = Field(
        default=None, description="Primary phone number is in the USA")

    chairPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
        default=None, description="Chair primary phone number")
    chairPrimaryPhoneExt: Optional[str] = Field(
        default=None, description="Chair primary phone number extension")

    @model_validator(mode="before")
    @classmethod
    def validate_addresses(cls, values):
        chair_fields = extract_address(values, "chair")
        values["chairAddress"] = _tec_address(**chair_fields)
        return values

class TECFiler(TECSettings, table=True):
    __tablename__ = "tx_filer"
    __table_args__ = {"schema": "texas"}
    filerIdent: int = Field(default=None, primary_key=True)
    recordType: str = Field(..., description="Record type code - always FILER")
    filerName: TECFilerName = Field(
        default=None, description="Filer name", sa_type=JSON)
    treasurer: list["TECTreasurer"] = Relationship(back_populates="treasurer", link_model=TECTreasurerLink)
    asstTreasurer: Optional[TECAsstTreasurer] = Field(default=None, description="Assistant Treasurer", sa_type=JSON)
    chair: Optional[TECChair] = Field(default=None, description="Chair", sa_type=JSON)
    contestSeekOfficeCd: Optional[str] = Field(
        default=None, description="Filer office sought ")
    contestSeekOfficeDistrict: Optional[str] = Field(
        default=None, description="Filer office sought district")
    contestSeekOfficePlace: Optional[str] = Field(
        default=None, description="Filer office sought place")
    contestSeekOfficeDescr: Optional[str] = Field(
        default=None, description="Filer office sought description")
    contestSeekOfficeCountyCd: Optional[str] = Field(
        default=None, description="Filer office sought county code")
    contestSeekOfficeCountyDescr: Optional[str] = Field(
        default=None, description="Filer office sought county description")
    file_origin: str = Field(
        ..., description="Origin of the file")
    download_date: date = Field(
        ..., description="Date the file was downloaded")

    format_phone_numbers = model_validator(mode="before")(tx_funcs.phone_number_validation)
    format_address = model_validator(mode="before")(tx_funcs.address_formatting)

    @model_validator(mode="before")
    @classmethod
    def validate_addresses(cls, values):
        _filer_dict = {k: v for k, v in values.items() if k.startswith('filer') and v}
        _treasurer = {k: v for k, v in values.items() if k.startswith('treas') and v}
        _asst_treasurer = {k: v for k, v in values.items() if k.startswith('assttreas') and v}
        _chair = {k: v for k, v in values.items() if k.startswith('chair') and v}

        if _filer_dict:
            values['filerName'] = TECFilerName(**_filer_dict)

        if _treasurer:
            values['treasurer'] = TECTreasurer(**_treasurer)

        if _asst_treasurer:
            values['asstTreasurer'] = TECAsstTreasurer(**_asst_treasurer)

        if _chair:
            values['chair'] = TECChair(**_chair)

        return values
