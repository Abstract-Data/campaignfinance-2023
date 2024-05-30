from __future__ import annotations
from datetime import date, datetime
from typing import Optional, Annotated, List, Any
from sqlmodel import SQLModel, Field, Relationship, Column, ARRAY, JSON
from sqlalchemy.orm import Mapped
from pydantic import field_validator, model_validator
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_core import PydanticCustomError
from .texas_settings import TECSettings
import funcs.validator_functions as funcs
import states.texas.funcs.tx_validation_funcs as tx_funcs



# TODO: Create a 'Corrections' variable to hold the corrections for
#  the data as a list to add each correction to the list.


class TECFiler(TECSettings, table=True):
    __tablename__ = "tx_filers"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, primary_key=True)
    recordType: str = Field(..., description="Record type code - always FILER")
    filerIdent: int = Field(..., description="Filer account #")
    filerTypeCd: str = Field(..., description="Type of filer")
    filerName: str = Field(..., description="Filer name")
    unexpendContribFilerFlag: Optional[bool] = Field(default=None, description="Unexpended contribution filer flag")
    modifiedElectCycleFlag: Optional[bool] = Field(default=None, description="Modified election cycle flag")
    filerJdiCd: Optional[str] = Field(default=None, description="Judicial declaration of intent code ")
    committeeStatusCd: Optional[str] = Field(default=None, description="PAC filing status code")
    ctaSeekOfficeCd: Optional[str] = Field(default=None, description="CTA office sought")
    ctaSeekOfficeDistrict: Optional[str] = Field(default=None, description="CTA office sought district")
    ctaSeekOfficePlace: Optional[str] = Field(default=None, description="CTA office sought place")
    ctaSeekOfficeDescr: Optional[str] = Field(default=None, description="CTA office sought description")
    ctaSeekOfficeCountyCd: Optional[str] = Field(default=None, description="CTA office sought county code")
    ctaSeekOfficeCountyDescr: Optional[str] = Field(default=None, description="CTA office sought county description")
    filerPersentTypeCd: Optional[str] = Field(default=None, description="Type of filer name data - INDIVIDUAL or ENTITY")
    filerNameOrganization: Optional[str] = Field(default=None, description="For ENTITY, the filer organization name")
    filerNameLast: Optional[str] = Field(default=None, description="For INDIVIDUAL, the filer last name")
    filerNameSuffixCd: Optional[str] = Field(default=None, description="For INDIVIDUAL, the filer suffix")
    filerNameFirst: Optional[str] = Field(default=None, description="For INDIVIDUAL, the filer first name")
    filerNameMiddle: Optional[str] = Field(default=None, description="For INDIVIDUAL, the filer middle name")
    filerNamePrefixCd: Optional[str] = Field(default=None, description="For INDIVIDUAL, the filer prefix")
    filerNameFull: Optional[str] = Field(default=None, description="For INDIVIDUAL, Full name of the filer")
    filerNameShort: Optional[str] = Field(default=None, description="For INDIVIDUAL, Short name of the filer")
    filerStreetAddr1: Optional[str] = Field(default=None, description="Filer street address line 1")
    filerStreetAddr2: Optional[str] = Field(default=None, description="Filer street address line 2")
    filerStreetCity: Optional[str] = Field(default=None, description="Filer street address city")
    filerStreetStateCd: Optional[str] = Field(default=None, description="Filer street address state code")

    filerStreetCountyCd: Optional[str] = Field(default=None, description="Texas county")
    filerStreetCountryCd: Optional[str] = Field(default=None, description="Filer street address country code")
    filerStreetPostalCode: Optional[str] = Field(default=None, description="Filer street address postal code")
    filerStreetRegion: Optional[str] = Field(default=None, description="Filer street address - region for country other than USA ")
    filerMailingAddr1: Optional[str] = Field(default=None, description="Filer mailing address line 1")
    filerMailingAddr2: Optional[str] = Field(default=None, description="Filer mailing address line 2")
    filerMailingCity: Optional[str] = Field(default=None, description="Filer mailing address city")
    filerMailingStateCd: Optional[str] = Field(default=None, description="Filer mailing address state code")

    filerMailingCountyCd: Optional[str] = Field(default=None, description="Texas county")
    filerMailingCountryCd: Optional[str] = Field(default=None, description="Filer mailing address country code")
    filerMailingPostalCode: Optional[str] = Field(default=None, description="Filer mailing address postal code")
    filerMailingRegion: Optional[str] = Field(default=None, description="Filer mailing address - region for country other than USA ")
    filerPrimaryUsaPhoneFlag: Optional[bool] = Field(default=None, description="Primary phone number is in the USA")

    filerPrimaryPhoneNumber: Optional[PhoneNumber] = Field(default=None, description="Filer primary phone number")
    filerPrimaryPhoneExt: Optional[str] = Field(default=None, description="Filer primary phone number extension")
    filerHoldOfficeCd: Optional[str] = Field(default=None, description="Filer office held by filer")
    filerHoldOfficeDistrict: Optional[str] = Field(default=None, description="Filer office held district")
    filerHoldOfficePlace: Optional[str] = Field(default=None, description="Filer office held place")
    filerHoldOfficeDescr: Optional[str] = Field(default=None, description="Filer office held description")
    filerHoldOfficeCountyCd: Optional[str] = Field(default=None, description="Filer office held county code")
    filerHoldOfficeCountyDescr: Optional[str] = Field(default=None, description="Filer office held county description")
    filerFilerpersStatusCd: Optional[str] = Field(default=None, description="Filer status (CURRENT, etc)")
    filerEffStartDt: Optional[date] = Field(default=None, description="Filer effective start date")
    filerEffStopDt: Optional[date] = Field(default=None, description="Filer effective stop date")
    contestSeekOfficeCd: Optional[str] = Field(default=None, description="Filer office sought ")
    contestSeekOfficeDistrict: Optional[str] = Field(default=None, description="Filer office sought district")
    contestSeekOfficePlace: Optional[str] = Field(default=None, description="Filer office sought place")
    contestSeekOfficeDescr: Optional[str] = Field(default=None, description="Filer office sought description")
    contestSeekOfficeCountyCd: Optional[str] = Field(default=None, description="Filer office sought county code")
    contestSeekOfficeCountyDescr: Optional[str] = Field(default=None, description="Filer office sought county description")
    treasPersentTypeCd: Optional[str] = Field(default=None, description="Type of treasurer name data - INDIVIDUAL or ENTITY")
    treasNameOrganization: Optional[str] = Field(default=None, description="For ENTITY, the treasurer organization name")
    treasNameLast: Optional[str] = Field(default=None, description="For INDIVIDUAL, the treasurer last name")
    treasNameSuffixCd: Optional[str] = Field(default=None, description="For INDIVIDUAL, the treasurer suffix")
    treasNameFirst: Optional[str] = Field(default=None, description="For INDIVIDUAL, the treasurer first name")
    treasNamePrefixCd: Optional[str] = Field(default=None, description="For INDIVIDUAL, the treasurer prefix")
    treasNameShort: Optional[str] = Field(default=None, description="For INDIVIDUAL, the treasurer short name")
    treasStreetAddr1: Optional[str] = Field(default=None, description="Treasurer street address line 1")
    treasStreetAddr2: Optional[str] = Field(default=None, description="Treasurer street address line 2")
    treasStreetCity: Optional[str] = Field(default=None, description="Treasurer street address city")
    treasStreetStateCd: Optional[str] = Field(default=None, description="Treasurer street address state code")

    treasStreetCountyCd: Optional[str] = Field(default=None, description="Texas county")
    treasStreetCountryCd: Optional[str] = Field(default=None, description="Treasurer street address country code")
    treasStreetPostalCode: Optional[str] = Field(default=None, description="Treasurer street address postal code")
    treasStreetRegion: Optional[str] = Field(default=None, description="Treasurer street address - region for country other than USA ")
    treasMailingAddr1: Optional[str] = Field(default=None, description="Treasurer mailing address line 1")
    treasMailingAddr2: Optional[str] = Field(default=None, description="Treasurer mailing address line 2")
    treasMailingCity: Optional[str] = Field(default=None, description="Treasurer mailing address city")
    treasMailingStateCd: Optional[str] = Field(default=None, description="Treasurer mailing address state code")

    treasMailingCountyCd: Optional[str] = Field(default=None, description="Texas county")
    treasMailingCountryCd: Optional[str] = Field(default=None, description="Treasurer mailing address country code")
    treasMailingPostalCode: Optional[str] = Field(default=None, description="Treasurer mailing address postal code")
    treasMailingRegion: Optional[str] = Field(default=None, description="Treasurer mailing address - region for country other than USA ")
    treasPrimaryUsaPhoneFlag: Optional[bool] = Field(default=None, description="Primary phone number is in the USA")

    treasPrimaryPhoneNumber: Optional[PhoneNumber] = Field(default=None, description="Treasurer primary phone number")
    treasPrimaryPhoneExt: Optional[str] = Field(default=None, description="Treasurer primary phone number extension")
    treasAppointorNameLast: Optional[str] = Field(default=None, description="Treasurer appointor last name")
    treasAppointorNameFirst: Optional[str] = Field(default=None, description="Treasurer appointor first name")
    treasFilerpersStatusCd: Optional[str] = Field(default=None, description="Treasurer status (CURRENT, etc)")
    treasEffStartDt: Optional[date] = Field(default=None, description="Treasurer effective start date")
    treasEffStopDt: Optional[date] = Field(default=None, description="Treasurer effective stop date")
    assttreasPersentTypeCd: Optional[str] = Field(default=None, description="Type of assistant treasurer name data - INDIVIDUAL or ENTITY")
    assttreasNameOrganization: Optional[str] = Field(default=None, description="For ENTITY, the assistant treasurer organization name")
    assttreasNameLast: Optional[str] = Field(default=None, description="For INDIVIDUAL, the assistant treasurer last name")
    assttreasNameSuffixCd: Optional[str] = Field(default=None, description="For INDIVIDUAL, the assistant treasurer suffix")
    assttreasNameFirst: Optional[str] = Field(default=None, description="For INDIVIDUAL, the assistant treasurer first name")
    assttreasNamePrefixCd: Optional[str] = Field(default=None, description="For INDIVIDUAL, the assistant treasurer prefix")
    assttreasNameShort: Optional[str] = Field(default=None, description="For INDIVIDUAL, the assistant treasurer short name")
    assttreasStreetAddr1: Optional[str] = Field(default=None, description="Assistant treasurer street address line 1")
    assttreasStreetAddr2: Optional[str] = Field(default=None, description="Assistant treasurer street address line 2")
    assttreasStreetCity: Optional[str] = Field(default=None, description="Assistant treasurer street address city")
    assttreasStreetStateCd: Optional[str] = Field(default=None, description="Assistant treasurer street address state code")

    assttreasStreetCountyCd: Optional[str] = Field(default=None, description="Texas county")
    assttreasStreetCountryCd: Optional[str] = Field(default=None, description="Assistant treasurer street address country code")
    assttreasStreetPostalCode: Optional[str] = Field(default=None, description="Assistant treasurer street address postal code")

    assttreasStreetRegion: Optional[str] = Field(default=None, description="Assistant treasurer street address - region for country other than USA ")
    assttreasPrimaryUsaPhoneFlag: Optional[bool] = Field(default=None, description="Primary phone number is in the USA")

    assttreasPrimaryPhoneNumber: Optional[PhoneNumber] = Field(default=None, description="Assistant treasurer primary phone number")
    assttreasPrimaryPhoneExt: Optional[str] = Field(default=None, description="Assistant treasurer primary phone number extension")
    assttreasAppointorNameLast: Optional[str] = Field(default=None, description="Assistant treasurer appointor last name")
    assttreasAppointorNameFirst: Optional[str] = Field(default=None, description="Assistant treasurer appointor first name")
    chairPersentTypeCd: Optional[str] = Field(default=None, description="Type of chair name data - INDIVIDUAL or ENTITY")
    chairNameOrganization: Optional[str] = Field(default=None, description="For ENTITY, the chair organization name")
    chairNameLast: Optional[str] = Field(default=None, description="For INDIVIDUAL, the chair last name")
    chairNameSuffixCd: Optional[str] = Field(default=None, description="For INDIVIDUAL, the chair suffix")
    chairNameFirst: Optional[str] = Field(default=None, description="For INDIVIDUAL, the chair first name")
    chairNamePrefixCd: Optional[str] = Field(default=None, description="For INDIVIDUAL, the chair prefix")
    chairNameShort: Optional[str] = Field(default=None, description="For INDIVIDUAL, the chair short name")
    chairStreetAddr1: Optional[str] = Field(default=None, description="Chair street address line 1")
    chairStreetAddr2: Optional[str] = Field(default=None, description="Chair street address line 2")
    chairStreetCity: Optional[str] = Field(default=None, description="Chair street address city")
    chairStreetStateCd: Optional[str] = Field(default=None, description="Chair street address state code")

    chairStreetCountyCd: Optional[str] = Field(default=None, description="Texas county")
    chairStreetCountryCd: Optional[str] = Field(default=None, description="Chair street address country code")
    chairStreetPostalCode: Optional[str] = Field(default=None, description="Chair street address postal code")
    chairStreetRegion: Optional[str] = Field(default=None, description="Chair street address - region for country other than USA ")
    chairMailingAddr1: Optional[str] = Field(default=None, description="Chair mailing address line 1")
    chairMailingAddr2: Optional[str] = Field(default=None, description="Chair mailing address line 2")
    chairMailingCity: Optional[str] = Field(default=None, description="Chair mailing address city")
    chairMailingStateCd: Optional[str] = Field(default=None, description="Chair mailing address state code")

    chairMailingCountyCd: Optional[str] = Field(default=None, description="Texas county")
    chairMailingCountryCd: Optional[str] = Field(default=None, description="Chair mailing address country code")
    chairMailingPostalCode: Optional[str] = Field(default=None, description="Chair mailing address postal code")
    chairMailingRegion: Optional[str] = Field(default=None, description="Chair mailing address - region for country other than USA ")
    chairPrimaryUsaPhoneFlag: Optional[bool] = Field(default=None, description="Primary phone number is in the USA")

    chairPrimaryPhoneNumber: Optional[PhoneNumber] = Field(default=None, description="Chair primary phone number")
    chairPrimaryPhoneExt: Optional[str] = Field(default=None, description="Chair primary phone number extension")
    file_origin: str = Field(..., description="Origin of the file")
    download_date: date = Field(..., description="Date the file was downloaded")

    clear_blank_strings = model_validator(mode="before")(funcs.clear_blank_strings)
    format_dates = model_validator(mode="before")(tx_funcs.validate_dates)
    format_zipcodes = model_validator(mode="before")(tx_funcs.check_zipcodes)
    format_phone_numbers = model_validator(mode="before")(tx_funcs.phone_number_validation)
    format_address = model_validator(mode="before")(tx_funcs.address_formatting)

    @model_validator(mode="before")
    @classmethod
    def check_filer_name(cls, values):
        if values["filerPersentTypeCd"] == "INDIVIDUAL":
            if not values["filerNameLast"]:
                raise PydanticCustomError(
                    'filer_name_check',
                    "filerNameLast is required for INDIVIDUAL filerPersentTypeCd",
                    {
                        'column': 'filerNameLast',
                        'value': values["filerNameLast"]}
                )
            if not values["filerNameFirst"]:
                raise PydanticCustomError(
                    'filer_name_check',
                    "filerNameFirst is required for INDIVIDUAL filerPersentTypeCd",
                    {
                        'column': 'filerNameFirst',
                        'value': values["filerNameFirst"]}
                )
        elif values["filerPersentTypeCd"] == "ENTITY":
            if not values["filerNameOrganization"]:
                raise PydanticCustomError(
                    'filer_name_check',
                    "filerNameOrganization is required for ENTITY filerPersentTypeCd",
                    {
                        'column': 'filerNameOrganization',
                        'value': values["filerNameOrganization"]}
                )
        return values

    @model_validator(mode="before")
    @classmethod
    def fill_filer_name_full(cls, values):
        if values["filerPersentTypeCd"] == "INDIVIDUAL":
            formatted_name = funcs.person_name_parser(values['filerName'])
            formatted_name.parse_full_name()

            values["filerNameLast"] = formatted_name.last

            values["filerNameFirst"] = formatted_name.first

            if formatted_name.middle != "":
                values["filerNameMiddle"] = formatted_name.middle

            if formatted_name.suffix != "":
                values["filerNameSuffixCd"] = formatted_name.suffix

            if formatted_name.title != "":
                values["filerNamePrefixCd"] = formatted_name.title
            values['filerNameFull'] = formatted_name.full_name
        return values



