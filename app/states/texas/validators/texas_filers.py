from datetime import date
from typing import Optional
from sqlmodel import Field, JSON, Relationship, Date
from pydantic import field_validator, model_validator, BeforeValidator, create_model
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_core import PydanticCustomError
import funcs.validator_functions as funcs
import states.texas.funcs.tx_validation_funcs as tx_funcs
from .texas_settings import TECBaseModel
from .texas_address import TECAddressBase, TECPersonAddressLinkModel
from .texas_flags import TECFlagsBase
from .texas_personname import TECPersonNameBase

from funcs.record_keygen import RecordKeyGenerator

# TODO: Create a 'Corrections' variable to hold the corrections for
#  the data as a list to add each correction to the list.

# TODO: Setup Filer Name as its Own Name, Then Create a Link Model of the Names and ID.

class TECAddress(TECAddressBase, table=True):
    __tablename__ = "tx_addresses"
    __table_args__ = {"schema": "texas"}
    person_name: list['TECPersonName'] = Relationship(
        back_populates="addresses",
        link_model=TECPersonAddressLinkModel)

class TECFilerIDNameLink(TECBaseModel, table=True):
    __tablename__ = "tx_filer_id_name_link"
    __table_args__ = {"schema": "texas"}
    filer_id: int = Field(foreign_key="texas.tx_filer.filerIdent", primary_key=True)
    filer_name_id: Optional[str] = Field(default=None, foreign_key="texas.tx_filer_name.filerIdent", primary_key=True)
    person_name_id: Optional[str] = Field(default=None, foreign_key="texas.tx_person_names.id", primary_key=True)
    office_id: Optional[str] = Field(default=None, foreign_key="texas.tx_cta.id", primary_key=True)
    treasurer_id: Optional[str] = Field(default=None, foreign_key="texas.tx_treasurer.id", primary_key=True)
    assttreas_id: Optional[str] = Field(default=None, foreign_key="texas.tx_assistant_treasurer.id", primary_key=True)
    chair_id: Optional[str] = Field(default=None, foreign_key="texas.tx_chair.id", primary_key=True)


class TECFlags(TECFlagsBase, table=True):
    pass


class TECPersonName(TECPersonNameBase, table=True):
    __tablename__ = "tx_person_names"
    __table_args__ = {"schema": "texas"}
    addresses: list["TECAddress"] = Relationship(back_populates="person_name", link_model=TECPersonAddressLinkModel)
    # filer_name: list["TECFilerName"] = Relationship(back_populates="filer_name", link_model=TECFilerIDNameLink)


class TECFilerID(TECBaseModel, table=True):
    __tablename__ = "tx_filer"
    __table_args__ = {"schema": "texas"}
    filerIdent: int = Field(..., description="Filer account #", primary_key=True)
    filerName: str = Field(..., description="Filer name")
    other_names: list["TECFilerName"] = Relationship(
        back_populates="filer_names",
        link_model=TECFilerIDNameLink)

# class TECFilerCTALinkModel(TECBaseModel, table=True):
#     __tablename__ = "tx_filer_cta_link"
#     __table_args__ = {"schema": "texas"}
#     filerIdent: int = Field(foreign_key="texas.tx_filer_name.filerIdent", primary_key=True)
#     ctaSeekOfficeCd: Optional[str] = Field(foreign_key="texas.tx_cta.id", primary_key=True)

class TECCampaignTreasurerAppointment(TECBaseModel, table=True):
    __tablename__ = "tx_cta"
    __table_args__ = {"schema": "texas"}
    id: Optional[int] = Field(default=None, primary_key=True)
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
    filer_name: Optional["TECFilerName"] = Relationship(back_populates="cta_details", link_model=TECFilerIDNameLink)

    def __init__(self, **data):
        super().__init__(**data)
        self.id = self.generate_key(self.ctaSeekOfficeCd, self.ctaSeekOfficeDistrict, self.ctaSeekOfficePlace)


# class TECFilerTreasurerLinkModel(TECBaseModel, table=True):
#     __tablename__ = "tx_filer_treasurer_link"
#     __table_args__ = {"schema": "texas"}
#     filer_name_id: int = Field(foreign_key="texas.tx_filer_name.filerIdent", primary_key=True)
#     treasurer_id: Optional[str] = Field(foreign_key="texas.tx_treasurer.id", primary_key=True)
#     assttreas_id: Optional[str] = Field(foreign_key="texas.tx_assistant_treasurer.id", primary_key=True)
#     chair_id: Optional[str] = Field(foreign_key="texas.tx_chair.id", primary_key=True)

class TECTreasurer(TECPersonNameBase, table=True):
    __tablename__ = "tx_treasurer"
    __table_args__ = {"schema": "texas"}
    treasAppointorNameLast: Optional[str] = Field(
        default=None, description="Treasurer appointor last name")
    treasAppointorNameFirst: Optional[str] = Field(
        default=None, description="Treasurer appointor first name")
    treasFilerpersStatusCd: Optional[str] = Field(
        default=None, description="Treasurer status (CURRENT, etc)")
    treasEffStartDt: Optional[date] = Field(
        default=None, description="Treasurer effective start date", sa_type=Date)
    treasEffStopDt: Optional[date] = Field(
        default=None, description="Treasurer effective stop date", sa_type=Date)
    # campaign: "TECFilerName" = Relationship(back_populates="treasurer", link_model=TECFilerIDNameLink)

class TECAssistantTreasurer(TECPersonNameBase, table=True):
    __tablename__ = "tx_assistant_treasurer"
    __table_args__ = {"schema": "texas"}
    assttreasAppointorNameLast: Optional[str] = Field(
        default=None, description="Assistant treasurer appointor last name")
    assttreasAppointorNameFirst: Optional[str] = Field(
        default=None, description="Assistant treasurer appointor first name")
    # campaign: "TECFilerName" = Relationship(back_populates="asstTreasurer", link_model=TECFilerIDNameLink)

class TECCampaignChair(TECPersonNameBase, table=True):
    __tablename__ = "tx_chair"
    __table_args__ = {"schema": "texas"}
    # campaign: "TECFilerName" = Relationship(back_populates="chair", link_model=TECFilerIDNameLink)

class TECFilerName(TECBaseModel, table=True):
    __tablename__ = "tx_filer_name"
    __table_args__ = {"schema": "texas"}
    id: Optional[int] = Field(default=None, primary_key=True)
    filerIdent: int = Field(foreign_key="texas.tx_filer.filerIdent")
    filerJdiCd: Optional[str] = Field(
        default=None, description="Judicial declaration of intent code ")
    committeeStatusCd: Optional[str] = Field(
        default=None, description="PAC filing status code")
    filerName: str = Field(..., description="Filer name")
    filerAddress: TECAddress = Field(
        default=None, description="Filer address", sa_type=JSON)
    filerMailing: TECAddress = Field(
        default=None, description="Filer mailing address", sa_type=JSON)
    cta_details: TECCampaignTreasurerAppointment = Relationship(
        back_populates="filer_name",
        link_model=TECFilerIDNameLink
    )
    filer_name: Optional["TECPersonName"] = Relationship(back_populates="filer_name", link_model=TECFilerIDNameLink)
    treasurer: TECTreasurer = Relationship(back_populates="campaign", link_model=TECFilerIDNameLink)
    asstTreasurer: TECAssistantTreasurer = Relationship(back_populates="campaign", link_model=TECFilerIDNameLink)
    chair: TECCampaignChair = Relationship(back_populates="campaign", link_model=TECFilerIDNameLink)

    filer_names: Optional["TECFilerID"] = Relationship(back_populates="other_names", link_model=TECFilerIDNameLink)
    @model_validator(mode='before')
    @classmethod
    def validate_name(cls, values):
        values['filerName'] = TECPersonName(**values)
        return values

    @model_validator(mode='before')
    @classmethod
    def validate_addresses(cls, values):
        _filer_street = {k: v for k, v in values.items() if k.startswith('filerStreet')}
        _filer_mailing = {k: v for k, v in values.items() if k.startswith('filerMailing')}
        values['filerAddress'] = TECAddress(**_filer_street)
        values['filerMailing'] = TECAddress(**_filer_mailing)
        return values

    @model_validator(mode='before')
    @classmethod
    def validate_treasurer(cls, values):
        _treas_values = {k: v for k, v in values.items() if k.startswith('treas')}
        values['treasurer'] = TECTreasurer(**_treas_values)
        return values

    @model_validator(mode='before')
    @classmethod
    def validate_assistant_treasurer(cls, values):
        _asst_treas_values = {k: v for k, v in values.items() if k.startswith('assttreas')}
        values['asstTreasurer'] = TECAssistantTreasurer(**_asst_treas_values)
        return values

    @model_validator(mode='before')
    @classmethod
    def validate_chair(cls, values):
        _chair_values = {k: v for k, v in values.items() if k.startswith('chair')}
        values['chair'] = TECCampaignChair(**_chair_values)
        return values

# class TECTreasurer(TECBaseModel, table=True):
#     __tablename__ = "tx_treasurer"
#     __table_args__ = {"schema": "texas"}
#     treasId: str = Field(..., description="Filer account #", primary_key=True)
#     treasPersentTypeCd: Optional[str] = Field(
#         default=None, description="Type of treasurer name data - INDIVIDUAL or ENTITY")
#     treasNameOrganization: Optional[str] = Field(
#         default=None, description="For ENTITY, the treasurer organization name")
#     treasNameLast: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the treasurer last name")
#     treasNameSuffixCd: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the treasurer suffix")
#     treasNameFirst: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the treasurer first name")
#     treasNamePrefixCd: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the treasurer prefix")
#     treasNameShort: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the treasurer short name")
#     treasPrimaryUsaPhoneFlag: Optional[bool] = Field(
#         default=None, description="Primary phone number is in the USA")
#
#     treasPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
#         default=None, description="Treasurer primary phone number")
#     treasPrimaryPhoneExt: Optional[str] = Field(
#         default=None, description="Treasurer primary phone number extension")
#     treasAppointorNameLast: Optional[str] = Field(
#         default=None, description="Treasurer appointor last name")
#     treasAppointorNameFirst: Optional[str] = Field(
#         default=None, description="Treasurer appointor first name")
#     treasFilerpersStatusCd: Optional[str] = Field(
#         default=None, description="Treasurer status (CURRENT, etc)")
#     treasEffStartDt: Optional[date] = Field(
#         default=None, description="Treasurer effective start date")
#     treasEffStopDt: Optional[date] = Field(
#         default=None, description="Treasurer effective stop date")
#     treasAddress: Optional[TECAddress] = Field(default=None, description="Treasurer address", sa_type=JSON)
#     treasMailing: Optional[TECAddress] = Field(
#         default=None, description="Treasurer mailing address", sa_type=JSON)
#
#     treasurer: list["TECFiler"] = Relationship(back_populates="treasurer")
#
#
#     @model_validator(mode='before')
#     @classmethod
#     def validate_addresses(cls, values):
#         _treas_street = {k: v for k, v in values.items() if k.startswith('treasStreet')}
#         _treas_mailing = {k: v for k, v in values.items() if k.startswith('treasMailing')}
#
#         values['treasAddress'] = TECAddress(**_treas_street) if _treas_street else None
#         values['treasMailing'] = TECAddress(**_treas_mailing) if _treas_mailing else None
#         return values
#
#     @model_validator(mode='after')
#     def check_for_address(self):
#         if not self.treasAddress and not self.treasMailing:
#             raise PydanticCustomError(
#                 'missing_address',
#                 "Treasurer address is missing",
#                 {
#                     'address': self.treasAddress,
#                     'mailing': self.treasMailing
#                 }
#             )
#         return self
#
#     @model_validator(mode='after')
#     def create_key(self):
#         _addresses = [self.treasAddress, self.treasMailing]
#         _address = next((_address.standardized for _address in _addresses if _address), None)
#         if not _address:
#             raise PydanticCustomError(
#                 'missing_address',
#                 "Treasurer address is missing",
#                 {
#                     'address': _address
#                 }
#             )
#         self.treasId = self.generate_key(self.treasNameFirst, self.treasNameLast, _address)
#         return self


# class TECAsstTreasurer(TECBaseModel):
#     assttreasPersentTypeCd: Optional[str] = Field(
#         default=None, description="Type of assistant treasurer name data - INDIVIDUAL or ENTITY")
#     assttreasNameOrganization: Optional[str] = Field(
#         default=None, description="For ENTITY, the assistant treasurer organization name")
#     assttreasNameLast: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the assistant treasurer last name")
#     assttreasNameSuffixCd: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the assistant treasurer suffix")
#     assttreasNameFirst: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the assistant treasurer first name")
#     assttreasNamePrefixCd: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the assistant treasurer prefix")
#     assttreasNameShort: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the assistant treasurer short name")
#     assttreasPrimaryUsaPhoneFlag: Optional[bool] = Field(
#         default=None, description="Primary phone number is in the USA")
#
#     assttreasPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
#         default=None, description="Assistant treasurer primary phone number")
#     assttreasPrimaryPhoneExt: Optional[str] = Field(
#         default=None, description="Assistant treasurer primary phone number extension")
#     assttreasAppointorNameLast: Optional[str] = Field(
#         default=None, description="Assistant treasurer appointor last name")
#     assttreasAppointorNameFirst: Optional[str] = Field(
#         default=None, description="Assistant treasurer appointor first name")
#
#     assttreasAddress: TECAddress = Field(
#         default=None, description="Assistant treasurer address", sa_type=JSON)
#
#
#
#     @model_validator(mode='before')
#     @classmethod
#     def validate_addresses(cls, values):
#         _assttreas_street = {k: v for k, v in values.items() if k.startswith('assttreasStreet')}
#         if _assttreas_street:
#             values['assttreasAddress'] = TECAddress(**_assttreas_street)
#         return values


# class TECChair(TECBaseModel):
#     chairPersentTypeCd: Optional[str] = Field(
#         default=None, description="Type of chair name data - INDIVIDUAL or ENTITY")
#     chairNameOrganization: Optional[str] = Field(
#         default=None, description="For ENTITY, the chair organization name")
#     chairNameLast: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the chair last name")
#     chairNameSuffixCd: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the chair suffix")
#     chairNameFirst: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the chair first name")
#     chairNamePrefixCd: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the chair prefix")
#     chairNameShort: Optional[str] = Field(
#         default=None, description="For INDIVIDUAL, the chair short name")
#     chairAddress: TECAddress = Field(..., description="Chair address", sa_type=JSON)
#     chairPrimaryUsaPhoneFlag: Optional[bool] = Field(
#         default=None, description="Primary phone number is in the USA")
#
#     chairPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
#         default=None, description="Chair primary phone number")
#     chairPrimaryPhoneExt: Optional[str] = Field(
#         default=None, description="Chair primary phone number extension")
#
#     @model_validator(mode='before')
#     @classmethod
#     def validate_addresses(cls, values):
#         _chair_street = {k: v for k, v in values.items() if k.startswith('chair')}
#         # _chair_mailing = {k: v for k, v in values.items() if k.startswith('chairMailing')}
#         values['chairAddress'] = TECAddress(**_chair_street)
#         # values['chairMailing'] = TECAddress(**_chair_mailing)
#         return values

# class TECFiler(TECBaseModel, table=True):
#     __tablename__ = "tx_filer"
#     __table_args__ = {"schema": "texas"}
#     filerIdent: int = Field(default=None, primary_key=True)
#     recordType: str = Field(..., description="Record type code - always FILER")
#     filerName: TECFilerName = Field(
#         default=None, description="Filer name", sa_type=JSON)
#     treasurer: list["TECTreasurer"] = Relationship(back_populates="treasurer")
#     asstTreasurer: Optional[TECAsstTreasurer] = Field(default=None, description="Assistant Treasurer", sa_type=JSON)
#     chair: Optional[TECChair] = Field(default=None, description="Chair", sa_type=JSON)
    # filerIdent: int = Field(..., description="Filer account #")
    # filerTypeCd: str = Field(..., description="Type of filer")
    # filerName: str = Field(..., description="Filer name")
    # unexpendContribFilerFlag: Optional[bool] = Field(
    #     default=None, description="Unexpended contribution filer flag")
    # modifiedElectCycleFlag: Optional[bool] = Field(
    #     default=None, description="Modified election cycle flag")
    # filerJdiCd: Optional[str] = Field(
    #     default=None, description="Judicial declaration of intent code ")
    # committeeStatusCd: Optional[str] = Field(
    #     default=None, description="PAC filing status code")
    # ctaSeekOfficeCd: Optional[str] = Field(
    #     default=None, description="CTA office sought")
    # ctaSeekOfficeDistrict: Optional[str] = Field(
    #     default=None, description="CTA office sought district")
    # ctaSeekOfficePlace: Optional[str] = Field(
    #     default=None, description="CTA office sought place")
    # ctaSeekOfficeDescr: Optional[str] = Field(
    #     default=None, description="CTA office sought description")
    # ctaSeekOfficeCountyCd: Optional[str] = Field(
    #     default=None, description="CTA office sought county code")
    # ctaSeekOfficeCountyDescr: Optional[str] = Field(
    #     default=None, description="CTA office sought county description")
    # filerPersentTypeCd: Optional[str] = Field(
    #     default=None, description="Type of filer name data - INDIVIDUAL or ENTITY")
    # filerNameOrganization: Optional[str] = Field(
    #     default=None, description="For ENTITY, the filer organization name")
    # filerNameLast: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the filer last name")
    # filerNameSuffixCd: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the filer suffix")
    # filerNameFirst: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the filer first name")
    # filerNameMiddle: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the filer middle name")
    # filerNamePrefixCd: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the filer prefix")
    # filerNameFull: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, Full name of the filer")
    # filerNameShort: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, Short name of the filer")
    # filerAddress: TECAddress = Field(
    #     default=None, description="Filer address", sa_type=JSON)
    # filerStreetAddr1: Optional[str] = Field(
    #     default=None, description="Filer street address line 1")
    # filerStreetAddr2: Optional[str] = Field(
    #     default=None, description="Filer street address line 2")
    # filerStreetCity: Optional[str] = Field(
    #     default=None, description="Filer street address city")
    # filerStreetStateCd: Optional[str] = Field(
    #     default=None, description="Filer street address state code")
    #
    # filerStreetCountyCd: Optional[str] = Field(
    #     default=None, description="Texas county")
    # filerStreetCountryCd: Optional[str] = Field(
    #     default=None, description="Filer street address country code")
    # filerStreetPostalCode: Optional[str] = Field(
    #     default=None, description="Filer street address postal code")
    # filerStreetRegion: Optional[str] = Field(
    #     default=None, description="Filer street address - region for country other than USA ")
    # filerMailingAddr1: Optional[str] = Field(
    #     default=None, description="Filer mailing address line 1")
    # filerMailingAddr2: Optional[str] = Field(
    #     default=None, description="Filer mailing address line 2")
    # filerMailingCity: Optional[str] = Field(
    #     default=None, description="Filer mailing address city")
    # filerMailingStateCd: Optional[str] = Field(
    #     default=None, description="Filer mailing address state code")
    #
    # filerMailingCountyCd: Optional[str] = Field(
    #     default=None, description="Texas county")
    # filerMailingCountryCd: Optional[str] = Field(
    #     default=None, description="Filer mailing address country code")
    # filerMailingPostalCode: Optional[str] = Field(
    #     default=None, description="Filer mailing address postal code")
    # filerMailingRegion: Optional[str] = Field(
    #     default=None, description="Filer mailing address - region for country other than USA ")
    # filerPrimaryUsaPhoneFlag: Optional[bool] = Field(
    #     default=None, description="Primary phone number is in the USA")
    #
    # filerPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
    #     default=None, description="Filer primary phone number")
    # filerPrimaryPhoneExt: Optional[str] = Field(
    #     default=None, description="Filer primary phone number extension")
    # filerHoldOfficeCd: Optional[str] = Field(
    #     default=None, description="Filer office held by filer")
    # filerHoldOfficeDistrict: Optional[str] = Field(
    #     default=None, description="Filer office held district")
    # filerHoldOfficePlace: Optional[str] = Field(
    #     default=None, description="Filer office held place")
    # filerHoldOfficeDescr: Optional[str] = Field(
    #     default=None, description="Filer office held description")
    # filerHoldOfficeCountyCd: Optional[str] = Field(
    #     default=None, description="Filer office held county code")
    # filerHoldOfficeCountyDescr: Optional[str] = Field(
    #     default=None, description="Filer office held county description")
    # filerFilerpersStatusCd: Optional[str] = Field(
    #     default=None, description="Filer status (CURRENT, etc)")
    # filerEffStartDt: Optional[date] = Field(
    #     default=None, description="Filer effective start date")
    # filerEffStopDt: Optional[date] = Field(
    #     default=None, description="Filer effective stop date")
    # contestSeekOfficeCd: Optional[str] = Field(
    #     default=None, description="Filer office sought ")
    # contestSeekOfficeDistrict: Optional[str] = Field(
    #     default=None, description="Filer office sought district")
    # contestSeekOfficePlace: Optional[str] = Field(
    #     default=None, description="Filer office sought place")
    # contestSeekOfficeDescr: Optional[str] = Field(
    #     default=None, description="Filer office sought description")
    # contestSeekOfficeCountyCd: Optional[str] = Field(
    #     default=None, description="Filer office sought county code")
    # contestSeekOfficeCountyDescr: Optional[str] = Field(
    #     default=None, description="Filer office sought county description")
    # treasPersentTypeCd: Optional[str] = Field(
    #     default=None, description="Type of treasurer name data - INDIVIDUAL or ENTITY")
    # treasNameOrganization: Optional[str] = Field(
    #     default=None, description="For ENTITY, the treasurer organization name")
    # treasNameLast: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the treasurer last name")
    # treasNameSuffixCd: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the treasurer suffix")
    # treasNameFirst: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the treasurer first name")
    # treasNamePrefixCd: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the treasurer prefix")
    # treasNameShort: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the treasurer short name")
    # treasStreetAddr1: Optional[str] = Field(
    #     default=None, description="Treasurer street address line 1")
    # treasStreetAddr2: Optional[str] = Field(
    #     default=None, description="Treasurer street address line 2")
    # treasStreetCity: Optional[str] = Field(
    #     default=None, description="Treasurer street address city")
    # treasStreetStateCd: Optional[str] = Field(
    #     default=None, description="Treasurer street address state code")
    #
    # treasStreetCountyCd: Optional[str] = Field(
    #     default=None, description="Texas county")
    # treasStreetCountryCd: Optional[str] = Field(
    #     default=None, description="Treasurer street address country code")
    # treasStreetPostalCode: Optional[str] = Field(
    #     default=None, description="Treasurer street address postal code")
    # treasStreetRegion: Optional[str] = Field(
    #     default=None, description="Treasurer street address - region for country other than USA ")
    # treasMailingAddr1: Optional[str] = Field(
    #     default=None, description="Treasurer mailing address line 1")
    # treasMailingAddr2: Optional[str] = Field(
    #     default=None, description="Treasurer mailing address line 2")
    # treasMailingCity: Optional[str] = Field(
    #     default=None, description="Treasurer mailing address city")
    # treasMailingStateCd: Optional[str] = Field(
    #     default=None, description="Treasurer mailing address state code")
    #
    # treasMailingCountyCd: Optional[str] = Field(
    #     default=None, description="Texas county")
    # treasMailingCountryCd: Optional[str] = Field(
    #     default=None, description="Treasurer mailing address country code")
    # treasMailingPostalCode: Optional[str] = Field(
    #     default=None, description="Treasurer mailing address postal code")
    # treasMailingRegion: Optional[str] = Field(
    #     default=None, description="Treasurer mailing address - region for country other than USA ")
    # treasPrimaryUsaPhoneFlag: Optional[bool] = Field(
    #     default=None, description="Primary phone number is in the USA")
    #
    # treasPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
    #     default=None, description="Treasurer primary phone number")
    # treasPrimaryPhoneExt: Optional[str] = Field(
    #     default=None, description="Treasurer primary phone number extension")
    # treasAppointorNameLast: Optional[str] = Field(
    #     default=None, description="Treasurer appointor last name")
    # treasAppointorNameFirst: Optional[str] = Field(
    #     default=None, description="Treasurer appointor first name")
    # treasFilerpersStatusCd: Optional[str] = Field(
    #     default=None, description="Treasurer status (CURRENT, etc)")
    # treasEffStartDt: Optional[date] = Field(
    #     default=None, description="Treasurer effective start date")
    # treasEffStopDt: Optional[date] = Field(
    #     default=None, description="Treasurer effective stop date")
    # assttreasPersentTypeCd: Optional[str] = Field(
    #     default=None, description="Type of assistant treasurer name data - INDIVIDUAL or ENTITY")
    # assttreasNameOrganization: Optional[str] = Field(
    #     default=None, description="For ENTITY, the assistant treasurer organization name")
    # assttreasNameLast: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the assistant treasurer last name")
    # assttreasNameSuffixCd: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the assistant treasurer suffix")
    # assttreasNameFirst: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the assistant treasurer first name")
    # assttreasNamePrefixCd: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the assistant treasurer prefix")
    # assttreasNameShort: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the assistant treasurer short name")
    # assttreasStreetAddr1: Optional[str] = Field(
    #     default=None, description="Assistant treasurer street address line 1")
    # assttreasStreetAddr2: Optional[str] = Field(
    #     default=None, description="Assistant treasurer street address line 2")
    # assttreasStreetCity: Optional[str] = Field(
    #     default=None, description="Assistant treasurer street address city")
    # assttreasStreetStateCd: Optional[str] = Field(
    #     default=None, description="Assistant treasurer street address state code")
    #
    # assttreasStreetCountyCd: Optional[str] = Field(
    #     default=None, description="Texas county")
    # assttreasStreetCountryCd: Optional[str] = Field(
    #     default=None, description="Assistant treasurer street address country code")
    # assttreasStreetPostalCode: Optional[str] = Field(
    #     default=None, description="Assistant treasurer street address postal code")
    #
    # assttreasStreetRegion: Optional[str] = Field(
    #     default=None, description="Assistant treasurer street address - region for country other than USA ")
    # assttreasPrimaryUsaPhoneFlag: Optional[bool] = Field(
    #     default=None, description="Primary phone number is in the USA")
    #
    # assttreasPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
    #     default=None, description="Assistant treasurer primary phone number")
    # assttreasPrimaryPhoneExt: Optional[str] = Field(
    #     default=None, description="Assistant treasurer primary phone number extension")
    # assttreasAppointorNameLast: Optional[str] = Field(
    #     default=None, description="Assistant treasurer appointor last name")
    # assttreasAppointorNameFirst: Optional[str] = Field(
    #     default=None, description="Assistant treasurer appointor first name")
    # chairPersentTypeCd: Optional[str] = Field(
    #     default=None, description="Type of chair name data - INDIVIDUAL or ENTITY")
    # chairNameOrganization: Optional[str] = Field(
    #     default=None, description="For ENTITY, the chair organization name")
    # chairNameLast: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the chair last name")
    # chairNameSuffixCd: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the chair suffix")
    # chairNameFirst: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the chair first name")
    # chairNamePrefixCd: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the chair prefix")
    # chairNameShort: Optional[str] = Field(
    #     default=None, description="For INDIVIDUAL, the chair short name")
    # chairStreetAddr1: Optional[str] = Field(
    #     default=None, description="Chair street address line 1")
    # chairStreetAddr2: Optional[str] = Field(
    #     default=None, description="Chair street address line 2")
    # chairStreetCity: Optional[str] = Field(
    #     default=None, description="Chair street address city")
    # chairStreetStateCd: Optional[str] = Field(
    #     default=None, description="Chair street address state code")
    #
    # chairStreetCountyCd: Optional[str] = Field(
    #     default=None, description="Texas county")
    # chairStreetCountryCd: Optional[str] = Field(
    #     default=None, description="Chair street address country code")
    # chairStreetPostalCode: Optional[str] = Field(
    #     default=None, description="Chair street address postal code")
    # chairStreetRegion: Optional[str] = Field(
    #     default=None, description="Chair street address - region for country other than USA ")
    # chairMailingAddr1: Optional[str] = Field(
    #     default=None, description="Chair mailing address line 1")
    # chairMailingAddr2: Optional[str] = Field(
    #     default=None, description="Chair mailing address line 2")
    # chairMailingCity: Optional[str] = Field(
    #     default=None, description="Chair mailing address city")
    # chairMailingStateCd: Optional[str] = Field(
    #     default=None, description="Chair mailing address state code")
    #
    # chairMailingCountyCd: Optional[str] = Field(
    #     default=None, description="Texas county")
    # chairMailingCountryCd: Optional[str] = Field(
    #     default=None, description="Chair mailing address country code")
    # chairMailingPostalCode: Optional[str] = Field(
    #     default=None, description="Chair mailing address postal code")
    # chairMailingRegion: Optional[str] = Field(
    #     default=None, description="Chair mailing address - region for country other than USA ")
    # chairPrimaryUsaPhoneFlag: Optional[bool] = Field(
    #     default=None, description="Primary phone number is in the USA")
    #
    # chairPrimaryPhoneNumber: Optional[PhoneNumber] = Field(
    #     default=None, description="Chair primary phone number")
    # chairPrimaryPhoneExt: Optional[str] = Field(
    #     default=None, description="Chair primary phone number extension")
    # file_origin: str = Field(
    #     ..., description="Origin of the file")
    # download_date: date = Field(
    #     ..., description="Date the file was downloaded")
    #
    # clear_blank_strings = model_validator(mode="before")(funcs.clear_blank_strings)
    # format_dates = model_validator(mode="before")(tx_funcs.validate_dates)
    # format_zipcodes = model_validator(mode="before")(tx_funcs.check_zipcodes)
    # format_phone_numbers = model_validator(mode="before")(tx_funcs.phone_number_validation)
    # format_address = model_validator(mode="before")(tx_funcs.address_formatting)

    # @model_validator(mode="before")
    # @classmethod
    # def check_filer_name(cls, values):
    #     if values["filerPersentTypeCd"] == "INDIVIDUAL":
    #         if not values["filerNameLast"]:
    #             raise PydanticCustomError(
    #                 'missing_required_value',
    #                 "filerNameLast is required for INDIVIDUAL filerPersentTypeCd",
    #                 {
    #                     'column': 'filerNameLast',
    #                     'value': values["filerNameLast"]}
    #             )
    #         if not values["filerNameFirst"]:
    #             raise PydanticCustomError(
    #                 'missing_required_value',
    #                 "filerNameFirst is required for INDIVIDUAL filerPersentTypeCd",
    #                 {
    #                     'column': 'filerNameFirst',
    #                     'value': values["filerNameFirst"]}
    #             )
    #     elif values["filerPersentTypeCd"] == "ENTITY":
    #         if not values["filerNameOrganization"]:
    #             raise PydanticCustomError(
    #                 'missing_required_value',
    #                 "filerNameOrganization is required for ENTITY filerPersentTypeCd",
    #                 {
    #                     'column': 'filerNameOrganization',
    #                     'value': values["filerNameOrganization"]}
    #             )
    #     return values
    #
    # @model_validator(mode="before")
    # @classmethod
    # def fill_filer_name_full(cls, values):
    #     if values["filerPersentTypeCd"] == "INDIVIDUAL":
    #         formatted_name = funcs.person_name_parser(values['filerName'])
    #         formatted_name.parse_full_name()
    #
    #         values["filerNameLast"] = formatted_name.last
    #
    #         values["filerNameFirst"] = formatted_name.first
    #
    #         if formatted_name.middle != "":
    #             values["filerNameMiddle"] = formatted_name.middle
    #
    #         if formatted_name.suffix != "":
    #             values["filerNameSuffixCd"] = formatted_name.suffix
    #
    #         if formatted_name.title != "":
    #             values["filerNamePrefixCd"] = formatted_name.title
    #         values['filerNameFull'] = formatted_name.full_name
    #     return values
    #
    # @field_validator('filerName', mode='before')
    # @classmethod
    # def validate_filer_name(cls, v):
    #     if not v:
    #         raise PydanticCustomError(
    #             'missing_required_value',
    #             "filerName is required",
    #             {
    #                 'column': 'filerName',
    #                 'value': v
    #             }
    #         )
    #     return v

    # @model_validator(mode='before')
    # @classmethod
    # def validate_addresses(cls, values):
    #     _filer_dict = {k: v for k, v in values.items() if k.startswith('filer') and v}
    #     _treasurer = {k: v for k, v in values.items() if k.startswith('treas') and v}
    #     _asst_treasurer = {k: v for k, v in values.items() if k.startswith('assttreas') and v}
    #     _chair = {k: v for k, v in values.items() if k.startswith('chair') and v}
    #
    #     if _filer_dict:
    #         values['filerName'] = TECFilerName(**_filer_dict)
    #
    #     if _treasurer:
    #         values['treasurer'] = TECTreasurer(**_treasurer)
    #
    #     if _asst_treasurer:
    #         values['asstTreasurer'] = TECAsstTreasurer(**_asst_treasurer)
    #
    #     if _chair:
    #         values['chair'] = TECChair(**_chair)
    #
    #     # _filer_street = {k: v for k, v in values.items() if k.startswith('filerStreet')}
    #     # _filer_mailing = {k: v for k, v in values.items() if k.startswith('filerMailing')}
    #     # values['filerAddress'] = TECAddress(**_filer_street)
    #     return values