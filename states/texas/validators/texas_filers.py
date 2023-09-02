from datetime import date
from typing import Optional, Annotated
import probablepeople as pp
from nameparser import HumanName
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_core import PydanticCustomError
from states.texas.validators.texas_settings import TECSettings


class TECFiler(TECSettings):
    recordType: str
    filerIdent: str
    filerTypeCd: str
    filerName: str
    unexpendContribFilerFlag: Optional[str]
    modifiedElectCycleFlag: Optional[str]
    filerJdiCd: Optional[str]
    committeeStatusCd: Optional[str]
    ctaSeekOfficeCd: Optional[str]
    ctaSeekOfficeDistrict: Optional[str]
    ctaSeekOfficePlace: Optional[str]
    ctaSeekOfficeDescr: Optional[str]
    ctaSeekOfficeCountyCd: Optional[str]
    ctaSeekOfficeCountyDescr: Optional[str]
    filerPersentTypeCd: str
    filerNameOrganization: Optional[str]
    filerNameLast: Optional[str]
    filerNameSuffixCd: Optional[str]
    filerNameFirst: Optional[str]
    filerNamePrefixCd: Optional[str]
    filerNameShort: Optional[str]
    filerStreetAddr1: Optional[str]
    filerStreetAddr2: Optional[str]
    filerStreetCity: Optional[str]
    filerStreetStateCd: Optional[str]

    filerStreetCountyCd: Optional[str]
    filerStreetCountryCd: Optional[str]
    filerStreetPostalCode: Optional[str]
    filerStreetRegion: Optional[str]
    filerMailingAddr1: Optional[str]
    filerMailingAddr2: Optional[str]
    filerMailingCity: Optional[str]
    filerMailingStateCd: Optional[str]

    filerMailingCountyCd: Optional[str]
    filerMailingCountryCd: Optional[str]
    filerMailingPostalCode: Optional[str]
    filerMailingRegion: Optional[str]
    filerPrimaryUsaPhoneFlag: Optional[str]

    filerPrimaryPhoneNumber: Optional[PhoneNumber]
    filerPrimaryPhoneExt: Optional[str]
    filerHoldOfficeCd: Optional[str]
    filerHoldOfficeDistrict: Optional[str]
    filerHoldOfficePlace: Optional[str]
    filerHoldOfficeDescr: Optional[str]
    filerHoldOfficeCountyCd: Optional[str]
    filerHoldOfficeCountyDescr: Optional[str]
    filerFilerpersStatusCd: Optional[str]
    filerEffStartDt: Optional[date]
    filerEffStopDt: Optional[date]
    contestSeekOfficeCd: Optional[str]
    contestSeekOfficeDistrict: Optional[str]
    contestSeekOfficePlace: Optional[str]
    contestSeekOfficeDescr: Optional[str]
    contestSeekOfficeCountyCd: Optional[str]
    contestSeekOfficeCountyDescr: Optional[str]
    treasPersentTypeCd: str
    treasNameOrganization: Optional[str]
    treasNameLast: Optional[str]
    treasNameSuffixCd: Optional[str]
    treasNameFirst: Optional[str]
    treasNamePrefixCd: Optional[str]
    treasNameShort: Optional[str]
    treasStreetAddr1: Optional[str]
    treasStreetAddr2: Optional[str]
    treasStreetCity: Optional[str]
    treasStreetStateCd: Optional[str]

    treasStreetCountyCd: Optional[str]
    treasStreetCountryCd: Optional[str]
    treasStreetPostalCode: Optional[str]
    treasStreetRegion: Optional[str]
    treasMailingAddr1: Optional[str]
    treasMailingAddr2: Optional[str]
    treasMailingCity: Optional[str]
    treasMailingStateCd: Optional[str]

    treasMailingCountyCd: Optional[str]
    treasMailingCountryCd: Optional[str]
    treasMailingPostalCode: Optional[str]
    treasMailingRegion: Optional[str]
    treasPrimaryUsaPhoneFlag: Optional[str]

    treasPrimaryPhoneNumber: Optional[PhoneNumber]
    treasPrimaryPhoneExt: Optional[str]
    treasAppointorNameLast: Optional[str]
    treasAppointorNameFirst: Optional[str]
    treasFilerpersStatusCd: Optional[str]
    treasEffStartDt: Optional[date]
    treasEffStopDt: Optional[date]
    assttreasPersentTypeCd: Optional[str]
    assttreasNameOrganization: Optional[str]
    assttreasNameLast: Optional[str]
    assttreasNameSuffixCd: Optional[str]
    assttreasNameFirst: Optional[str]
    assttreasNamePrefixCd: Optional[str]
    assttreasNameShort: Optional[str]
    assttreasStreetAddr1: Optional[str]
    assttreasStreetAddr2: Optional[str]
    assttreasStreetCity: Optional[str]
    assttreasStreetStateCd: Optional[str]

    assttreasStreetCountyCd: Optional[str]
    assttreasStreetCountryCd: Optional[str]
    assttreasStreetPostalCode: Optional[str]

    assttreasStreetRegion: Optional[str]
    assttreasPrimaryUsaPhoneFlag: Optional[str]

    assttreasPrimaryPhoneNumber: Optional[PhoneNumber]
    assttreasPrimaryPhoneExt: Optional[str]
    assttreasAppointorNameLast: Optional[str]
    assttreasAppointorNameFirst: Optional[str]
    chairPersentTypeCd: Optional[str]
    chairNameOrganization: Optional[str]
    chairNameLast: Optional[str]
    chairNameSuffixCd: Optional[str]
    chairNameFirst: Optional[str]
    chairNamePrefixCd: Optional[str]
    chairNameShort: Optional[str]
    chairStreetAddr1: Optional[str]
    chairStreetAddr2: Optional[str]
    chairStreetCity: Optional[str]
    chairStreetStateCd: Optional[str]

    chairStreetCountyCd: Optional[str]
    chairStreetCountryCd: Optional[str]
    chairStreetPostalCode: Optional[str]
    chairStreetRegion: Optional[str]
    chairMailingAddr1: Optional[str]
    chairMailingAddr2: Optional[str]
    chairMailingCity: Optional[str]
    chairMailingStateCd: Optional[str]

    chairMailingCountyCd: Optional[str]
    chairMailingCountryCd: Optional[str]
    chairMailingPostalCode: Optional[str]
    chairMailingRegion: Optional[str]
    chairPrimaryUsaPhoneFlag: Optional[str]

    chairPrimaryPhoneNumber: Optional[PhoneNumber]
    chairPrimaryPhoneExt: Optional[str]

    @model_validator(mode="before")
    @classmethod
    def filer_type_checker(cls, values):
        # TODO: Fix Persent ID not exisiting to pass
        # TODO: Parse Names for Name Columns (take from Expenses class)
        persent_type_cols = [
            "filerPersentTypeCd",
            "treasPersentTypeCd",
            "assttreasPersentTypeCd",
            "chairPersentTypeCd",
        ]
        persent_fname_cols = [
            "filerNameFirst",
            "treasNameFirst",
            "assttreasNameFirst",
            "chairNameFirst",
        ]
        persent_lname_cols = [
            "filerNameLast",
            "treasNameLast",
            "assttreasNameLast",
            "chairNameLast",
        ]
        persent_org_cols = [
            "filerNameOrganization",
            "treasNameOrganization",
            "assttreasNameOrganization",
            "chairNameOrganization",
        ]
        cols_zip = list(
            zip(
                persent_type_cols,
                persent_fname_cols,
                persent_lname_cols,
                persent_org_cols,
            )
        )
        for column in cols_zip:
            if values[column[0]]:
                if values[column[0]] == "INDIVIDUAL":
                    if not values[column[1]]:
                        raise PydanticCustomError(
                            'filer_type_check',
                            f"{column[1]} is required for INDIVIDUAL {column[0]}",
                        )
                if not values[column[2]]:
                    raise PydanticCustomError(
                        'filer_type_check',
                        f"{column[2]} is required for INDIVIDUAL {column[0]}",
                    )
                elif values[column[0]] and values[column[0]] == "ENTITY":
                    if not values[column[4]]:
                        raise PydanticCustomError(
                            'filer_type_check',
                            f"{column[4]} is required for ENTITY {column[0]}",
                        )
                else:
                    # raise ValueError(f"{column[0]} must be INDIVIDUAL or ENTITY")
                    pass
        return values

    @field_validator(
        "filerEffStartDt",
        "filerEffStopDt",
        "treasEffStartDt",
        "treasEffStopDt",
        mode="before",
    )
    @classmethod
    def _check_expend_date(cls, value):
        if value:
            if isinstance(value, str):
                return date(
                    int(str(value[:4])), int(str(value[4:6])), int(str(value[6:8]))
                )

    @model_validator(mode="before")
    @classmethod
    def _check_state_code(cls, values):
        street_country_cols = [
            "filerStreetCountryCd",
            "treasStreetCountryCd",
            "assttreasStreetCountryCd",
            "chairStreetCountryCd",
            "chairMailingCountryCd",
        ]
        street_postal_cols = [
            "filerStreetPostalCode",
            "treasStreetPostalCode",
            "assttreasStreetPostalCode",
            "chairStreetPostalCode",
            "chairMailingPostalCode",
        ]
        street_region_cols = [
            "filerStreetRegion",
            "treasStreetRegion",
            "assttreasStreetRegion",
            "chairStreetRegion",
            "chairMailingRegion",
        ]
        code_zip = list(
            zip(street_country_cols, street_postal_cols, street_region_cols)
        )
        for col in code_zip:
            if values[col[0]] and values[col[0]] == "USA":
                if not values[col[1]]:
                    raise PydanticCustomError(
                        'state_code_check',
                        f"{col[1]} is required for USA {col[0]}",
                        {
                            'value': values[col[1]]
                        }
                    )
            elif values[col[0]] and values[col[0]] != "UMI":
                if not values[col[2]]:
                    raise PydanticCustomError(
                        'state_code_check',
                        f"{col[0]} is required for non-USA country",
                        {
                            'values': values[col[2]]
                        }
                    )
            else:
                pass
        return values
