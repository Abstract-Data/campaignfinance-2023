from datetime import date
from typing import Optional, Annotated
import probablepeople as pp
from nameparser import HumanName
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_core import PydanticCustomError


def format_dates(v):
    if v:
        return date(int(str(v[:4])), int(str(v[4:6])), int(str(v[6:8])))


"""
======================
==== TEC Settings ====
======================
"""


class TECSettings(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        str_to_upper=True,
        from_attributes=True,
    )

class TECValidator(TECSettings):
    recordType: Optional[str]
    formTypeCd: Optional[str]
    schedFormTypeCd: Optional[str]
    reportInfoIdent: Optional[int]
    receivedDt: Optional[date]
    infoOnlyFlag: Optional[bool]
    filerIdent: Optional[int]
    filerTypeCd: Optional[str]
    filerTitle: Optional[str]
    filerName: Optional[str]
    filerNameFormatted: Optional[str]
    filerFirstName: Optional[str]
    filerLastName: Optional[str]
    filerMiddleName: Optional[str]
    filerPrefix: Optional[str]
    filerSuffix: Optional[str]
    filerCompanyName: Optional[str]
    filerCompanyNameFormatted: Optional[str]
    expendInfoId: Optional[int]
    expendDt: Optional[date]
    expendAmount: Optional[float]
    expendDescr: Optional[str]
    expendCatCd: Optional[str]
    expendCatDescr: Optional[str]
    itemizeFlag: Optional[bool]
    travelFlag: Optional[bool]
    politicalExpendCd: Optional[bool]
    reimburseIntendedFlag: Optional[bool]
    srcCorpContribFlag: Optional[bool]
    capitalLivingexpFlag: Annotated[Optional[str], Field(max_length=1)]
    payeePersentTypeCd: Optional[str]
    payeeNameOrganization: Optional[str]
    payeeCompanyName: Optional[str]
    payeeCompanyNameFormatted: Optional[str]
    payeeNameLast: Optional[str]
    payeeNameSuffixCd: Optional[str]
    payeeNameFirst: Optional[str]
    payeeNamePrefixCd: Optional[str]
    payeeNameShort: Optional[str]
    payeeStreetAddr1: Optional[str]
    payeeStreetAddr2: Optional[str]
    payeeStreetCity: Optional[str]
    payeeStreetStateCd: Optional[str]
    payeeStreetCountyCd: Optional[str]
    payeeStreetCountryCd: Optional[str]
    payeeStreetPostalCode: Optional[int]
    payeeStreetRegion: Optional[str]
    creditCardIssuer: Optional[str]
    repaymentDt: Optional[date]
    contributionInfoId: Optional[int]
    contributionDt: Optional[date]
    contributionAmount: Optional[float]
    contributionDescr: Optional[str]
    contributionCatCd: Optional[str]
    contributorNameOrganization: Optional[str]
    contributorNameLast: Optional[str]
    contributorNameSuffixCd: Optional[str]
    contributorNameFirst: Optional[str]
    contributorNamePrefixCd: Optional[str]
    contributorNameShort: Optional[str]
    contributorStreetCity: Optional[str]
    contributorStreetStateCd: Optional[str]
    contributorStreetCountyCd: Optional[str]
    contributorStreetCountryCd: Optional[str]
    contributorStreetPostalCode: Optional[int]
    contributorStreetRegion: Optional[str]
    contributorEmployer: Optional[str]
    contributorOccupation: Optional[str]
    contributorJobTitle: Optional[str]
    contributorPacFein: Optional[str]
    contributorOosPacFlag: Optional[bool]
    contributorLawFirmName: Optional[str]
    contributorSpouseLawFirmName: Optional[str]
    contributorParent1LawFirmName: Optional[str]
    contributorParent2LawFirmName: Optional[str]

    @model_validator(mode="before")
    @classmethod
    def uppercase_values(cls, values):
        for k, v in values.items():
            if isinstance(v, str):
                values[k] = v.upper()
        return values

    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", '"']:
                values[k] = None
        return values

    @field_validator(
        "contributorStreetPostalCode", "payeeStreetPostalCode", mode="before"
    )
    @classmethod
    def _postal_code(cls, value):
        if value:
            if isinstance(value, str):
                return int(value.split("-")[0])

    @model_validator(mode="before")
    @classmethod
    def filername_parser(cls, values):
        filer_name = values.get("filerName", None)
        if filer_name:
            details = pp.parse(filer_name)

            if "GivenName" in [x[1] for x in details]:
                person_split = HumanName(filer_name)
                values["filerTitle"] = person_split.title
                values["filerFirstName"] = person_split.first
                values["filerLastName"] = person_split.last
                values["filerMiddleName"] = person_split.middle
                values["filerSuffix"] = person_split.suffix

                if person_split.nickname == "The Honorable":
                    pfx = person_split.nickname
                    name_fmt = " ".join(
                        [
                            person_split.nickname,
                            person_split.first,
                            person_split.middle,
                            person_split.last,
                        ]
                    )
                elif person_split.title:
                    pfx = person_split.title
                    name_fmt = " ".join(
                        [
                            person_split.title,
                            person_split.first,
                            person_split.middle,
                            person_split.last,
                            person_split.suffix,
                        ]
                    ).replace("  ", " ")

                else:
                    pfx = None
                    name_fmt = " ".join(
                        [
                            person_split.first,
                            person_split.middle,
                            person_split.last,
                            person_split.suffix,
                        ]
                    )

                    values["filerPrefix"] = pfx
                    values["filerNameFormatted"] = name_fmt.strip()

            elif "CorporationName" in [x[1] for x in details]:
                companyname = " ".join(
                    [x[0] for x in details if x[1] == "CorporationName"]
                )
                andcompany = " ".join(
                    [x[0] for x in details if x[1] == "CorporationNameAndCompany"]
                )
                legaltype = " ".join(
                    [x[0] for x in details if x[1] == "CorporationLegalType"]
                )

                if all([companyname, andcompany, legaltype]):
                    values["filerCompanyName"] = " ".join(
                        [companyname, andcompany]
                    ).replace("  ", " ")
                    values["filerCompanyNameFormatted"] = " ".join(
                        [companyname, andcompany, legaltype]
                    ).replace("  ", " ")
                elif all([companyname, andcompany]):
                    values["filerCompanyName"] = " ".join(
                        [companyname, andcompany]
                    ).replace("  ", " ")
                    values["filerCompanyNameFormatted"] = " ".join(
                        [companyname, andcompany]
                    ).replace("  ", " ")
                elif all([companyname, legaltype]):
                    values["filerCompanyName"] = companyname
                    values["filerCompanyNameFormatted"] = " ".join(
                        [companyname, legaltype]
                    ).replace("  ", " ")
                elif companyname:
                    values["filerCompanyName"] = companyname
                    values["filerCompanyNameFormatted"] = companyname

                else:
                    values["filerCompanyName"] = None
                    values["filerCompanyNameFormatted"] = None

            else:
                pass

        return values

    @model_validator(mode="before")
    @classmethod
    def payeename_parser(cls, values):
        filer_name = values.get("payeeNameOrganization", None)
        if filer_name:
            details = pp.parse(filer_name)
            if "CorporationName" in [x[1] for x in details]:
                companyname = (
                    " ".join([x[0] for x in details if x[1] == "CorporationName"])
                    .replace("  ", " ")
                    .replace(",", "")
                )
                andcompany = " ".join(
                    [x[0] for x in details if x[1] == "CorporationNameAndCompany"]
                )
                legaltype = " ".join(
                    [x[0] for x in details if x[1] == "CorporationLegalType"]
                )

                if all([companyname, andcompany, legaltype]):
                    values["payeeCompanyName"] = " ".join([companyname, andcompany])
                    values["payeeCompanyNameFormatted"] = " ".join(
                        [companyname, andcompany, legaltype]
                    )
                elif all([companyname, andcompany]):
                    values["payeeCompanyName"] = " ".join([companyname, andcompany])
                    values["payeeCompanyNameFormatted"] = " ".join(
                        [companyname, andcompany]
                    )
                elif all([companyname, legaltype]):
                    values["payeeCompanyName"] = companyname
                    values["payeeCompanyNameFormatted"] = " ".join(
                        [companyname, legaltype]
                    )
                elif companyname:
                    values["payeeCompanyName"] = companyname
                    values["payeeCompanyNameFormatted"] = companyname

                else:
                    values["payeeCompanyName"] = None
                    values["payeeCompanyNameFormatted"] = None

        return values

    expenditure_date = field_validator("expendDt")(format_dates)
    recieved_date = field_validator("receivedDt", mode="before")(format_dates)
    repayment_date = field_validator("repaymentDt", mode="before")(format_dates)
    contribution_date = field_validator("repaymentDt", mode="before")

