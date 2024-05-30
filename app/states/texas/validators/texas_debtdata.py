from datetime import date, datetime
from typing import Optional, Annotated, List
from pydantic import field_validator, model_validator
from sqlmodel import SQLModel, Field
from pydantic_core import PydanticCustomError
from states.texas.validators.texas_settings import TECSettings
import funcs.validator_functions as funcs
import states.texas.funcs.tx_validation_funcs as tx_funcs


class DebtData(TECSettings, table=True):
    __tablename__ = "tx_debt_data"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, description="Unique record ID")
    record_type: str = Field(max_length=20, description="Record type code - always DEBT")
    form_type_cd: str = Field(max_length=20, description="TEC form used")
    sched_form_type_cd: str = Field(max_length=20, description="TEC Schedule Used")
    report_info_ident: int = Field(description="Unique report #")
    received_dt: date = Field(description="Date report received by TEC")
    info_only_flag: Optional[bool] = Field(..., description="Superseded by other report")
    filer_ident: str = Field(max_length=100, description="Filer account #")
    filer_type_cd: str = Field(max_length=30, description="Type of filer")
    filer_name: str = Field(max_length=200, description="Filer name")
    loan_info_id: int = Field(description="Loan unique identifier", primary_key=True)
    loan_guaranteed_flag: Optional[bool] = Field(..., description="Loan guaranteed indicator")
    lender_persent_type_cd: str = Field(max_length=30, description="Type of lender name data - INDIVIDUAL or ENTITY")
    lender_name_organization: str = Field(max_length=100, description="For ENTITY, the lender organization name")
    lender_name_last: str = Field(max_length=100, description="For INDIVIDUAL, the lender last name")
    lender_name_suffix_cd: str = Field(max_length=30, description="For INDIVIDUAL, the lender name suffix (e.g. JR, MD, II)")
    lender_name_first: str = Field(max_length=45, description="For INDIVIDUAL, the lender first name")
    lender_name_prefix_cd: str = Field(max_length=30, description="For INDIVIDUAL, the lender name prefix (e.g. MR, MRS, MS)")
    lender_name_short: str = Field(max_length=25, description="For INDIVIDUAL, the lender short name (nickname)")
    lender_street_city: str = Field(max_length=30, description="Lender street address - city")
    lender_street_state_cd: str = Field(max_length=2, description="Lender street address - state code (e.g. TX, CA) - for country=USA/UMI only")
    lender_street_county_cd: str = Field(max_length=5, description="Lender street address - Texas county")
    lender_street_country_cd: str = Field(max_length=3, description="Lender street address - country (e.g. USA, UMI, MEX, CAN)")
    lender_street_postal_code: str = Field(max_length=20, description="Lender street address - postal code - for USA addresses only")
    lender_street_region: str = Field(max_length=30, description="Lender street address - region for country other than USA")
    file_origin: str = Field(..., description="File origin", max_length=20)
    download_date: date = Field(..., description="Date file downloaded")
    @model_validator(mode='before')
    @classmethod
    def check_lender_type(cls, values):
        if values['lender_persent_type_cd'] not in ('INDIVIDUAL', 'ENTITY'):
            raise PydanticCustomError(
                'lender_type',
                'Lender type must be INDIVIDUAL or ENTITY',
                {
                    'column': 'lender_persent_type_cd',
                    'value': values['lender_persent_type_cd']
                }
            )
        return values

    @model_validator(mode='before')
    @classmethod
    def check_individual_lender_info_filled(cls, values):
        if values['lender_persent_type_cd'] == 'INDIVIDUAL':
            if not values['lender_name_last'] or not values['lender_name_first']:
                raise PydanticCustomError(
                    'individual_lender_info',
                    'For INDIVIDUAL lender, last name and first name must be provided',
                    {
                        'column': 'lender_persent_type_cd',
                        'value': values['lender_persent_type_cd']
                    }
                )
        return values

    @model_validator(mode='before')
    @classmethod
    def check_entity_lender_info_filled(cls, values):
        if values['lender_persent_type_cd'] == 'ENTITY':
            if not values['lender_name_organization']:
                raise PydanticCustomError(
                    'entity_lender_info',
                    'For ENTITY lender, organization name must be provided',
                    {
                        'column': 'lender_persent_type_cd',
                        'value': values['lender_persent_type_cd']
                    }
                )
        return values
