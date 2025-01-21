from dataclasses import Field
from typing import Optional
from datetime import date
from pydantic import ConfigDict, model_validator, BaseModel, BeforeValidator
from sqlmodel import SQLModel, Field
from .validators.texas_settings import TECSettings
import states.texas.funcs.tx_validation_funcs as tx_funcs
import funcs.validator_functions as funcs
from funcs.record_keygen import RecordKeyGenerator

class TECFlags(TECSettings, table=True):
    __tablename__ = "tx_flags"
    __table_args__ = {"schema": "texas"}
    id: Optional[int] = Field(default=None, primary_key=True, description="Unique record ID")
    primaryUsaPhoneFlag: Optional[bool] = Field(
        BeforeValidator(funcs.check_contains_factory('PrimaryUsaPhoneFlag')),
        description="Y indicates that the phone is a USA")
    capitalLivingexpFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution was for capital living expenses")
    contributorOosPacFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contributor is an out-of-state PAC")
    finalOfficeholderAckFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution was acknowledged by the final officeholder")
    finalRetainedAssetsFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution was retained as final assets")
    finalUnexpendContribFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution was unexpended")
    infoOnlyFlag: Optional[bool] = Field(
        default=None,
        description="Superseded by other report")
    itemizeFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution is itemized")
    loanGuaranteedFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the loan was guaranteed")
    modifiedElectCycleFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution was modified by the election cycle")
    reimburseIntendedFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution was intended for reimbursement")
    srcCorpContribFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution was from a corporate source")
    travelFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution has associated travel")
    unexpendContribFilerFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution was unexpended by the filer")

class TECBaseModel(TECSettings):
    recordType: Optional[str] = Field(BeforeValidator(funcs.check_contains_factory('recordType')), description="Record type code")
    formTypeCd: Optional[str] = Field(BeforeValidator(funcs.check_contains_factory('formType')), description="TEC form used")
    schedFormTypeCd: Optional[str] = Field(BeforeValidator(funcs.check_contains_factory('schedFormType')), description="TEC Schedule Used")
    receivedDt: Optional[date] = Field(BeforeValidator(funcs.check_contains_factory('receivedDt')), description="Date report received by TEC")
    filerIdent: Optional[str] = Field(default=None, description="Filer account #", max_length=100)
    filerTypeCd: Optional[str] = Field(default=None, description="Type of filer", max_length=30)