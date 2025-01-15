from typing import Optional
from pydantic import BeforeValidator
from sqlmodel import Field
from .texas_settings import TECBaseModel
import funcs.validator_functions as funcs


class TECFlagsBase(TECBaseModel):
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