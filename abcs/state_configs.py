from dataclasses import dataclass, field
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
from pathlib import Path
from typing import Protocol, ClassVar, Union, Type


VALIDATOR_PLACEHOLDER = Union[Type[BaseModel], None]
SQL_MODEL_PLACEHOLDER = Union[Type[declarative_base], None]
STRING_PLACEHOLDER = str
FOLDER_PATH_PLACEHOLDER = Path

@dataclass
class StateCampaignFinanceConfigs(Protocol):
    FOLDER: ClassVar[Path]

    VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER

    EXPENSE_VALIDATOR: ClassVar[BaseModel]
    EXPENSE_MODEL: ClassVar[declarative_base]
    EXPENSE_FILE_PREFIX: ClassVar[str]

    CONTRIBUTION_VALIDATOR: ClassVar[BaseModel]
    CONTRIBUTION_MODEL: ClassVar[declarative_base]
    CONTRIBUTION_FILE_PREFIX: ClassVar[str]

    FILERS_VALIDATOR: ClassVar[BaseModel]
    FILERS_MODEL: ClassVar[declarative_base]
    FILERS_FILE_PREFIX: ClassVar[str]

    STATE_CAMPAIGN_FINANCE_AGENCY: ClassVar[str]
    ZIPFILE_URL: ClassVar[str]

    VENDOR_NAME_COLUMN: ClassVar[str] = field(init=False)
    FILER_NAME_COLUMN: ClassVar[str] = field(init=False)

    PAYMENT_RECEIVED_DATE_COLUMN: ClassVar[str] = field(init=False)
    EXPENDITURE_DATE_COLUMN: ClassVar[str] = field(init=False)
    CONTRIBUTION_DATE_COLUMN: ClassVar[str] = field(init=False)

    EXPENDITURE_AMOUNT_COLUMN: ClassVar[str] = field(init=False)
