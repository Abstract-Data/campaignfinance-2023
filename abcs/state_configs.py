from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
from pathlib import Path


VALIDATOR_PLACEHOLDER = BaseModel
SQL_MODEL_PLACEHOLDER = declarative_base
STRING_PLACEHOLDER = str
FOLDER_PATH_PLACEHOLDER = Path

@dataclass
class StateCampaignFinanceConfigs(ABC):
    FOLDER: Path = FOLDER_PATH_PLACEHOLDER

    VALIDATOR: BaseModel = VALIDATOR_PLACEHOLDER
    EXPENSE_FILE_PREFIX: str = STRING_PLACEHOLDER
    CONTRIBUTION_FILE_PREFIX: str = STRING_PLACEHOLDER
    FILERS_FILE_PREFIX: str = STRING_PLACEHOLDER
    SQL_MODEL: declarative_base = SQL_MODEL_PLACEHOLDER
    STATE_CAMPAIGN_FINANCE_AGENCY: str = STRING_PLACEHOLDER
    ZIPFILE_URL: str = STRING_PLACEHOLDER

    VENDOR_NAME_COLUMN: str = field(init=False)
    FILER_NAME_COLUMN: str = field(init=False)

    PAYMENT_RECEIVED_DATE_COLUMN: str = field(init=False)
    EXPENDITURE_DATE_COLUMN: str = field(init=False)
    CONTRIBUTION_DATE_COLUMN: str = field(init=False)

    EXPENDITURE_AMOUNT_COLUMN: str = field(init=False)

    def __post_init__(self):
        self.DATE_COLUMNS = [
            self.PAYMENT_RECEIVED_DATE_COLUMN,
            self.EXPENDITURE_DATE_COLUMN,
            self.CONTRIBUTION_DATE_COLUMN
        ]

        self.UPPERCASE_COLUMNS = [
            self.VENDOR_NAME_COLUMN,
            self.FILER_NAME_COLUMN
        ]