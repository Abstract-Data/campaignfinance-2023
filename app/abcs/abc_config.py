from dataclasses import dataclass, field
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from pathlib import Path
from typing import Protocol, ClassVar, Union, Type


VALIDATOR_PLACEHOLDER = Union[Type[BaseModel], None]
SQL_MODEL_PLACEHOLDER = Union[Type[declarative_base], None]
STRING_PLACEHOLDER = str
FOLDER_PATH_PLACEHOLDER = Path

@dataclass
class StateCampaignFinanceConfigClass(Protocol):
    STATE: ClassVar[str]
    STATE_ABBREVIATION: ClassVar[str]
    TEMP_FOLDER: ClassVar[Path] = None
    TEMP_FILENAME: ClassVar[Path] = None

    VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER

    DB_BASE: ClassVar[Type[declarative_base]] = SQL_MODEL_PLACEHOLDER
    DB_ENGINE: ClassVar[create_engine] = None
    DB_SESSION: ClassVar[sessionmaker] = None

    EXPENSE_VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER
    EXPENSE_MODEL: ClassVar[Type[declarative_base]] = SQL_MODEL_PLACEHOLDER
    EXPENSE_FILE_PREFIX: ClassVar[str] = None

    CONTRIBUTION_VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER
    CONTRIBUTION_MODEL: ClassVar[Type[declarative_base]] = SQL_MODEL_PLACEHOLDER
    CONTRIBUTION_FILE_PREFIX: ClassVar[str] = None

    FILERS_VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER
    FILERS_MODEL: ClassVar[Type[declarative_base]] = SQL_MODEL_PLACEHOLDER
    FILERS_FILE_PREFIX: ClassVar[str] = None

    REPORTS_FILE_PREFIX: ClassVar[str] = None
    TRAVEL_FILE_PREFIX: ClassVar[str] = None
    CANDIDATE_FILE_PREFIX: ClassVar[str] = None
    DEBT_FILE_PREFIX: ClassVar[str] = None
    LOAN_FILE_PREFIX: ClassVar[str] = None

    STATE_CAMPAIGN_FINANCE_AGENCY: ClassVar[str]
    ZIPFILE_URL: ClassVar[str]

    VENDOR_NAME_COLUMN: ClassVar[str] = field(init=False)
    FILER_NAME_COLUMN: ClassVar[str] = field(init=False)

    PAYMENT_RECEIVED_DATE_COLUMN: ClassVar[str] = field(init=False)
    EXPENDITURE_DATE_COLUMN: ClassVar[str] = field(init=False)
    CONTRIBUTION_DATE_COLUMN: ClassVar[str] = field(init=False)

    EXPENDITURE_AMOUNT_COLUMN: ClassVar[str] = field(init=False)