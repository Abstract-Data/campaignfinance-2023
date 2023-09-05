from dataclasses import dataclass, field
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import create_engine
from pathlib import Path
from typing import Protocol, ClassVar, Union, Type


VALIDATOR_PLACEHOLDER = Union[Type[BaseModel], None]
SQL_MODEL_PLACEHOLDER = Union[Type[DeclarativeBase], None]
STRING_PLACEHOLDER = str
FOLDER_PATH_PLACEHOLDER = Path

@dataclass
class StateCampaignFinanceConfigs(Protocol):
    STATE: ClassVar[str]
    STATE_ABBREVIATION: ClassVar[str]
    FOLDER: ClassVar[Path] = None

    VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER

    DB_BASE: ClassVar[Type[DeclarativeBase]] = SQL_MODEL_PLACEHOLDER
    DB_ENGINE: ClassVar[create_engine] = None
    DB_SESSION: ClassVar[sessionmaker] = None

    EXPENSE_VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER
    EXPENSE_MODEL: ClassVar[Type[DeclarativeBase]] = SQL_MODEL_PLACEHOLDER
    EXPENSE_FILE_PREFIX: ClassVar[str]

    CONTRIBUTION_VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER
    CONTRIBUTION_MODEL: ClassVar[Type[DeclarativeBase]] = SQL_MODEL_PLACEHOLDER
    CONTRIBUTION_FILE_PREFIX: ClassVar[str]

    FILERS_VALIDATOR: ClassVar[BaseModel] = VALIDATOR_PLACEHOLDER
    FILERS_MODEL: ClassVar[Type[DeclarativeBase]] = SQL_MODEL_PLACEHOLDER
    FILERS_FILE_PREFIX: ClassVar[str]

    STATE_CAMPAIGN_FINANCE_AGENCY: ClassVar[str]
    ZIPFILE_URL: ClassVar[str]

    VENDOR_NAME_COLUMN: ClassVar[str] = field(init=False)
    FILER_NAME_COLUMN: ClassVar[str] = field(init=False)

    PAYMENT_RECEIVED_DATE_COLUMN: ClassVar[str] = field(init=False)
    EXPENDITURE_DATE_COLUMN: ClassVar[str] = field(init=False)
    CONTRIBUTION_DATE_COLUMN: ClassVar[str] = field(init=False)

    EXPENDITURE_AMOUNT_COLUMN: ClassVar[str] = field(init=False)
