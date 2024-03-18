from __future__ import annotations
from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import field, dataclass
import contextlib
import inject
from tqdm import tqdm
from zipfile import ZipFile
import requests
import os
import sys
import ssl
import urllib.request
from typing import Generator, Type, Optional, Any
from collections import namedtuple, defaultdict
from sqlmodel import SQLModel
import funcs
from logger import Logger
from abcs import (
    StateCampaignFinanceConfigs,
    FileDownloader,
)
from funcs.validation import StateFileValidation
import states.oklahoma.validators as validators


# TODO: Change File Prefix Configurations to Oklahoma
# TODO: Make sure file folder reads only CSVs in Oklahoma so it doesn't try to read Zip files


logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")

# SQLModels = Generator[SQLModel, None, None]

FileValidationResults = namedtuple(
    'FileValidationResults', ['passed', 'failed', 'passed_count', 'failed_count'])


def download_base_config(binder):
    binder.bind(StateCampaignFinanceConfigs, OklahomaConfigs)


def category_base_config(binder):
    binder.bind(StateCampaignFinanceConfigs, OklahomaConfigs)
    binder.bind_to_provider(Session, session_scope)


@contextlib.contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session(engine)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def generate_file_list(folder: Path):
    return list(sorted([file for file in folder.glob("*.csv")]))


class OklahomaConfigs(StateCampaignFinanceConfigs):
    STATE: ClassVar[str] = "Oklahoma"
    STATE_ABBREVIATION: ClassVar[str] = "OK"
    TEMP_FOLDER: ClassVar[StateCampaignFinanceConfigs.TEMP_FOLDER] = Path.cwd().parent / "tmp" / "oklahoma"
    # TEMP_FILENAME: ClassVar[StateCampaignFinanceConfigs.TEMP_FILENAME] = (
    #         Path.cwd().parent / "tmp" / "texas" / "TEC_CF_CSV.zip")

    # EXPENSE_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.EXPENSE_VALIDATOR]
    # ] = TECExpense
    # EXPENSE_MODEL: ClassVar[Type[SQLModel]] = None
    EXPENSE_FILE_PREFIX: ClassVar[str] = "expenditures"

    # CONTRIBUTION_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR]
    # ] = TECContribution
    # CONTRIBUTION_MODEL: ClassVar[Type[SQLModel]] = None
    CONTRIBUTION_FILE_PREFIX: ClassVar[str] = "contribution"

    # FILERS_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.FILERS_VALIDATOR]
    # ] = TECFiler
    # FILERS_MODEL: ClassVar[Type[SQLModel]] = None
    FILERS_FILE_PREFIX: ClassVar[str] = "filer"

    REPORTS_FILE_PREFIX: ClassVar[str] = "finals"

    STATE_CAMPAIGN_FINANCE_AGENCY: ClassVar[str] = "Oklahoma Ethics Commission"
    # ZIPFILE_URL: ClassVar[
    #     str
    # ] = "https://ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip"

    VENDOR_NAME_COLUMN: ClassVar[str] = "payeeCompanyName"
    FILER_NAME_COLUMN: ClassVar[str] = "filerNameFormatted"

    PAYMENT_RECEIVED_DATE_COLUMN: ClassVar[str] = "receivedDt"
    EXPENDITURE_DATE_COLUMN: ClassVar[str] = "expendDt"
    CONTRIBUTION_DATE_COLUMN: ClassVar[str] = "contributionDt"

    EXPENDITURE_AMOUNT_COLUMN: ClassVar[str] = "expendAmount"


@dataclass
class OklahomaCategory:
    category: str
    records: Generator[Dict, None, None] = field(init=False)
    validation: StateFileValidation = field(init=False)
    validator: Type[validators.OklahomaSettings] = field(init=False)
    config: StateCampaignFinanceConfigs = field(init=False)
    _files: Optional[Generator[Path, Any, None]] = None
    __logger: Logger = field(init=False)

    def __repr__(self):
        return f"TECFileCategories({self.category})"

    def init(self):
        inject.configure(category_base_config, clear=True)

    def __post_init__(self):
        self.init()
        self.create_file_list()
        self.get_validators()

    @property
    def logger(self):
        self.__logger = Logger(self.__class__.__name__)
        return self.__logger

    @inject.autoparams()
    def create_file_list(self, config: StateCampaignFinanceConfigs) -> Generator[Path, Any, None]:
        if self.category == "expenses":
            self._files = (
                x for x in generate_file_list(config.TEMP_FOLDER)
                if x.name.startswith(config.EXPENSE_FILE_PREFIX))
        elif self.category == "contributions":
            self._files = (
                x for x in generate_file_list(config.TEMP_FOLDER)
                if x.name.startswith(config.CONTRIBUTION_FILE_PREFIX))
        elif self.category == "filers":
            self._files = (
                x for x in generate_file_list(config.TEMP_FOLDER)
                if x.name.startswith(config.FILERS_FILE_PREFIX))
        elif self.category == "reports":
            self._files = (
                x for x in generate_file_list(config.TEMP_FOLDER)
                if x.name.startswith(config.REPORTS_FILE_PREFIX))
        return self._files

    def get_validators(self) -> Type[validators.OklahomaSettings]:
        if self.category == "expenses":
            self.validator = validators.OklahomaExpenditure
        elif self.category == "contributions":
            self.validator = validators.OklahomaContribution
        elif self.category == "lobby":
            self.validator = validators.OklahomaLobbyistExpenditure
        else:
            raise ValueError(f"Invalid category: {self.category}")
        return self.validator

    def read(self) -> Generator[Dict, None, None]:
        self.records = (record for file in list(self._files) for record in funcs.FileReader.read_file(file))
        # if self.category == "filers":
        #     records = merge_filer_names(records)
        return self.records

    def load(self) -> List[Dict]:
        return list(x for x in self.read())

    def validate(self,
                 records: Generator[Dict, None, None] = None,
                 validator: Type[SQLModel] = None
                 ) -> StateFileValidation:
        if not records:
            records = self.records

        if not validator:
            validator = self.validator

        self.validation = StateFileValidation()
        return self.validation.validate(records=records, validator=validator)

    @inject.autoparams()
    def load_to_db(self, records: Generator[Dict, None, None], session: Session) -> None:
        session.add_all(records)
        session.commit()