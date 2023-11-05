from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import field, dataclass
from tqdm import tqdm
from zipfile import ZipFile
import requests
import os
import sys
import ssl
import urllib.request
from typing import Generator, Tuple, Type, Iterator
from collections import namedtuple
import pandas as pd
from pydantic import ValidationError, BaseModel
from funcs import FileReader
from logger import Logger
from abcs import (
    StateFileValidation,
    StateCampaignFinanceConfigs,
    FileDownloader,
    StateCategories,
)
from db_loaders.postgres_loader import PostgresLoader
from states.texas.database import (
    DeclarativeBase,
    sessionmaker,
    create_engine,
    Base,
    engine,
    SessionLocal
)
from states.texas.validators import (
    TECExpense,
    TECFiler,
    TECContribution
)
from states.texas.models import (
    TECContributionRecord,
    TECFilerRecord,
    TECExpenseRecord
)

logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")

FileValidationResults = namedtuple(
    'FileValidationResults', ['passed', 'failed', 'passed_count', 'failed_count'])
def generate_file_list(folder: Path):
    return sorted([file for file in folder.glob("*.csv")])


class TexasConfigs(StateCampaignFinanceConfigs):
    STATE: ClassVar[str] = "Texas"
    STATE_ABBREVIATION: ClassVar[str] = "TX"
    FOLDER: ClassVar[StateCampaignFinanceConfigs.FOLDER] = Path.cwd().parent / "tmp"

    DB_BASE: ClassVar[Type[DeclarativeBase]] = Base
    DB_ENGINE: ClassVar[create_engine] = engine
    DB_SESSION: ClassVar[sessionmaker] = SessionLocal

    EXPENSE_VALIDATOR: ClassVar[
        Type[StateCampaignFinanceConfigs.EXPENSE_VALIDATOR]
    ] = TECExpense
    EXPENSE_MODEL: ClassVar[Type[DeclarativeBase]] = TECExpenseRecord
    EXPENSE_FILE_PREFIX: ClassVar[str] = "expend"

    CONTRIBUTION_VALIDATOR: ClassVar[
        Type[StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR]
    ] = TECContribution
    CONTRIBUTION_MODEL: ClassVar[Type[DeclarativeBase]] = TECContributionRecord
    CONTRIBUTION_FILE_PREFIX: ClassVar[str] = "contribs"

    FILERS_VALIDATOR: ClassVar[
        Type[StateCampaignFinanceConfigs.FILERS_VALIDATOR]
    ] = TECFiler
    FILERS_MODEL: ClassVar[Type[DeclarativeBase]] = TECFilerRecord
    FILERS_FILE_PREFIX: ClassVar[str] = "filer"

    STATE_CAMPAIGN_FINANCE_AGENCY: ClassVar[str] = "TEC"
    ZIPFILE_URL: ClassVar[
        str
    ] = "https://ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip"

    VENDOR_NAME_COLUMN: ClassVar[str] = "payeeCompanyName"
    FILER_NAME_COLUMN: ClassVar[str] = "filerNameFormatted"

    PAYMENT_RECEIVED_DATE_COLUMN: ClassVar[str] = "receivedDt"
    EXPENDITURE_DATE_COLUMN: ClassVar[str] = "expendDt"
    CONTRIBUTION_DATE_COLUMN: ClassVar[str] = "contributionDt"

    EXPENDITURE_AMOUNT_COLUMN: ClassVar[str] = "expendAmount"


@dataclass
class TECFileDownloader(FileDownloader):
    _configs: ClassVar[StateCampaignFinanceConfigs] = TexasConfigs
    _folder: StateCampaignFinanceConfigs.FOLDER = TexasConfigs.FOLDER
    __logger: Logger = field(init=False)

    @property
    def folder(self) -> Path:
        return self._folder

    @folder.setter
    def folder(self, value: Path) -> None:
        self._folder = value

    @property
    def _tmp(self) -> Path:
        return self.folder if self.folder else self._configs.FOLDER

    def check_if_folder_exists(self) -> Path:
        self.__logger.info(f"Checking if {self.folder} exists...")
        if self.folder.exists():
            return self.folder

        self.__logger.debug(f"{self.folder} does not exist...")
        self.__logger.debug(f"Throwing input prompt...")
        _create_folder = input("Temp folder does not exist. Create? (y/n): ")
        self.__logger.debug(f"User input: {_create_folder}")
        if _create_folder.lower() == "y":
            self.folder.mkdir()
            return self.folder
        else:
            print("Exiting...")
            self.__logger.info("User selected 'n'. Exiting...")
            sys.exit()

    def download(
        self,
        read_from_temp: bool = True,
        config: StateCampaignFinanceConfigs = TexasConfigs,
    ) -> None:
        tmp = self._tmp
        temp_filename = tmp / "TEC_CF_CSV.zip"

        self.__logger.info(f"Setting temp filename to {temp_filename} in download func")

        def download_file_with_requests() -> None:
            # download files
            with requests.get(config.ZIPFILE_URL, stream=True) as resp:
                # check header to get content length, in bytes
                total_length = int(resp.headers.get("Content-Length"))

                # Chunk download of zip file and write to temp folder
                with open(temp_filename, "wb") as f:
                    for chunk in tqdm(
                        resp.iter_content(chunk_size=1024),
                        total=round(total_length / 1024, 2),
                        unit="KB",
                        desc="Downloading",
                    ):
                        if chunk:
                            f.write(chunk)
                    print("Download Complete")
                return None

        def download_file_with_urllib3() -> None:
            self.__logger.info(
                f"Downloading {config.STATE_CAMPAIGN_FINANCE_AGENCY} Files..."
            )
            ssl_context = ssl.create_default_context()
            ssl_context.options |= ssl.OP_CIPHER_SERVER_PREFERENCE
            ssl_context.set_ciphers("DEFAULT@SECLEVEL=2")
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            self.__logger.info(f"SSL Context: {ssl_context}")
            self.__logger.debug(
                f"Downloading {config.STATE_CAMPAIGN_FINANCE_AGENCY} Files..."
            )

            opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=ssl_context)
            )
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(config.ZIPFILE_URL, temp_filename)

        def extract_zipfile() -> None:
            # extract zip file to temp folder
            self.__logger.debug(f"Extracting {temp_filename} to {tmp}...")
            with ZipFile(temp_filename, "r") as myzip:
                print("Extracting Files...")
                for _ in tqdm(myzip.namelist()):
                    myzip.extractall(tmp)
                os.unlink(temp_filename)
                self.folder = tmp  # set folder to temp folder
                self.__logger.debug(
                    f"Extracted {temp_filename} to {tmp}, set folder to {tmp}"
                )

        try:
            if read_from_temp is False:
                # check if tmp folder exists
                if tmp.is_dir():
                    ask_to_make_folder = input(
                        "Temp folder already exists. Overwrite? (y/n): "
                    )
                    if ask_to_make_folder.lower() == "y":
                        print("Overwriting Temp Folder...")
                        download_file_with_urllib3()
                        extract_zipfile()
                    else:
                        as_to_change_folder = input(
                            "Use temp folder as source? (y/n): "
                        )
                        if as_to_change_folder.lower() == "y":
                            if tmp.glob("*.csv") == 0 and tmp.glob("*.zip") == 1:
                                print("No CSV files in temp folder. Found .zip file...")
                                print("Extracting .zip file...")
                                extract_zipfile()
                            else:
                                self.folder = tmp  # set folder to temp folder
                        else:
                            print("Exiting...")
                            sys.exit()

                # else:
                #     self.folder = tmp  # set folder to temp folder

            # remove tmp folder if user cancels download
        except KeyboardInterrupt:
            print("Download Cancelled")
            print("Removing Temp Folder...")
            for file in tmp.iterdir():
                file.unlink()
            tmp.rmdir()
            print("Temp Folder Removed")

    def read(self):
        self.folder = self._tmp

    def __post_init__(self):
        TECFileDownloader.__logger = Logger(self.__class__.__name__)
        self.check_if_folder_exists()


@dataclass
class TECFileValidator:
    """ Validator class for TEC Files"""
    __logger: Logger = field(init=False)
    _db: ClassVar[PostgresLoader] = field(init=False)

    def __post_init__(self):
        TECFileValidator.__logger = Logger(self.__class__.__name__)

    @staticmethod
    def validate_generator(file: 'TECFile') -> Generator[Dict, None, None]:
        for x in file.records:
            yield file.validator(**dict(x))

    @staticmethod
    def validate(file):
        _passed, _failed = [], []
        for x in tqdm(TECFileValidator.validate_generator(file.read()), desc=f"VALIDATING: {file.file.name} records"):
            try:
                _passed.append(x)
            except ValidationError as e:
                x['error'] = e.errors()
                _failed.append(x)
        return _passed, _failed


@dataclass
class TECFile:
    file: Path
    _validator: StateCampaignFinanceConfigs.VALIDATOR = field(
        default_factory=TECFileValidator
    )
    _sql_model: Type[DeclarativeBase] = field(init=False)
    records: Generator[Dict, None, None] | List[Dict] = None
    _passed: Iterator[BaseModel] = field(init=False)
    _failed: List[Dict] = field(init=False)
    _models: List[DeclarativeBase] = field(init=False)

    def __repr__(self):
        return f"TECFile({self.file.name})"

    @property
    def validator(self):
        _record = iter(self.read()).__next__()
        if _record['file_origin'].startswith("filer"):
            _validator, _model = TexasConfigs.FILERS_VALIDATOR, TexasConfigs.FILERS_MODEL
        elif _record['file_origin'].startswith("expend"):
            _validator, _model = TexasConfigs.EXPENSE_VALIDATOR, TexasConfigs.EXPENSE_MODEL
        elif _record['file_origin'].startswith("contrib"):
            _validator, _model = TexasConfigs.CONTRIBUTION_VALIDATOR, TexasConfigs.CONTRIBUTION_MODEL
        else:
            raise ValueError(f"Invalid file name: {_record['file_origin']}")

        self._validator = _validator
        self._sql_model = _model
        return self._validator

    def read(self) -> Generator[Dict, None, None]:
        self.records = FileReader.read_file(self.file)
        return self.records

    def load_records(self) -> List[Dict]:
        if not self.records:
            self.read()
        _records = []
        for x in tqdm(self.records, desc=f"LOADING: {self.file.name} records"):
            _records.extend(x)

        self.records = _records
        return self.records

    def validate(self) -> (Iterator[BaseModel], List[Dict[str, ValidationError]]):
        self.read()
        _passed, _failed = [], []
        for x in tqdm(self.records, desc=f"VALIDATING: {self.file.name} records"):
            try:
                _record = self.validator(**x)
                _passed.append(_record)
            except ValidationError as e:
                x['error'] = e.errors()
                _failed.append(x)

        self._passed = iter(_passed)
        self._failed = _failed
        return self._passed, self._failed

    def create_models(self, records: Iterator[BaseModel] = None):
        self._models = [
            self._sql_model(**dict(x)) for x in tqdm(
                self._passed if not records else records,
                desc=f"CREATING MODELS: {self.file.name} records"
            )
        ]
        return self._models





@dataclass
class TECCategories:
    expenses: ClassVar[Generator[TECFile, None, None]] = None
    contributions: ClassVar[Generator[TECFile, None, None]] = None
    filers: ClassVar[Generator[TECFile, None, None]] = None
    __logger: Logger = field(init=False)

    def __repr__(self):
        return f"TECFileCategories()"

    def __post_init__(self):
        TECCategories.__logger = Logger(self.__class__.__name__)
        TECCategories.expenses = self.create_tec_files(prefix=TexasConfigs.EXPENSE_FILE_PREFIX)
        TECCategories.contributions = self.create_tec_files(prefix=TexasConfigs.CONTRIBUTION_FILE_PREFIX)
        TECCategories.filers = self.create_tec_files(prefix=TexasConfigs.FILERS_FILE_PREFIX)

    @classmethod
    def create_tec_files(cls, prefix: str, folder: StateCampaignFinanceConfigs.FOLDER = TexasConfigs.FOLDER):
        return (TECFile(x) for x in generate_file_list(folder) if x.name.startswith(prefix))

    @classmethod
    def read(cls, category: Generator[TECFile, None, None]):
        all_records = []
        for file in tqdm(category):
            all_records.extend(file.read())
        yield all_records

    @classmethod
    def load(cls, read) -> List[TECFile]:
        _files = [x for x in read]
        _all_records = []
        for file in tqdm(_files):
            _record = [x for x in file.read()]
            _all_records.extend(_record)
        return _all_records

    @classmethod
    def validate_category(cls, load, to_db: bool = False, update: bool = False):
        _cat_passed, _cat_failed = [], []
        for x in load:
            _passed, _failed = x.validate()
            _cat_passed.extend(_passed)
            _cat_failed.extend(_failed)

            if to_db:
                _db = PostgresLoader(Base)
                if update:
                    _models = x.create_models(_passed)
                    _db.update_records(records=_models, session=SessionLocal, table=x._sql_model)
                else:
                    _db.load(records=_passed, session=SessionLocal, table=x._sql_model)
        return _cat_passed, _cat_failed

    def update_database(self, category: List[TECFile]) -> None:
        #TODO: Figure out why Updating Database not working, returns a 'genexp' object
        _db = PostgresLoader(Base)
        for file in tqdm(category, desc=f"UPDATING DATABASE: {category.__name__}"):
            _models = file.create_models()
            _db.update_records(records=_models, session=SessionLocal, table=file._sql_model)
        return None

