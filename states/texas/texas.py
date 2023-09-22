from states.texas.validators import TECExpense, TECFiler, TECContribution
from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import field, dataclass
import csv
from tqdm import tqdm
from pydantic import ValidationError, BaseModel, ConfigDict
from states.texas.database import DeclarativeBase, sessionmaker, SessionLocal, Base, create_engine
from states.texas.models import TECContributionRecord, TECFilerRecord, TECExpenseRecord
from zipfile import ZipFile
import requests
import os
import sys
import ssl
import urllib.request
from typing import Generator, Iterator, Type, Any
import datetime
from collections import Counter
import pandas as pd
from abcs import (
    StateFileValidation,
    StateCampaignFinanceConfigs,
    FileDownloader,
    StateCategories,
)
from funcs import FileReader
from db_loaders.postgres_loader import PostgresLoader
from states.texas.database import Base, engine, SessionLocal
from logger import Logger

logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")


class TexasConfigs(StateCampaignFinanceConfigs):
    STATE: ClassVar[str] = "Texas"
    STATE_ABBREVIATION: ClassVar[str] = "TX"
    FOLDER: ClassVar[StateCampaignFinanceConfigs.FOLDER] = Path.cwd() / "tmp"

    DB_BASE: ClassVar[Type[DeclarativeBase]] = Base
    DB_ENGINE: ClassVar[create_engine] = engine
    DB_SESSION: ClassVar[sessionmaker] = SessionLocal

    EXPENSE_VALIDATOR: ClassVar[
        Type[StateCampaignFinanceConfigs.EXPENSE_VALIDATOR]
    ] = TECExpense
    EXPENSE_MODEL: ClassVar[Type[DeclarativeBase]]  = TECExpenseRecord
    EXPENSE_FILE_PREFIX: ClassVar[str] = "expend"

    CONTRIBUTION_VALIDATOR: ClassVar[
        Type[StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR]
    ] = TECContribution
    CONTRIBUTION_MODEL: ClassVar[Type[DeclarativeBase]]  = TECContributionRecord
    CONTRIBUTION_FILE_PREFIX: ClassVar[str] = "contribs"

    FILERS_VALIDATOR: ClassVar[
        Type[StateCampaignFinanceConfigs.FILERS_VALIDATOR]
    ] = TECFiler
    FILERS_MODEL: ClassVar[Type[DeclarativeBase]]  = TECFilerRecord
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
class TECCategories(StateCategories):
    expenses: ClassVar[Iterator[List]] = None
    contributions: ClassVar[Iterator[List]] = None
    filers: ClassVar[Iterator[List]] = None
    __logger: Logger = field(init=False)

    def __repr__(self):
        return f"TECFileCategories()"

    def __post_init__(self):
        TECCategories.__logger = Logger(self.__class__.__name__)

    @classmethod
    def read_file(cls, file: Path) -> Generator[Dict[str, Generator], None, None]:
        yield from FileReader.read_file(file)

        # yield {records[x].values() for x in records}

    @classmethod
    def _generate_list(
            cls,
            pfx: [
                StateCampaignFinanceConfigs.EXPENSE_FILE_PREFIX,
                StateCampaignFinanceConfigs.CONTRIBUTION_FILE_PREFIX,
                StateCampaignFinanceConfigs.FILERS_FILE_PREFIX],
            fldr: StateCampaignFinanceConfigs.FOLDER = TexasConfigs.FOLDER
    ) -> List:
        _files = []
        for file in fldr.glob("*.csv"):
            cls.__logger.debug(f"Checking if {file.name} starts with {pfx}...")
            if file.stem.startswith(pfx):
                _files.append(file)
            else:
                pass
        cls.__logger.debug(f"Appended {pfx} files to a sorted list...")
        # return {k: v for file in f for k, v in file.items()}
        return sorted(_files)

    @classmethod
    def generate(
        cls,
        record_kind: str = None,
        _config: StateCampaignFinanceConfigs = TexasConfigs,
    ):
        def extract_records(record_type: List) -> Generator[List, None, None]:
            # cls.__logger.debug(f"Initialized extract_records method")
            for file in record_type:
                yield cls.read_file(file)

        _expenses, _contributions, _filers = [
            TECCategories._generate_list(x)
            for x in [
                _config.EXPENSE_FILE_PREFIX,
                _config.CONTRIBUTION_FILE_PREFIX,
                _config.FILERS_FILE_PREFIX,
            ]
        ]
        cls.__logger.debug(
            f"Generated lists of files for expenses, contributions, and filers..."
        )

        # if record_kind == "filers":
        #     cls.filers = extract_records(_filers)
        #     return cls.filers
        #
        # if record_kind == "expenses":
        #     cls.expenses = extract_records(_expenses)
        #     return cls.expenses
        #
        # elif record_kind == "contributions":
        #     cls.contributions = extract_records(_contributions)
        #     return cls.contributions
        #
        # else:
        cls.expenses = extract_records(_expenses)
        cls.contributions = extract_records(_contributions)
        cls.filers = extract_records(_filers)

        return cls

    @classmethod
    def read(cls, records):
        return {k: v for record in tqdm(records, desc=f"Loading {records.__class__.__name__}") for k, v in record}


@dataclass
class TECValidator(StateFileValidation):
    passed: Iterator[List] = field(init=False)
    failed: Iterator[Dict[str, ValidationError]] = field(init=False)
    errors: pd.DataFrame = field(init=False)
    __logger: Logger = field(init=False)

    def __post_init__(self):
        self.__logger = Logger(self.__class__.__name__)

    def validate(
        self,
        records: Iterator[List],
        validator: StateCampaignFinanceConfigs.VALIDATOR,
        load_to_db: bool = False,
    ):
        self.__logger.debug(f"Called validate()")
        _passed, _failed = [], []
        _pass_count, _fail_count = 0, 0
        for each_file in tqdm(iter(records), desc=f"Validating file"):
            for each_record in each_file:
                try:
                    r = validator(**each_record[1])
                    _passed.append(r)
                    _pass_count += 1
                except ValidationError as e:
                    each_record[1]["error"] = e.errors()
                    _failed.append({"error": e, "record": each_record[1]})
                    _fail_count += 1

                if len(_passed) == 16000:
                    if load_to_db:
                        _db = PostgresLoader(TexasConfigs.DB_BASE)
                        _db.add_to_db(_passed, validator, TexasConfigs)
                    # elif db_insert_method.lower() == "insert":
                    #     _db.insert_to_db(_passed, validator, TexasConfigs)
                        _passed = []
                    sys.stdout.write(
                        f"\rPassed: {_pass_count:,} | Failed: {_fail_count:,}"
                    )
                    sys.stdout.flush()
        self.__logger.debug(
            f"Returning {_pass_count:,} passed and {_fail_count:,} failed records..."
        )
        self.passed, self.failed = iter(_passed), iter(_failed)
        return self.passed, self.failed

    def error_report(self):
        _errors = [
            {
                "type": f["error"].errors()[0]["type"],
                "msg": f["error"].errors()[0]["msg"],
            }
            for f in self.failed
        ]
        error_df = (
            pd.DataFrame.from_dict(
                Counter([str(e) for e in _errors]), orient="index", columns=["count"]
            )
            .rename_axis("error")
            .reset_index()
        )
        error_df.loc["Total"] = error_df["count"].sum()
        self.errors = error_df
        self.__logger.debug(f"Returning error report...")
        return self.errors


# @dataclass
# class TECFileReader(finance.CampaignFinanceFileReader):
#     """
#     TECFileReader
#     =============
#     This class is used to read TEC campaign finance files.
#     It is used to read the files from the TEC website and
#     validate the data in the files.
#     """
#     file_list: TECFileCategories.expenses or TECFileCategories.contributions
#     records: Dict = field(init=False)
#     # FILE_VALIDATOR: ClassVar[Type[TECValidator]] = TexasConfigs.VALIDATOR
#
#     def __repr__(self):
#         return f"{self.file_list}"
#
#     def __post_init__(self):
#         self.path: Path = field(default_factory=Path)
#
#     def read_files(self, category: str):
#         _file_dicts = {}
#         for x in tqdm(self.file_list, desc=f'Reading {category.lower()} files'):
#             with open(x, 'r') as file:
#                 for k, v in enumerate(csv.DictReader(file)):
#                     _file_dicts.update({k: v})
#
#         self.records = _file_dicts
#         return self.records
#
# def validate(self):
#     RecordValidation = namedtuple('RecordValidation', ['passed', 'failed'])
#     valid_records, errors = [], []
#     for record in self.records:
#         for v in tqdm(record, desc=f'Validating {self.file.name} records'):
#             try:
#                 valid_records.append(TECFileReader.FILE_VALIDATOR(**v).dict())
#                 # valid_records.append(valid)
#             except ValidationError as e:
#                 errors.append(e.json())
#                 # errors.append(error)
#     yield RecordValidation(valid_records, errors)


# @dataclass
# class TECFolderReader:
#     """
#     TECFolderReader
#     ===============
#     This class is used to pull Campaign finance files from the TEC website.
#     It is used to download the files from the TEC website and
#     validate the data in the files.
#     """
#     folder: TECFileCategories = field(default_factory=TECFileCategories)
#
#     @property
#     def expenses(self):
#         return TECFileReader(self.folder.expenses).read_files('expense')
#
#     @property
#     def contributions(self):
#         return TECFileReader(self.folder.contributions).read_files('contribution')


# @dataclass
# class TECValidator:
#     """
#     TECReportLoader
#     ===============
#     This class is used to load the TEC campaign finance data into a pandas DataFrame.
#     """
#     records: TECFolderReader.expenses or TECFolderReader.contributions
#     passed: List = field(init=False)
#     failed: List = field(init=False)
#     to_sql: List = field(init=False)
#     df: pd.DataFrame = field(init=False)
#
#     def validate(self):
#         self.passed, self.failed = [], []
#         for record in tqdm(self.records.values(), desc=f'Validating records'):
#             try:
#                 r = TexasConfigs.FILE_VALIDATOR(**record)
#                 self.passed.append(r)
#             except ValidationError as e:
#                 self.failed.append(e.json())
#
#         return self.passed, self.failed

# def to_dataframe(self, list_of_records: list) -> pd.DataFrame:
#     _data = pd.DataFrame(list_of_records)
#     _data['payeeNameOrganization'] = _data['payeeNameOrganization'].str.strip().str.upper()
#     _data['filerName'] = _data['filerName'].str.strip().str.upper()
#     _data['receivedDt'] = pd.to_datetime(_data['receivedDt'])
#     _data['expendDt'] = pd.to_datetime(_data['expendDt'])
#     _data['contributionDt'] = pd.to_datetime(_data['contributionDt'])
#     self.df = _data
#     return self.df

# def load_records(self):
#     self.records = [record for file in TECReportLoader.load(self.file.records) for record in file]
#     return self.records

# def load_validated_records(self):
#     _records = TECReportLoader.load(self.file.validated)
#     self.passed = [record for file in _records for record in file.passed]
#     self.failed = [record for file in _records for record in file.failed]
#     print(f'Passed: {len(list(self.passed)):,}')
#     print(f'Failed: {len(list(self.failed)):,}')
#     return self

# def create_record_models(self):
#     if not self.passed:
#         self.load_validated_records()
#
#     self.to_sql = [TexasConfigs.SQL_MODEL(**record) for record in self.passed]
