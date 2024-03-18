
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
import states.texas.validators as validators
from states.texas.database import engine, Session

# from db_loaders.postgres_loader import PostgresLoader
# from states.texas.database import (
#     SQLModel,
#     create_engine,
#     engine,
#     SessionLocal,
#     Session
# )
# import itertools
# import states.texas.updated_validators as validators
# import states.texas.updated_models as models

logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")

# SQLModels = Generator[SQLModel, None, None]

FileValidationResults = namedtuple(
    'FileValidationResults', ['passed', 'failed', 'passed_count', 'failed_count'])


def download_base_config(binder):
    binder.bind(StateCampaignFinanceConfigs, TexasConfigs)


def category_base_config(binder):
    binder.bind(StateCampaignFinanceConfigs, TexasConfigs)
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


def merge_filer_names(records: Generator[Dict, None, None]) -> Generator[Dict, None, None]:
    # Create a dictionary of filerIdent's and filerNames
    org_names = defaultdict(set)
    records_dict = {}  # Use a dictionary to store records

    for record in records:
        if record['file_origin'].startswith("filer"):
            org_names[record['filerIdent']].add(record['filerName'])
            records_dict[record['filerIdent']] = record  # Store record in dictionary

    # Convert set of names to comma-separated string
    for _id in org_names:
        org_names[_id] = ', '.join(org_names[_id])

    # Add filerNames to each record and yield the result
    for _id, record in records_dict.items():
        if _id in org_names:
            record['org_names'] = org_names[_id]
    _unique_filers = {x['filerIdent']: x for x in records_dict.values()}
    return (x for x in _unique_filers.values())


class TexasConfigs(StateCampaignFinanceConfigs):
    STATE: ClassVar[str] = "Texas"
    STATE_ABBREVIATION: ClassVar[str] = "TX"
    TEMP_FOLDER: ClassVar[StateCampaignFinanceConfigs.TEMP_FOLDER] = Path.cwd().parent / "tmp" / "texas"
    TEMP_FILENAME: ClassVar[StateCampaignFinanceConfigs.TEMP_FILENAME] = (
            Path.cwd().parent / "tmp" / "texas" / "TEC_CF_CSV.zip")

    # EXPENSE_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.EXPENSE_VALIDATOR]
    # ] = TECExpense
    # EXPENSE_MODEL: ClassVar[Type[SQLModel]] = None
    EXPENSE_FILE_PREFIX: ClassVar[str] = "expend"

    # CONTRIBUTION_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR]
    # ] = TECContribution
    # CONTRIBUTION_MODEL: ClassVar[Type[SQLModel]] = None
    CONTRIBUTION_FILE_PREFIX: ClassVar[str] = "contribs"

    # FILERS_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.FILERS_VALIDATOR]
    # ] = TECFiler
    # FILERS_MODEL: ClassVar[Type[SQLModel]] = None
    FILERS_FILE_PREFIX: ClassVar[str] = "filer"

    REPORTS_FILE_PREFIX: ClassVar[str] = "finals"

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
    _configs: ClassVar[StateCampaignFinanceConfigs]
    _folder: Path = TexasConfigs.TEMP_FOLDER
    __logger: Logger = field(init=False)

    def init(self):
        inject.configure(download_base_config)

    @property
    def folder(self) -> StateCampaignFinanceConfigs.TEMP_FOLDER:
        return self._folder

    @folder.setter
    def folder(self, value: Path) -> None:
        self._folder = value

    @property
    def _tmp(self) -> Path:
        return self.folder if self.folder else StateCampaignFinanceConfigs.TEMP_FOLDER

    @classmethod
    def download_file_with_requests(cls, config: StateCampaignFinanceConfigs) -> None:
        # download files
        with requests.get(config.ZIPFILE_URL, stream=True) as resp:
            # check header to get content length, in bytes
            total_length = int(resp.headers.get("Content-Length"))

            # Chunk download of zip file and write to temp folder
            with open(config.TEMP_FILENAME, "wb") as f:
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

    @inject.autoparams()
    def download(
        self,
        config: StateCampaignFinanceConfigs,
        read_from_temp: bool = False,
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
        return None

    def read(self):
        self.folder = self._tmp

    def __post_init__(self):
        TECFileDownloader.__logger = Logger(self.__class__.__name__)
        self.init()
        self.check_if_folder_exists()


@dataclass
class TECCategory:
    category: str
    records: Generator[Dict, None, None] = field(init=False)
    validation: StateFileValidation = field(init=False)
    validator: Type[validators.TECSettings] = field(init=False)
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

    def get_validators(self) -> Type[validators.TECSettings]:
        if self.category == "expenses":
            self.validator = validators.TECExpense
        elif self.category == "contributions":
            self.validator = validators.TECContribution
        elif self.category == "filers":
            self.validator = validators.TECFiler
        elif self.category == 'reports':
            self.validator = validators.TECFinalReport
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
