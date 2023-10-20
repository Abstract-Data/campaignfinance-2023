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
from typing import Generator, Type
from collections import namedtuple
import pandas as pd
from pydantic import ValidationError
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
    FOLDER: ClassVar[StateCampaignFinanceConfigs.FOLDER] = Path.cwd() / "tmp"

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
class TECValidator(StateFileValidation):
    __logger: Logger = field(init=False)
    _db: ClassVar[PostgresLoader] = field(init=False)

    def __post_init__(self):
        TECValidator.__logger = Logger(self.__class__.__name__)
        TECValidator._db = PostgresLoader(Base).build(engine=engine)

    @classmethod
    def validate_file(cls, records) -> FileValidationResults:
        _passed_file, _failed_file = [], []
        _passed_count_file, _failed_count_file = 0, 0
        for _each_record in tqdm(records.records.values(), desc=f"VALIDATING: {records.file.name} records"):
            try:
                if _each_record['file_origin'].startswith("filer"):
                    _record = TexasConfigs.FILERS_VALIDATOR(**_each_record)
                elif _each_record['file_origin'].startswith("expend"):
                    _record = TexasConfigs.EXPENSE_VALIDATOR(**_each_record)
                elif _each_record['file_origin'].startswith("contrib"):
                    _record = TexasConfigs.CONTRIBUTION_VALIDATOR(**_each_record)
                else:
                    raise ValueError(f"Invalid file name: {_each_record['file_origin']}")
                _passed_file.append(dict(_record))
                _passed_count_file += 1
            except ValidationError as e:
                _each_record["error"] = e.errors()
                _failed_file.append({"error": e, "record": _each_record})
                _failed_count_file += 1

        if _passed_file[0]['file_origin'].startswith(TexasConfigs.FILERS_FILE_PREFIX.upper()):
            filer_ids = list(set([x['filerIdent'] for x in _passed_file]))
            merged_filers = {}
            for filer in _passed_file:
                for _id in filer_ids:
                    org_names = []
                    if filer['filerIdent'] == _id:
                        _org = filer['filerNameOrganization']
                        _name = filer['filerName']
                        if _org is not None not in org_names:
                            org_names.append(_org)
                        if _name is not None not in org_names:
                            org_names.append(_name)
                        filer['org_names'] = ", ".join(org_names)
                        merged_filers[_id] = filer
            _passed_file = list(merged_filers.values())

        return FileValidationResults(_passed_file, _failed_file, _passed_count_file, _failed_count_file)

    @classmethod
    def validate_category(cls, category, to_db: bool = False):
        _validation_report = []
        passed, failed = [], []
        _passed_count_total, _failed_count_total = 0, 0
        for file in category:
            file.load_records()
            _results = cls.validate_file(records=file)

            passed.extend(iter(_results.passed))
            failed.append(_results.failed)
            _passed_count_total += _results.passed_count
            _failed_count_total += _results.failed_count
            _file_report = {
                'filename': file.file.name,
                'passed': _results.passed_count,
                'failed': _results.failed_count,
                'total': _results.passed_count + _results.failed_count,
                'file_pass_pct': round(_results.passed_count/(_results.passed_count + _results.failed_count), 4),
                'file_fail_pct': round(_results.failed_count/(_results.passed_count + _results.failed_count), 4),
                'total_pass_pct': round(_passed_count_total/(_passed_count_total + _failed_count_total), 4),
                'total_fail_pct': round(_failed_count_total/(_passed_count_total + _failed_count_total), 4)
            }
            _validation_report.append(_file_report)

            cls.__logger.info(f"""  \
            
            === VALIDATION REPORT FOR {_file_report['filename'].upper()} ===
            Passed: {_file_report['passed']} \
[File: {_file_report['file_pass_pct']:.2%}, Total: {_file_report['total_pass_pct']:.2%}]
            Failed: {_file_report['failed']} \
[File: {_file_report['file_fail_pct']:.2%}, Total: {_file_report['total_fail_pct']:.2%}]""")

            if to_db:
                cls.to_database(file=_results.passed)

        validation_report = pd.DataFrame.from_records(_validation_report)
        validation_report.loc['total'] = validation_report.sum()
        print("=== VALIDATION REPORT ===")
        print(validation_report.to_markdown())
        return iter(passed), failed, validation_report

    @classmethod
    def to_database(cls, file: List[Dict[str, str]]) -> None:

        if file[0]['file_origin'].startswith(TexasConfigs.FILERS_FILE_PREFIX.upper()):
            _model = TexasConfigs.FILERS_MODEL
        elif file[0]['file_origin'].startswith(TexasConfigs.EXPENSE_FILE_PREFIX.upper()):
            _model = TexasConfigs.EXPENSE_MODEL
        elif file[0]['file_origin'].startswith(TexasConfigs.CONTRIBUTION_FILE_PREFIX.upper()):
            _model = TexasConfigs.CONTRIBUTION_MODEL
        else:
            raise ValueError(f"Invalid file name: {file[0]['file_origin']}")

        # cls.db.create(values=_records, table=_model)
        cls._db.load(records=file, session=SessionLocal, table=_model)
        logger.info(f"Loaded {file[0]['file_origin']} to database...")


@dataclass
class TECFile:
    file: Path
    validation: StateCampaignFinanceConfigs.VALIDATOR = field(
        default_factory=TECValidator
    )
    records: Generator[Dict, None, None] | Dict = None

    def __repr__(self):
        return f"TECFile({self.file.name})"

    def read(self) -> Generator[Dict, None, None]:
        self.records = FileReader.read_file(self.file)
        return self.records

    def load_records(self) -> Dict:
        if not self.records:
            self.read()
        self.records = dict(x for x in tqdm(self.records, desc=f"LOADING: {self.file.name} records")
        )
        return self.records


@dataclass
class TECCategories(StateCategories):
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
        yield (file.read() for file in category)

    @classmethod
    def load(cls, read):
        return [x.load_records() for x in read]

    @classmethod
    def validate_category(cls, category: Generator[TECFile, None, None], to_db: bool = False):
        return TECValidator.validate_category(category=category, to_db=to_db)
