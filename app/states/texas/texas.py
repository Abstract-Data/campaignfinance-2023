from pathlib import Path
from typing import ClassVar, Dict, List, Any, Generator
from dataclasses import field, dataclass
from tqdm import tqdm
from zipfile import ZipFile
import requests
import os
import sys
import ssl
import urllib.request
import itertools
from typing import Generator, Tuple, Type, Iterator, Optional, Any, Protocol
from collections import namedtuple, defaultdict
from pydantic import ValidationError, BaseModel
import funcs
from logger import Logger
from abcs import (
    StateCampaignFinanceConfigs,
    FileDownloader,
)
# from db_loaders.postgres_loader import PostgresLoader
from states.texas.database import (
    sessionmaker,
    create_engine,
    Base,
    engine,
    SessionLocal
)
import itertools
import states.texas.updated_validators as validators
import states.texas.updated_models as models


logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")

SQLModels = Generator[Base, None, None]

FileValidationResults = namedtuple(
    'FileValidationResults', ['passed', 'failed', 'passed_count', 'failed_count'])


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


def validation_result_generator(
        gen: Iterator[Tuple],
        name: str) -> Generator[BaseModel | Dict, None, None]:
    return (record for status, record in gen if status == name)


class TexasConfigs(StateCampaignFinanceConfigs):
    STATE: ClassVar[str] = "Texas"
    STATE_ABBREVIATION: ClassVar[str] = "TX"
    TEMP_FOLDER: ClassVar[StateCampaignFinanceConfigs.TEMP_FOLDER] = Path.cwd().parent / "tmp"
    TEMP_FILENAME: ClassVar[StateCampaignFinanceConfigs.TEMP_FILENAME] = Path.cwd().parent / "tmp" / "TEC_CF_CSV.zip"

    DB_BASE: ClassVar[Type[Base]] = Base
    DB_ENGINE: ClassVar[create_engine] = engine
    DB_SESSION: ClassVar[sessionmaker] = SessionLocal

    # EXPENSE_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.EXPENSE_VALIDATOR]
    # ] = TECExpense
    EXPENSE_MODEL: ClassVar[Type[Base]] = None
    EXPENSE_FILE_PREFIX: ClassVar[str] = "expend"

    # CONTRIBUTION_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR]
    # ] = TECContribution
    CONTRIBUTION_MODEL: ClassVar[Type[Base]] = None
    CONTRIBUTION_FILE_PREFIX: ClassVar[str] = "contribs"

    # FILERS_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.FILERS_VALIDATOR]
    # ] = TECFiler
    FILERS_MODEL: ClassVar[Type[Base]] = None
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
class TECFileDownload:
    _configs: StateCampaignFinanceConfigs = TexasConfigs
    _folder: StateCampaignFinanceConfigs.TEMP_FOLDER = TexasConfigs.TEMP_FOLDER
    __logger: Logger = field(init=False)

    @property
    def folder(self) -> StateCampaignFinanceConfigs.TEMP_FOLDER:
        return self._folder

    @folder.setter
    def folder(self, value: Path) -> None:
        self._folder = value

    @property
    def _tmp(self) -> Path:
        return self.folder if self.folder else self.folder

    def download_file_with_requests(self) -> None:
        return ...


@dataclass
class TECFileDownloader(FileDownloader):
    _configs: ClassVar[StateCampaignFinanceConfigs] = TexasConfigs
    _folder: Path = TexasConfigs.TEMP_FOLDER
    __logger: Logger = field(init=False)

    @property
    def folder(self) -> Path:
        return self._folder

    @folder.setter
    def folder(self, value: Path) -> None:
        self._folder = value

    @property
    def _tmp(self) -> Path:
        return self.folder if self.folder else self._configs.TEMP_FOLDER

    @classmethod
    def download_file_with_requests(cls) -> None:
        # download files
        with requests.get(cls._configs.ZIPFILE_URL, stream=True) as resp:
            # check header to get content length, in bytes
            total_length = int(resp.headers.get("Content-Length"))

            # Chunk download of zip file and write to temp folder
            with open(cls._configs.TEMP_FILENAME, "wb") as f:
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

    def download(
        self,
        read_from_temp: bool = False,
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
        return None

    def read(self):
        self.folder = self._tmp

    def __post_init__(self):
        TECFileDownloader.__logger = Logger(self.__class__.__name__)
        self.check_if_folder_exists()

#
# @dataclass
# class TECFileValidator:
#     """ Validator class for TEC Files"""
#     __logger: Logger = field(init=False)
#     _db: ClassVar[PostgresLoader] = field(init=False)
#
#     def __post_init__(self):
#         TECFileValidator.__logger = Logger(self.__class__.__name__)
#
#     @staticmethod
#     def validate_generator(file: 'TECFile') -> Generator[Dict, None, None]:
#         for x in file.records:
#             yield file.validator(**dict(x))
#
#     @staticmethod
#     def validate(file):
#         _passed, _failed = [], []
#         for x in tqdm(TECFileValidator.validate_generator(file.read()), desc=f"VALIDATING: {file.file.name} records"):
#             try:
#                 _passed.append(x)
#             except ValidationError as e:
#                 x['error'] = e.errors()
#                 _failed.append(x)
#         return _passed, _failed


@dataclass
class CategoryValidator(Protocol):

    def validate(self, records):
        return self

    def run_validation(self, records):
        return self


@dataclass
class TECFilerValidation(CategoryValidator):
    filer_passed: funcs.PassedRecords = field(init=False)
    filer_failed: funcs.FailedRecords = field(init=False)
    filer_name_passed: funcs.PassedRecords = field(init=False)
    filer_name_failed: funcs.FailedRecords = field(init=False)
    treasurer_passed: funcs.PassedRecords = field(init=False)
    treasurer_failed: funcs.FailedRecords = field(init=False)
    assistant_treasurer_passed: funcs.PassedRecords = field(init=False)
    assistant_treasurer_failed: funcs.FailedRecords = field(init=False)
    chair_passed: funcs.PassedRecords = field(init=False)
    chair_failed: funcs.FailedRecords = field(init=False)

    def process_records(self, _records):
        for record in tqdm(_records, desc="Validating filer records", unit=" records"):
            try:
                _filer = validators.Filer(**record)
                yield 'filer_passed', _filer
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'filer_failed', record

            try:
                _treasurer = validators.Treasurer(**record)
                yield 'treasurer_passed', _treasurer
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'treasurer_failed', record

            try:
                _assistant_treasurer = validators.AssistantTreasurer(**record)
                yield 'assistant_treasurer_passed', _assistant_treasurer
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'assistant_treasurer_failed', record

            try:
                _chair = validators.Chair(**record)
                yield 'chair_passed', _chair
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'chair_failed', record

            try:
                _filer_name = validators.FilerName(**record,
                                                   treasurerKey=validators.Treasurer(**record).treasurerNameKey,
                                                   asstTreasurerKey=validators.AssistantTreasurer(**record).assistantTreasurerNameKey,
                                                   chairKey=validators.Chair(**record).chairNameKey,
                                                   contributionKey=validators.Treasurer(**record).filerIdent)
                yield 'filer_name_passed', _filer_name
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'filer_name_failed', record

    def run_validation(self, records):
        filer_passed, filer_failed,  \
            filer_name_passed, filer_name_failed,  \
            treasurer_passed, treasurer_failed,  \
            assistant_treasurer_passed, assistant_treasurer_failed,  \
            chair_passed, chair_failed = itertools.tee(self.process_records(records), 10)
        self.filer_passed = validation_result_generator(filer_passed, 'filer_passed')
        self.filer_failed = validation_result_generator(filer_failed, 'filer_failed')
        self.filer_name_passed = validation_result_generator(filer_name_passed, 'filer_name_passed')
        self.filer_name_failed = validation_result_generator(filer_name_failed, 'filer_name_failed')
        self.treasurer_passed = validation_result_generator(treasurer_passed, 'treasurer_passed')
        self.treasurer_failed = validation_result_generator(treasurer_failed, 'treasurer_failed')
        self.assistant_treasurer_passed = validation_result_generator(
            assistant_treasurer_passed, 'assistant_treasurer_passed')
        self.chair_passed = validation_result_generator(chair_passed, 'chair_passed')
        self.chair_failed = validation_result_generator(chair_failed, 'chair_failed')
        return self


@dataclass
class TECExpenseValidators(CategoryValidator):
    payee_passed: funcs.PassedRecords = None
    payee_failed: funcs.FailedRecords = None
    expenditure_passed: funcs.PassedRecords = None
    expenditure_failed: funcs.FailedRecords = None

    def process_records(self, records):
        for record in tqdm(records, desc="Validating expense records", unit=" records"):
            try:
                _payee = validators.Payee(**record)
                yield 'payee_passed', _payee
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'payee_failed', record

            try:
                _expenditure = validators.Expenditure(**record, payeeId=_payee.payeeId)
                yield 'expenditure_passed', _expenditure
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'expenditure_failed', record
        return self

    def run_validation(self, records):
        payee_passed, payee_failed,  \
            expenditure_passed, expenditure_failed = itertools.tee(self.process_records(records), 4)
        self.payee_passed = validation_result_generator(payee_passed, 'payee_passed')
        self.payee_failed = validation_result_generator(payee_failed, 'payee_failed')
        self.expenditure_passed = validation_result_generator(expenditure_passed, 'expenditure_passed')
        self.expenditure_failed = validation_result_generator(expenditure_failed, 'expenditure_failed')
        return self


@dataclass
class TECContributionValidators(CategoryValidator):
    contributor_details_passed: funcs.PassedRecords = field(init=False)
    contributor_details_failed: funcs.FailedRecords = field(init=False)
    contribution_data_passed: funcs.PassedRecords = field(init=False)
    contribution_data_failed: funcs.FailedRecords = field(init=False)
    _logger: Logger = field(init=False)

    @property
    def logger(self):
        self._logger = Logger(self.__class__.__name__)
        return self._logger

    def process_records(self, records):
        self.logger.info(f"Processing records...")
        for record in records:
            try:
                _contributor_details = validators.ContributorDetails(**record)
                _contribution_data = validators.ContributionData(
                    **record,
                    contributorNameAddressKey=_contributor_details.contributorNameAddressKey,
                    contributorOrgKey=_contributor_details.contributorOrgKey)
                yield 'contributor_details_passed', _contributor_details
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'contributor_details_failed', record

            try:
                _contribution_data = validators.ContributionData(**record)
                yield 'contribution_data_passed', _contribution_data
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'contribution_data_failed', record
        self.logger.info(f"Finished processing records...")
        return self

    def run_validation(self, records):
        contributor_details_passed, contributor_details_failed, \
            contribution_data_passed, contribution_data_failed = itertools.tee(self.process_records(records), 4)
        self.contributor_details_passed = validation_result_generator(contributor_details_passed, 'contributor_details_passed')
        self.contributor_details_failed = validation_result_generator(contributor_details_failed, 'contributor_details_failed')
        self.contribution_data_passed = validation_result_generator(contribution_data_passed, 'contribution_data_passed')
        self.contribution_data_failed = validation_result_generator(contribution_data_failed, 'contribution_data_failed')
        return self


class TECReportsValidator(CategoryValidator):
    reports_passed: funcs.PassedRecords = None
    reports_failed: funcs.FailedRecords = None

    def process_records(self, records):
        for record in tqdm(records, desc="Validating contribution records", unit=" records"):
            try:
                _report_details = validators.FinalReport(**record)
                yield 'report_details_passed', _report_details
            except ValidationError as e:
                record['error'] = e.errors()
                yield 'report_details_failed', record
        return self


@dataclass
class TECCategory:
    category: str
    records: Generator[Dict, None, None] = field(init=False)
    validators: CategoryValidator = field(init=False)
    config: StateCampaignFinanceConfigs = TexasConfigs
    _files: Optional[Generator[Path, Any, None]] = None
    __logger: Logger = field(init=False)

    def __repr__(self):
        return f"TECFileCategories({self.category})"

    def __post_init__(self):
        self.create_file_list()
        self.get_validators()

    @property
    def logger(self):
        self.__logger = Logger(self.__class__.__name__)
        return self.__logger

    def create_file_list(self):
        if self.category == "expenses":
            self._files = (
                x for x in generate_file_list(TexasConfigs.TEMP_FOLDER)
                if x.name.startswith(self.config.EXPENSE_FILE_PREFIX))
        elif self.category == "contributions":
            self._files = (
                x for x in generate_file_list(TexasConfigs.TEMP_FOLDER)
                if x.name.startswith(self.config.CONTRIBUTION_FILE_PREFIX))
        elif self.category == "filers":
            self._files = (
                x for x in generate_file_list(TexasConfigs.TEMP_FOLDER)
                if x.name.startswith(self.config.FILERS_FILE_PREFIX))
        elif self.category == "reports":
            self._files = (
                x for x in generate_file_list(TexasConfigs.TEMP_FOLDER)
                if x.name.startswith(self.config.REPORTS_FILE_PREFIX))
        return self._files

    def get_validators(self):
        if self.category == "expenses":
            self.validators = TECExpenseValidators()
        elif self.category == "contributions":
            self.validators = TECContributionValidators()
        elif self.category == "filers":
            self.validators = TECFilerValidation()
        elif self.category == 'reports':
            self.validators = TECReportsValidator()
        else:
            raise ValueError(f"Invalid category: {self.category}")
        return self.validators

    def read(self) -> Generator[Dict, None, None]:
        self.records = (record for file in list(self._files) for record in funcs.FileReader.read_file(file))
        # if self.category == "filers":
        #     records = merge_filer_names(records)
        return self.records

    def load(self) -> List[Dict]:
        return list(x for x in self.read())

    def validate(self,
                 records: Generator[Dict, None, None] = None,
                 validator: Type[BaseModel] = None
                 ) -> 'CategoryValidator':
        return self.validators.run_validation(records=self.read())

    # def create_models(self, records: funcs.PassedRecords = None) -> SQLModels:
    #     if not records:
    #         self.models = (self.sql_model(**dict(x)) for x in self.passed)
    #     self.logger.info(f"Created {self.category} model generator...")
    #     return self.models
    #
    # def add_to_database(self, models: SQLModels = None) -> None:
    #     _db = PostgresLoader(Base)
    #     _db.build(engine=engine)
    #     if not models:
    #         models = self.create_models()
    #     _db.load(
    #         records=models,
    #         session=SessionLocal,
    #         table=self.sql_model
    #     )
    #
    # def update_database(self, models: Generator[BaseModel, None, None] = None) -> None:
    #     _db = PostgresLoader(Base)
    #     _db.build(engine=engine)
    #     if not models:
    #         models = (dict(x) for x in self.passed)
    #         _db = PostgresLoader(Base)
    #         _db.update(
    #             records=models,
    #             session=SessionLocal,
    #             table=self.sql_model,
    #             primary_key=self.primary_key
    #         )

        # _db = PostgresLoader(Base)
        # errors = None
        # for file in tqdm(category, desc=f"UPDATING DATABASE"):
        #     file.create_models()
        #     print([x for x in file._models])
            # errors = _db.update(records=_models, session=SessionLocal, table=file._sql_model)
        # return _models

