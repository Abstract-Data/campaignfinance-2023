from __future__ import annotations
from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import field, dataclass
import contextlib
import inject
from typing import Generator, Type, Optional, Callable, Iterator, Iterable
from collections import namedtuple, defaultdict
from sqlmodel import SQLModel, Session
import funcs
from funcs.db_loader import DBLoader
from logger import Logger
from abcs import StateCampaignFinanceConfigClass
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn
from states.oklahoma.oklahoma_database import oklahoma_snowpark
import states.oklahoma.validators as validators
from functools import singledispatch

# TODO: Change File Prefix Configurations to Oklahoma
# TODO: Make sure file folder reads only CSVs in Oklahoma so it doesn't try to read Zip files

ENGINE = oklahoma_snowpark
logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")

# SQLModels = Generator[SQLModel, None, None]

CategoryFileList = List[Path]
OklahomaValidatorType = Type[validators.OklahomaSettings]
FileRecords = Generator[Dict, None, None]

FileValidationResults = namedtuple(
    'FileValidationResults', ['passed', 'failed', 'passed_count', 'failed_count'])


@contextlib.contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session(ENGINE)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@singledispatch
def generate_file_list(folder: Path):
    return list(sorted([file for file in folder.glob("*.csv")]))


@generate_file_list.register
def _(folder: Path) -> tuple[list[Path], Callable[[str], CategoryFileList]]:
    _folder = list(folder.glob("*.csv"))

    def contains_prefix(prefix: str) -> CategoryFileList:
        return [file for file in _folder if file.stem.endswith(prefix)]

    return _folder, contains_prefix


def category_base_config(binder):
    binder.bind(StateCampaignFinanceConfigClass, OklahomaConfigs)
    binder.bind_to_provider(Session, session_scope)


class OklahomaConfigs(StateCampaignFinanceConfigClass):
    STATE: ClassVar[str] = "Oklahoma"
    STATE_ABBREVIATION: ClassVar[str] = "OK"
    TEMP_FOLDER: ClassVar[StateCampaignFinanceConfigClass.TEMP_FOLDER] = Path(__file__).parents[3] / "tmp" / "oklahoma"
    # TEMP_FILENAME: ClassVar[StateCampaignFinanceConfigs.TEMP_FILENAME] = (
    #         Path.cwd().parent / "tmp" / "texas" / "TEC_CF_CSV.zip")

    # EXPENSE_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.EXPENSE_VALIDATOR]
    # ] = TECExpense
    # EXPENSE_MODEL: ClassVar[Type[SQLModel]] = None
    EXPENSE_FILE_SUFFIX: ClassVar[str] = "ExpenditureExtract"

    # CONTRIBUTION_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR]
    # ] = TECContribution
    # CONTRIBUTION_MODEL: ClassVar[Type[SQLModel]] = None
    CONTRIBUTION_FILE_SUFFIX: ClassVar[str] = "ContributionLoanExtract"

    # FILERS_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.FILERS_VALIDATOR]
    # ] = TECFiler
    # FILERS_MODEL: ClassVar[Type[SQLModel]] = None
    LOBBY_FILE_SUFFIX: ClassVar[str] = "LobbyistExpenditures"

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


def merge_filer_names(records: Generator[Dict, None, None]) -> FileRecords:
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


@dataclass
class OklahomaCategory:
    category: str
    records: Generator[Dict, None, None] = field(init=False)
    record_count: int = field(init=False, default=0)
    validation: funcs.StateFileValidation = field(init=False)
    config: StateCampaignFinanceConfigClass = field(init=False)
    _files: Optional[List[Path]] = None
    __logger: Logger = field(init=False)

    def __repr__(self):
        return f"TECFileCategories({self.category})"

    def init(self):
        inject.configure(category_base_config, clear=True)

    def __post_init__(self):
        self.init()
        self._create_file_list()
        self.validation = funcs.StateFileValidation(validator_to_use=self.get_validator())

    @property
    def logger(self):
        self.__logger = Logger(self.__class__.__name__)
        return self.__logger

    @inject.autoparams()
    def _create_file_list(self, config: StateCampaignFinanceConfigClass) -> CategoryFileList:
        """
        Create a list of files based on the category.
        :param config: StateCampaignFinanceConfigs object
        :return: Generator[Path, Any, None]
        """
        _, contains_prefix = generate_file_list(config.TEMP_FOLDER)
        match self.category:
            case "expenses":
                self._files = contains_prefix(config.EXPENSE_FILE_SUFFIX)
            case "contributions":
                self._files = contains_prefix(config.CONTRIBUTION_FILE_SUFFIX)
            case "lobby":
                self._files = contains_prefix(config.LOBBY_FILE_SUFFIX)
            case _:
                raise ValueError(f"Invalid category: {self.category}")
        return self._files

    def get_validator(self) -> OklahomaValidatorType:
        """
        Get the validator based on the category.
        :return: Type[validators.TECSettings]
        """
        match self.category:
            case "expenses":
                validator = validators.OklahomaExpenditure
            case "contributions":
                validator = validators.OklahomaContribution
            case "lobby":
                validator = validators.OklahomaLobbyistExpenditure
            case _:
                raise ValueError(f"Invalid category: {self.category}")
        return validator

    def read(self, replace_space=True, lowercase_headers=True) -> Generator[Dict, None, None]:
        """
        Read the files based on the category.
        :return: Generator[Dict, None, None]
        """
        pbar = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            MofNCompleteColumn()
        )
        file_reader = funcs.FileReader()
        file_list = list(self._files)
        folder_task = pbar.add_task(f"[cyan]Reading {self.category.title()} Files...", total=len(file_list))
        with pbar as progress:

            for _file in file_list:
                file_task = progress.add_task(f"[yellow]Reading {_file.stem}...", total=None)
                for record in file_reader.read_csv(
                        _file,
                        change_space_in_headers=replace_space,
                        lowercase_headers=lowercase_headers):
                    file_reader.record_count += 1
                    yield record
                    progress.update(file_task, advance=1)

                progress.update(file_task, completed=file_reader.record_count)
                progress.advance(folder_task)
            progress.stop_task(folder_task)

    def load(self) -> Iterable[Dict]:
        """
        Load the records locally as an iterator.
        :return: Iterable[Dict]
        """
        return iter(x for x in self.read())

    def validate(self,
                 records: FileRecords = None,
                 ) -> tuple[Iterator[SQLModel], Iterator[dict]]:
        """
        Validate the records based on the category.
        :param records: Generator[Dict, None, None]
        :return: StateFileValidation
        """
        if not records:
            records = self.read()

        self.validation.passed = self.validation.passed_records(records)
        self.validation.failed = self.validation.failed_records(records)
        return self.validation.passed, self.validation.failed

    @inject.autoparams()
    def write_to_csv(self, records, validation_status, config: StateCampaignFinanceConfigClass):
        funcs.write_records_to_csv_validation(
            records=records,
            folder_path=config.TEMP_FOLDER,
            record_type=self.category,
            validation_status=validation_status
        )
        return self

    @inject.autoparams()
    def load_to_db(self, records: Iterator[Type[OklahomaValidatorType]], **kwargs) -> None:
        _db_loader = DBLoader(engine=ENGINE)
        if kwargs.get("create_table") is True:
            _db_loader.create_all()
        self.logger.info(f"Loading {self.category} records to database")
        _db_loader.add(
            self.read() if not records else records,
            record_type=self.validation.validator_to_use,
            add_limit=kwargs.get("limit", None)
        )
        return None
