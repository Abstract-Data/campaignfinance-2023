from __future__ import annotations
from pathlib import Path
from typing import (
    Dict,
    Iterable,
    Type,
    Generator,
    Callable,
    Optional,
    Any,
    Iterator, Tuple, List
)
import contextlib
import inject
from collections import defaultdict
from dataclasses import field, dataclass

from sqlmodel import Session, SQLModel
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn

import funcs
from logger import Logger
from abcs import StateCampaignFinanceConfigClass
from abcs.abc_validation import StateFileValidationClass

from funcs.validation import PassedFailedRecordList
from states.texas.texas_configs import TexasConfigs
from states.texas.texas_database import engine
import states.texas.validators as validators
from funcs.db_loader import DBLoader

logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")

# SQLModels = Generator[SQLModel, None, None]
CategoryFileList = List[Path]
TexasValidatorType = Type[validators.TECSettings]
FileRecords = Generator[Dict, None, None]



def category_base_config(binder):
    binder.bind(StateCampaignFinanceConfigClass, TexasConfigs)
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


def generate_file_list(folder: Path) -> tuple[list[Path], Callable[[str], CategoryFileList]]:
    _folder = list(folder.glob("*.csv"))

    def contains_prefix(prefix: str) -> CategoryFileList:
        return [file for file in _folder if file.stem.startswith(prefix)]

    return _folder, contains_prefix


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
class TECCategory:
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
        self.validation = funcs.StateFileValidation(validator_used=self.get_validator())

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
                self._files = contains_prefix(config.EXPENSE_FILE_PREFIX)
            case "contributions":
                self._files = contains_prefix(config.CONTRIBUTION_FILE_PREFIX)
            case "filers":
                self._files = contains_prefix(config.FILERS_FILE_PREFIX)
            case "reports":
                self._files = contains_prefix(config.REPORTS_FILE_PREFIX)
            case "travel":
                self._files = contains_prefix(config.TRAVEL_FILE_PREFIX)
            case "candidates":
                self._files = contains_prefix(config.CANDIDATE_FILE_PREFIX)
            case "debts":
                self._files = contains_prefix(config.DEBT_FILE_PREFIX)
        return self._files

    def get_validator(self) -> TexasValidatorType:
        """
        Get the validator based on the category.
        :return: Type[validators.TECSettings]
        """
        match self.category:
            case "expenses":
                validator = validators.TECExpense
            case "contributions":
                validator = validators.TECContribution
            case "filers":
                validator = validators.TECFiler
            case 'reports':
                validator = validators.TECFinalReport
            case 'travel':
                validator = validators.TECTravelData
            case 'candidates':
                validator = validators.CandidateData
            case 'debts':
                validator = validators.DebtData
            case _:
                raise ValueError(f"Invalid category: {self.category}")
        return validator

    def read(self) -> Generator[Dict, None, None]:
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
                for record in file_reader.read_csv(_file):
                    file_reader.record_count += 1
                    yield record
                    progress.update(file_task, advance=1)

                progress.update(file_task, completed=file_reader.record_count)
                progress.advance(folder_task)
            progress.stop_task(folder_task)

        # for _file in tqdm(_file_list, desc=f"Reading {self.category.title()} Files", position=0, total=len(_file_list)):
        #     for record in file_reader.read_csv(_file):
        #         file_reader.record_count += 1
        #         yield record

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
    def load_to_db(self, records: Iterator[Type[TexasValidatorType]], **kwargs) -> None:
        _db_loader = DBLoader(engine=engine)
        if kwargs.get("create_table") is True:
            _db_loader.create_all()
        self.logger.info(f"Loading {self.category} records to database")
        _db_loader.add(
            self.read() if not records else records,
            record_type=self.validation.validator_used,
            add_limit=kwargs.get("limit", None)
        )
        return None
