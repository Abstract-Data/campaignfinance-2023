from __future__ import annotations
import abc

import sqlmodel

from . import abc_validation as abc_validation
from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import dataclass, field
import csv
from tqdm import tqdm
from typing import Generator, Protocol, Iterator, Tuple, Optional, Any, Callable, Type, Iterable
from collections import defaultdict
import datetime
from abcs.abc_config import StateCampaignFinanceConfigClass
from abcs.abc_download import FileDownloader
from abcs.abc_db_loader import DBLoaderClass
from logger import Logger
import funcs
import inject
from sqlmodel import SQLModel

logger = Logger(__name__)

CategoryFileList = Generator[Path, Any, None]
ValidatorType = Type[SQLModel]
FileRecords = Generator[Dict, None, None]


def generate_file_list(folder: Path) -> tuple[list[Path], Callable[[str], CategoryFileList]]:
    _folder = list(folder.glob("*.csv"))

    def contains_prefix(prefix: str) -> CategoryFileList:
        for file in _folder:
            if file.stem.startswith(prefix):
                yield file

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
class StateCategoryClass(abc.ABC):
    category: str
    records: Generator[Dict, None, None] = field(init=False)
    record_count: int = field(init=False, default=0)
    validation: validation.StateFileValidationClass = field(init=False)
    config: StateCampaignFinanceConfigClass = field(init=False)
    _files: Optional[Generator[Path, Any, None]] = None
    __logger: Logger = field(init=False)

    def __repr__(self):
        return f"{self.__class__.__name__}:({self.category})"

    @abc.abstractmethod
    def init(self):
        ...

    @abc.abstractmethod
    def __post_init__(self):
        self.init()
        self._create_file_list()
        self.validation = abc_validation.StateFileValidationClass(validator_used=self.get_validator())

    @property
    def logger(self):
        self.__logger = Logger(self.__class__.__name__)
        return self.__logger

    @abc.abstractmethod
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
                self._files = ...
            case _:
                raise ValueError(f"Invalid category: {self.category}")
        return self._files

    @abc.abstractmethod
    def get_validator(self) -> ValidatorType:
        """
        Get the validator based on the category.
        :return: Type[validators.TECSettings]
        """
        match self.category:
            case "expenses":
                ...
            case _:
                raise ValueError(f"Invalid category: {self.category}")
        return ValidatorType

    def read(self) -> FileRecords:
        """
        Read the files based on the category.
        :return: Generator[Dict, None, None]
        """
        file_reader = funcs.FileReader()

        def read_files() -> FileRecords:
            """
            Generator func to read files based on the category.
            :return: Generator[Dict, None, None]
            """
            for _file in list(self._files):
                for record in file_reader.read_csv(_file):
                    file_reader.record_count += 1
                    yield record

        self.logger.info(f"Reading {self.category.title()} Files")
        self.records = read_files()
        self.record_count = file_reader.record_count
        return self.records

    def load(self) -> Iterable[Dict]:
        """
        Load the records locally as an iterator.
        :return: Iterable[Dict]
        """
        return iter(x for x in self.read())

    def validate(self,
                 records: FileRecords = None
                 ) -> abc_validation.PassedFailedRecordList:
        """
        Validate the records based on the category.
        :param records: Generator[Dict, None, None]
        :return: StateFileValidation
        """
        if not records:
            if not self.records:
                self.read()

            records = self.records

        return self.validation.validate(
            records=records
        )

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
    def load_to_db(self, records: Iterator[Type[ValidatorType]], engine: sqlmodel.create_engine, **kwargs) -> None:
        _db_loader = DBLoaderClass(engine=engine)
        if kwargs.get("create_table") is True:
            _db_loader.create_all(_db_loader.engine)
        _db_loader.add(records, record_type=self.validation.validator_used, add_limit=kwargs.get("limit", None))
        return None
