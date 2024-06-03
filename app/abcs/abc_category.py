from __future__ import annotations
import abc

import sqlalchemy
import sqlmodel
import abcs.abc_validation as validation
from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import dataclass, field
import csv
from tqdm import tqdm
from typing import Generator, overload, Iterator, Tuple, Optional, Any, Callable, Type, Iterable, Annotated, Union
from collections import defaultdict
import datetime
# from abcs.abc_download import FileDownloader
from abcs.abc_db_loader import DBLoaderClass
from abcs.abc_state_config import StateConfig
from logger import Logger
import funcs
import inject
from sqlmodel import SQLModel
from pydantic import BaseModel, Field, ConfigDict, model_validator, computed_field

logger = Logger(__name__)

CategoryFileList = Generator[Path, None, None]
ValidatorType = Type[SQLModel]
FileRecords = Generator[Dict, None, None]


@dataclass
class StateCategoryClass(abc.ABC):
    category: str
    config: StateConfig
    _files: CategoryFileList = field(init=False)
    __logger: Logger = field(init=False)
    _records: FileRecords = field(init=False)

    @property
    def records(self):
        return self._records

    @records.setter
    def records(self, value):
        self._records = value

    @records.getter
    def records(self):
        return self._records

    @property
    def files(self):

        def generate_files(pfx_sfx: str) -> CategoryFileList:
            for _file in self.config.TEMP_FOLDER.glob("*.csv"):
                if _file.stem.endswith(pfx_sfx) \
                        if self.config.CATEGORY_TYPES[self.category].SUFFIX else _file.stem.startswith(pfx_sfx):
                    yield _file

        if self.category not in self.config.CATEGORY_TYPES:
            raise ValueError(f"Invalid category: {self.category}")

        _category = self.config.CATEGORY_TYPES[self.category]
        if _category.SUFFIX:
            _files = generate_files(_category.SUFFIX)
        elif _category.PREFIX:
            _files = generate_files(_category.PREFIX)
        else:
            raise ValueError("Either PREFIX or SUFFIX must be defined.")

        self._files = _files
        return self._files

    @property
    def validation(self):
        """
    Get the validator based on the category.
    :return: Type[validators.TECSettings]
    """
        if self.category in self.config.CATEGORY_TYPES:
            return validation.StateFileValidation(
                validator_to_use=self.config.CATEGORY_TYPES[self.category].VALIDATOR)
        else:
            raise ValueError(f"Invalid category: {self.category}")

    @validation.setter
    def validation(self, value):
        self.validation = value

    def __repr__(self):
        return f"{self.__class__.__name__}:({self.category}), {self.config.STATE_NAME}"

    @property
    def logger(self):
        self.__logger = Logger(self.__class__.__name__)
        return self.__logger

    def read(self, **kwargs) -> FileRecords:
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
            for _file in list(self.files):
                for record in file_reader.read_csv(
                        _file,
                        change_space_in_headers=self.config.CSV_CONFIG.replace_space_in_headers,
                        lowercase_headers=self.config.CSV_CONFIG.lowercase_headers):
                    file_reader.record_count += 1
                    yield record

        self.logger.info(f"Reading {self.category.title()} Files")
        self._records = read_files()
        return self._records

    def load(self) -> Iterable[Dict]:
        """
        Load the records locally as an iterator.
        :return: Iterable[Dict]
        """
        if not self.records:
            self.read()
        return list(self.records)

    def validate(self,
                 records: FileRecords = None
                 ) -> validation.PassedFailedRecordList:
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

    def write_to_csv(self, records, validation_status):
        funcs.write_records_to_csv_validation(
            records=records,
            folder_path=self.config.TEMP_FOLDER,
            record_type=self.category,
            validation_status=validation_status
        )
        return self

    @inject.autoparams()
    def load_to_db(self, records: Iterator[Type[ValidatorType]], **kwargs) -> None:
        _db_loader = DBLoaderClass(engine=self.config.DATABASE_ENGINE)
        if kwargs.get("create_table") is True:
            _db_loader.create_all(_db_loader.engine)
        _db_loader.add(records, record_type=self.validation.validator_to_use, add_limit=kwargs.get("limit", None))
        return None
