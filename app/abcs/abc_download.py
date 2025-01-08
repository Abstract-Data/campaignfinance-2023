from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import abc
from abcs.abc_state_config import CategoryConfig, StateConfig, CategoryTypes
from logger import Logger
import sys
from typing import Any, Optional, Generator, Dict, Annotated, Type
from icecream import ic
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic import BaseModel, Field as PydanticField
import itertools


RecordGen = Annotated[Optional[Generator[Dict, None, None]], PydanticField(default=None)]
FilteredRecordGen = RecordGen

# @pydantic_dataclass
# class FileTypeListABC(abc.ABC):
#     data: Generator[Dict, None, None]
#     contributions: FilteredRecordGen
#     expenses: FilteredRecordGen
#     filers: FilteredRecordGen
#     reports: FilteredRecordGen
#     loans: FilteredRecordGen
#     debts: FilteredRecordGen
#     notices: FilteredRecordGen
#     credits: FilteredRecordGen
#     candidates: FilteredRecordGen
#     spacs: FilteredRecordGen
#     travel: FilteredRecordGen

#     @classmethod
#     def _is_generator_empty(cls, gen: Generator) -> bool:
#         """Check if generator is empty without consuming it"""
#         gen1, gen2 = itertools.tee(gen)
#         try:
#             next(gen1)
#             return False
#         except StopIteration:
#             return True
#         finally:
#             del gen1
#             return gen2

#     def _filter_by(self, category: CategoryConfig) -> RecordGen | None:
#         """Filter data generator by file_origin prefix or suffix"""
#         if all([category.PREFIX, category.SUFFIX]):
#             raise ValueError("Exactly one of PREFIX or SUFFIX must be defined")
#         elif not any([category.PREFIX, category.SUFFIX]):
#             raise ValueError("Either PREFIX or SUFFIX must be defined")
        

#         pattern = category.SUFFIX or category.PREFIX
#         is_suffix = bool(category.SUFFIX)

#         def _filter(x: Dict) -> bool:
#             return x["file_origin"].endswith(pattern) if is_suffix else x["file_origin"].startswith(pattern)

#         filtered = (record for record in self.data if _filter(record))
#         # if self._is_generator_empty(filtered):
#         #     return None
#         return filtered

@dataclass
class FileDownloaderABC(abc.ABC):
    config: StateConfig
    folder: Path = field(init=False)
    data: RecordGen | CategoryTypes = None

    def __post_init__(self):

        self.check_if_folder_exists()        
        self.folder = self.config.TEMP_FOLDER

    def check_if_folder_exists(self) -> Path:
        _temp_folder_name = self.config.TEMP_FOLDER.stem.title()
        ic(f"Checking if {_temp_folder_name} temp folder exists...")
        if self.config.TEMP_FOLDER.exists():
            self.folder = self.config.TEMP_FOLDER
            return self.folder

        ic(f"{_temp_folder_name} temp folder does not exist...")
        ic("Throwing input prompt...")
        _create_folder = input("Temp folder does not exist. Create? (y/n): ")
        ic(f"User input: {_create_folder}")
        if _create_folder.lower() == "y":
            self.config.TEMP_FOLDER.mkdir()
            self.folder = self.config.TEMP_FOLDER
            return self.folder
        else:
            print("Exiting...")
            ic("User selected 'n'. Exiting...")
            sys.exit()

    @abc.abstractmethod
    def download(self, overwrite: bool, read_from_temp: bool) -> FileDownloaderABC:
        ...

    @abc.abstractmethod
    def read(self):
        ...

    def sort_categories(self) -> CategoryTypes:
        if self.data:
            self.data = self.config.filter_categories(self.data)
        return self.data