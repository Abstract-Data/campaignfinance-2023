from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import abc
from abcs.abc_state_config import StateConfig, CategoryTypes
import sys
from typing import Optional, Generator, Dict, Annotated
from icecream import ic
from pydantic import Field as PydanticField
import itertools


RecordGen = Annotated[Optional[Generator[Dict, None, None]], PydanticField(default=None)]
FilteredRecordGen = RecordGen



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
        """Filter data into respective categories"""
        categories = self.config.CATEGORY_TYPES
        ic(f"Sorting categories: {categories.__dataclass_fields__}")
        source_data = itertools.tee(self.data, len(categories.__dataclass_fields__))
        
        ic("Filtering data into categories...")
        for category_name, data_gen in zip(categories.__dataclass_fields__, source_data):
            ic(f"Filtering {category_name}...")
            category = getattr(categories, category_name, None)
            if category:
                try:
                    # Get filtered data and explicitly set to DATA
                    category.filter_category(data_gen)
                    ic(f"Added data to {category_name}")
                    setattr(categories, category_name, category)
                except Exception as e:
                    ic(f"Error filtering {category_name}: {e}")
                    continue
        self.data = categories
        return self.data