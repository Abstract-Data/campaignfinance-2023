from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import abc
import sys
from typing import Optional, Generator, Dict, Annotated, ClassVar
from icecream import ic
from pydantic import Field as PydanticField
import itertools
from datetime import datetime
import polars as pl

from abcs.abc_state_config import StateConfig, CategoryTypes
from web_scrape_utils import CreateWebDriver
from live_display import ProgressTracker

RecordGen = Annotated[Optional[Generator[Dict, None, None]], PydanticField(default=None)]
FilteredRecordGen = RecordGen

progress = ProgressTracker()
progress.start()

@dataclass
class FileDownloaderABC(abc.ABC):
    config: StateConfig
    driver: ClassVar[Optional[CreateWebDriver]] = None
    folder: Path = field(init=False)
    data: RecordGen | CategoryTypes = None

    def __post_init__(self):
        self.check_if_folder_exists()        
        self.folder = self.config.TEMP_FOLDER
        FileDownloaderABC.driver = CreateWebDriver(download_folder=self.folder)

    @classmethod
    def not_headless(cls):
        if cls.driver:
            cls.driver.headless = False
        return cls

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

    @classmethod
    def extract_zipfile(cls, zip_ref, tmp):
        zip_file_info = zip_ref.infolist()
        _extract_task = progress.add_task("T4", "Extract Zip", "In Progress")
        for file in zip_file_info:
            try:
                cls._process_csv(zip_ref, file, tmp)
            except Exception as e:
                ic(f"Zip File Extraction Error on {file.filename.upper()}: {e}")
        progress.update_task(_extract_task, "Complete")

    @classmethod
    def _process_csv(cls, zip_ref, file, tmp):
        file_name = Path(file.filename)
        if file_name.suffix not in ('.csv', '.txt'):
            ic(f"File {file_name.stem} is not a CSV/TXT file. Skipping...")
            return

        _csv_task = progress.add_task("T5", f"Extract CSV {file_name.stem}", "Started")
        zip_ref.extract(file, tmp)

        if file_name.suffix == '.txt':
            return
        
        rename = f"{file_name.stem}_{datetime.now():%Y%m%d}dl"
        pl_file = pl.scan_csv(tmp / file_name, low_memory=False, infer_schema=False)
        pl_file = (
            pl_file
            .with_columns(
                pl.lit(file_name.stem)
                .alias('file_origin')
            ))

        pl_file = (
            pl_file
            .with_columns([
                pl.col(col)
                .cast(pl.String)
                for col in pl_file.collect_schema().names()
                ]))

        pl_file.collect().write_parquet(tmp / f"{rename}.parquet", compression='lz4')
        progress.update_task(_csv_task, "Complete")
        # Clean up original CSV file
        (tmp / file_name).unlink()

    @classmethod
    @abc.abstractmethod
    def download(cls, overwrite: bool, read_from_temp: bool) -> FileDownloaderABC:
        ...

    @classmethod
    @abc.abstractmethod
    def consolidate_files(cls):
        ...

    @classmethod
    @abc.abstractmethod
    def read(cls):
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