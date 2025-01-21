from __future__ import annotations
import re
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import zipfile
from tqdm import tqdm
import time
from icecream import ic
from funcs.csv_reader import FileReader
import polars as pl
import itertools


from abcs import (
    FileDownloaderABC, StateConfig, CategoryTypes, RecordGen, CategoryConfig, progress)
from web_scrape_utils import CreateWebDriver

    
@dataclass
class TECDownloader(FileDownloaderABC):
    config: StateConfig
    driver: CreateWebDriver = None
    
    def __post_init__(self):
        self.folder = self.config.TEMP_FOLDER
        self.driver = CreateWebDriver(download_folder=self.folder)

    def not_headless(self):
        self.driver.headless = False
        return self
    def download(
            self,
            read_from_temp: bool = True,
            headless: bool = True
    ) -> TECDownloader:
        tmp = self.config.TEMP_FOLDER
        _existing_files = itertools.chain(*map(lambda x: tmp.glob(x), ["*.csv", "*.parquet", "*.txt"]))
        _safe_to_delete = False

        if not headless or not self.driver.headless:
            self.not_headless()

        _task2 = progress.add_task("T2", "Downloading TEC Zipfile...", "In Progress")
        ic()
        self.driver.create_driver()
        driver = self.driver.chrome_driver
        wait = self.driver.wait


        driver.get("https://ethics.state.tx.us/")
        wait.for_clickable_link_text("Search")
        wait.for_clickable_link_text("Campaign Finance Reports")
        wait.for_clickable_link_text("Database of Campaign Finance Reports")
        wait.for_clickable_partial_link_text("Campaign Finance CSV Database")
        time.sleep(5)

        attempts = 0
        while driver.title == 'Error':
            attempts += 1
            ic(f"Error page detected. Refreshing... (Attempt #: {attempts})")
            driver.refresh()
            time.sleep(5)
            if attempts > 5:
                raise ConnectionError("Error downloading TEC Zipfile. You will need to contact TEC for more information.")


        in_progress = False
        while True:
            dl_files = list(tmp.glob("*.crdownload"))
            if dl_files:
                if not in_progress:
                    progress.update_task(_task2, "Still in progress")
                    in_progress = True
                time.sleep(10)
            else:
                if in_progress:
                    progress.update_task(_task2, "Download Complete")
                    _safe_to_delete = True
                break

        if _safe_to_delete:
            _task1 = progress.add_task("T1", "Removing existing files", "In Progress")
            ic()
            list(map(lambda x: x.unlink(), _existing_files))
            progress.update_task(_task1, "Complete")


        _zip_files = list(Path(x) for x in tmp.glob("*.zip"))
        if not _zip_files:
            raise FileNotFoundError("No zip files found in folder")

        _latest_file = max(_zip_files, key=lambda x: x.stat().st_ctime)
        with zipfile.ZipFile(_latest_file, "r") as zip_ref:
            TECDownloader.extract_zipfile(zip_ref, tmp)

        list(map(lambda x: x.unlink(), _zip_files))
        self.consolidate_files()
        progress.stop()
        return self

    def _create_file_type_dict(self) -> dict:
        _folder = self.config.TEMP_FOLDER
        _file_count = len(list(_folder.glob("*.parquet")))
        _file_type_dict = {}
        digit_pattern = re.compile(r'^[a-zA-Z]+_\d{2}')
        string_pattern = re.compile(r'^[a-zA-Z]+_[a-zA-Z]{1,2}')
        for file in _folder.glob("*.parquet"):
            if digit_pattern.match(file.stem):
                parts = file.stem.split('_', 1)[0]  # Split at the first underscore
            elif string_pattern.match(file.stem):
                parts = file.stem.rsplit('_', 1)[0]  # Split at the last underscore
            else:
                raise ValueError(f"File {file} does not match the expected pattern")
            _file_type_dict.setdefault(parts, []).append(file)

        return _file_type_dict


    def consolidate_files(self):
        _file_types = self._create_file_type_dict()
        _file_count = len(_file_types)
        ic(_file_types)

        _task = progress.add_task("Consolidation", "Consolidating Files By Category...", "Started")
        ic()
        for k, v in _file_types.items():
            if len(v) > 1:
                ic("More than 1 file found")
                _files = iter(v)
                # Start with scanning the first file instead of empty DataFrame
                _first_file = next(_files)
                df = (
                    pl.read_parquet(_first_file)
                    .with_columns(
                        pl.lit(_first_file.stem)
                        .alias('file_origin')
                    )
                )
                for _file in _files:
                    _fdf = (
                        pl.read_parquet(_file)
                        .with_columns(
                            pl.lit(_file.stem)
                            .alias('file_origin')
                        )
                    )
                    df = df.vstack(_fdf)
                    ic(f"Added {_file.stem} to DataFrame {k}")
            else:
                df = (
                    pl.read_parquet(v[0])
                    .with_columns(
                        pl.lit(v[0].stem)
                        .alias('file_origin')
                    )
                )

            df = (
                df
                .with_columns(
                    [
                        pl.col(col)
                        .cast(pl.String) for col in df.columns
                    ]
                )
            )

            df.write_parquet(
                file=self.config.TEMP_FOLDER / f"{k}_{datetime.now():%Y%m%d}w.parquet",
                compression='lz4')
            progress.update_task(_task, "In Progress")
        list(map(lambda x: x.unlink(), itertools.chain(*_file_types.values())))
        progress.update_task(_task, "Complete")
        return self


    def read(self, parquet: bool = True) -> RecordGen:

        if not parquet:
            _reader = FileReader()
            self.data = _reader.read_folder(self.folder, file_counts=self.config.FILE_COUNTS)
        else:
            parquet_files = list(self.config.TEMP_FOLDER.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError("No parquet files found in folder")
            _data = {}
            for file in parquet_files:
                _type = file.stem.rsplit('_', 1)[0]
                _data.setdefault(_type, []).append(file)

            _data = (
                {
                    k: (
                        pl.scan_parquet(v)
                        .collect()
                        .to_dicts()
                    ) for k, v in _data.items()
                }
            )
            self.data = _data
        return self.data

    def dataframes(self):
        _files = self._create_file_type_dict()
        return {k: pl.scan_parquet(v) for k, v in _files.items()}
