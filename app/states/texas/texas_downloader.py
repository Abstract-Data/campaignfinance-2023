from __future__ import annotations
from typing import Self, ClassVar
import re
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import zipfile
import time
from app.funcs.csv_reader import FileReader
from app.logger import Logger
import polars as pl
import itertools


from app.abcs import (
    FileDownloaderABC, StateConfig, RecordGen, progress)
from web_scrape_utils import By

logger = Logger(__name__)

    
@dataclass
class TECDownloader(FileDownloaderABC):
    config: ClassVar[StateConfig]

    def __init__(self, config: StateConfig):
        super().__init__(config=config)
        TECDownloader.config = config

    @classmethod
    def download(
            cls,
            read_from_temp: bool = True,
            headless: bool = True
    ) -> Self:
        tmp = cls.config.TEMP_FOLDER
        _existing_files = itertools.chain(*map(lambda x: tmp.glob(x), ["*.csv", "*.parquet", "*.txt"]))
        _safe_to_delete = False

        if not headless or not cls.driver.headless:
            cls.not_headless()

        _task2 = progress.add_task("T2", "Downloading TEC Zipfile...", "In Progress")
        logger.debug("Starting TEC download process")
        cls.driver.create_driver()
        driver = cls.driver.chrome_driver
        wait = cls.driver


        driver.get("https://ethics.state.tx.us/")
        wait.wait_until_clickable(By.LINK_TEXT, "Search")
        wait.wait_until_clickable(By.LINK_TEXT, "Campaign Finance Reports")
        wait.wait_until_clickable(By.LINK_TEXT, "Database of Campaign Finance Reports")
        wait.wait_until_clickable(By.PARTIAL_LINK_TEXT, "Campaign Finance CSV Database")
        time.sleep(5)

        attempts = 0
        while driver.title == 'Error':
            attempts += 1
            logger.warning("Error page detected, refreshing", extra={"attempt": attempts})
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
            logger.debug("Removing existing files from temp folder")
            list(map(lambda x: x.unlink(), _existing_files))
            progress.update_task(_task1, "Complete")


        _zip_files = list(Path(x) for x in tmp.glob("*.zip"))
        if not _zip_files:
            raise FileNotFoundError("No zip files found in folder")

        _latest_file = max(_zip_files, key=lambda x: x.stat().st_ctime)
        with zipfile.ZipFile(_latest_file, "r") as zip_ref:
            TECDownloader.extract_zipfile(zip_ref, tmp)

        list(map(lambda x: x.unlink(), _zip_files))
        cls.consolidate_files()
        progress.stop()
        return cls

    @classmethod
    def _create_file_type_dict(cls) -> dict:
        _folder = cls.config.TEMP_FOLDER
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
            _file_type_dict[parts] = sorted(_file_type_dict[parts])

        return _file_type_dict

    @classmethod
    def consolidate_files(cls):
        _file_types = cls._create_file_type_dict()
        _file_count = len(_file_types)
        logger.debug("File types to consolidate", extra={"file_types": list(_file_types.keys())})

        _task = progress.add_task("Consolidation", "Consolidating Files By Category...", "Started")
        logger.info("Starting file consolidation")
        for k, v in _file_types.items():
            if len(v) > 1:
                logger.debug("Multiple files found for category", extra={"category": k, "file_count": len(v)})
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
                
                # Get all unique columns from all files in this category
                all_columns = set(df.columns)
                for _file in v[1:]:  # Skip first file since we already loaded it
                    try:
                        _fdf = pl.read_parquet(_file)
                        all_columns.update(_fdf.columns)
                    except Exception as e:
                        logger.warning("Error reading parquet file", extra={"file": str(_file), "error": str(e)})
                        continue
                
                # Ensure all columns exist in the main DataFrame (cast to String for compatibility)
                for col in all_columns:
                    if col not in df.columns:
                        df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))
                
                # Now process each file and align columns
                for _file in _files:
                    try:
                        _fdf = (
                            pl.read_parquet(_file)
                            .with_columns(
                                pl.lit(_file.stem)
                                .alias('file_origin')
                            )
                        )
                        
                        # Add missing columns to _fdf (cast to String for compatibility)
                        for col in all_columns:
                            if col not in _fdf.columns:
                                _fdf = _fdf.with_columns(pl.lit(None).cast(pl.String).alias(col))
                        
                        # Ensure column order matches
                        _fdf = _fdf.select(df.columns)
                        
                        # Now stack the aligned DataFrames
                        df = df.vstack(_fdf)
                        logger.debug("Added file to DataFrame", extra={"file": _file.stem, "category": k})
                    except Exception as e:
                        logger.warning("Error processing file", extra={"file": str(_file), "error": str(e)})
                        continue
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
                file=cls.config.TEMP_FOLDER / f"{k}_{datetime.now():%Y%m%d}w.parquet",
                compression='lz4')
            progress.update_task(_task, "In Progress")
        list(map(lambda x: x.unlink(), itertools.chain(*_file_types.values())))
        progress.update_task(_task, "Complete")
        return cls

    @classmethod
    def read(cls, parquet: bool = True) -> RecordGen:

        if not parquet:
            _reader = FileReader()
            cls.data = _reader.read_folder(cls.folder, file_counts=cls.config.FILE_COUNTS)
        else:
            parquet_files = list(cls.config.TEMP_FOLDER.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError("No parquet files found in folder")
            _data = {}
            for file in parquet_files:
                _type = file.stem.rsplit('_', 1)[0]
                _data.setdefault(_type, []).append(file)

            _data = {}
            for k, v in _data.items():
                if len(v) == 1:
                    # Single file - read directly
                    _data[k] = pl.read_parquet(v[0]).to_dicts()
                else:
                    # Multiple files - need to align schemas
                    try:
                        # Read first file
                        df = pl.read_parquet(v[0])
                        
                        # Get all unique columns from all files
                        all_columns = set(df.columns)
                        for _file in v[1:]:
                            try:
                                _fdf = pl.read_parquet(_file)
                                all_columns.update(_fdf.columns)
                            except Exception as e:
                                logger.warning("Error reading parquet file", extra={"file": str(_file), "error": str(e)})
                                continue
                        
                        # Ensure all columns exist in the main DataFrame (cast to String for compatibility)
                        for col in all_columns:
                            if col not in df.columns:
                                df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))
                        
                        # Process remaining files
                        for _file in v[1:]:
                            try:
                                _fdf = pl.read_parquet(_file)
                                
                                # Add missing columns to _fdf (cast to String for compatibility)
                                for col in all_columns:
                                    if col not in _fdf.columns:
                                        _fdf = _fdf.with_columns(pl.lit(None).cast(pl.String).alias(col))
                                
                                # Ensure column order matches
                                _fdf = _fdf.select(df.columns)
                                
                                # Stack the aligned DataFrames
                                df = df.vstack(_fdf)
                            except Exception as e:
                                logger.warning("Error processing file", extra={"file": str(_file), "error": str(e)})
                                continue

                        _data[k] = df.to_dicts()
                    except Exception as e:
                        logger.error("Error processing category", extra={"category": k, "error": str(e)})
                        _data[k] = []
            
            cls.data = _data
        return cls.data

    @classmethod
    def dataframes(cls):
        _files = cls._create_file_type_dict()
        return {k: pl.scan_parquet(v) for k, v in _files.items()}
