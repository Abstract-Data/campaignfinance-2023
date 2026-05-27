import csv
import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Generator

import polars as pl
from rich.progress import Progress

from app.logger import Logger

logger = Logger(__name__)


def async_include_file_origin(func):
    async def wrapper(*args, **kwargs):
        file = args[1]
        async for record in func(*args, **kwargs):
            record["file_origin"] = file.stem
            yield record

    return wrapper


def async_include_download_date(func):
    async def wrapper(*args, **kwargs):
        file = Path(args[1])
        last_modified_timestamp = file.stat().st_mtime
        last_modified_date = datetime.datetime.fromtimestamp(last_modified_timestamp).strftime(
            "%Y-%m-%d"
        )
        async for record in func(*args, **kwargs):
            record["download_date"] = last_modified_date
            yield record

    return wrapper


def include_file_origin(func: Callable) -> Callable:
    def wrapper(*args, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        file = args[1]
        for record in func(*args, **kwargs):
            record["file_origin"] = file.stem
            yield record

    return wrapper


def include_download_date(func: Callable) -> Callable:
    def wrapper(*args, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        file = Path(args[1])
        last_modified_timestamp = file.stat().st_mtime
        last_modified_date = datetime.datetime.fromtimestamp(last_modified_timestamp).strftime(
            "%Y-%m-%d"
        )
        for record in func(*args, **kwargs):
            record["download_date"] = last_modified_date
            yield record

    return wrapper


@dataclass
class FileReader:
    record_count: int = 0

    def __init__(self):
        super().__init__()

    @include_file_origin
    @include_download_date
    def read_csv(self, file: Path, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        try:
            with open(file, "r", encoding="utf-8") as _file:
                _records = csv.DictReader(_file)
                if kwargs.get("lowercase_headers"):
                    _records.fieldnames = [x.lower() for x in _records.fieldnames]
                for _record in _records:
                    if kwargs.get("change_space_in_headers"):
                        _record = {
                            k.replace(" ", "_"): v for k, v in _record.items() if k is not None
                        }
                    yield _record
        except UnicodeDecodeError:
            with open(file, "r", encoding="ISO-8859-1") as f:
                _records = csv.DictReader(f, delimiter=",", quotechar='"')
                if kwargs.get("lowercase_headers"):
                    _records.fieldnames = [x.lower() for x in _records.fieldnames]
                for _record in _records:
                    if kwargs.get("change_space_in_headers"):
                        _record = {
                            k.replace(" ", "_"): v for k, v in _record.items() if k is not None
                        }
                    yield _record

    def read_parquet(self, file: Path, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        """Read a Parquet file and yield each row as a dictionary."""
        try:
            df = pl.scan_parquet(file, **kwargs).collect()
            yield from df.iter_rows(named=True)
        except Exception as exc:
            logger.error(f"Error reading parquet file {file}: {exc}")
            try:
                df = pl.read_parquet(file, **kwargs)
                yield from df.to_dicts()
            except Exception as fallback_exc:
                logger.error(f"Fallback parquet read failed for {file}: {fallback_exc}")

    def read(self, file_path: Path, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        """Dispatch to read_parquet or read_csv based on file extension."""
        if file_path.suffix.lower() == ".parquet":
            return self.read_parquet(file_path, **kwargs)
        return self.read_csv(file_path, **kwargs)

    def read_folder(self, folder: Path, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        files = list(folder.glob("*.csv"))
        with Progress() as pbar:
            task = pbar.add_task("Reading files...", total=len(files))
            for file in files:
                pbar.update(task, advance=1)
                recs = self.read_csv(file, **kwargs)
                for rec in recs:
                    yield rec
