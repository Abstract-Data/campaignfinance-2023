
from concurrent.futures import ThreadPoolExecutor
import csv
from pathlib import Path
from typing import Callable, Dict, Generator
import datetime
from dataclasses import dataclass
from app.logger import Logger
from rich.progress import Progress
import polars as pl
from rich.progress import Progress


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
        last_modified_date = datetime.datetime.fromtimestamp(last_modified_timestamp).strftime("%Y-%m-%d")
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
        last_modified_date = datetime.datetime.fromtimestamp(last_modified_timestamp).strftime("%Y-%m-%d")
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
            with open(file, "r", encoding='utf-8') as _file:
                _records = csv.DictReader(_file)
                if kwargs.get('lowercase_headers'):
                    _records.fieldnames = [x.lower() for x in _records.fieldnames]
                for _record in _records:
                    if kwargs.get('change_space_in_headers'):
                        _record = {k.replace(' ', '_'): v for k, v in _record.items() if k is not None}
                    yield _record
        except UnicodeDecodeError:
            with open(file, 'r', encoding='ISO-8859-1') as f:
                _records = csv.DictReader(f, delimiter=',', quotechar='"')
                if kwargs.get('lowercase_headers'):
                    _records.fieldnames = [x.lower() for x in _records.fieldnames]
                for _record in _records:
                    if kwargs.get('change_space_in_headers'):
                        _record = {k.replace(' ', '_'): v for k, v in _record.items() if k is not None}
                    yield _record

    def read_folder(self, folder: Path, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        # Get both CSV and Parquet files
        csv_files = list(folder.glob("*.csv"))
        parquet_files = list(folder.glob("*.parquet"))
        all_files = csv_files + parquet_files
        
        with Progress() as pbar:
            task = pbar.add_task("Reading files...", total=len(all_files))
            for file in all_files:
                pbar.update(task, advance=1)
                try:
                    if file.suffix.lower() == '.csv':
                        recs = self.read_csv(file, **kwargs)
                    elif file.suffix.lower() == '.parquet':
                        recs = self.read_parquet(file, **kwargs)
                    else:
                        continue
                        
                    for rec in recs:
                        yield rec
                except Exception as e:
                    print(f"Error processing file {file}: {str(e)}")
                    continue

    @include_file_origin
    @include_download_date
    def read_parquet(self, file: Path, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        """
        Read a Parquet file and yield each row as a dictionary.
        Handles schema mismatches by reading row by row.
        """
        try:
            # Use scan_parquet for lazy evaluation
            lazy_df = pl.scan_parquet(file, **kwargs)
            
            # Collect the lazy frame and then iterate over rows
            df = lazy_df.collect()
            
            # Use iter_rows to get each row as a dictionary
            for row in df.iter_rows(named=True):
                yield row
                
        except Exception as e:
            print(f"Error reading parquet file {file}: {str(e)}")
            # Try alternative approach if scan_parquet fails
            try:
                df = pl.read_parquet(file, **kwargs)
                for row_dict in df.to_dicts():
                    yield row_dict
            except Exception as e2:
                print(f"Alternative approach also failed for {file}: {str(e2)}")

    @include_file_origin
    @include_download_date
    def read_txt_file(self, file: Path, **kwargs) -> Generator[Dict[int, Dict], None, None]:
        with open(file, "r", encoding='utf-8') as _file:
            _records = csv.DictReader(_file)
            if kwargs.get('lowercase_headers'):
                _records.fieldnames = [x.lower() for x in _records.fieldnames]
            for _record in _records:
                if kwargs.get('change_space_in_headers'):
                    _record = {k.replace(' ', '_'): v for k, v in _record.items() if k is not None}
                yield _record
