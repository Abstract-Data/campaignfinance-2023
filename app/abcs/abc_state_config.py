from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic import Field as PydanticField, ConfigDict, model_validator
from typing import Optional, Type, Annotated, Dict, Generator, Iterator
import sqlmodel
from pathlib import Path
import polars as pl
import itertools
from icecream import ic
from enum import StrEnum
from rich.progress import track

import funcs


def check_for_empty_gen(func):
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        gen1, gen2 = itertools.tee(gen)
        try:
            next(gen1)
            return gen2
        except StopIteration:
            ic(f"Generator for {args[0].__class__.__name__} is empty")
            return None
        finally:
            del gen1
    return wrapper



class FieldType(StrEnum):
    PREFIX = "file-prefixes"
    SUFFIX = "file-suffixes"


@pydantic_dataclass
class CSVReaderConfig:
    lowercase_headers: bool = False
    replace_space_in_headers: bool = False


# class FilteredRecords:
#     """Named generator wrapper for category filtered records"""
#     def __init__(self, generator, category_name: str):
#         self.generator = generator
#         self.category_name = category_name

#     def __call__(self):
#         return self.generator
        
#     def __iter__(self):
#         return self.generator
        
#     def __repr__(self):
#         return f"FilteredRecords(category={self.category_name})"

#     def __next__(self):
#         return next(self.generator)
    

@pydantic_dataclass
class CategoryConfig:
    VALIDATOR: Type[sqlmodel.SQLModel]
    FIELDS: dict
    DESC: str
    DATA: Optional[Generator[Dict, None, None]] = None
    PREFIX: Optional[str] = None
    SUFFIX: Optional[str] = None


    def __iter__(self) -> Iterator[Dict]:
        """Iterator over category data"""
        if not self.DATA:
            raise StopIteration
        return self.DATA

    def __next__(self) -> Dict:
        """Get next item from data"""
        if not self.DATA:
            raise StopIteration
        return next(self.DATA)
    
    def __post_init__(self):
        _fields = self.FIELDS
        if _fields.get(FieldType.PREFIX.value):
            self.PREFIX = _fields[FieldType.PREFIX.value][self.DESC]
        if _fields.get(FieldType.SUFFIX.value):
            self.SUFFIX = _fields[FieldType.SUFFIX.value][self.DESC]
        
        if not any([self.PREFIX, self.SUFFIX]):
            raise ValueError("Either PREFIX or SUFFIX must be defined")
        
    
    def _filter(self, x: Dict) -> bool:
        pattern = self.SUFFIX or self.PREFIX
        is_suffix = bool(self.SUFFIX)
        return x["file_origin"].endswith(pattern) if is_suffix else x["file_origin"].startswith(pattern)


    def filter_category(self, data: Generator[Dict, None, None]) -> Generator[Dict, None, None] | None:
        filtered = (record for record in data if self._filter(record))
        self.DATA = filtered
        return self.DATA
    

@pydantic_dataclass
class CategoryTypes:
    model_config = ConfigDict(exclude_none=True)
    expenses: Optional[CategoryConfig] = None
    contributions: Optional[CategoryConfig] = None
    filers: Optional[CategoryConfig] = None
    reports: Optional[CategoryConfig] = None
    travel: Optional[CategoryConfig] = None
    candidates: Optional[CategoryConfig] = None
    debts: Optional[CategoryConfig] = None
    notices: Optional[CategoryConfig] = None
    credits: Optional[CategoryConfig] = None
    spacs: Optional[CategoryConfig] = None
    loans: Optional[CategoryConfig] = None

    def __iter__(self) -> Iterator[CategoryConfig]:
        """Get all non-None CategoryConfig objects"""
        return iter([
            value for value in vars(self).values()
            if isinstance(value, CategoryConfig)
        ])

    def __next__(self) -> CategoryConfig:
        """Get next CategoryConfig object"""
        return next(self.__iter__())
    
    def __len__(self) -> int:
        return len(list(self.__iter__()))
    

@pydantic_dataclass
class StateConfig:
    model_config = ConfigDict(arbitrary_types_allowed=True)
    STATE_NAME: str
    STATE_ABBREVIATION: Annotated[str, PydanticField(max_length=2)]
    CSV_CONFIG: CSVReaderConfig
    CATEGORY_TYPES: Optional[CategoryTypes] = PydanticField(default=None)
    FILE_COUNTS: Optional[dict] = PydanticField(default=None)

    @property
    def TEMP_FOLDER(self) -> Path:
        return Path(__file__).parents[2] / "tmp" / self.STATE_NAME.lower()
    
    @property
    def FIELD_DATA(self) -> dict:
        return (
            funcs
            .read_toml(
                Path(__file__)
                .parents[1] / 'states'/ (_state := self.STATE_NAME.lower()) / f"{_state}_fields.toml"))
    
    @staticmethod
    @lru_cache
    def get_file_count(file: Path) -> int:
        return pl.scan_csv(
            file, 
            ignore_errors=True, 
            low_memory=True
        ).collect().height
    
    def get_record_counts(self) -> Optional[Dict[str, int]]:
        files = list(self.TEMP_FOLDER.glob('*.csv'))
        if not files:
            return None
            
        with ThreadPoolExecutor() as executor:
            counts = executor.map(self.get_file_count, files)
            return dict(zip((f.stem for f in files), counts))
    
    def __post_init__(self):
        self.FILE_COUNTS = self.get_record_counts()
        return self