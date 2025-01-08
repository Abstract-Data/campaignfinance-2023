from __future__ import annotations
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic import Field as PydanticField, ConfigDict, model_validator, computed_field, HttpUrl, field_validator
from typing import Optional, Type, Annotated, Dict, Generator, Self, Iterator
import sqlmodel
from pathlib import Path
import polars as pl
import itertools
from icecream import ic

import funcs


def check_for_empty_gen(func):
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        gen1, gen2 = itertools.tee(gen)
        try:
            next(gen1)
            return False
        except StopIteration:
            return True
        finally:
            del gen1
            return gen2
    return wrapper


@check_for_empty_gen
def filter_records(self: CategoryConfig, data: Generator[Dict, None, None]) -> Generator[Dict, None, None] | None:
        pattern = self.SUFFIX or self.PREFIX
        is_suffix = bool(self.SUFFIX)

        def _filter(x: Dict) -> bool:
            return x["file_origin"].endswith(pattern) if is_suffix else x["file_origin"].startswith(pattern)

        filtered = (record for record in data if _filter(record))
        return filtered


@pydantic_dataclass
class CSVReaderConfig:
    lowercase_headers: bool = False
    replace_space_in_headers: bool = False


@pydantic_dataclass
class CategoryConfig:
    VALIDATOR: Type[sqlmodel.SQLModel]
    PREFIX: Optional[str] = None
    SUFFIX: Optional[str] = None
    DATA: Optional[Generator[Dict, None, None]] = None

    def __iter__(self) -> Iterator[Dict]:
        """
        Iterator dunder method that allows direct iteration over the instance's data.

        This method enables iteration over self.DATA using the instance directly. If self.DATA
        is None, returns self. Otherwise, returns an iterator of self.DATA.

        Returns
        --------
        Iterator[Dict]
            An iterator over the instance's data collection, where each item is a dictionary.

        Examples
        --------
        >>> data_instance = CategoryConfig()
        >>> first_item = next(data_instance)  # Gets first item in data collection
        >>> for item in data_instance:        # Iterates through all items
        ...     print(item)
        """ 
        if self.DATA is None:
            return self
        return self.DATA

    @model_validator(mode='after')
    def check_if_prefix_or_suffix(self):
        if not self.PREFIX and not self.SUFFIX:
            raise ValueError("Either PREFIX or SUFFIX must be defined.")
        return self
    
    # @classmethod
    # def _is_generator_empty(cls, gen: Generator) -> bool:
    #     """Check if generator is empty without consuming it"""
    #     gen1, gen2 = itertools.tee(gen)
    #     try:
    #         next(gen1)
    #         return False
    #     except StopIteration:
    #         return True
    #     finally:
    #         del gen1
    #         return gen2

    def filter_category(self, data: Generator[Dict, None, None]) -> Generator[Dict, None, None] | None:
        self.DATA = filter_records(self, data)
        return self.DATA
        

        # pattern = self.SUFFIX or self.PREFIX
        # is_suffix = bool(self.SUFFIX)

        # def _filter(x: Dict) -> bool:
        #     return x["file_origin"].endswith(pattern) if is_suffix else x["file_origin"].startswith(pattern)

        # filtered = (record for record in data if _filter(record))
        # if self._is_generator_empty(filtered):
        #     return None
        # return filtered

@pydantic_dataclass
class CategoryTypes:
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

    # def filter_all(self, data: Generator[Dict, None, None]) -> Self:
    #     _categories = self.__dataclass_fields__
    #     for _category in _categories:
    #         category = getattr(self, _category, None)
    #         if category:
    #             category.filter_category(data)
    #             setattr(self, _category, category)
    #     return self
    

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
        return funcs.read_toml(Path(__file__).parents[1] / 'states'/ (_state := self.STATE_NAME.lower()) / f"{_state}_fields.toml")
    
    def get_record_counts(self):
        files = self.TEMP_FOLDER.glob('*.csv')
        if not files:
            return None
        return {file.stem: pl.scan_csv(file, ignore_errors=True, low_memory=True).collect().height for file in files}
    
    def __post_init__(self):
        self.FILE_COUNTS = self.get_record_counts()
        return self
        
    def filter_categories(self, data: Generator[Dict, None, None]) -> CategoryTypes:
        """Filter data into respective categories"""
        categories = self.CATEGORY_TYPES
        source_data = itertools.tee(data, len(categories.__dataclass_fields__))
        
        for category_name, data_gen in zip(categories.__dataclass_fields__, source_data):
            category = getattr(categories, category_name, None)
            if category:
                try:
                    category.filter_category(data_gen)
                except Exception as e:
                    ic(f"Error filtering {category_name}: {e}")
                    continue
                    
        return categories