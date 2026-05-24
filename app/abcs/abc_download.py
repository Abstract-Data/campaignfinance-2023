from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import abc
from abcs.abc_state_config import StateConfig, CategoryTypes
from typing import Annotated, Generator
from pydantic import Field as PydanticField
import itertools

from app.logger import Logger

logger = Logger(__name__)

RecordGen = Annotated[Generator[dict, None, None] | None, PydanticField(default=None)]
FilteredRecordGen = RecordGen


@dataclass
class FileDownloaderABC(abc.ABC):
    config: StateConfig
    folder: Path = field(init=False)
    data: RecordGen | CategoryTypes = None

    def __post_init__(self) -> None:
        self.check_if_folder_exists()
        self.folder = self.config.TEMP_FOLDER

    def check_if_folder_exists(self) -> Path:
        self.config.TEMP_FOLDER.mkdir(parents=True, exist_ok=True)
        self.folder = self.config.TEMP_FOLDER
        logger.debug(f"Ensured temp folder exists: {self.folder}")
        return self.folder

    @abc.abstractmethod
    def download(self, *, overwrite: bool = False, headless: bool = False) -> Path:
        ...

    @abc.abstractmethod
    def read(self):
        ...

    def sort_categories(self) -> CategoryTypes:
        """Filter data into respective categories"""
        categories = self.config.CATEGORY_TYPES
        logger.debug(f"Sorting categories: {categories.__dataclass_fields__}")
        source_data = itertools.tee(self.data, len(categories.__dataclass_fields__))

        logger.debug("Filtering data into categories...")
        for category_name, data_gen in zip(categories.__dataclass_fields__, source_data):
            logger.debug(f"Filtering {category_name}...")
            category = getattr(categories, category_name, None)
            if category:
                try:
                    category.filter_category(data_gen)
                    logger.debug(f"Added data to {category_name}")
                    setattr(categories, category_name, category)
                except Exception as exc:
                    logger.error(f"Error filtering {category_name}: {exc}")
                    continue
        self.data = categories
        return self.data
