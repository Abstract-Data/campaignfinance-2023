from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Dict, Generator, List, NamedTuple, Type
from abcs.state_configs import StateCampaignFinanceConfigs
import csv
from pydantic import ValidationError
from tqdm import tqdm
from collections import namedtuple
import requests
from zipfile import ZipFile
import os
import sys
import pandas as pd
from pydantic import BaseModel


@dataclass
class CampaignFinanceFileReader(ABC):
    """
    TECFileReader
    =============
    This class is used to read TEC campaign finance files.
    It is used to read the files from the TEC website and
    validate the data in the files.
    """
    file_list: Path

    VALIDATOR: ClassVar[Type[BaseModel]] = StateCampaignFinanceConfigs.VALIDATOR

    def __repr__(self):
        return f"{self.file_list.name}"

    def __str__(self):
        return f"{self.file_list.name}"

    def __post_init__(self):
        self.path: Path = field(default_factory=Path)
        self.records: Dict = self.read_files()

    @abstractmethod
    def read_files(self, category: str):
        ...

    # @abstractmethod
    # def validate(self):
    #     ...


@dataclass
class CampaignFinanceFolderReader(ABC):
    """
    TECFolderReader
    ===============
    This class is used to pull Campaign finance files from the TEC website.
    It is used to download the files from the TEC website and
    validate the data in the files.
    """
    _EXPENSE_FILE_PREFIX: str = StateCampaignFinanceConfigs.EXPENSE_FILE_PREFIX
    _CONTRIBUTION_FILE_PREFIX: str = StateCampaignFinanceConfigs.CONTRIBUTION_FILE_PREFIX
    _ZIPFILE_URL: str = StateCampaignFinanceConfigs.ZIPFILE_URL

    @property
    def folder(self):
        fldr = Path.cwd()
        tmp = fldr / 'tmp'
        return tmp

    @folder.setter
    def folder(self, value):
        self.folder = value

    def __post_init__(self):

        self.expenses = CampaignFinanceFileGroup(
            CampaignFinanceFileReader(file) for file in self.folder.glob('*.csv') if file.stem.startswith(
                self._EXPENSE_FILE_PREFIX)
        )
        self.contributions = CampaignFinanceFileGroup(
            CampaignFinanceFileReader(file) for file in self.folder.glob('*.csv') if file.stem.startswith(
                self._CONTRIBUTION_FILE_PREFIX)
        )

    @abstractmethod
    def download(self, read_from_temp: bool = True):
        ...

        def download_file():
            ...

        def extract_zipfile():
            ...


class CampaignFinanceFileGroup(ABC):
    """
    TECFileGroup
    ============
    This class is used to group TECFileReader objects together.
    """

    def __init__(self, file):
        self.file: Generator[CampaignFinanceFileReader, Any, None] = file

    @property
    def records(self):
        return (f.read_file() for f in self.file)

    # @property
    # def validated(self):
    #     return (f.validate() for f in self.file)


class CampaignFinanceReportLoader(ABC):
    """
    TECReportLoader
    ===============
    This class is used to load the TEC campaign finance data into a pandas DataFrame.
    """

    def __init__(self, file):
        self.file: CampaignFinanceFileGroup = file

    @staticmethod
    def load(record_type):
        ...

    @abstractmethod
    def to_dataframe(self, list_of_records: list) -> pd.DataFrame:
        ...

    @abstractmethod
    def load_records(self):
        ...

    @abstractmethod
    def load_validated_records(self):
        ...

    @abstractmethod
    def create_record_models(self):
        ...
