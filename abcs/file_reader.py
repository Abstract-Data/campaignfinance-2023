from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Dict, Generator, Type, Protocol
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


# @dataclass
# class CampaignFinanceFileReader(Protocol):
#     """
#     TECFileReader
#     =============
#     This class is used to read TEC campaign finance files.
#     It is used to read the files from the TEC website and
#     validate the data in the files.
#     """
#     file_list: Path
#
#
#     def __repr__(self):
#         return f"{self.file_list.name}"
#
#     def __str__(self):
#         return f"{self.file_list.name}"
#
#     def __post_init__(self):
#         self.path: Path = field(default_factory=Path)
#         self.records: Dict = self.read_files()
#
#     def read_files(self, category: str):
#         ...

    # @abstractmethod
    # def validate(self):
    #     ...
