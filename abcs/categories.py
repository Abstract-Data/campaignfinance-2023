from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import dataclass
import csv
from tqdm import tqdm
from typing import  Generator
import datetime
from abc import ABC, abstractmethod
from abcs import StateCampaignFinanceConfigs

@dataclass
class StateCategories(ABC):
    expenses: ClassVar[List] = None
    contributions: ClassVar[List] = None
    filers: ClassVar[List] = None
    _config: ClassVar[StateCampaignFinanceConfigs]

    def __repr__(self):
        return f"StateCategories()"

    @abstractmethod
    def read_file(cls, file: Path) -> Generator[Dict, None, None]:
        with open(file, "r") as _file:
            _records = csv.DictReader(_file)
            records = {}
            for _record in tqdm(enumerate(_records), desc=f"Reading {file.name}"):
                _record[1]["file_origin"] = file.name + str(
                    datetime.date.today()
                ).replace("-", "")
                yield _record

        # yield {records[x].values() for x in records}

    @abstractmethod
    def _generate_list(cls, pfx) -> List:
        _files = []
        for file in StateCategories._config.FOLDER.glob("*.csv"):
            if file.stem.startswith(pfx):
                _files.append(file)
            else:
                pass
        # return {k: v for file in f for k, v in file.items()}
        return sorted(_files)

    @abstractmethod
    def load(cls, record_kind: str = None):
        def extract_records(record_type):
            _records = []
            for file in record_type:
                file_records = cls.read_file(file)
                _records.extend(x[1] for x in file_records)
            return _records

        _expenses, _contributions, _filers = [
            cls._generate_list(x)
            for x in [
                StateCategories._config.EXPENSE_FILE_PREFIX,
                StateCategories._config.CONTRIBUTION_FILE_PREFIX,
                StateCategories._config.FILERS_FILE_PREFIX,
            ]
        ]

        StateCategories.filers = extract_records(_filers)

        if record_kind == "expenses":
            StateCategories.expenses = extract_records(_expenses)
            return StateCategories.expenses

        elif record_kind == "contributions":
            StateCategories.contributions = extract_records(_contributions)
            return StateCategories.contributions

        else:
            StateCategories.expenses = extract_records(_expenses)
            StateCategories.contributions = extract_records(_contributions)
            return StateCategories.expenses, StateCategories.contributions