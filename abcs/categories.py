from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import dataclass
import csv
from tqdm import tqdm
from typing import Generator, Protocol
import datetime
from abcs import StateCampaignFinanceConfigs, FileDownloader

@dataclass
class StateCategories(Protocol):
    expenses: ClassVar[List] = None
    contributions: ClassVar[List] = None
    filers: ClassVar[List] = None
    _config: ClassVar[StateCampaignFinanceConfigs]

    def __repr__(self):
        return f"StateCategories()"

    @classmethod
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
    @classmethod
    def _generate_list(cls, pfx, fldr: FileDownloader.folder) -> List:
        _files = []
        for file in fldr.glob("*.csv"):
            if file.stem.startswith(pfx):
                _files.append(file)
            else:
                pass
        # return {k: v for file in f for k, v in file.items()}
        return sorted(_files)

    @classmethod
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
                cls._config.EXPENSE_FILE_PREFIX,
                cls._config.CONTRIBUTION_FILE_PREFIX,
                cls._config.FILERS_FILE_PREFIX,
            ]
        ]

        StateCategories.filers = extract_records(_filers)

        if record_kind == "expenses":
            cls.expenses = extract_records(_expenses)
            return cls.expenses

        elif record_kind == "contributions":
            cls.contributions = extract_records(_contributions)
            return cls.contributions

        else:
            cls.expenses = extract_records(_expenses)
            cls.contributions = extract_records(_contributions)
            return cls.expenses, cls.contributions