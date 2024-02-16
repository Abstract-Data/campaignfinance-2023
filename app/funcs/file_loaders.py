import csv
from pathlib import Path
from typing import Dict, Generator
import datetime
from dataclasses import dataclass


@dataclass
class FileReader:

    @classmethod
    def read_file(cls, file: Path) -> Generator[Dict[int, Dict], None, None]:
        with open(file, "r") as _file:
            _records = csv.DictReader(_file)
            for _record in enumerate(_records):
                _record[1]["file_origin"] = file.stem + '_' + str(
                    datetime.date.today()
                ).replace("-", "")
                yield _record[1]
