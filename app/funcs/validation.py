from typing import Tuple, Dict, Generator, Type
from dataclasses import dataclass
from tqdm import tqdm
from pydantic import ValidationError, BaseModel
from logger import Logger
import itertools


PassedRecords = Generator[BaseModel, None, None]
FailedRecords = Generator[Dict, None, None]
PassedFailedRecords = Tuple[PassedRecords, FailedRecords]


@dataclass
class StateFileValidation:
    _logger: Logger = None
    passed: PassedRecords = None
    failed: FailedRecords = None

    @property
    def logger(self) -> Logger:
        self._logger = Logger(self.__class__.__name__)
        return self._logger

    def validate_records(self, _records: Generator[Dict, None, None], _validator: Type[BaseModel]) -> PassedFailedRecords:
        self.logger.info(f"Started validation")
        for y in tqdm(_records, desc="Validating records", unit=" records"):
            try:
                _record = _validator(**y)
                yield 'passed', dict(_record)
            except ValidationError as e:
                y['error'] = str(e.errors())
                yield 'failed', dict(y)

    def validate(self, records: Generator[Dict, None, None], validator: Type[BaseModel]) -> 'StateFileValidation':
        self.logger.info(f"Created validation generators")
        passed_gen, failed_gen = itertools.tee((x for x in self.validate_records(records, validator)))

        self.passed = (record for status, record in passed_gen if status == 'passed')
        self.failed = (record for status, record in failed_gen if status == 'failed')
        return self
