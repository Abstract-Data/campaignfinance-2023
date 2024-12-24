import abc
from dataclasses import dataclass, field
from typing import Type, Tuple, Iterator, Dict, Generator
import itertools
from sqlmodel import SQLModel
import csv
from tqdm import tqdm
from pydantic import ValidationError
from abcs.abc_validation_errors import ValidationErrorList
from funcs.validator_functions import create_record_id
from logger import Logger
from icecream import ic

ic.configureOutput(prefix='abc_validation|')

ValidatorType = Type[SQLModel]
PassedRecord = Tuple[str, SQLModel]
FailedRecord = Tuple[str, Dict]
PassedFailedIndividualRecord = PassedRecord or FailedRecord
PassedRecordList = Iterator[SQLModel]
FailedRecordList = Iterator[Dict]
PassedFailedRecordList = Tuple[PassedRecordList, FailedRecordList]


@dataclass
class StateFileValidation(abc.ABC):
    validator_to_use: ValidatorType
    _logger: Logger = None
    passed: PassedRecordList = None
    failed: FailedRecordList = None
    errors: ValidationErrorList = field(default_factory=ValidationErrorList)

    @property
    def logger(self) -> Logger:
        self._logger = Logger(self.__class__.__name__)
        return self._logger

    def validate_record(self, record: Dict) -> PassedFailedIndividualRecord:
        try:
            _record = self.validator_to_use.model_validate(record)
            _record.id = create_record_id(_record)
            return 'passed', _record
        except ValidationError as e:
            _errors = e.errors()
            for error in _errors:
                error['validator'] = self.validator_to_use.__name__
            record['error'] = _errors
            self.errors.add_record_errors(record)
            return 'failed', record

    def validate(self, records: Generator[Dict, None, None]) -> Generator[PassedFailedIndividualRecord, None, None]:
        validator = self.validator_to_use

        self.logger.info(f"Started {validator.__name__} validation")
        for record in tqdm(records, desc=f"Validating {validator.__name__}", unit='records', leave=True, mininterval=1):
            result = self.validate_record(record)
            yield result

    def passed_records(self, records: Generator[Dict, None, None]) -> Generator[SQLModel, None, None]:
        for status, record in self.validate(records):
            if status == 'passed':
                yield record

    def failed_records(self, records: Generator[Dict, None, None]) -> Generator[Dict, None, None]:
        for status, record in self.validate(records):
            if status == 'failed':
                yield record

    def _create_error_report(self) -> ValidationErrorList:
        if not self.failed:
            ic("No failed records found")
        if self.failed:
            self.failed, fails = itertools.tee(self.failed, 2)
            self.errors.create_error_list(list(fails))
        return self.errors

    def show_errors(self):
        self._create_error_report()
        self.errors.show_errors()

    @staticmethod
    def write_records_to_csv(records, filename) -> None:
        if records:
            with open(filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=records[0].keys())
                writer.writeheader()
                writer.writerows(records)
