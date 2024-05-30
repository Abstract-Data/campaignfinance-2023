import abc
from dataclasses import dataclass
from typing import Type, Tuple, Iterator, Dict, Generator
import itertools
from sqlmodel import SQLModel
import csv
from tqdm import tqdm
from pydantic import ValidationError
from funcs.validation_errors import ValidationErrorList
from funcs.validator_functions import create_record_id
from logger import Logger


ValidatorType = Type[SQLModel]
PassedRecord = Tuple[str, SQLModel]
FailedRecord = Tuple[str, Dict]
PassedFailedIndividualRecord = PassedRecord or FailedRecord
PassedRecordList = Iterator[SQLModel]
FailedRecordList = Iterator[Dict]
PassedFailedRecordList = Tuple[PassedRecordList, FailedRecordList]


@dataclass
class StateFileValidationClass(abc.ABC):
    _logger: Logger = None
    validator_used: ValidatorType = None
    passed: PassedRecordList = None
    failed: FailedRecordList = None
    errors: ValidationErrorList = ValidationErrorList()

    @property
    def logger(self) -> Logger:
        self._logger = Logger(self.__class__.__name__)
        return self._logger

    def validate_record(self, record: Dict) -> PassedFailedIndividualRecord:
        try:
            _record = self.validator_used.model_validate(record)
            _record.id = create_record_id(_record)
            return 'passed', _record
        except ValidationError as e:
            record['error'] = e.errors()
            record['validator'] = self.validator_used.__name__
            self.errors.add_record_errors(record)
            return 'failed', record

    def validate(self, records: Generator[Dict, None, None]) -> Generator[PassedFailedIndividualRecord, None, None]:
        validator = self.validator_used

        def validate_all() -> Generator[PassedFailedIndividualRecord, None, None]:
            self.logger.info(f"Started {validator.__name__} validation")
            for record in records:
                result = self.validate_record(record)
                yield result

        return validate_all()

    def passed_records(self, records: Generator[Dict, None, None]) -> Generator[SQLModel, None, None]:
        for status, record in self.validate(records):
            if status == 'passed':
                yield record

    def failed_records(self, records: Generator[Dict, None, None]) -> Generator[Dict, None, None]:
        for status, record in self.validate(records):
            if status == 'failed':
                yield record

    def _create_error_report(self) -> ValidationErrorList:
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
