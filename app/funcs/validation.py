from typing import Tuple, Dict, Generator, Type
from dataclasses import dataclass
from tqdm import tqdm
from pydantic import ValidationError
from sqlmodel import SQLModel
from logger import Logger
from joblib import Parallel, delayed
import itertools


PassedRecords = Generator[SQLModel, None, None]
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

    def validate_record(self, record: Dict, _validator: Type[SQLModel]) -> Tuple[str, SQLModel] | Tuple[str, Dict]:
        try:
            _record = _validator.model_validate(record)
            return 'passed', _record
        except ValidationError as e:
            record['error'] = str(e.errors())
            return 'failed', dict(record)

    def validate(self, records: Generator[Dict, None, None], validator: Type[SQLModel]) -> 'StateFileValidation':
        self.logger.info(f"Started validation")
        results = Parallel(
            n_jobs=-1
        )(
            delayed(
                self.validate_record
            )(
                record,
                validator
            ) for record in tqdm(
                records,
                desc=f"Validating records {validator.__name__}",
                unit=" records",
            )
        )
        self.logger.info(f"Finished validation")

        self.logger.info(f"Created validation generators")
        passed_gen, failed_gen = itertools.tee((x for x in results))

        self.passed = (record for status, record in passed_gen if status == 'passed')
        self.failed = (record for status, record in failed_gen if status == 'failed')
        return self
