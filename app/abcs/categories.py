from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import dataclass, field
import csv
from tqdm import tqdm
from typing import Generator, Protocol, Iterator, Tuple, Optional, Any
import datetime
from abcs import StateCampaignFinanceConfigs, FileDownloader
from logger import Logger
from abcs import StateCampaignFinanceConfigs
import funcs

logger = Logger(__name__)


# @dataclass
# class TECCategory:
#     category: str
#     config: StateCampaignFinanceConfigs
#     _files: Optional[Generator[Path, Any, None]] = None
#     __logger: Logger = field(init=False)
#
#
#     def __repr__(self):
#         return f"TECFileCategories({self.category})"
#
#     @property
#     def logger(self):
#         self.__logger = Logger(self.__class__.__name__)
#         return self.__logger
#
#     def __post_init__(self):
#         if self.category == "expenses":
#             _f, _v, _m, _pk = (self._create_tec_files(
#                 StateCampaignFinanceConfigs.EXPENSE_FILE_PREFIX
#             ),
#                                StateCampaignFinanceConfigs.EXPENSE_VALIDATOR,
#                                StateCampaignFinanceConfigs.EXPENSE_MODEL,
#                                "expendInfoId"
#             )
#         elif self.category == "contributions":
#             _f, _v, _m, _pk = (self._create_tec_files(
#                 StateCampaignFinanceConfigs.CONTRIBUTION_FILE_PREFIX
#             ),
#                                StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR,
#                                StateCampaignFinanceConfigs.CONTRIBUTION_MODEL,
#                                "contributionInfoId"
#             )
#         elif self.category == "filers":
#             _f, _v, _m, _pk = (self._create_tec_files(
#                 StateCampaignFinanceConfigs.FILERS_FILE_PREFIX
#             ),
#                                StateCampaignFinanceConfigs.FILERS_VALIDATOR,
#                                StateCampaignFinanceConfigs.FILERS_MODEL,
#                                "filerIdent"
#             )
#         else:
#             raise ValueError(f"Invalid category: {self.category}")
#         self.files = _f
#         self.validator = _v
#         self.sql_model = _m
#         self.primary_key = _pk
#
#     @classmethod
#     def _create_tec_files(
#             cls,
#             prefix: str,
#             folder: StateCampaignFinanceConfigs.TEMP_FOLDER = StateCampaignFinanceConfigs.TEMP_FOLDER
#     ) -> Generator[Path, Any, None]:
#         return (x for x in generate_file_list(folder) if x.name.startswith(prefix))
#
#     def read(self) -> Generator[Dict, None, None]:
#         records = (record for file in self.files for record in funcs.FileReader.read_file(file))
#         # if self.category == "filers":
#         #     records = merge_filer_names(records)
#         return records
#
#     def load(self) -> List[Dict]:
#         return list(x for x in self.read())
#
#     def validate(self,
#                  records: Generator[Dict, None, None] = None,
#                  validator: Type[BaseModel] = None
#                  ) -> funcs.PassedFailedRecords:
#         return funcs.StateFileValidation.validate(records=self.read(), validator=self.validator)
#
#     def create_models(self, records: funcs.PassedRecords = None) -> SQLModels:
#         if not records:
#             self.models = (self.sql_model(**dict(x)) for x in self.passed)
#         self.logger.info(f"Created {self.category} model generator...")
#         return self.models
#
#     def add_to_database(self, models: SQLModels = None) -> None:
#         _db = PostgresLoader(Base)
#         _db.build(engine=engine)
#         if not models:
#             models = self.create_models()
#         _db.load(
#             records=models,
#             session=SessionLocal,
#             table=self.sql_model
#         )
#
#     def update_database(self, models: Generator[BaseModel, None, None] = None) -> None:
#         _db = PostgresLoader(Base)
#         _db.build(engine=engine)
#         if not models:
#             models = (dict(x) for x in self.passed)
#             _db = PostgresLoader(Base)
#             _db.update(
#                 records=models,
#                 session=SessionLocal,
#                 table=self.sql_model,
#                 primary_key=self.primary_key
#             )
