from typing import Protocol, List, Iterable, Type
from pydantic import BaseModel
from dataclasses import dataclass, field
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import create_engine
from tqdm import tqdm
from abcs import StateCampaignFinanceConfigs
from logger import Logger
from sqlalchemy.dialects.postgresql import insert


@dataclass
class PostgresLoader:
    _base: Type[DeclarativeBase]
    table: Type[DeclarativeBase] = field(init=False)
    models: Iterable[DeclarativeBase] = field(init=False)
    __logger: Logger = field(init=False)

    def __post_init__(self):
        self.__logger = Logger(PostgresLoader.__class__.__name__)

    def build(self, engine: create_engine):
        self._base.metadata.create_all(engine)
        print("Created Postgres tables")

    def create(self, values: List[dict], table: Type[DeclarativeBase]):
        self.table = table

        _models = []
        for v in tqdm(values, desc=f"Creating {table.__tablename__} models"):
            _v = dict(v)
            _models.append(table(**_v))
        self.models = iter(_models)
        return self.table, self.models

    def load(self, session: sessionmaker, records: List = None, table: Type[DeclarativeBase] = None):

        _records = records if records else self.models
        _table = table if table else self.table
        def upload_records(recs):
            _errors = []
            with session() as upload:
                try:
                    upload.add_all(recs)
                    upload.commit()
                except Exception as e:
                    upload.rollback()
                    self.__logger.error(f"Error: {e}")
                    _errors.append(e)
            return _errors

        errors = []
        _queue = []
        for rec in tqdm(_records, desc=f"Loading to {_table.__tablename__}"):
            rec_model = _table(**dict(rec))
            _queue.append(rec_model)
            if len(_queue) == 16000:
                _errors = upload_records(_queue)
                _queue = []
                errors.append(_errors)
        _errors = upload_records(_queue)
        errors.append(_errors)
        return errors

    def _get_model(self, _validator: Type[BaseModel], _config: StateCampaignFinanceConfigs):
        if _validator.__name__ == "TECExpense":
            _model = _config.EXPENSE_MODEL
        elif _validator.__name__ == "TECContribution":
            _model = _config.CONTRIBUTION_MODEL
        elif _validator.__name__ == "TECFiler":
            _model = _config.FILERS_MODEL
        else:
            raise ValueError(f"Invalid type: {_validator.__name__}")
        return _model

    # def insert_to_db(self, _list: list, _validator: BaseModel, _config: StateCampaignFinanceConfigs):
    #     _model = self._get_model(_validator, _config)
    #     with _config.DB_SESSION() as upload:
    #         _list = [dict(x) for x in _list]
    #         _add = insert(_model).values(_list).on_conflict_do_nothing()
    #         upload.execute(_add)
    #         upload.commit()

    def add_to_db(self, _list: list,  _validator: BaseModel, _config: StateCampaignFinanceConfigs):
        self.__logger.debug(f"Called _load_to_db() for {_validator}...")
        _model = self._get_model(_validator, _config)
        _loader = PostgresLoader(_config.DB_BASE)
        _loader.build(engine=_config.DB_ENGINE)
        _loader.create(values=_list, table=_model)
        _loader.load(session=_config.DB_SESSION)
        self.__logger.debug(f"Loaded {len(_list):,} records to database...")




