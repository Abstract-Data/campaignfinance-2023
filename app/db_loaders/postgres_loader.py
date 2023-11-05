import itertools
from typing import Protocol, List, Iterable, Type, Generator, Iterator
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
    _built: bool = False
    table: Type[DeclarativeBase] = field(init=False)
    models: Iterable[DeclarativeBase] = field(init=False)
    __logger: Logger = field(init=False)

    def __post_init__(self):
        self.__logger = Logger(PostgresLoader.__class__.__name__)

    def build(self, engine: create_engine):
        self._base.metadata.create_all(engine)
        print("Created Postgres tables")
        return self

    def create(self, values: List[dict], table: Type[DeclarativeBase]):
        self.table = table

        def model_generator() -> Generator[DeclarativeBase, dict, None]:
            for v in iter(values):
                yield table(**v)

        self.__logger.info("Generating models...")
        return model_generator()

    def load(self, session: sessionmaker, records: List[dict] = None, **kwargs):
        _table = kwargs.get('table') if kwargs.get('table') else self.table

        def upload_records(recs: List[dict], table=_table):
            # No limit on the number of records to upload as postgres can handle parsing
            _errors = []
            with session() as upload:
                try:
                    upload.add_all([x for x in self.create(recs, table)])
                    upload.commit()
                except Exception as e:
                    upload.rollback()
                    self.__logger.error(f"Error: {e}")
                    _errors.append(e)
            return _errors

        errors = upload_records(records)
        return errors

    def _get_model(
        self, _validator: Type[BaseModel], _config: StateCampaignFinanceConfigs
    ):
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

    def update_records(self, session: sessionmaker, records: List[BaseModel] | List[DeclarativeBase], **kwargs):
        _table = kwargs.get('table') if kwargs.get('table') else self.table
        _errors = []
        with session() as upload:
            try:
                upload.execute(insert(_table).values(records).on_conflict_do_nothing())
                upload.commit()
            except Exception as e:
                upload.rollback()
                self.__logger.error(f"Error: {e}")
                _errors.append(e)
        return _errors
    # Write an update function that will update any existing records or insert new ones
    # def update_db(self, _list: list, _validator: BaseModel, _config: StateCampaignFinanceConfigs):
    #     self.__logger.debug(f"Called _update_db() for {_validator}...")
    #     _model = self._get_model(_validator, _config)
    #     _loader = PostgresLoader(_config.DB_BASE)
    #     _loader.build(engine=_config.DB_ENGINE)
    #     _loader.create(values=_list, table=_model)
    #     _loader.load(session=_config.DB_SESSION)
    #     self.__logger.debug(f"Loaded {len(_list):,} records to database...")
