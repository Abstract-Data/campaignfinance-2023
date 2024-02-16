# import itertools
# from typing import Iterable, Type, Generator, Dict
# from dataclasses import dataclass, field
# from sqlalchemy.orm import DeclarativeBase, sessionmaker, declarative_base
# from sqlalchemy import create_engine
# from logger import Logger
#
#
# @dataclass
# class PostgresLoader:
#     _base: Type[DeclarativeBase]
#     _built: bool = False
#     table: Type[DeclarativeBase] = field(init=False)
#     models: Iterable[DeclarativeBase] = field(init=False)
#     __logger: Logger = field(init=False)
#
#
#     @property
#     def logger(self):
#         self.__logger = Logger(PostgresLoader.__class__.__name__)
#         return self.__logger
#
#     def build(self, engine: create_engine):
#         self._base.metadata.create_all(engine)
#         self.logger.debug(f"Created tables for {self._base.metadata.tables.keys()}")
#         return self
#
#     def load(self, session: sessionmaker, records: Generator[DeclarativeBase, None, None] = None, **kwargs):
#         _table = kwargs.get('table') if kwargs.get('table') else self.table
#
#         def upload_records(
#                 recs: Generator[DeclarativeBase, None, None],
#                 table=_table):
#             # No limit on the number of records to upload as postgres can handle parsing
#             _errors = []
#             with session() as upload:
#                 self.logger.debug(f"Loading records to {_table}...")
#                 # Load records in chunks of 100,000 at a time from the generator
#                 while True:
#                     _chunks = list(itertools.islice(recs, 1000000))
#                     if not _chunks:
#                         break
#
#                     try:
#                         upload.add_all(_chunks)
#                         upload.commit()
#                     except Exception as e:
#                         upload.rollback()
#                         self.logger.error(f"Error: {e}")
#                         _errors.append(e)
#             return _errors
#
#         errors = upload_records(records)
#         return errors

    # def insert_to_db(self, _list: list, _validator: BaseModel, _config: StateCampaignFinanceConfigs):
    #     _model = self._get_model(_validator, _config)
    #     with _config.DB_SESSION() as upload:
    #         _list = [dict(x) for x in _list]
    #         _add = insert(_model).values(_list).on_conflict_do_nothing()
    #         upload.execute(_add)
    #         upload.commit()

    # def update(self,
    #            session: sessionmaker,
    #            records: Generator[Dict, None, None],
    #            table: declarative_base = None,
    #            primary_key: str = None):
    #     _primary_key = primary_key if primary_key else 'filerIdent'
    #     _table = table if table else self.table
    #     _errors = []
    #
    #     _existing_records, _insert_records = [], []
    #     with session() as upload:
    #         _existing = upload.query(_table).all()
    #         for record in records:
    #             if record[_primary_key] in _existing:
    #                 _existing_records.append(record)
    #             else:
    #                 _insert_records.append(record)
    #         upload.bulk_update_mappings(_table, _existing_records)
    #         upload.bulk_insert_mappings(_table, _insert_records)
    #         upload.commit()
    #     return _errors
        # _table = kwargs.get('table') or self.table
        # _errors = []
        #
        # with session() as upload:
        #     records_to_update = [insert(_table).values(**x.__dict__).on_conflict_do_nothing() for x in records]
        #     try:
        #         upload.add_all(records_to_update)
        #         upload.commit()
        #     except Exception as e:
        #         upload.rollback()
        #         self.__logger.error(f"Error: {e}")
        #         _errors.append(e)

        # return _errors
    # Write an update function that will update any existing records or insert new ones
    # def update_db(self, _list: list, _validator: BaseModel, _config: StateCampaignFinanceConfigs):
    #     self.__logger.debug(f"Called _update_db() for {_validator}...")
    #     _model = self._get_model(_validator, _config)
    #     _loader = PostgresLoader(_config.DB_BASE)
    #     _loader.build(engine=_config.DB_ENGINE)
    #     _loader.create(values=_list, table=_model)
    #     _loader.load(session=_config.DB_SESSION)
    #     self.__logger.debug(f"Loaded {len(_list):,} records to database...")
