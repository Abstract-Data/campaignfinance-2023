from __future__ import annotations
import abc
from sqlmodel import SQLModel, Session, create_engine, select
from logger import Logger
from typing import Iterator, List, Type
from dataclasses import dataclass
import itertools
import contextlib
import inject


@dataclass
class DBLoaderClass(abc.ABC):
    """
    DBLoader
    =============
    This class is used to load data into the database.

    :param session: Session (SQLModel Session object)
    """

    engine: create_engine

    def __post_init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.logger.info(f"DBLoader for {self.__class__.__name__} initialized")

    def init(self):
        inject.configure(self.db_loader_base_config)

    @abc.abstractmethod
    def db_loader_base_config(self, binder):
        binder.bind_to_provider(Session, self.session_scope)
        self.logger.info(f"DBLoader base config set")

    @contextlib.contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""

        if self.engine:
            _engine = self.engine
        else:
            raise ValueError("No engine provided")

        session = Session(_engine)
        try:
            yield session
            session.commit()
            self.logger.info(f"Session committed using session_scope method")
        except:
            session.rollback()
            raise
        finally:
            session.close()
            self.logger.info(f"Session closed using session_scope method")

    @abc.abstractmethod
    def create_all(self, engine: create_engine) -> None:
        """
        Create all tables in the database.
        :param engine: SQLModel engine
        :return: None
        """
        self.logger.info(f"Creating all tables")
        SQLModel.metadata.create_all(engine)

    @inject.autoparams()
    def remove_existing_records(self,
                                records: List[SQLModel] | Iterator[SQLModel],
                                validator: Type[SQLModel],
                                session: Session) -> Iterator[SQLModel]:
        """
        Check for existing data in the database.
        :param session:
        :param records:
        :param validator: SQLModel
        :return: Iterator[SQLModel]
        """
        self.logger.info(f"Checking for existing data in the database")
        _existing_records = set(record.id for record in session.exec(select(validator.id)).all())
        self.logger.info(f"Found {len(_existing_records):,} existing records in the database")
        remaining_records = (x for x in records if x.id not in _existing_records)
        return iter(remaining_records)

    @inject.autoparams()
    def add_all(self, records: Iterator[SQLModel], session: Session) -> None:
        """
        Add all records to the database.
        :param session:
        :param records: Iterable[SQLModel]
        :return: None
        """
        try:
            for record in records:
                session.add(record)
            session.commit()
        except StopIteration:
            self.logger.info(f"No records in iterator.")

    @inject.autoparams()
    def add_with_limits(self, records: Iterator[SQLModel], limit: int, session: Session) -> None:
        """
        Add all records with a limit.
        :param session:
        :param records: Iterable[SQLModel]
        :param limit: int
        :return:
        """
        while True:
            record_limit = list(itertools.islice(records, limit))
            if not record_limit:
                break
            session.add_all(record_limit)
            session.commit()

    @inject.autoparams()
    def add(self, records: Iterator[SQLModel], record_type: SQLModel, session: Session, **kwargs) -> None:
        """
        Add records to the database with optional limit.
        Uses add_all or add_with_limits.
        :param session:
        :param record_type: Category of records (str)
        :param records: Iterator[SQLModel]
        :param kwargs: add_limit: int
        :return: None
        """
        _type = record_type.__name__
        _add_limit = kwargs.get("add_limit")

        # Convert records to an iterator
        records = iter(records)

        if _add_limit:
            self.logger.info(f"Adding {_type} records with limit of {_add_limit:,}.")
            self.add_with_limits(records, _add_limit, session)
        else:
            self.logger.info(f"Adding all {_type} records.")
            self.add_all(records, session)

        self.logger.info(f"{_type} records added")

    @inject.autoparams()
    def update_records(self, records: Iterator[SQLModel], session: Session):
        for new_record in records:
            existing_record = session.get(new_record.__class__, new_record.id)
            if existing_record:
                for attr in new_record.__dict__.keys():
                    if hasattr(existing_record, attr):
                        setattr(existing_record, attr, getattr(new_record, attr))
        self.logger.info(f"Updating records")
        session.commit()
        self.logger.info(f"Records updated")

    @inject.autoparams()
    def rollback(self, session: Session):
        """
        Rollback the session.
        :return: None
        """
        self.logger.info(f"Rolling back session")
        session.rollback()
        self.logger.info(f"Session rolled back")
