from __future__ import annotations
from sqlmodel import SQLModel, Session, create_engine, select
from logger import Logger
from typing import Iterator, List, Type, Generator
from dataclasses import dataclass
import itertools
import contextlib
from tqdm import tqdm


@contextlib.contextmanager
def session_scope(_engine: create_engine):
    """Provide a transactional scope around a series of operations."""

    session = Session(_engine)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@dataclass
class DBLoader:
    """
    DBLoader
    =============
    This class is used to load data into the database.

    :param session: Session (SQLModel Session object)
    """

    engine: create_engine

    def __post_init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.logger.info(f"DBLoader initialized")

    def create_all(self) -> None:
        """
        Create all tables in the database.
        :param engine: SQLModel engine
        :return: None
        """
        self.logger.info(f"Creating all tables")
        SQLModel.metadata.create_all(self.engine)

    def remove_existing_records(self,
                                records: List[SQLModel] | Iterator[SQLModel],
                                validator: Type[SQLModel]) -> Iterator[SQLModel]:
        """
        Check for existing data in the database.
        :param session:
        :param records:
        :param validator: SQLModel
        :return: Iterator[SQLModel]
        """
        with Session(self.engine) as session:
            self.logger.info(f"Checking for existing data in the database")
            _existing_records = set(record.id for record in session.exec(select(validator.id)).all())
            self.logger.info(f"Found {len(_existing_records):,} existing records in the database")
            remaining_records = (x for x in records if x.id not in _existing_records)
        return iter(remaining_records)

    def add_all(self, records: Generator[SQLModel, None, None], session: Session) -> None:
        """
        Add all records to the database.
        :param session:
        :param records: Iterable[SQLModel]
        :return: None
        """
        try:
            self.logger.info(f"Adding all records.")
            for record in records:
                session.add(record)
            session.commit()
        except StopIteration:
            self.logger.info(f"No records in iterator.")

    def add_with_limits(self, records: Generator[SQLModel, None, None], limit: int, session: Session) -> None:
        """
        Add all records with a limit.
        :param session:
        :param records: Iterable[SQLModel]
        :param limit: int
        :return:
        """
        added_count = 0
        while True:
            self.logger.info(f"Adding records with limit of {limit:,}.")
            record_limit = list(itertools.islice(records, limit))
            if not record_limit:
                break
            session.add_all(record_limit)
            session.commit()
            added_count += len(record_limit)
            self.logger.info(f"{added_count:,} records added")

    def add(self, records: Iterator[SQLModel], record_type: Type[SQLModel], **kwargs) -> None:
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
        _add_limit = kwargs.get("add_limit", None)

        # Convert records to an iterator
        _records = (record for record in records)

        with Session(self.engine) as session:
            if isinstance(_add_limit, int):
                self.add_with_limits(_records, _add_limit, session)
            else:
                self.add_all(_records, session)

        self.logger.info(f"{_type} records added")

    def update_records(self, records: Generator[SQLModel, None, None]):
        with Session(self.engine) as session:
            for new_record in records:
                existing_record = session.get(new_record.__class__, new_record.id)
                if existing_record:
                    for attr in new_record.__dict__.keys():
                        if hasattr(existing_record, attr):
                            setattr(existing_record, attr, getattr(new_record, attr))
            self.logger.info(f"Updating records")
            session.commit()
        self.logger.info(f"Records updated")

    def rollback(self):
        """
        Rollback the session.
        :return: None
        """
        with Session(self.engine) as session:
            self.logger.info(f"Rolling back session")
            session.rollback()
            self.logger.info(f"Session rolled back")
