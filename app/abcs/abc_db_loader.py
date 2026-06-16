from __future__ import annotations

import abc
import contextlib
import itertools
from dataclasses import dataclass
from typing import Iterator, List, Optional, Type

import inject
from sqlmodel import Session, SQLModel, create_engine, select

from app.core.upsert import bulk_upsert
from app.logger import Logger


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
        self.logger.info("DBLoader base config set")

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
            self.logger.info("Session committed using session_scope method")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
            self.logger.info("Session closed using session_scope method")

    @abc.abstractmethod
    def create_all(self, engine: create_engine) -> None:
        """
        Create all tables in the database.
        :param engine: SQLModel engine
        :return: None
        """
        self.logger.info("Creating all tables")
        SQLModel.metadata.create_all(engine)

    @staticmethod
    def _pk_cols(model: Type[SQLModel]) -> List[str]:
        """Return the primary-key column names for *model*.

        Used internally to derive *conflict_cols* for :func:`bulk_upsert`.

        Raises a clear ``TypeError`` if *model* is not a table-backed SQLModel
        (``table=True``) — otherwise ``model.__table__`` would raise an opaque
        ``AttributeError`` deep in the upsert path.
        """
        if not hasattr(model, "__table__"):
            raise TypeError(
                f"{getattr(model, '__name__', model)!r} is not a table-backed "
                "SQLModel (no __table__); upsert needs a table=True model."
            )
        return [c.name for c in model.__table__.primary_key.columns]

    @inject.autoparams()
    def remove_existing_records(
        self,
        records: List[SQLModel] | Iterator[SQLModel],
        validator: Type[SQLModel],
        session: Session,
    ) -> Iterator[SQLModel]:
        """Check for existing data in the database and return only new records.

        .. deprecated::
            This method has **zero callers** in the current codebase and is
            superseded by the DO UPDATE upsert path in :meth:`add` /
            :meth:`add_all` / :meth:`add_with_limits`.  It is retained for
            backward-compatibility but should not be used in new code.

        :param session: active SQLModel session (injected)
        :param records: iterable of SQLModel instances to filter
        :param validator: SQLModel subclass whose ``.id`` column is queried
        :return: Iterator[SQLModel] — records not already in the database
        """
        self.logger.info("Checking for existing data in the database")
        _existing_records = set(record.id for record in session.exec(select(validator.id)).all())
        self.logger.info(f"Found {len(_existing_records):,} existing records in the database")
        remaining_records = (x for x in records if x.id not in _existing_records)
        return iter(remaining_records)

    @inject.autoparams()
    def add_all(
        self,
        records: Iterator[SQLModel],
        session: Session,
        record_type: Optional[Type[SQLModel]] = None,
    ) -> None:
        """Add all records to the database using DO UPDATE upsert semantics.

        Converts each SQLModel instance to a plain dict via ``model_dump()``
        and delegates to :func:`~app.core.upsert.bulk_upsert` so that
        re-inserted rows overwrite existing ones (amended TEC records are
        handled correctly).

        When *record_type* is not provided the model class is inferred from
        the first record in the iterator.  If the iterator is empty the call
        is a no-op.

        :param session: active SQLModel session (injected)
        :param records: iterable of SQLModel instances
        :param record_type: optional model class; inferred from first record
            when omitted
        :return: None
        """
        record_list = list(records)
        if not record_list:
            self.logger.info("No records in iterator.")
            return

        model = record_type if record_type is not None else type(record_list[0])
        conflict_cols = self._pk_cols(model)
        row_dicts = [r.model_dump() for r in record_list]
        total = bulk_upsert(session, model, row_dicts, conflict_cols=conflict_cols)
        self.logger.info(f"Upserted {total:,} records via add_all.")

    @inject.autoparams()
    def add_with_limits(
        self,
        records: Iterator[SQLModel],
        limit: int,
        session: Session,
        record_type: Optional[Type[SQLModel]] = None,
    ) -> None:
        """Add records in batches of *limit* using DO UPDATE upsert semantics.

        Each batch is converted to dicts and forwarded to
        :func:`~app.core.upsert.bulk_upsert`.  The model class is taken from
        *record_type* when provided, otherwise inferred from the first record
        in each batch.

        :param session: active SQLModel session (injected)
        :param records: iterable of SQLModel instances
        :param limit: maximum records per batch
        :param record_type: optional model class; inferred from first record
            when omitted
        :return: None
        """
        records = iter(records)
        model: Optional[Type[SQLModel]] = record_type

        while True:
            record_limit = list(itertools.islice(records, limit))
            if not record_limit:
                break

            if model is None:
                model = type(record_limit[0])

            conflict_cols = self._pk_cols(model)
            row_dicts = [r.model_dump() for r in record_limit]
            total = bulk_upsert(session, model, row_dicts, conflict_cols=conflict_cols)
            self.logger.info(f"Upserted {total:,} records in batch (limit={limit:,}).")

    @inject.autoparams()
    def add(
        self, records: Iterator[SQLModel], record_type: SQLModel, session: Session, **kwargs
    ) -> None:
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
            self.add_with_limits(records, _add_limit, session, record_type=record_type)
        else:
            self.logger.info(f"Adding all {_type} records.")
            self.add_all(records, session, record_type=record_type)

        self.logger.info(f"{_type} records added")

    @inject.autoparams()
    def update_records(self, records: Iterator[SQLModel], session: Session):
        for new_record in records:
            existing_record = session.get(new_record.__class__, new_record.id)
            if existing_record:
                for attr in new_record.__dict__.keys():
                    if hasattr(existing_record, attr):
                        setattr(existing_record, attr, getattr(new_record, attr))
        self.logger.info("Updating records")
        session.commit()
        self.logger.info("Records updated")

    @inject.autoparams()
    def rollback(self, session: Session):
        """
        Rollback the session.
        :return: None
        """
        self.logger.info("Rolling back session")
        session.rollback()
        self.logger.info("Session rolled back")
