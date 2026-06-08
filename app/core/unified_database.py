"""
Database manager for unified SQLModels backed by PostgreSQL.

Wave 3c: engine/session/bootstrap live here; versioning, officers, and analytics
are delegated to focused sub-modules with backward-compat method bindings.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, or_, text
from sqlalchemy.orm import selectinload
from sqlmodel import Session, SQLModel, create_engine, select

from app.core.analytics import UnifiedAnalyticsService
from app.core.enums import TransactionType
from app.core.models import (
    State,
    UnifiedCommittee,
    UnifiedPerson,
    UnifiedTransaction,
)
from app.core.officer_repository import UnifiedOfficerRepository
from app.core.processor import unified_sql_processor
from app.core.repository import (
    UnifiedVersionedRepository,
    _to_json_safe,
)
from app.logger import Logger
from app.states.postgres_config import PostgresConfig

_logger = Logger(__name__)

# Re-export for tests and legacy imports (TASK-4b / task-3c)
__all__ = [
    "UnifiedDatabaseManager",
    "UnifiedVersionedRepository",
    "UnifiedOfficerRepository",
    "UnifiedAnalyticsService",
    "get_db_manager",
    "reset_db_manager_cache",
    "db_manager",
    "_to_json_safe",
]

_REPO_DELEGATES = (
    "update_transaction",
    "get_transaction_versions",
    "update_person",
    "get_person_versions",
    "update_committee",
    "get_committee_versions",
    "update_address",
    "get_address_versions",
    "update_committee_person",
    "get_committee_person_versions",
)

_OFFICER_DELEGATES = (
    "add_person_to_committee",
    "remove_person_from_committee",
    "get_person_committee_roles",
    "get_committee_persons",
    "get_active_treasurers",
    "get_committee_officers",
    "link_transaction_to_committee_role",
    "get_officer_contributions",
    "get_officer_expenditures",
    "get_committee_officer_activities",
    "get_person_committee_financial_summary",
    "auto_link_transactions_to_committee_roles",
    "process_transaction_with_officer_linking",
    "get_unlinked_officer_transactions",
)

_ANALYTICS_DELEGATES = (
    "get_summary_statistics",
    "get_cross_state_analysis",
    "export_to_json",
)


class UnifiedDatabaseManager:
    """Database manager for unified campaign finance data."""

    def __init__(self, database_url: str | None = None, *, echo: bool = False):
        self._config: PostgresConfig | None = None

        if database_url is None:
            config = PostgresConfig()
            if not config.validate_connection():
                raise RuntimeError("Failed to validate PostgreSQL connection.")
            database_url = config.database_url
            self._config = config

        self.database_url = database_url
        engine_kwargs: dict[str, Any] = {"echo": echo}

        if self._config:
            engine_kwargs.update(
                {
                    "pool_size": self._config.pool_size,
                    "max_overflow": self._config.max_overflow,
                    "pool_timeout": self._config.pool_timeout,
                    "pool_recycle": self._config.pool_recycle,
                }
            )

        self.engine = create_engine(self.database_url, **engine_kwargs)

        self.repo = UnifiedVersionedRepository(self.get_session)
        self.officers = UnifiedOfficerRepository(self.get_session, repo=self.repo)
        self.analytics = UnifiedAnalyticsService(
            self.get_session,
            get_transactions_fn=self.get_transactions,
        )
        self._bind_backward_compat_delegates()

    def _bind_backward_compat_delegates(self) -> None:
        """Expose sub-service methods on the manager for existing callers (RF-CPLX-001)."""
        for name in _REPO_DELEGATES:
            setattr(self, name, getattr(self.repo, name))
        for name in _OFFICER_DELEGATES:
            setattr(self, name, getattr(self.officers, name))
        for name in _ANALYTICS_DELEGATES:
            setattr(self, name, getattr(self.analytics, name))

    # Fix 7: partial unique indexes for dedup enforcement (no Alembic — raw DDL here).
    _DEDUP_INDEXES: tuple[str, ...] = (
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uix_persons_name_state
        ON unified_persons (lower(first_name), lower(last_name), state_id)
        WHERE organization IS NULL
          AND first_name IS NOT NULL
          AND last_name IS NOT NULL;
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uix_persons_org_state
        ON unified_persons (lower(organization), state_id)
        WHERE organization IS NOT NULL;
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uix_addresses_city_state_zip_nostreet
        ON unified_addresses (lower(city), lower(state), zip_code)
        WHERE street_1 IS NULL;
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uix_addresses_full
        ON unified_addresses (lower(street_1), lower(city), lower(state), zip_code)
        WHERE street_1 IS NOT NULL;
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uix_txperson_txid_personid_role
        ON unified_transaction_persons (transaction_id, person_id, role);
        """,
        # NOTE: uniqueness on unified_transactions is enforced via
        # ``uix_transactions_state_type_sourceid`` below, which keys on
        # (state_id, transaction_type, transaction_id).  Including
        # ``transaction_type`` is critical: TEC ``transaction_id`` values are only
        # unique *within* a record type, so EXPN and CAND rows reuse the same id
        # for the same committee — a two-column (transaction_id, committee_id)
        # index dropped whole files (cand, credits, assets) as false duplicates.
        # Adding transaction_type disambiguates those record types, making the
        # unique constraint safe and correct.  The plain ix_transactions_source_id
        # index is retained to support fast report-linking lookups.
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uix_transactions_state_type_sourceid
        ON unified_transactions (state_id, transaction_type, transaction_id)
        WHERE transaction_id IS NOT NULL;
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_transactions_source_id
        ON unified_transactions (transaction_id, committee_id)
        WHERE transaction_id IS NOT NULL;
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uix_entities_type_name_state
        ON unified_entities (entity_type, normalized_name, state_id)
        WHERE state_id IS NOT NULL;
        """,
    )

    def bootstrap(self) -> None:
        """Create tables via SQLModel.metadata.create_all and apply dedup indexes (Fix 7)."""
        SQLModel.metadata.create_all(self.engine)
        self._apply_dedup_indexes()

    def _apply_dedup_indexes(self) -> None:
        """Run Fix-7 partial unique indexes idempotently (CREATE … IF NOT EXISTS)."""
        with self.engine.connect() as conn:
            for ddl in self._DEDUP_INDEXES:
                conn.execute(text(ddl))
            conn.commit()
        _logger.info("Dedup unique indexes applied.")

    def _resolve_state_record(self, session: Session, state_identifier: str) -> State | None:
        if not state_identifier:
            return None
        code = state_identifier.strip().upper()
        name = state_identifier.strip().lower()
        return session.exec(
            select(State).where(or_(State.code == code, func.lower(State.name) == name))
        ).first()

    def get_session(self) -> Session:
        return Session(self.engine)

    def load_data_from_file(self, file_path: Path, state: str) -> list[UnifiedTransaction]:
        from ..funcs.csv_reader import FileReader

        reader = FileReader()
        records: list[dict] = []

        if file_path.suffix.lower() == ".parquet":
            records = list(reader.read_parquet(file_path))
        elif file_path.suffix.lower() == ".csv":
            records = list(reader.read_csv(file_path))

        with self.get_session() as session:
            state_record = self._resolve_state_record(session, state)
            if not state_record:
                raise ValueError(f"State '{state}' is not present in the states table.")
            transactions = unified_sql_processor.process_records(
                records, state, state_id=state_record.id, state_code=state_record.code
            )

        _logger.info(f"Loaded {len(transactions)} transactions from {file_path}")
        return transactions

    def save_transactions(self, transactions: list[UnifiedTransaction]) -> int:
        with self.get_session() as session:
            for transaction in transactions:
                session.add(transaction)
            session.commit()

        _logger.info(f"Saved {len(transactions)} transactions to database")
        return len(transactions)

    def load_and_save_file(self, file_path: Path, state: str) -> int:
        transactions = self.load_data_from_file(file_path, state)
        return self.save_transactions(transactions)

    def get_transactions(
        self,
        state: str | None = None,
        transaction_type: TransactionType | None = None,
        limit: int | None = None,
        load_relationships: bool = True,
    ) -> list[UnifiedTransaction]:
        with self.get_session() as session:
            query = select(UnifiedTransaction)

            if state:
                state_record = self._resolve_state_record(session, state)
                if not state_record:
                    return []
                query = query.where(UnifiedTransaction.state_id == state_record.id)

            if transaction_type:
                query = query.where(UnifiedTransaction.transaction_type == transaction_type)

            if limit:
                query = query.limit(limit)

            if load_relationships:
                query = query.options(
                    selectinload(UnifiedTransaction.persons),
                    selectinload(UnifiedTransaction.committee),
                    selectinload(UnifiedTransaction.state),
                    selectinload(UnifiedTransaction.file_origin),
                )

            return session.exec(query).all()

    def get_transaction_by_id(self, transaction_id: str) -> UnifiedTransaction | None:
        with self.get_session() as session:
            query = select(UnifiedTransaction).where(
                UnifiedTransaction.transaction_id == transaction_id
            )
            return session.exec(query).first()

    def get_person_by_name(self, first_name: str, last_name: str) -> list[UnifiedPerson]:
        with self.get_session() as session:
            query = select(UnifiedPerson).where(
                UnifiedPerson.first_name == first_name, UnifiedPerson.last_name == last_name
            )
            return session.exec(query).all()

    def get_committee_by_name(self, name: str) -> list[UnifiedCommittee]:
        with self.get_session() as session:
            query = select(UnifiedCommittee).where(UnifiedCommittee.name == name)
            return session.exec(query).all()

    def get_transactions_by_amount_range(
        self, min_amount: float, max_amount: float, state: str | None = None
    ) -> list[UnifiedTransaction]:
        with self.get_session() as session:
            query = select(UnifiedTransaction).where(
                UnifiedTransaction.amount >= min_amount, UnifiedTransaction.amount <= max_amount
            )

            if state:
                state_record = self._resolve_state_record(session, state)
                if not state_record:
                    return []
                query = query.where(UnifiedTransaction.state_id == state_record.id)

            return session.exec(query).all()

    def get_transactions_by_date_range(
        self, start_date: str, end_date: str, state: str | None = None
    ) -> list[UnifiedTransaction]:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        with self.get_session() as session:
            query = select(UnifiedTransaction).where(
                UnifiedTransaction.transaction_date >= start,
                UnifiedTransaction.transaction_date <= end,
            )

            if state:
                state_record = self._resolve_state_record(session, state)
                if not state_record:
                    return []
                query = query.where(UnifiedTransaction.state_id == state_record.id)

            return session.exec(query).all()


db_manager: UnifiedDatabaseManager | None = None
_db_manager_cached: UnifiedDatabaseManager | None = None


def get_db_manager(
    database_url: str | None = None,
    *,
    echo: bool = False,
    bootstrap: bool = True,
) -> UnifiedDatabaseManager:
    """Return a process-wide cached UnifiedDatabaseManager (lazy init, P2-ARC-002)."""
    global _db_manager_cached
    if _db_manager_cached is None:
        _db_manager_cached = UnifiedDatabaseManager(database_url, echo=echo)
        if bootstrap:
            _db_manager_cached.bootstrap()
    return _db_manager_cached


def reset_db_manager_cache() -> None:
    """Reset the cached manager (tests only)."""
    global _db_manager_cached
    _db_manager_cached = None
