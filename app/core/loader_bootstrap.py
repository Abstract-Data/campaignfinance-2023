"""Shared loader bootstrap helpers (session + reference seed rows)."""

from __future__ import annotations

from typing import Any

from app.logger import Logger

logger = Logger(__name__)

_STATE_CODES: dict[str, tuple[str, str]] = {
    "texas": ("TX", "Texas"),
    "oklahoma": ("OK", "Oklahoma"),
}


def get_session(db_url: str | None = None):
    """Create a SQLModel session with all source + unified tables registered.

    Defaults to the project PostgreSQL database (``POSTGRES_*`` env / ``.env``
    via :class:`PostgresConfig`).  Pass an explicit ``sqlite://`` URL (or use the
    CLI ``--sqlite`` flag) for local smoke tests.  Tables and the Fix-7 dedup
    unique indexes are created idempotently before returning the session.
    """
    from sqlmodel import Session, SQLModel, create_engine

    from app.core import models  # noqa: F401 — registers unified_* tables
    from app.core.source_models import (  # noqa: F401 — registers Phase-0 tables
        CommitteePurpose,
        ExpenditureCategory,
        SpacLink,
        UnifiedNotice,
        UnifiedPledge,
        UnifiedReport,
    )

    if db_url is None:
        from app.states.postgres_config import PostgresConfig

        db_url = PostgresConfig().database_url

    from sqlalchemy import event, text

    engine = create_engine(db_url)

    if db_url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, _conn_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    SQLModel.metadata.create_all(engine)

    from app.core.unified_database import ensure_unified_additive_columns

    ensure_unified_additive_columns(engine)

    if not db_url.startswith("sqlite"):
        from app.core.unified_database import UnifiedDatabaseManager

        with engine.connect() as conn:
            for ddl in UnifiedDatabaseManager._DEDUP_INDEXES:
                conn.execute(text(ddl))
            conn.commit()

    return Session(engine, expire_on_commit=False)


def ensure_committee_types(session: Any) -> int:
    """Upsert the committee_types seed rows.  Safe to call on every run."""
    from app.core.models.tables import CommitteeType
    from app.core.seeds.committee_types import COMMITTEE_TYPE_SEEDS

    inserted = 0
    for seed in COMMITTEE_TYPE_SEEDS:
        existing = session.get(CommitteeType, seed["code"])
        if not existing:
            session.add(CommitteeType(**seed))
            inserted += 1
    if inserted:
        session.commit()
        logger.info(f"[loader] seeded {inserted} committee_type(s)")
    return inserted


def ensure_state(session: Any, state_name: str) -> Any:
    """Return the ``states`` row for *state_name*, creating it if needed."""
    from sqlmodel import select

    from app.core.models import State

    code, name = _STATE_CODES.get(state_name.lower(), (state_name[:2].upper(), state_name.title()))
    existing = session.exec(select(State).where(State.code == code)).first()
    if existing:
        return existing

    row = State(code=code, name=name)
    session.add(row)
    session.commit()
    session.refresh(row)
    return row
