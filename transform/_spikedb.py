"""Bootstrap for the dedicated ELT-spike Postgres database.

Targets a SEPARATE local database (default ``campaignfinance_elt_spike``) so the
spike's truncate/rebuild cycle never touches the real ``campaign_finance`` data,
while still creating the genuine ORM ``public.unified_*`` schema (enum types +
Fix-7 partial unique indexes) that ``app/resolve`` reads and that the dbt publish
step + the production_loader baseline both write — keeping reconciliation honest.
"""

from __future__ import annotations

import os

import psycopg2
from psycopg2 import sql
from sqlalchemy.schema import CreateSchema
from sqlmodel import Session, SQLModel, create_engine, select

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
PGUSER = os.environ.get("PGUSER", "johneakin")
PGPASSWORD = os.environ.get("PGPASSWORD", "")
SPIKE_DB = os.environ.get("ELT_SPIKE_DB", "campaignfinance_elt_spike")

SILVER_SCHEMA = "silver"


def _auth() -> str:
    return f"{PGUSER}:{PGPASSWORD}" if PGPASSWORD else PGUSER


def admin_url(dbname: str = "postgres") -> str:
    return f"postgresql+psycopg2://{_auth()}@{PGHOST}:{PGPORT}/{dbname}"


def spike_url(dbname: str | None = None) -> str:
    return admin_url(dbname or SPIKE_DB)


def create_database_if_absent() -> None:
    """CREATE DATABASE (idempotent) — needs autocommit, can't run in a txn.

    Identifier is composed via psycopg2.sql.Identifier (safe quoting), never an
    interpolated SQL string.
    """
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        user=PGUSER,
        password=PGPASSWORD or None,
        dbname="postgres",
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (SPIKE_DB,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(SPIKE_DB)))
    finally:
        conn.close()


def bootstrap_unified() -> str:
    """Create the spike DB, its schemas, and the full ORM unified_* schema.

    Returns the spike database URL.
    """
    create_database_if_absent()

    # Import the ORM models so they register on SQLModel.metadata before create_all.
    import app.core.models  # noqa: F401
    from app.core.unified_database import UnifiedDatabaseManager

    url = spike_url()
    engine = create_engine(url)
    # Pre-create every schema the metadata references (texas/resolve/…) plus silver,
    # so SQLModel.metadata.create_all doesn't trip on a missing schema. CreateSchema
    # quotes the identifier — no interpolated SQL.
    schemas = {t.schema for t in SQLModel.metadata.tables.values() if t.schema}
    schemas.add(SILVER_SCHEMA)
    with engine.connect() as conn:
        for schema in sorted(schemas):
            conn.execute(CreateSchema(schema, if_not_exists=True))
        conn.commit()
    engine.dispose()

    UnifiedDatabaseManager(database_url=url).bootstrap()
    return url


def ensure_state(url: str, code: str = "TX", name: str = "Texas") -> int:
    """Get-or-create the states row; return its id (the dbt state_id var)."""
    from app.core.models.tables import State

    engine = create_engine(url)
    try:
        with Session(engine) as session:
            row = session.exec(select(State).where(State.code == code)).first()
            if row is None:
                row = State(code=code, name=name)
                session.add(row)
                session.commit()
                session.refresh(row)
            return row.id
    finally:
        engine.dispose()
