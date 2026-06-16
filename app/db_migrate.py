"""Programmatic Alembic runner — the production schema-migration entry point.

`cf migrate` (and any deploy step) calls :func:`upgrade_head` to bring a database to the
latest revision: on a fresh DB it creates the schema (the baseline revision = create_all +
dedup indexes), on an existing DB it applies pending deltas. This is how schema changes reach
EXISTING databases — the gap the pre-Alembic create_all + additive-shim mechanism could not
close for non-additive changes. See migrations/ and MIGRATIONS.md.

The app's in-process bootstrap (``UnifiedDatabaseManager.bootstrap`` / ``_get_session``) keeps
using ``create_all`` for speed (the test suite spins up many throwaway sqlite DBs); Alembic is
the source of truth for deployed Postgres schema evolution. Both stay in sync by construction:
the baseline revision IS ``create_all`` + the dedup indexes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ALEMBIC_INI = _PROJECT_ROOT / "alembic.ini"
_MIGRATIONS_DIR = _PROJECT_ROOT / "migrations"


def alembic_config(db_url: str) -> Any:
    """Build an Alembic ``Config`` targeting *db_url*, with absolute paths so it works
    regardless of the process's current working directory."""
    from alembic.config import Config

    cfg = Config(str(_ALEMBIC_INI))
    cfg.set_main_option("script_location", str(_MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def upgrade_head(db_url: str) -> None:
    """Run ``alembic upgrade head`` against *db_url* (idempotent)."""
    from alembic import command

    command.upgrade(alembic_config(db_url), "head")


def current_revision(db_url: str) -> str | None:
    """Return the DB's current Alembic revision (None if unmanaged / empty)."""
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import create_engine

    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            return MigrationContext.configure(conn).get_current_revision()
    finally:
        engine.dispose()
