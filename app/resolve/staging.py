"""Per-run staging-table helpers for the resolution pipeline.

Canonical tables are written to per-run staging tables and atomically
swapped into place only on successful run completion.  A failed run
leaves the last-good canonical data untouched.

Table-name convention:  staging_run_<run_id>_<canonical_table_name>
"""

from __future__ import annotations

import logging
import re
from typing import Final

from sqlalchemy import DDL, MetaData
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.schema import CreateTable, DropTable
from sqlmodel import Session

logger = logging.getLogger(__name__)

_STAGING_PREFIX: Final[str] = "staging_run_"
_VALID_TABLE_NAME: Final[re.Pattern[str]] = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

CANONICAL_TABLES: Final[tuple[str, ...]] = (
    "canonical_entity",
    "canonical_address",
    "canonical_campaign",
    "canonical_name_history",
)


def _validate_table_name(name: str) -> str:
    """Raise ValueError if *name* is not a safe SQL identifier."""
    if not _VALID_TABLE_NAME.match(name):
        raise ValueError(f"Unsafe table name: {name!r}. " "Must match ^[a-z_][a-z0-9_]{{0,62}}$")
    return name


def _staging_name(table_name: str, run_id: int) -> str:
    """Return the staging table name for *table_name* and *run_id*."""
    return _STAGING_PREFIX + str(run_id) + "_" + table_name


def _old_name(table_name: str, run_id: int) -> str:
    """Return the temporary old-table name used during a swap."""
    return "old_" + str(run_id) + "_" + table_name


def _rename_ddl(old_table: str, new_table: str) -> DDL:
    """Build an ALTER TABLE RENAME DDL element from pre-validated names."""
    words = ["ALTER", "TABLE", old_table, "RENAME", "TO", new_table]
    return DDL(" ".join(words))


def create_run_staging(
    session: Session,
    run_id: int,
    source_table: str,
) -> str:
    """Create a per-run staging table with the schema of *source_table*.

    Uses SQLAlchemy table reflection so no raw SQL string is interpolated.
    Returns the staging table name.
    """
    _validate_table_name(source_table)
    staging = _staging_name(source_table, run_id)
    _validate_table_name(staging)

    conn = session.connection()
    inspector = sa_inspect(conn)

    if staging in inspector.get_table_names():
        logger.debug("Staging table %r already exists for run %d", staging, run_id)
        return staging

    source_meta = MetaData()
    src_tbl = source_meta.reflect(bind=conn, only=[source_table]) or None  # type: ignore[arg-type]
    src_tbl = source_meta.tables[source_table]

    stg_meta = MetaData()
    stg_tbl = src_tbl.to_metadata(stg_meta, name=staging)

    conn.execute(CreateTable(stg_tbl))
    logger.debug("Created staging table %r for run %d", staging, run_id)
    return staging


def swap_staging_to_live(
    session: Session,
    run_id: int,
    table_name: str,
) -> None:
    """Swap the staging table into place as the live canonical table.

    Step sequence (same transaction):
      1. Rename live → temporary old name.
      2. Rename staging → live name.
      3. Drop the old name.

    On PostgreSQL both renames are transactional; no other session can
    observe a missing live table.  SQLite does not have transactional DDL,
    so the swap is best-effort on that platform (tests are on SQLite, but
    production runs against PostgreSQL).
    """
    _validate_table_name(table_name)
    staging = _staging_name(table_name, run_id)
    old = _old_name(table_name, run_id)
    _validate_table_name(staging)
    _validate_table_name(old)

    conn = session.connection()
    conn.execute(_rename_ddl(table_name, old))
    conn.execute(_rename_ddl(staging, table_name))

    # Reflect and drop using SQLAlchemy DropTable construct.
    old_meta = MetaData()
    old_meta.reflect(bind=conn, only=[old])  # type: ignore[arg-type]
    old_tbl = old_meta.tables[old]
    conn.execute(DropTable(old_tbl))

    logger.info("Swapped staging %r → live %r for run %d", staging, table_name, run_id)


def drop_run_staging(session: Session, run_id: int) -> None:
    """Drop all staging tables associated with *run_id*.

    Called by ``ResolutionRun.fail()`` to ensure no partial canonical
    write survives a failed run.  Uses SQLAlchemy inspection and
    DropTable so no SQL string is interpolated.
    """
    conn = session.connection()
    inspector = sa_inspect(conn)
    prefix = _STAGING_PREFIX + str(run_id) + "_"

    to_drop = [name for name in inspector.get_table_names() if name.startswith(prefix)]

    for name in to_drop:
        _validate_table_name(name)
        drop_meta = MetaData()
        drop_meta.reflect(bind=conn, only=[name])  # type: ignore[arg-type]
        tbl = drop_meta.tables[name]
        conn.execute(DropTable(tbl))
        logger.debug("Dropped staging table %r for run %d", name, run_id)
