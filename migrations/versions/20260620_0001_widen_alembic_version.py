"""widen alembic_version.version_num from varchar(32) to varchar(64)

Revision ID: widen_alembic_version
Revises: dc131e864993
Create Date: 2026-06-20

Alembic creates ``alembic_version(version_num VARCHAR(32))`` by default. The
human-readable revision IDs used in this project (e.g.
``0003_upsert_dimension_unique_indexes``, 35 chars) exceed that limit, causing
``cf migrate`` to fail with a string-length violation when Alembic tries to
stamp the new head.

This revision widens the column to VARCHAR(64) so any reasonable revision ID
fits. It must run before any revision whose ID exceeds 32 characters.

Postgres: ALTER TABLE ... TYPE VARCHAR(64).
SQLite: no-op — SQLite stores text without enforcing varchar lengths.
downgrade: intentionally a no-op; narrowing back to VARCHAR(32) while a
wide revision ID is still current would itself fail.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "widen_alembic_version"
down_revision: str | None = "dc131e864993"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "sqlite":
        bind.execute(
            sa.text(
                "ALTER TABLE alembic_version"
                " ALTER COLUMN version_num TYPE VARCHAR(64)"
            )
        )


def downgrade() -> None:
    # Narrowing back to VARCHAR(32) is unsafe while a >32-char revision ID may
    # be stored in the table. Leave the column wide on downgrade.
    pass
