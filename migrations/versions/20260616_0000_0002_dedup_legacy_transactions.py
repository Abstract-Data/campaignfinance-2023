"""dedup legacy unified_transactions duplicates, then apply the strict unique index

Revision ID: 0002_dedup_legacy_transactions
Revises: 0001_baseline
Create Date: 2026-06-16

Pre-Wave-2 (insert-only + FileOrigin-guard) loads produced duplicate
``(state_id, transaction_type, transaction_id)`` groups. Wave 2 introduced the strict partial
unique index ``uix_transactions_state_type_sourceid`` (in ``_DEDUP_INDEXES``), but it can only be
created on a database with no such duplicates — so existing polluted DBs could never get it, and
re-bootstrapping them crashed on the duplicate key (see ``legacy-transaction-dedup-deferred``).

This revision makes ``cf migrate`` / ``alembic upgrade head`` upgrade those DBs:

1. Stage the non-surviving duplicate parent ids (keep the lowest ``id`` per group).
2. Purge their children deepest-FK-first (all FKs are ``ON DELETE NO ACTION``), then the parents.
3. Create the unique partial index ``IF NOT EXISTS``.

Ordering vs the baseline: ``0001_baseline`` creates this index (IF NOT EXISTS) on a FRESH DB, but
existing polluted DBs are ``alembic stamp 0001_baseline``'d — the baseline never runs on them, so
they never got the index. Hence this revision must dedup BEFORE creating the index. On a fresh DB
it is a no-op: zero duplicates, and the index already exists from the baseline.

The interactive/dry-run equivalent is ``scripts/dedup_unified_transactions.py``. The SQL here is
INLINED (migrations are frozen, self-contained snapshots — they must not import from scripts that
may later change). All SQL is static (no identifier interpolation).

Postgres-only: the partial/functional unique index and the staging temp table are PG features, and
sqlite databases (tests) never created the index (baseline skips ``_DEDUP_INDEXES`` off Postgres),
so there is nothing to dedup there.

``downgrade`` only drops the index — deleted duplicate rows cannot be restored.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_dedup_legacy_transactions"
down_revision: str | None = "0001_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Children of a doomed unified_transactions row, ordered deepest-FK-first. loan_guarantors is a
# GRANDCHILD (FK to unified_loans.id / unified_debts.id), so it must be purged before those detail
# tables. The remaining ten reference unified_transactions.id via their own transaction_id column.
# All FKs are ON DELETE NO ACTION, so descendants must go before ancestors. This list mirrors the
# live SQLModel.metadata FK graph (verified 2026-06-16) and scripts/dedup_unified_transactions.py.
_CHILD_PURGES: tuple[str, ...] = (
    # grandchildren — reference the loan/debt detail tables; purge first
    "DELETE FROM loan_guarantors WHERE loan_id IN "
    "(SELECT id FROM unified_loans WHERE transaction_id IN (SELECT id FROM _doomed_txn))",
    "DELETE FROM loan_guarantors WHERE debt_id IN "
    "(SELECT id FROM unified_debts WHERE transaction_id IN (SELECT id FROM _doomed_txn))",
    # children — reference unified_transactions.id via transaction_id
    "DELETE FROM unified_transaction_persons  WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_transaction_versions WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_contributions        WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_expenditures         WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_loans                WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_debts                WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_credits              WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_travel               WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_assets               WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
    "DELETE FROM unified_pledges              WHERE transaction_id IN (SELECT id FROM _doomed_txn)",
)

_CREATE_DOOMED = "CREATE TEMP TABLE _doomed_txn (id INTEGER PRIMARY KEY)"
_POPULATE_DOOMED = """
    INSERT INTO _doomed_txn (id)
    SELECT id FROM (
        SELECT id,
               row_number() OVER (
                   PARTITION BY state_id, transaction_type, transaction_id
                   ORDER BY id
               ) AS rn
        FROM unified_transactions
        WHERE transaction_id IS NOT NULL
    ) ranked
    WHERE ranked.rn > 1
"""
_PURGE_PARENTS = "DELETE FROM unified_transactions WHERE id IN (SELECT id FROM _doomed_txn)"
_DROP_DOOMED = "DROP TABLE _doomed_txn"
_CREATE_INDEX = """
    CREATE UNIQUE INDEX IF NOT EXISTS uix_transactions_state_type_sourceid
    ON unified_transactions (state_id, transaction_type, transaction_id)
    WHERE transaction_id IS NOT NULL
"""
_DROP_INDEX = "DROP INDEX IF EXISTS uix_transactions_state_type_sourceid"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        # sqlite never created the partial unique index (baseline skips _DEDUP_INDEXES off
        # Postgres), so there is nothing to dedup or index here.
        return

    bind.execute(sa.text(_CREATE_DOOMED))
    bind.execute(sa.text(_POPULATE_DOOMED))
    for stmt in _CHILD_PURGES:
        bind.execute(sa.text(stmt))
    bind.execute(sa.text(_PURGE_PARENTS))
    bind.execute(sa.text(_DROP_DOOMED))
    bind.execute(sa.text(_CREATE_INDEX))


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return
    # Removing the duplicates is not reversible; downgrade only drops the constraint.
    bind.execute(sa.text(_DROP_INDEX))
