"""One-off dedup of legacy duplicate unified_transactions rows.

Background
----------
Before the Wave-2 natural-key upsert (review-response-2026-06-07), the
insert-only + FileOrigin-guard load path produced duplicate
``(state_id, transaction_type, transaction_id)`` groups whenever the same TEC
record arrived under a differently-named file. The new unique partial index
``uix_transactions_state_type_sourceid`` cannot be created on a database that
still holds those duplicates. Fresh loads are already clean (the upsert prevents
new duplicates); this script removes the *existing* legacy duplicates so the
index can be applied to a pre-existing database.

What it does (single transaction)
---------------------------------
1. Stage the non-surviving duplicate parent ids into a temp table: for each
   ``(state_id, transaction_type, transaction_id)`` group (transaction_id NOT
   NULL) it KEEPS the lowest ``id`` and marks the rest for removal.
2. Purge the children of those doomed parents in every FK-referencing detail
   table (all reference ``unified_transactions.id`` via their own
   ``transaction_id`` column, all ``ON DELETE NO ACTION`` — children first).
3. Purge the doomed parent rows.
4. Optionally create the unique index.

Safety
------
- DRY-RUN BY DEFAULT: prints the counts it *would* purge and rolls back.
  Pass ``--apply`` to commit.
- All-or-nothing: everything runs in one transaction.
- ``--create-index`` additionally creates the unique partial index after the
  cleanup (only meaningful with ``--apply``).

Usage
-----
    uv run python scripts/dedup_unified_transactions.py \
        --db-url postgresql+psycopg2://USER@localhost:5432/DBNAME            # dry-run
    uv run python scripts/dedup_unified_transactions.py --db-url ... --apply --create-index

All SQL is static (no identifier interpolation).
"""

from __future__ import annotations

import argparse
import sys

from sqlalchemy import create_engine, text

# (table, static purge statement), ordered deepest-FK-first. loan_guarantors is a
# GRANDCHILD: it FK-references unified_loans.id (loan_id) and unified_debts.id
# (debt_id), so it must be purged before those detail tables. Everything else
# FK-references unified_transactions.id via its own transaction_id column. All FKs
# are ON DELETE NO ACTION, so descendants must be removed before ancestors.
_CHILD_PURGES: tuple[tuple[str, str], ...] = (
    # grandchildren (reference the loan/debt detail tables) — purge first
    ("loan_guarantors", "DELETE FROM loan_guarantors WHERE loan_id IN (SELECT id FROM unified_loans WHERE transaction_id IN (SELECT id FROM _doomed_txn))"),
    ("loan_guarantors", "DELETE FROM loan_guarantors WHERE debt_id IN (SELECT id FROM unified_debts WHERE transaction_id IN (SELECT id FROM _doomed_txn))"),
    # children (reference unified_transactions.id via transaction_id)
    ("unified_transaction_persons", "DELETE FROM unified_transaction_persons  WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_transaction_versions", "DELETE FROM unified_transaction_versions WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_contributions", "DELETE FROM unified_contributions        WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_expenditures", "DELETE FROM unified_expenditures         WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_loans", "DELETE FROM unified_loans                WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_debts", "DELETE FROM unified_debts                WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_credits", "DELETE FROM unified_credits              WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_travel", "DELETE FROM unified_travel               WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_assets", "DELETE FROM unified_assets               WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
    ("unified_pledges", "DELETE FROM unified_pledges              WHERE transaction_id IN (SELECT id FROM _doomed_txn)"),
)

_CREATE_DOOMED = text("CREATE TEMP TABLE _doomed_txn (id INTEGER PRIMARY KEY) ON COMMIT DROP")

_POPULATE_DOOMED = text(
    """
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
)

_COUNT_DOOMED = text("SELECT count(*) FROM _doomed_txn")
_COUNT_GROUPS = text(
    """
    SELECT count(*) FROM (
        SELECT 1 FROM unified_transactions
        WHERE transaction_id IS NOT NULL
        GROUP BY state_id, transaction_type, transaction_id
        HAVING count(*) > 1
    ) g
    """
)
_PURGE_PARENTS = text("DELETE FROM unified_transactions WHERE id IN (SELECT id FROM _doomed_txn)")
_CREATE_INDEX = text(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uix_transactions_state_type_sourceid
    ON unified_transactions (state_id, transaction_type, transaction_id)
    WHERE transaction_id IS NOT NULL
    """
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dedup legacy unified_transactions duplicates.")
    parser.add_argument("--db-url", required=True, help="SQLAlchemy database URL")
    parser.add_argument(
        "--apply", action="store_true", help="Commit the cleanup (default: dry-run + rollback)"
    )
    parser.add_argument(
        "--create-index",
        action="store_true",
        help="Also create uix_transactions_state_type_sourceid after cleanup",
    )
    args = parser.parse_args(argv)

    # PostgreSQL-only: the staging temp table uses `ON COMMIT DROP`, which SQLite
    # does not support. Fail with a clear message rather than an opaque syntax error.
    if "postgresql" not in args.db_url and "postgres" not in args.db_url:
        sys.exit(
            "This dedup script supports PostgreSQL only "
            "(uses ON COMMIT DROP temp tables). Got: " + args.db_url.split("://", 1)[0]
        )

    engine = create_engine(args.db_url)
    conn = engine.connect()
    trans = conn.begin()  # single transaction, explicit commit/rollback below
    try:
        conn.execute(_CREATE_DOOMED)
        conn.execute(_POPULATE_DOOMED)
        groups = conn.execute(_COUNT_GROUPS).scalar_one()
        doomed = conn.execute(_COUNT_DOOMED).scalar_one()
        print(f"duplicate groups: {groups:,}")
        print(f"non-surviving rows to purge (keeping lowest id per group): {doomed:,}")

        for table, stmt in _CHILD_PURGES:
            res = conn.execute(text(stmt))
            if res.rowcount:
                print(f"  purged {res.rowcount:,} child rows in {table}")

        parent_res = conn.execute(_PURGE_PARENTS)
        print(f"purged {parent_res.rowcount:,} parent unified_transactions rows")

        if args.create_index:
            conn.execute(_CREATE_INDEX)
            print("created uix_transactions_state_type_sourceid")

        if args.apply:
            trans.commit()
            print("\nAPPLIED — committed.")
        else:
            trans.rollback()
            print("\nDRY RUN — rolled back. Re-run with --apply to commit.")
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
