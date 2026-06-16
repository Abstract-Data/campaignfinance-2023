"""
Trace what actually ends up in the DB after saving one transaction.
Uses a TEST transaction_id prefix so we can find and clean it up.
"""

import sys

sys.path.insert(0, "/Users/johneakin/PyCharmProjects/campaignfinance")

from pathlib import Path

import polars as pl
from sqlmodel import select

from app.core.models.tables import State, UnifiedTransaction
from app.core.processor import unified_sql_processor  # noqa: F401 (side-effect import)
from app.core.unified_database import get_db_manager

# Get a real contribution record
contribs_file = Path(
    "/Users/johneakin/PyCharmProjects/campaignfinance/tmp/texas/contribs_05_20260524.parquet"
)
df = pl.scan_parquet(contribs_file).limit(1).collect()
raw_record = df.to_dicts()[0]

# Use a test transaction_id so we can identify and clean it up
TEST_ID = "DEBUG_TEST_001"
raw_record["contributionInfoId"] = TEST_ID
raw_record["state"] = "texas"
raw_record["file_origin"] = "debug_test.parquet"

print("Raw contributionDt:", raw_record["contributionDt"])
print("Raw filerIdent:", raw_record["filerIdent"])
print("Raw contributionAmount:", raw_record["contributionAmount"])

db = get_db_manager(bootstrap=False)

# Step 1: build the transaction (but don't save)
with db.get_session() as session:
    state_rec = session.exec(select(State).where(State.code == "TX")).first()
    state_id = state_rec.id
    state_code = state_rec.code

    txn = unified_sql_processor.process_record(
        raw_record, "texas", state_id=state_id, state_code=state_code, session=session
    )
    print("\n=== AFTER process_record (before save) ===")
    print(f"  transaction_date: {txn.transaction_date}")
    print(f"  committee_id:     {txn.committee_id}")
    print(f"  amount:           {txn.amount}")

    # Step 2: simulate what _persist_transaction_from_record does with the committee
    from app.core.models.tables import UnifiedCommittee

    if txn.committee:
        filer_id = txn.committee.filer_id
        existing = session.get(UnifiedCommittee, filer_id)
        if existing:
            txn.committee = existing
            txn.committee_id = filer_id
            print(f"\n  [After committee dedup] committee_id: {txn.committee_id}")
        else:
            session.add(txn.committee)
            txn.committee_id = filer_id
            print(f"\n  [New committee added] committee_id: {txn.committee_id}")

    # Add transaction to session
    session.add(txn)
    print(f"\n  [After session.add] transaction_date: {txn.transaction_date}")
    print(f"  [After session.add] committee_id:     {txn.committee_id}")

    # Flush to see what SQLAlchemy sends to DB (without committing)
    try:
        session.flush()
        print(f"\n  [After flush] transaction_date: {txn.transaction_date}")
        print(f"  [After flush] committee_id:     {txn.committee_id}")
        print(f"  [After flush] txn.id (PK): {txn.id}")

        # Query directly to see what was flushed
        result = session.exec(
            select(UnifiedTransaction).where(UnifiedTransaction.transaction_id == TEST_ID)
        ).first()
        if result:
            print(f"\n  [DB after flush] transaction_date: {result.transaction_date}")
            print(f"  [DB after flush] committee_id:     {result.committee_id}")
            print(f"  [DB after flush] amount:           {result.amount}")
        else:
            print("\n  [DB after flush] NOT FOUND!")

        # Rollback so we don't pollute the DB
        session.rollback()
        print("\n  [Rolled back - no permanent changes]")
    except Exception as e:
        print(f"\n  [FLUSH ERROR: {type(e).__name__}]: {e}")
        session.rollback()
