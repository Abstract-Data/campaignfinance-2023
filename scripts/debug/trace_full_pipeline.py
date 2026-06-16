"""
Full end-to-end trace of the ingest pipeline for a single record.
Does NOT save to DB — just traces what fields get set.
"""

import sys

sys.path.insert(0, "/Users/johneakin/PyCharmProjects/campaignfinance")

from pathlib import Path

import polars as pl

# Must import processor first for side-effect UnifiedReport registration
from app.core.processor import unified_sql_processor  # noqa: F401
from app.core.unified_database import get_db_manager
from app.core.unified_state_loader import UnifiedStateLoader

# Get a real record from the parquet file
contribs_file = Path(
    "/Users/johneakin/PyCharmProjects/campaignfinance/tmp/texas/contribs_05_20260524.parquet"
)
df = pl.scan_parquet(contribs_file).limit(1).collect()
raw_record = df.to_dicts()[0]

print("=== RAW RECORD ===")
for k, v in list(raw_record.items())[:12]:
    print(f"  {k}: {v!r}")

# Simulate what process_records_batch does
raw_record["state"] = "texas"
raw_record["file_origin"] = contribs_file.name

db = get_db_manager(bootstrap=False)
loader = UnifiedStateLoader("texas", "/Users/johneakin/PyCharmProjects/campaignfinance/tmp")

# Get state_id
with db.get_session() as session:
    state_id, state_code = None, None
    from sqlmodel import select

    from app.core.models.tables import State

    state_rec = session.exec(select(State).where(State.code == "TX")).first()
    if state_rec:
        state_id = state_rec.id
        state_code = state_rec.code
        print(f"\nState: {state_rec.name}, id={state_id}, code={state_code}")

    # Call process_record exactly as _persist_transaction_from_record does
    txn = unified_sql_processor.process_record(
        raw_record,
        "texas",
        state_id=state_id,
        state_code=state_code,
        session=session,
    )

    print("\n=== BUILT TRANSACTION ===")
    print(f"  transaction_id:   {txn.transaction_id}")
    print(f"  transaction_date: {txn.transaction_date}")
    print(f"  amount:           {txn.amount}")
    print(f"  transaction_type: {txn.transaction_type}")
    print(f"  committee_id:     {txn.committee_id}")
    print(f"  committee obj:    {txn.committee}")
    print(f"  state_id:         {txn.state_id}")
    print(f"  persons:          {len(txn.persons)} attached")
    for tp in txn.persons:
        p = tp.person
        print(f"    role={tp.role}, name={p.first_name} {p.last_name} / org={p.organization}")
