"""Smoke-test: run the entity-pass resolve pipeline against one already-loaded bench DB
and report canonical_entity counts. Usage: python -m tmp.resolve_one <db_name>
"""

from __future__ import annotations

import sys

from sqlmodel import Session, create_engine, text

from app.resolve.run import ResolutionRun, ensure_resolution_schema
from app.resolve.stages import (
    run_blocking_stage,
    run_classify_stage,
    run_cluster_stage,
    run_fastpath_stage,
    run_score_stage,
    run_survivorship_stage,
    stage1_build_resolution_input,
)

db = sys.argv[1] if len(sys.argv) > 1 else "bench_vec_copy"
url = f"postgresql+psycopg2://localhost:5432/{db}"
engine = create_engine(url)

STAGES = [
    stage1_build_resolution_input,
    run_blocking_stage,
    run_fastpath_stage,
    run_score_stage,
    run_classify_stage,
    run_cluster_stage,
    run_survivorship_stage,
]
CONFIG = {
    "state_code": "TX",
    "pass_type": "entity",
    "auto_threshold": 0.99,
    "review_threshold": 0.80,
    "max_cluster_size": 200,
    "max_pairs_per_run": 2_000_000,
    "seed": 42,
}

_COUNTS = {
    "resolution_input": "SELECT count(*) FROM resolution_input",
    "candidate_pairs": "SELECT count(*) FROM candidate_pairs",
    "scored_pairs": "SELECT count(*) FROM scored_pairs",
    "cluster_assignment": "SELECT count(*) FROM cluster_assignment",
    "canonical_entity": "SELECT count(*) FROM canonical_entity",
    "entity_crosswalk": "SELECT count(*) FROM entity_crosswalk",
}
_BY_TYPE = (
    "SELECT entity_type, count(*) FROM canonical_entity GROUP BY entity_type ORDER BY 2 DESC"
)

print(f"== resolve entity pass on {db} ==")
ensure_resolution_schema(engine)
run = ResolutionRun(state_code="TX", config=CONFIG)
with Session(engine) as session:
    result = run.run(session, STAGES)
print(f"run result: {result}")

with engine.connect() as c:
    for tbl, q in _COUNTS.items():
        try:
            print(f"  {tbl}: {c.execute(text(q)).scalar()}")
        except Exception as e:  # noqa: BLE001
            print(f"  {tbl}: ERR {type(e).__name__}: {str(e)[:80]}")
    try:
        rows = c.execute(text(_BY_TYPE)).fetchall()
        print("  canonical_entity by type:", [(str(r[0]), r[1]) for r in rows])
    except Exception as e:  # noqa: BLE001
        print(f"  by-type ERR {type(e).__name__}: {str(e)[:80]}")
