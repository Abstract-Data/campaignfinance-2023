"""End-to-end smoke for the default-flip: drive the real `_run_vectorized_load` path
(bootstrap via _get_session → fixtures_dir → run_vectorized) on a real slice into a fresh
DB with constraints ENFORCED (production posture), and confirm it completes + any dirty
rows land in ingest_errors instead of failing the load. Also exercises dry-run + should_stop.
"""

from __future__ import annotations

import dataclasses
import time
from pathlib import Path

from sqlalchemy import create_engine, text

import app.entrypoint as ep
import scripts.benchmarks.bench_ingest as B
from scripts.loaders.loader_config import STATE_GLOB_CONFIGS, get_config

_COUNTS = {
    "unified_committees": "SELECT count(*) FROM unified_committees",
    "unified_persons": "SELECT count(*) FROM unified_persons",
    "unified_addresses": "SELECT count(*) FROM unified_addresses",
    "unified_transactions": "SELECT count(*) FROM unified_transactions",
    "unified_contributions": "SELECT count(*) FROM unified_contributions",
    "ingest_errors": "SELECT count(*) FROM ingest_errors",
}

with B._fresh_database("postgresql+psycopg2://localhost:5432", "cf_flip_smoke") as url:
    orig = STATE_GLOB_CONFIGS["texas"]
    STATE_GLOB_CONFIGS["texas"] = dataclasses.replace(orig, base_dir=Path("tmp/_bench_slice"))
    config = get_config("production")
    try:
        # dry-run first (must write nothing)
        dry = ep._run_vectorized_load("texas", config, url, dry_run=True, should_stop=lambda: False)
        print("DRY-RUN:", dry)

        t0 = time.perf_counter()
        result = ep._run_vectorized_load(
            "texas", config, url, dry_run=False, should_stop=lambda: False
        )
        print(f"LOAD ({time.perf_counter() - t0:.1f}s):", result)
    finally:
        STATE_GLOB_CONFIGS["texas"] = orig

    eng = create_engine(url)
    with eng.connect() as c:
        print("\n== table counts (constraints enforced) ==")
        for t, q in _COUNTS.items():
            print(f"  {t}: {c.execute(text(q)).scalar()}")
        # sample an ingest_errors row if any
        err = c.execute(text(
            "SELECT record_type, error_type, left(error_message, 90) FROM ingest_errors LIMIT 1"
        )).fetchone()
        print("  sample ingest_error:", tuple(err) if err else "(none)")
    eng.dispose()
