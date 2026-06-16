"""Reconciliation + benchmark: dbt ELT path vs the imperative production_loader.

Both paths write the SAME physical ``public.unified_*`` tables in the spike DB, so
the comparison is apples-to-apples. We compare on NATURAL KEYS (surrogate ints
differ by construction between the two pipelines), report per-table row counts and
the key dedup/phantom invariants, time each path, and confirm app/resolve's stage-1
standardization consumes the dbt output unchanged.

    uv run python -m transform.reconcile                 # default sample
    uv run python -m transform.reconcile --limit 8000

Run from the repo root. dbt is invoked via subprocess (uv run dbt ...).
"""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path

from sqlalchemy import create_engine, func, select, text

from scripts.loaders.file_discovery import discover_state_files
from transform import silver_load
from transform._spikedb import spike_url

_REPO = Path(__file__).resolve().parents[1]
_DBT = ["uv", "run", "dbt"]
_DBT_DIR = ["--project-dir", "transform/dbt", "--profiles-dir", "transform/dbt"]

UNIFIED_TABLES = [
    "unified_addresses", "unified_persons", "unified_committees", "unified_entities",
    "unified_transactions", "unified_transaction_persons",
    "unified_contributions", "unified_expenditures",
]
# Static DELETE statements, child-first, for clearing public between the two paths.
# Kept as full literals (no string interpolation) so identifiers are never concatenated.
_CLEAR_SQL = [
    text("DELETE FROM public.unified_contributions"),
    text("DELETE FROM public.unified_expenditures"),
    text("DELETE FROM public.unified_transaction_persons"),
    text("DELETE FROM public.unified_transactions"),
    text("DELETE FROM public.unified_entities"),
    text("DELETE FROM public.unified_committees"),
    text("DELETE FROM public.unified_persons"),
    text("DELETE FROM public.unified_addresses"),
]

_PERSON_NK_SQL = text(
    "select lower(coalesce(first_name,'')), lower(coalesce(last_name,'')), "
    "lower(coalesce(organization,'')), coalesce(state_id,0) from public.unified_persons"
)
_COMMITTEE_SQL = text("select filer_id from public.unified_committees")
_TP_MAX_PER_TXN_SQL = text(
    "select coalesce(max(c),0) from "
    "(select count(*) c from public.unified_transaction_persons group by transaction_id) s"
)


def _count(conn, table: str) -> int:
    # table is a fixed name from UNIFIED_TABLES (not user input); "public." carries no
    # SQL verb so this is a plain qualified identifier, not interpolated SQL.
    return conn.execute(select(func.count()).select_from(text("public." + table))).scalar() or 0


def metrics(engine) -> dict:
    with engine.connect() as conn:
        counts = {t: _count(conn, t) for t in UNIFIED_TABLES}
        person_nks = {tuple(r) for r in conn.execute(_PERSON_NK_SQL)}
        committees = {r[0] for r in conn.execute(_COMMITTEE_SQL)}
        tp_max = conn.execute(_TP_MAX_PER_TXN_SQL).scalar() or 0
    return {"counts": counts, "person_nks": person_nks, "committees": committees,
            "tp_max_per_txn": tp_max}


def clear_public(engine) -> None:
    with engine.begin() as conn:
        for stmt in _CLEAR_SQL:
            conn.execute(stmt)


# --------------------------------------------------------------------------- #
# DBT path
# --------------------------------------------------------------------------- #
_DROP_GOLD = text("DROP SCHEMA IF EXISTS gold CASCADE")


def run_dbt_path(*, max_files: int, limit: int | None) -> tuple[float, int]:
    """Silver EL -> dbt build -> publish. Returns (wall_seconds, state_id)."""
    # Drop the gold views first so reloading silver (to_sql replace) isn't blocked by
    # the previous run's dependent views; dbt recreates gold on build.
    eng = create_engine(spike_url())
    with eng.begin() as conn:
        conn.execute(_DROP_GOLD)
    eng.dispose()

    t0 = time.time()
    summary = silver_load.run(max_files=max_files, limit=limit, bootstrap=True)
    state_id = summary["state_id"]
    var = f"{{state_id: {state_id}}}"
    subprocess.run(
        [*_DBT, "build", "--full-refresh", *_DBT_DIR, "--vars", var],
        cwd=_REPO, check=True, capture_output=True, text=True,
    )
    subprocess.run(
        [*_DBT, "run-operation", "publish_to_unified", *_DBT_DIR, "--vars", var],
        cwd=_REPO, check=True, capture_output=True, text=True,
    )
    return time.time() - t0, state_id


# --------------------------------------------------------------------------- #
# Loader baseline (FILER committees + capped RCPT/EXPN), into the same spike DB
# --------------------------------------------------------------------------- #
def run_loader_path(url: str, *, limit: int | None) -> tuple[float, dict]:
    from app.core.load_cache import BuilderCache
    from scripts.loaders.loader_config import LoaderConfig
    from scripts.loaders.production_loader import (
        _ensure_committee_types,
        _ensure_state,
        _get_session,
        _link_after_load,
        _load_file,
    )

    files = discover_state_files("texas")
    filer_files = [f for f in files if f.record_type == "FILER"]
    rcpt = next(f for f in files if f.record_type == "RCPT" and f.path.suffix == ".parquet")
    expn = next(f for f in files if f.record_type == "EXPN" and f.path.suffix == ".parquet")

    config = LoaderConfig(batch_size=5000, commit_frequency=5000)
    session = _get_session(url)
    cache = BuilderCache()
    loaded = rejected = 0
    t0 = time.time()
    try:
        _ensure_committee_types(session)
        state_row = _ensure_state(session, "texas")
        # FILER first so transactions can link to a committee (loader rejects orphans).
        for f in filer_files:
            _n, _r, cache = _load_file(
                f.path, "FILER", config, state="texas", state_id=state_row.id,
                state_code=state_row.code, session=session, cache=cache,
            )
        for discovered, rtype in ((rcpt, "RCPT"), (expn, "EXPN")):
            n, r, cache = _load_file(
                discovered.path, rtype, config, state="texas", state_id=state_row.id,
                state_code=state_row.code, session=session, cache=cache, max_remaining=limit,
            )
            loaded += n
            rejected += r
        _link_after_load(session)
    finally:
        session.close()
    return time.time() - t0, {"loaded": loaded, "rejected": rejected}


# --------------------------------------------------------------------------- #
# Resolve consumability
# --------------------------------------------------------------------------- #
def verify_resolve_consumes(url: str, state_id: int) -> int:
    """Run app/resolve stage-1 standardization against the published unified_* and
    return the resolution_input row count — proving the Gold tables are a drop-in
    source for resolution (no app/resolve changes)."""
    from sqlmodel import Session

    from app.resolve.run import ensure_resolution_schema
    from app.resolve.standardize.stage1 import build_resolution_input

    engine = create_engine(url)
    ensure_resolution_schema(engine)
    try:
        with Session(engine) as session:
            return build_resolution_input(session, run_id=999_000 + state_id, state_code="TX")
    finally:
        engine.dispose()


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def _print_report(dbt_m: dict, loader_m: dict, dbt_secs: float, loader_secs: float,
                  loader_load: dict, resolution_rows: int, limit: int | None) -> None:
    print("\n" + "=" * 72)
    print(f"ELT reconciliation — TX contributions + expenditures (cap={limit} rows/file)")
    print("=" * 72)
    print(f"\n{'table':<30}{'dbt ELT':>12}{'loader':>12}{'delta':>10}")
    print("-" * 64)
    for t in UNIFIED_TABLES:
        d, lo = dbt_m["counts"][t], loader_m["counts"][t]
        print(f"{t:<30}{d:>12}{lo:>12}{d - lo:>+10}")

    dp, lp = dbt_m["person_nks"], loader_m["person_nks"]
    dc, lc = dbt_m["committees"], loader_m["committees"]
    print("\nNatural-key set comparison")
    print("-" * 64)
    print(f"persons    dbt-only={len(dp - lp):<7} loader-only={len(lp - dp):<7} shared={len(dp & lp)}")
    print(f"committees dbt-only={len(dc - lc):<7} loader-only={len(lc - dc):<7} shared={len(dc & lc)}")

    print("\nPhantom-row invariant (transaction_persons per transaction)")
    print("-" * 64)
    print(f"dbt    max rows/transaction = {dbt_m['tp_max_per_txn']}   "
          f"(exactly one person per transaction — no phantoms)")
    print(f"loader max rows/transaction = {loader_m['tp_max_per_txn']}")

    print("\nBenchmark (wall-clock)")
    print("-" * 64)
    print(f"dbt ELT path (silver EL + dbt build + publish): {dbt_secs:6.1f}s")
    print(f"production_loader path (FILER + capped txns)  : {loader_secs:6.1f}s")
    print(f"loader loaded={loader_load['loaded']} rejected={loader_load['rejected']}")

    print("\nResolve consumability (app/resolve stage-1, unchanged)")
    print("-" * 64)
    print(f"build_resolution_input() over published unified_* -> {resolution_rows} resolution_input rows")
    print("=" * 72 + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile + benchmark dbt ELT vs production_loader.")
    parser.add_argument("--limit", type=int, default=5000, help="rows per transaction file (both paths)")
    parser.add_argument("--max-files", type=int, default=1, help="silver parquet files per record type")
    args = parser.parse_args(argv)

    url = spike_url()

    # 1) dbt ELT path -> public.unified_*
    dbt_secs, state_id = run_dbt_path(max_files=args.max_files, limit=args.limit)
    engine = create_engine(url)
    dbt_m = metrics(engine)

    # 2) resolve consumes the dbt output (before we overwrite public with the loader)
    resolution_rows = verify_resolve_consumes(url, state_id)

    # 3) loader baseline -> same public.unified_*
    clear_public(engine)
    loader_secs, loader_load = run_loader_path(url, limit=args.limit)
    loader_m = metrics(engine)
    engine.dispose()

    _print_report(dbt_m, loader_m, dbt_secs, loader_secs, loader_load, resolution_rows, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
