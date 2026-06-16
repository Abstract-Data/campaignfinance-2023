"""Gate: the vectorized flat_txns + flat_txns_dims families == the ORM loader.

Loads ONLY the RCPT (contribs_golden) and EXPN (expend_golden) fixtures via both
the ORM loader and ``run_vectorized`` (flat_txns + flat_txns_dims families registered),
then asserts that ``diff_snapshots`` restricted to the expected tables is empty.

Foreign-key parent tables (unified_reports, etc.) are NOT populated on the
vectorized side — ``enforce_fk=False`` is used.  Surrogate FK columns are dropped
by the harness anyway; natural-key columns (committee_id string, etc.) are kept.
"""

from __future__ import annotations

from pathlib import Path

from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES, _make_engine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FLAT_TXN_RECORD_TYPES = frozenset({"RCPT", "EXPN"})
_FLAT_TXN_FILENAMES = frozenset({"contribs_golden.parquet", "expend_golden.parquet"})


def _load_golden_rcpt_expn(engine) -> None:
    """ORM-load ONLY the RCPT and EXPN golden fixtures.

    Mirrors ``test_harness._load_golden`` but filters discovered files to just
    contribs_golden (RCPT) and expend_golden (EXPN).  FK parents (committees,
    reports) are NOT seeded — matching the vectorized side where enforce_fk=False.
    """
    from sqlmodel import Session

    from app.core.load_cache import BuilderCache
    from scripts.loaders import production_loader as P
    from scripts.loaders.file_discovery import discover_state_files
    from scripts.loaders.loader_config import LoaderConfig

    session = Session(engine, expire_on_commit=False)
    try:
        P._ensure_committee_types(session)
        state = P._ensure_state(session, "texas")
        cache = BuilderCache()
        cfg = LoaderConfig(batch_size=1000, commit_frequency=1000)
        discovered = sorted(
            (
                (item.path, item.record_type)
                for item in discover_state_files("texas", base_dir=FIXTURES)
                if item.record_type in _FLAT_TXN_RECORD_TYPES
            ),
            key=lambda p_rt: (P._FILE_PRIORITY.get(p_rt[1] or "", 50), str(p_rt[0])),
        )
        assert discovered, "no RCPT/EXPN golden fixtures discovered"
        for path, rtype in discovered:
            _n, _rej, cache = P._load_file(
                path,
                rtype,
                cfg,
                state="texas",
                state_id=state.id,
                state_code=state.code,
                session=session,
                cache=cache,
                max_remaining=None,
            )
        session.commit()
    finally:
        session.close()


def _flat_txns_only(snap: dict) -> dict:
    """Restrict a snapshot to unified_transactions only."""
    return {"unified_transactions": snap.get("unified_transactions", [])}


# Tables made equivalent by flat_txns_dims — added here in parity order.
# Dim tables this slice brings to real parity. Detail/junction tables
# (contributions/expenditures/transaction_persons) are deferred to the linkage slice
# (real id-joins + harness FK->natural-key resolution), not gated here.
_DIM_TABLES = (
    "unified_addresses",
    "unified_persons",
    "unified_entities",
    "unified_committees",
)


def _dims_only(snap: dict) -> dict:
    """Restrict a snapshot to the dim tables gated by flat_txns_dims."""
    return {t: snap.get(t, []) for t in _DIM_TABLES}


# ---------------------------------------------------------------------------
# Vectorized-only fixture directory containing ONLY RCPT / EXPN files
# ---------------------------------------------------------------------------


def _make_rcpt_expn_fixtures_dir(tmp_path: Path) -> Path:
    """Symlink (or copy) only contribs/expend golden parquets into a sub-dir.

    run_vectorized runs file_discovery on the fixtures_dir, which would also
    pick up CVR1/FILER/etc. files.  We point discovery at a directory containing
    ONLY the two files so the flat_txns worker sees exactly what we ORM-load.
    """
    import shutil

    sub = tmp_path / "flat_only"
    sub.mkdir()
    for name in _FLAT_TXN_FILENAMES:
        src = FIXTURES / name
        shutil.copy2(src, sub / name)
    return sub


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_flat_txns_family_matches_orm(tmp_path: Path):
    """unified_transactions from RCPT+EXPN must be row-for-row equal (ORM vs vectorized)."""
    # ORM side: load ONLY RCPT+EXPN into its own DB (FK=ON is fine; committees
    # aren't added so committee_id FK is a dangling reference, but SQLite won't
    # enforce it without explicit PRAGMA foreign_keys=ON).
    orm_engine = _make_engine(tmp_path / "orm.db", enforce_fk=False)
    _load_golden_rcpt_expn(orm_engine)

    # Vectorized side: run only flat_txns worker over the RCPT/EXPN files.
    flat_fixtures = _make_rcpt_expn_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, flat_fixtures)

    orm = _flat_txns_only(snapshot_unified(orm_engine))
    vec = _flat_txns_only(snapshot_unified(vec_engine))

    assert orm["unified_transactions"], "ORM produced no transactions — fixture/loader problem"
    assert vec["unified_transactions"], "vectorized produced no transactions — family not running"

    diffs = diff_snapshots(orm, vec)
    assert diffs == [], "unified_transactions diverge from ORM:\n" + "\n".join(diffs)


def test_flat_txns_contribution_count(tmp_path: Path):
    """RCPT fixture must produce at least one CONTRIBUTION transaction."""
    flat_fixtures = _make_rcpt_expn_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, flat_fixtures)
    snap = snapshot_unified(vec_engine)
    txns = snap.get("unified_transactions", [])
    contributions = [t for t in txns if t.get("transaction_type") == "CONTRIBUTION"]
    assert contributions, "expected CONTRIBUTION transactions from RCPT fixture"


def test_flat_txns_expenditure_count(tmp_path: Path):
    """EXPN fixture must produce at least one EXPENDITURE transaction."""
    flat_fixtures = _make_rcpt_expn_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, flat_fixtures)
    snap = snapshot_unified(vec_engine)
    txns = snap.get("unified_transactions", [])
    expenditures = [t for t in txns if t.get("transaction_type") == "EXPENDITURE"]
    assert expenditures, "expected EXPENDITURE transactions from EXPN fixture"


# ---------------------------------------------------------------------------
# Dim layer gate (flat_txns_dims family)
# ---------------------------------------------------------------------------


def test_flat_txns_dims_family_matches_orm(tmp_path: Path):
    """The 4 dim tables (unified_addresses, unified_persons, unified_entities,
    unified_committees) must be row-for-row equal (ORM vs vectorized).

    Scope is restricted to the 4 tables in ``_DIM_TABLES``.  Detail/junction
    tables (contributions, expenditures, transaction_persons) are NOT gated here
    — they belong to a future linkage slice that performs real id-joins and adds
    FK->natural-key resolution to the harness.

    Uses enforce_fk=False — FK parent tables (unified_reports, etc.) are NOT seeded;
    surrogate FK columns are dropped by the harness, natural-key columns are compared.
    """
    orm_engine = _make_engine(tmp_path / "orm_dims.db", enforce_fk=False)
    _load_golden_rcpt_expn(orm_engine)

    flat_fixtures = _make_rcpt_expn_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec_dims.db", enforce_fk=False)
    run_vectorized(vec_engine, flat_fixtures)

    orm = _dims_only(snapshot_unified(orm_engine))
    vec = _dims_only(snapshot_unified(vec_engine))

    # Sanity: ORM must have produced rows in every dim table
    for tbl in _DIM_TABLES:
        assert orm.get(tbl), f"ORM produced no {tbl} rows — fixture/loader problem"

    diffs = diff_snapshots(orm, vec)
    assert diffs == [], "dim tables diverge from ORM:\n" + "\n".join(diffs)
