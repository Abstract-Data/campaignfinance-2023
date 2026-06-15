"""P0 baseline: the ingest equivalence harness works against the ORM loader.

Loads the committed golden fixtures through the current ORM loader into sqlite,
snapshots the unified tables, and asserts the snapshot is non-trivial, deterministic
(load twice -> identical), and that `diff_snapshots` flags an injected change. This
is the gate the future vectorized engine must pass (orm_snap == vec_snap).
"""
from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import event
from sqlmodel import Session, SQLModel

from app.core import models  # noqa: F401 — register unified_* tables
from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.load_cache import BuilderCache
from app.core.source_models import (  # noqa: F401 — register Phase-0 tables
    CommitteePurpose,
    ExpenditureCategory,
    SpacLink,
    UnifiedNotice,
    UnifiedPledge,
    UnifiedReport,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ingest_golden"


def _make_engine(db_path: Path, *, enforce_fk: bool = True):
    """Build a file-backed sqlite engine. ``enforce_fk=False`` for per-family gate
    runs that load only one family (its FK-parent tables aren't populated, but the
    natural-key FK columns are still compared)."""
    from sqlalchemy import create_engine

    engine = create_engine(f"sqlite:///{db_path}")

    @event.listens_for(engine, "connect")
    def _fk(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON" if enforce_fk else "PRAGMA foreign_keys=OFF")
        cur.close()

    # Only non-schema-qualified tables: a plain create_all on sqlite raises
    # "unknown database texas" once schema-qualified models are registered.
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    return engine


def _load_golden(engine) -> None:
    """Run the ORM loader over the golden fixtures (controlled schema creation)."""
    # Link transactions->reports, but skip reconcile_report_totals: it does a
    # Decimal - float subtraction that only works on Postgres (sqlite returns
    # NUMERIC as float). Report-total reconciliation is a derived post-step, not
    # part of ingest equivalence; validate it separately on Postgres.
    from app.core.source_models import link_transactions_to_reports
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
            ),
            key=lambda p_rt: (P._FILE_PRIORITY.get(p_rt[1] or "", 50), str(p_rt[0])),
        )
        assert discovered, "no golden fixtures discovered"
        for path, rtype in discovered:
            _n, _rej, cache = P._load_file(
                path, rtype, cfg, state="texas", state_id=state.id,
                state_code=state.code, session=session, cache=cache, max_remaining=None,
            )
        link_transactions_to_reports(session)
        session.commit()
    finally:
        session.close()


def _snapshot_fresh_load(tmp_path: Path, name: str):
    engine = _make_engine(tmp_path / f"{name}.db")
    _load_golden(engine)
    return snapshot_unified(engine)


@pytest.fixture(scope="module")
def golden_snapshot(tmp_path_factory):
    return _snapshot_fresh_load(tmp_path_factory.mktemp("golden_a"), "a")


def test_snapshot_is_non_trivial(golden_snapshot):
    """The ORM baseline produced rows in the key tables."""
    assert golden_snapshot.get("unified_committees"), "no committees loaded"
    assert golden_snapshot.get("unified_transactions"), "no transactions loaded"
    assert golden_snapshot.get("unified_persons"), "no persons loaded"
    # CAND enrichment present (candidate-role links on expenditures).
    tp = golden_snapshot.get("unified_transaction_persons", [])
    assert any(r.get("role") in ("CANDIDATE", "candidate") for r in tp), \
        "expected at least one candidate-role link from the CAND fixture"


def test_snapshot_excludes_volatile_columns(golden_snapshot):
    for table, rows in golden_snapshot.items():
        if not rows:
            continue
        cols = set(rows[0].keys())
        assert "id" not in cols, f"{table} leaked surrogate id"
        assert "uuid" not in cols, f"{table} leaked uuid"
        assert "created_at" not in cols, f"{table} leaked created_at"


def test_load_is_deterministic(golden_snapshot, tmp_path_factory):
    """A second independent load yields an identical snapshot (diff == [])."""
    second = _snapshot_fresh_load(tmp_path_factory.mktemp("golden_b"), "b")
    diffs = diff_snapshots(golden_snapshot, second)
    assert diffs == [], "ORM load is not deterministic:\n" + "\n".join(diffs)


def test_diff_detects_injected_change(golden_snapshot):
    """Sanity: diff_snapshots flags a deliberate difference (the gate has teeth)."""
    import copy

    mutated = copy.deepcopy(golden_snapshot)
    # Drop one transaction row from the mutated side.
    assert mutated["unified_transactions"], "need transactions to mutate"
    mutated["unified_transactions"].pop()
    diffs = diff_snapshots(golden_snapshot, mutated)
    assert any("unified_transactions" in d for d in diffs), \
        "diff_snapshots failed to detect a dropped transaction row"
