"""Gate: the vectorized detail_children family == the ORM loader.

Loads ONLY the LOAN/DEBT/CRED/TRVL/ASSET/PLDG golden fixtures via both the ORM
loader and ``run_vectorized`` (detail_children family registered), then asserts
that ``diff_snapshots`` (with ``resolve_fks=True`` so relational linkage is
verified) restricted to the tables this family writes is empty.

Tables gated: the six detail tables + ``loan_guarantors`` + ``unified_transactions``
(restricted to these record types).
"""

from __future__ import annotations

from pathlib import Path

from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES, _make_engine

_RECORD_TYPES = frozenset({"LOAN", "DEBT", "CRED", "TRVL", "ASSET", "PLDG"})
_FILENAMES = frozenset(
    {
        "loans_golden.parquet",
        "debts_golden.parquet",
        "credits_golden.parquet",
        "travel_golden.parquet",
        "assets_golden.parquet",
        "pledges_golden.parquet",
    }
)

# Transaction types these record types produce (used to restrict unified_transactions).
_TXN_TYPES = frozenset({"LOAN", "DEBT", "CREDIT", "TRAVEL", "ASSET", "PLEDGE"})

# Tables this family writes (the equivalence gate scope).
_TABLES = (
    "unified_loans",
    "unified_debts",
    "unified_credits",
    "unified_travel",
    "unified_assets",
    "unified_pledges",
    "loan_guarantors",
    "unified_transactions",
)


def _load_golden_detail(engine) -> None:
    """ORM-load ONLY the six detail-children golden fixtures, in load order."""
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
                if item.record_type in _RECORD_TYPES
            ),
            key=lambda p_rt: (P._FILE_PRIORITY.get(p_rt[1] or "", 50), str(p_rt[0])),
        )
        assert discovered, "no detail-children golden fixtures discovered"
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


def _make_detail_fixtures_dir(tmp_path: Path) -> Path:
    import shutil

    sub = tmp_path / "detail_only"
    sub.mkdir()
    for name in _FILENAMES:
        shutil.copy2(FIXTURES / name, sub / name)
    return sub


def _restrict(snap: dict) -> dict:
    """Restrict a resolved snapshot to the family's tables; filter
    unified_transactions to this family's record types."""
    out: dict[str, list[dict]] = {}
    for t in _TABLES:
        rows = snap.get(t, [])
        if t == "unified_transactions":
            rows = [r for r in rows if r.get("transaction_type") in _TXN_TYPES]
        out[t] = rows
    return out


def test_detail_children_family_matches_orm(tmp_path: Path):
    """Every detail table + loan_guarantors + the family's transactions must be
    row-for-row equal (ORM vs vectorized), with FKs resolved to natural keys."""
    orm_engine = _make_engine(tmp_path / "orm.db", enforce_fk=False)
    _load_golden_detail(orm_engine)

    fixtures = _make_detail_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, fixtures)

    orm = _restrict(snapshot_unified(orm_engine, resolve_fks=True))
    vec = _restrict(snapshot_unified(vec_engine, resolve_fks=True))

    # Both sides non-empty for every gated table the ORM populated.
    for t in _TABLES:
        assert orm[t], f"ORM produced no {t} rows — fixture/loader problem"
        assert vec[t], f"vectorized produced no {t} rows — family not writing {t}"

    diffs = diff_snapshots(orm, vec)
    assert diffs == [], "detail_children tables diverge from ORM:\n" + "\n".join(diffs)
