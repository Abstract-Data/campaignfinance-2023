"""Gate: the vectorized CAND enrichment family == the ORM loader.

CAND is an enrichment pass — it links a candidate person/entity onto an EXISTING
expenditure (it creates no transaction). So both sides load BOTH the EXPN
(expend_golden) and CAND (cand_golden) fixtures: the EXPN slice creates the
expenditures, then the CAND slice enriches them.

On the VEC side ``run_vectorized`` runs flat_txns_dims (9) + flat_txns (10) +
flat_txns_detail (11) to build the expenditures, then the cand family (12) to add
the candidate links. We assert ``diff_snapshots`` restricted to the candidate
tables is empty with ``resolve_fks=True`` so the candidate <-> expenditure LINKAGE
(person + entity + transaction surrogate ids resolved to natural keys) is verified,
not just row counts.

Mirrors ``test_flat_txns_detail_family``: ``_make_engine(enforce_fk=...)``, the
ORM-load-subset pattern, and the single-fixture-dir trick.
"""

from __future__ import annotations

from pathlib import Path

from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES, _make_engine

_CAND_RECORD_TYPES = frozenset({"EXPN", "CAND"})
_CAND_FILENAMES = frozenset({"expend_golden.parquet", "cand_golden.parquet"})

# Tables the CAND enrichment writes / enriches, all linkage-resolved.
#   unified_expenditures        — the rows CAND enriches (must stay intact).
#   unified_persons             — candidate persons (find-or-create).
#   unified_entities            — candidate entities (find-or-create).
#   unified_transaction_persons — the CANDIDATE junction rows (the linkage itself).
TABLES = (
    "unified_expenditures",
    "unified_persons",
    "unified_entities",
    "unified_transaction_persons",
)


def _load_golden_expn_cand(engine) -> None:
    """ORM-load ONLY the EXPN and CAND golden fixtures (FK parents not seeded)."""
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
                if item.record_type in _CAND_RECORD_TYPES
            ),
            key=lambda p_rt: (P._FILE_PRIORITY.get(p_rt[1] or "", 50), str(p_rt[0])),
        )
        assert discovered, "no EXPN/CAND golden fixtures discovered"
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


def _make_expn_cand_fixtures_dir(tmp_path: Path) -> Path:
    """Copy only expend/cand golden parquets into a sub-dir for discovery."""
    import shutil

    sub = tmp_path / "cand_only"
    sub.mkdir()
    for name in _CAND_FILENAMES:
        shutil.copy2(FIXTURES / name, sub / name)
    return sub


def test_cand_family_matches_orm(tmp_path: Path):
    """Candidate persons / entities / CANDIDATE junction rows must be row-for-row
    equal to the ORM loader, with surrogate FKs RESOLVED to parent natural keys.

    resolve_fks=True is what verifies the candidate <-> expenditure linkage (the
    junction's transaction_id -> expenditure natural key, person_id -> candidate
    person, entity_id -> candidate entity) — not just row counts.
    """
    orm_engine = _make_engine(tmp_path / "orm_cand.db", enforce_fk=False)
    _load_golden_expn_cand(orm_engine)

    cand_fixtures = _make_expn_cand_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec_cand.db", enforce_fk=False)
    run_vectorized(vec_engine, cand_fixtures)

    orm_full = snapshot_unified(orm_engine, resolve_fks=True)
    vec_full = snapshot_unified(vec_engine, resolve_fks=True)

    for tbl in TABLES:
        assert orm_full.get(tbl), f"ORM produced no {tbl} rows — fixture/loader problem"
        assert vec_full.get(tbl), f"vectorized produced no {tbl} rows — family not running"

    # The candidate links must actually be present on BOTH sides.
    assert any(
        r.get("role") in ("CANDIDATE", "candidate")
        for r in orm_full["unified_transaction_persons"]
    ), "ORM produced no CANDIDATE junction rows"
    assert any(
        r.get("role") in ("CANDIDATE", "candidate")
        for r in vec_full["unified_transaction_persons"]
    ), "vectorized produced no CANDIDATE junction rows"

    orm = {t: orm_full.get(t, []) for t in TABLES}
    vec = {t: vec_full.get(t, []) for t in TABLES}

    diffs = diff_snapshots(orm, vec)
    assert diffs == [], "candidate tables diverge from ORM:\n" + "\n".join(diffs)


def test_cand_family_links_present(tmp_path: Path):
    """The CAND fixture must produce at least one candidate-role link (unresolved
    snapshot — quick smoke check that the family runs at all)."""
    cand_fixtures = _make_expn_cand_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, cand_fixtures)
    snap = snapshot_unified(vec_engine)
    tp = snap.get("unified_transaction_persons", [])
    assert any(r.get("role") in ("CANDIDATE", "candidate") for r in tp), \
        "expected candidate-role links from the CAND fixture"
