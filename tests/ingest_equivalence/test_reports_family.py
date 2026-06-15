"""Gate: the vectorized reports family == the ORM loader for unified_reports.

Loads the golden fixtures via the ORM loader and via run_vectorized (reports family
only registered), then asserts diff_snapshots restricted to unified_reports is empty.
"""

from __future__ import annotations

from pathlib import Path

from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES, _load_golden, _make_engine


def _reports_only(snap: dict) -> dict:
    return {"unified_reports": snap.get("unified_reports", [])}


def test_reports_family_matches_orm(tmp_path: Path):
    orm_engine = _make_engine(tmp_path / "orm.db")
    _load_golden(orm_engine)

    # FK off: this gate loads only the reports family, so committee/file_origin FK
    # parents aren't populated — the natural-key committee_id is still compared.
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, FIXTURES)

    orm = _reports_only(snapshot_unified(orm_engine))
    vec = _reports_only(snapshot_unified(vec_engine))

    assert orm["unified_reports"], "ORM produced no reports — fixture/loader problem"
    diffs = diff_snapshots(orm, vec)
    assert diffs == [], "reports diverge from ORM:\n" + "\n".join(diffs)


def test_reports_family_sets_is_final_from_finl(tmp_path: Path):
    """FINL records flip is_final on the matching report (parity with build_final_report)."""
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, FIXTURES)
    snap = snapshot_unified(vec_engine)["unified_reports"]
    assert any(r.get("is_final") in (True, 1) for r in snap), (
        "expected at least one is_final report from the FINL fixture"
    )
