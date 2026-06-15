"""Gate: the vectorized FILER family == the ORM loader (committees + officers).

Loads the FULL golden fixture set (ALL files, including ``filers_golden.parquet``)
via BOTH the ORM loader and ``run_vectorized``, then asserts that
``diff_snapshots`` (with ``resolve_fks=True`` so relational linkage is verified)
restricted to the two tables the FILER family owns —
``unified_committees`` and ``unified_committee_persons`` — is empty.

The pre-existing dim test only loads RCPT/EXPN, so it never exercised FILER; this
closes that gap by proving the authoritative committee (name / type / status /
address) and every committee officer match the ORM row-for-row.
"""

from __future__ import annotations

from pathlib import Path

from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES, _load_golden, _make_engine

# Tables the FILER family owns / authoritatively populates (the gate scope).
_TABLES = ("unified_committees", "unified_committee_persons")


def _restrict(snap: dict) -> dict:
    return {t: snap.get(t, []) for t in _TABLES}


def test_filer_family_matches_orm(tmp_path: Path):
    """Committees + committee_persons must be row-for-row equal (ORM vs vectorized),
    with FKs resolved to natural keys, when the FULL golden set is loaded both ways."""
    orm_engine = _make_engine(tmp_path / "orm.db", enforce_fk=False)
    _load_golden(orm_engine)

    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, FIXTURES)

    orm = _restrict(snapshot_unified(orm_engine, resolve_fks=True))
    vec = _restrict(snapshot_unified(vec_engine, resolve_fks=True))

    for t in _TABLES:
        assert orm[t], f"ORM produced no {t} rows — fixture/loader problem"
        assert vec[t], f"vectorized produced no {t} rows — FILER family not writing {t}"

    diffs = diff_snapshots(orm, vec)
    assert diffs == [], "FILER tables diverge from ORM:\n" + "\n".join(diffs)
