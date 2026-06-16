"""Tests for snapshot_unified(resolve_fks=True): the harness FK->natural-key resolution
that lets the gate verify relational linkage (the linkage-infra enabling detail/junction
+ detail_children/cand families to be gated on their entity/person links).
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, insert
from sqlmodel import Session, SQLModel

from app.core import models  # noqa: F401 — register tables
from app.core.enums import PersonType
from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.models import UnifiedAddress, UnifiedPerson


def _engine(db_path: Path):
    engine = create_engine(f"sqlite:///{db_path}")
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    return engine


def _seed(engine, *, addr_id: int, person_id: int, street: str, last: str) -> None:
    with Session(engine) as s:
        s.execute(
            insert(UnifiedAddress.__table__),
            [
                {
                    "id": addr_id,
                    "uuid": f"a{addr_id}",
                    "street_1": street,
                    "city": "X",
                    "state": "TX",
                    "zip_code": "1",
                }
            ],
        )
        s.execute(
            insert(UnifiedPerson.__table__),
            [
                {
                    "id": person_id,
                    "uuid": f"p{person_id}",
                    "first_name": "J",
                    "last_name": last,
                    "person_type": PersonType.INDIVIDUAL,
                    "address_id": addr_id,
                }
            ],
        )
        s.commit()


def test_resolution_equal_despite_different_surrogate_ids(tmp_path: Path):
    """Same logical data with different surrogate ids compares equal under resolution."""
    a = _engine(tmp_path / "a.db")
    _seed(a, addr_id=1, person_id=1, street="100 A St", last="DOE")
    b = _engine(tmp_path / "b.db")
    _seed(b, addr_id=77, person_id=42, street="100 A St", last="DOE")

    assert (
        diff_snapshots(snapshot_unified(a, resolve_fks=True), snapshot_unified(b, resolve_fks=True))
        == []
    )


def test_resolution_detects_linkage_difference_that_drop_fk_misses(tmp_path: Path):
    """A person linked to a DIFFERENT address: drop-FK can't see it; resolution does."""
    a = _engine(tmp_path / "a.db")
    _seed(a, addr_id=1, person_id=1, street="100 A St", last="DOE")
    c = _engine(tmp_path / "c.db")
    _seed(c, addr_id=1, person_id=1, street="999 DIFFERENT Blvd", last="DOE")

    pa = {"unified_persons": snapshot_unified(a)["unified_persons"]}
    pc = {"unified_persons": snapshot_unified(c)["unified_persons"]}
    # Default (surrogate FK dropped): the person rows look identical — the address
    # divergence is invisible. This is the limitation resolution closes.
    assert diff_snapshots(pa, pc) == []

    ra = {"unified_persons": snapshot_unified(a, resolve_fks=True)["unified_persons"]}
    rc = {"unified_persons": snapshot_unified(c, resolve_fks=True)["unified_persons"]}
    assert diff_snapshots(ra, rc) != []


def test_resolution_default_off_is_backward_compatible(tmp_path: Path):
    """Default snapshot still drops surrogate FKs (address_id absent from person rows)."""
    a = _engine(tmp_path / "a.db")
    _seed(a, addr_id=1, person_id=1, street="100 A St", last="DOE")
    persons = snapshot_unified(a)["unified_persons"]
    assert persons and "address_id" not in persons[0]
