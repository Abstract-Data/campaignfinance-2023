"""Characterization tests for app.core.ingest_vectorized.id_maps."""

from __future__ import annotations

from sqlmodel import Session, SQLModel, create_engine

from app.core.ingest_vectorized.id_maps import address_id_map
from app.core.models import UnifiedAddress


def _make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine, tables=[UnifiedAddress.__table__])
    return engine


def test_address_id_map_empty_engine_returns_empty_frame():
    engine = _make_engine()
    frame = address_id_map(engine)
    assert frame.height == 0
    assert set(frame.columns) == {"address_id", "_k_s1", "_k_city", "_k_state", "_k_zip"}


def test_address_id_map_lowercases_keys():
    engine = _make_engine()
    with Session(engine) as session:
        session.add(
            UnifiedAddress(
                street_1="123 MAIN ST",
                city="Austin",
                state="TX",
                zip_code="78701",
            )
        )
        session.commit()
    frame = address_id_map(engine)
    row = frame.row(0, named=True)
    assert row["_k_s1"] == "123 main st"
    assert row["_k_city"] == "austin"
    assert isinstance(row["address_id"], int)
