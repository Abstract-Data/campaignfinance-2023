"""Characterization tests for app.core.ingest_vectorized.id_maps."""

from __future__ import annotations

from sqlmodel import Session, SQLModel, create_engine

from app.core.ingest_vectorized.id_maps import address_id_map, address_key_frame
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


def test_address_key_frame_empty_engine_returns_empty_frame():
    engine = _make_engine()
    frame = address_key_frame(engine)
    assert frame.height == 0
    assert set(frame.columns) == {"street_1", "city", "state", "zip_code"}


def test_address_key_frame_derives_from_id_map():
    """address_key_frame delegates to address_id_map: no address_id, values pre-lowercased."""
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
    frame = address_key_frame(engine)
    assert frame.height == 1
    assert "address_id" not in frame.columns
    row = frame.row(0, named=True)
    assert row["street_1"] == "123 main st"
    assert row["city"] == "austin"
    assert row["state"] == "tx"
    assert row["zip_code"] == "78701"


def test_address_key_frame_normalize_lower_idempotent():
    """filter_new_rows with normalize_lower on pre-lowercased values is idempotent."""
    import polars as pl

    from app.core.ingest_vectorized.common import filter_new_rows

    engine = _make_engine()
    with Session(engine) as session:
        session.add(
            UnifiedAddress(
                street_1="456 ELM AVE",
                city="Dallas",
                state="TX",
                zip_code="75201",
            )
        )
        session.commit()

    existing = address_key_frame(engine)
    # Incoming frame replicates the same address in mixed case — should be filtered out.
    incoming = pl.DataFrame(
        {"street_1": ["456 Elm Ave"], "city": ["Dallas"], "state": ["TX"], "zip_code": ["75201"]},
        schema={"street_1": pl.Utf8, "city": pl.Utf8, "state": pl.Utf8, "zip_code": pl.Utf8},
    )
    result = filter_new_rows(
        incoming,
        existing,
        key_cols=["street_1", "city", "state", "zip_code"],
        normalize_lower=["street_1", "city", "state"],
        join_nulls=True,
    )
    assert result.height == 0, "existing address should be filtered out by anti-join"


def test_flat_txns_dims_does_not_import_detail_children():
    import ast
    from pathlib import Path

    src = Path("app/core/ingest_vectorized/families/flat_txns_dims.py").read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "detail_children" in node.module:
            raise AssertionError("flat_txns_dims must not import detail_children")
