"""Smoke test for common.write_frame — the Polars-frame -> bulk_upsert -> DB glue."""

from __future__ import annotations

import polars as pl
from sqlmodel import Session, SQLModel, create_engine, select

from app.core import models  # noqa: F401 — register tables
from app.core.ingest_vectorized.common import write_frame
from app.core.source_models import ExpenditureCategory  # noqa: F401


def _engine():
    engine = create_engine("sqlite://")
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    return engine


def test_write_frame_inserts_rows():
    engine = _engine()
    frame = pl.DataFrame({"code": ["A1", "B2"], "description": ["Advertising", "Bank fees"]})
    with Session(engine, expire_on_commit=False) as s:
        n = write_frame(s, ExpenditureCategory, frame, conflict_cols=["code"])
        s.commit()
        rows = s.exec(select(ExpenditureCategory)).all()
    assert n == 2
    assert {r.code: r.description for r in rows} == {"A1": "Advertising", "B2": "Bank fees"}


def test_write_frame_is_idempotent_on_conflict():
    engine = _engine()
    frame = pl.DataFrame({"code": ["A1"], "description": ["Advertising"]})
    with Session(engine, expire_on_commit=False) as s:
        write_frame(s, ExpenditureCategory, frame, conflict_cols=["code"])
        s.commit()
        # Re-write with an updated description -> upsert, no duplicate row.
        frame2 = pl.DataFrame({"code": ["A1"], "description": ["Advertising (rev)"]})
        write_frame(
            s, ExpenditureCategory, frame2, conflict_cols=["code"], update_cols=["description"]
        )
        s.commit()
        rows = s.exec(select(ExpenditureCategory)).all()
    assert len(rows) == 1
    assert rows[0].description == "Advertising (rev)"


def test_write_frame_empty_is_noop():
    engine = _engine()
    empty = pl.DataFrame(
        {"code": [], "description": []}, schema={"code": pl.Utf8, "description": pl.Utf8}
    )
    with Session(engine, expire_on_commit=False) as s:
        assert write_frame(s, ExpenditureCategory, empty, conflict_cols=["code"]) == 0
