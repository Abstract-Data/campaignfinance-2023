"""Unit tests for ``common.filter_new_rows`` — Polars-only, no DB required."""

from __future__ import annotations

import polars as pl


def test_filter_new_rows_drops_existing_and_inbatch_dups():
    """Rows already in the DB (case-insensitive match) and in-batch dups are removed."""
    from app.core.ingest_vectorized import common

    frame = pl.DataFrame(
        [
            {"first_name": "Jane", "last_name": "Doe", "state_id": 1},  # existing (case-diff)
            {"first_name": "JANE", "last_name": "DOE", "state_id": 1},  # in-batch dup of above
            {"first_name": "Ann", "last_name": "Lee", "state_id": 1},  # new
        ]
    )
    existing = pl.DataFrame([{"first_name": "jane", "last_name": "doe", "state_id": 1}])
    out = common.filter_new_rows(
        frame,
        existing,
        key_cols=["first_name", "last_name", "state_id"],
        normalize_lower=["first_name", "last_name"],
    )
    assert out.height == 1
    assert out["last_name"].to_list() == ["Lee"]


def test_filter_new_rows_all_new():
    """When no rows exist, all frame rows are returned."""
    from app.core.ingest_vectorized import common

    frame = pl.DataFrame(
        [
            {"name": "Alpha", "state_id": 1},
            {"name": "Beta", "state_id": 2},
        ]
    )
    existing = pl.DataFrame(schema={"name": pl.Utf8, "state_id": pl.Int64})
    out = common.filter_new_rows(frame, existing, key_cols=["name", "state_id"])
    assert out.height == 2


def test_filter_new_rows_all_existing():
    """When all rows already exist, the result is empty."""
    from app.core.ingest_vectorized import common

    frame = pl.DataFrame([{"name": "Alpha", "state_id": 1}])
    existing = pl.DataFrame([{"name": "Alpha", "state_id": 1}])
    out = common.filter_new_rows(frame, existing, key_cols=["name", "state_id"])
    assert out.height == 0


def test_filter_new_rows_inbatch_dedup_only():
    """Batch has two identical rows, existing is empty — only one survives."""
    from app.core.ingest_vectorized import common

    frame = pl.DataFrame(
        [
            {"name": "Alpha", "state_id": 1},
            {"name": "Alpha", "state_id": 1},
        ]
    )
    existing = pl.DataFrame(schema={"name": pl.Utf8, "state_id": pl.Int64})
    out = common.filter_new_rows(frame, existing, key_cols=["name", "state_id"])
    assert out.height == 1


def test_filter_new_rows_mixed_state_ids():
    """Same name in different states is NOT a duplicate."""
    from app.core.ingest_vectorized import common

    frame = pl.DataFrame(
        [
            {"name": "Alpha", "state_id": 1},
            {"name": "Alpha", "state_id": 2},
        ]
    )
    existing = pl.DataFrame([{"name": "Alpha", "state_id": 1}])
    out = common.filter_new_rows(frame, existing, key_cols=["name", "state_id"])
    assert out.height == 1
    assert out["state_id"].to_list() == [2]


def test_filter_new_rows_drops_key_cols_from_result():
    """Temporary ``_k_*`` key columns do not appear in the output."""
    from app.core.ingest_vectorized import common

    frame = pl.DataFrame([{"first_name": "Ann", "last_name": "Lee", "state_id": 1}])
    existing = pl.DataFrame(
        schema={"first_name": pl.Utf8, "last_name": pl.Utf8, "state_id": pl.Int64}
    )
    out = common.filter_new_rows(
        frame,
        existing,
        key_cols=["first_name", "last_name", "state_id"],
        normalize_lower=["first_name", "last_name"],
    )
    for col in out.columns:
        assert not col.startswith("_k_"), f"key column {col!r} leaked into result"
