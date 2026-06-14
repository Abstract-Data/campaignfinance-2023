"""Pure-unit tests for diff_snapshots (no DB) — the equality contract of the gate."""
from __future__ import annotations

from app.core.ingest_equivalence import diff_snapshots


def test_identical_snapshots_are_equal():
    snap = {"unified_committees": [{"filer_id": "A", "name": "X"}]}
    assert diff_snapshots(snap, snap) == []


def test_order_independent():
    a = {"t": [{"k": "1"}, {"k": "2"}]}
    b = {"t": [{"k": "2"}, {"k": "1"}]}
    assert diff_snapshots(a, b) == []


def test_none_distinct_from_empty_string():
    a = {"t": [{"k": None}]}
    b = {"t": [{"k": ""}]}
    assert diff_snapshots(a, b) != []


def test_detects_extra_row():
    a = {"t": [{"k": "1"}]}
    b = {"t": [{"k": "1"}, {"k": "2"}]}
    diffs = diff_snapshots(a, b)
    assert any("t:" in d and "right-only" in d for d in diffs)


def test_detects_missing_table():
    a = {"t": [{"k": "1"}], "u": [{"k": "9"}]}
    b = {"t": [{"k": "1"}]}
    diffs = diff_snapshots(a, b)
    assert any("u" in d and "only in left" in d for d in diffs)


def test_duplicate_multiplicity_matters():
    a = {"t": [{"k": "1"}, {"k": "1"}]}
    b = {"t": [{"k": "1"}]}
    assert diff_snapshots(a, b) != []
