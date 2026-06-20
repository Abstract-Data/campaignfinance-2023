"""Smoke-test that the detail_children subpackage exports correctly."""

from __future__ import annotations


def test_detail_children_subpackage_exports_worker():
    from app.core.ingest_vectorized.families.detail_children import DetailChildrenWorker

    assert DetailChildrenWorker.record_types == frozenset({"LOAN", "DEBT", "CRED", "TRVL", "ASSET", "PLDG"})
    assert DetailChildrenWorker.priority == 11
