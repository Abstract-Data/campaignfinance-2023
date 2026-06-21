"""Smoke-test that the detail_children subpackage exports correctly."""

from __future__ import annotations


def test_detail_children_subpackage_exports_worker():
    from app.core.ingest_vectorized.families.detail_children import DetailChildrenWorker

    assert DetailChildrenWorker.record_types == frozenset(
        {"LOAN", "DEBT", "CRED", "TRVL", "ASSET", "PLDG"}
    )
    assert DetailChildrenWorker.priority == 11


def test_detail_children_dims_importable():
    from app.core.ingest_vectorized.families.detail_children.dims import (
        all_parties,
        write_committees,
        write_dims,
    )

    assert callable(write_committees)
    assert callable(all_parties)
    assert callable(write_dims)


def test_detail_children_transactions_importable():
    from app.core.ingest_vectorized.families.detail_children.transactions import (
        transaction_frame,
        write_transactions,
    )

    assert callable(transaction_frame)
    assert callable(write_transactions)


def test_detail_children_builders_importable():
    from app.core.ingest_vectorized.families.detail_children.builders import (
        write_details,
    )

    assert callable(write_details)


def test_detail_children_worker_importable():
    from app.core.ingest_vectorized.families.detail_children.worker import (
        DetailChildrenWorker,
    )

    assert DetailChildrenWorker.record_types == frozenset(
        {"LOAN", "DEBT", "CRED", "TRVL", "ASSET", "PLDG"}
    )
    assert DetailChildrenWorker.priority == 11
