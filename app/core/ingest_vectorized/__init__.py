"""Vectorized (Polars + bulk-write) ingest engine.

A parallel ingest path to the per-row ORM loader, gated by the equivalence harness
(`app/core/ingest_equivalence.py`): it ships only when
``diff_snapshots(orm_snapshot, vectorized_snapshot) == []``.

See docs/design/vectorized-ingest-plan.md.
"""

from __future__ import annotations

from app.core.ingest_vectorized.dispatcher import run_vectorized

__all__ = ["run_vectorized"]
