"""detail_children family package: LOAN, DEBT, CRED, TRVL, ASSET, PLDG."""

from __future__ import annotations

from app.core.ingest_vectorized.registry import register

from .worker import DetailChildrenWorker

register(DetailChildrenWorker())

__all__ = ["DetailChildrenWorker"]
