"""Thin orchestrator for the detail_children family: LOAN, DEBT, CRED, TRVL, ASSET, PLDG.

Priority 11 runs AFTER flat_txns_dims (9) and flat_txns (10). Actual work is
delegated to :mod:`.dims`, :mod:`.transactions`, and :mod:`.builders`.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from app.core.ingest_vectorized.registry import FamilyContext
from app.logger import Logger

from .builders import write_details
from .dims import write_committees, write_dims
from .exprs import _ensure_cols, _read, _spec_cols
from .specs import _SPECS
from .transactions import write_transactions

_logger = Logger(__name__)


class DetailChildrenWorker:
    """LOAN/DEBT/CRED/TRVL/ASSET/PLDG: dims + transactions + detail children."""

    record_types = frozenset({"LOAN", "DEBT", "CRED", "TRVL", "ASSET", "PLDG"})
    priority = 11  # after flat_txns_dims (9) and flat_txns (10)

    def run(self, files_by_type: dict[str, list[Path]], ctx: FamilyContext) -> dict[str, int]:
        # Read & normalize every present type, in load-priority order. The ORIGINAL
        # source columns (before _ensure_cols pads missing ones) are tracked per type
        # for potential future provenance use (and for pledge raw_json).
        frames: dict[str, pl.DataFrame] = {}
        self._orig_cols: dict[str, list[str]] = {}
        for rt in sorted(files_by_type, key=lambda r: _SPECS[r].priority):
            df = _read(files_by_type[rt])
            if df is None or df.height == 0:
                continue
            spec = _SPECS[rt]
            self._orig_cols[rt] = list(df.columns)
            df = _ensure_cols(df, _spec_cols(spec))
            frames[rt] = df

        if not frames:
            return {"loaded": 0}

        ordered = sorted(frames, key=lambda r: _SPECS[r].priority)

        # Omit-null address match lookup, built ONCE from addresses already in the DB
        # (FILER + flat_txns, all earlier priorities) so a street-less loan/debt/etc.
        # party inherits a fuller existing address's street — the ORM's
        # _find_address_by_fields. Built before any address this family writes, and
        # reused by BOTH the dim layer (write_dims) and the detail->person link
        # (_party_keys) so the two compute the SAME person key.
        self._addr_lookup = ctx.get_address_lookup()

        counts: dict[str, int] = {}

        # 1. Committees (shared natural-key dim).
        counts["committees"] = write_committees(frames, ordered, ctx)

        # 2. Addresses + persons + entities.
        counts["addresses"], counts["persons"], counts["entities"] = write_dims(
            self, frames, ordered, ctx
        )

        # 3. Transactions.
        counts["transactions"] = write_transactions(frames, ordered, ctx)

        # 4. Detail children (+ guarantors).
        counts.update(write_details(self, frames, ordered, ctx))

        loaded = sum(counts.values())
        _logger.info(f"[vectorized.detail_children] loaded {loaded} rows: {counts}")
        return {"loaded": loaded, **counts}
