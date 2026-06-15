"""Per-record-type family workers for the vectorized ingest engine.

Each family module registers its worker (via `registry.register`) on import. Phase B
adds: reports (CVR1/FINL), refs (FILER/lookups), flat_txns (RCPT/EXPN),
detail_children (LOAN/DEBT/...), cand. Importing this package imports them all.
"""

from __future__ import annotations

# Family modules are imported here as they land so the dispatcher registers them.
from app.core.ingest_vectorized.families import (  # noqa: F401
    cand,
    detail_children,
    filer,
    flat_txns,
    flat_txns_detail,
    flat_txns_dims,
    reports,
)
