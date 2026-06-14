"""`run_vectorized` — the vectorized ingest entrypoint.

Mirrors `scripts/loaders/production_loader.discover_and_load`: seed reference rows,
discover source files, then dispatch each registered family worker in FK order. The
per-family transforms live in `app/core/ingest_vectorized/families/`; importing this
package registers them.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from app.core.ingest_vectorized.registry import FAMILY_WORKERS, FamilyContext
from app.logger import Logger

_logger = Logger(__name__)


def _seed(session: Any, state: str):
    """Seed committee_types + the State row (reuse the ORM loader helpers)."""
    from scripts.loaders.production_loader import _ensure_committee_types, _ensure_state

    _ensure_committee_types(session)
    return _ensure_state(session, state)


def run_vectorized(
    engine: Any,
    fixtures_dir: Path | str,
    *,
    state: str = "texas",
) -> dict[str, int]:
    """Vectorized ingest of *state* source files under *fixtures_dir* into *engine*.

    Returns counters: ``{discovered, loaded, families_run}``. Families register
    themselves on import; each is run in ascending ``priority`` (FK order).
    """
    from sqlmodel import Session

    # Import families for their registration side effect (deferred to avoid cycles).
    from app.core.ingest_vectorized import families  # noqa: F401
    from scripts.loaders.file_discovery import discover_state_files

    fixtures_dir = Path(fixtures_dir)
    discovered = discover_state_files(state, base_dir=fixtures_dir)
    by_type: dict[str, list[Path]] = {}
    for item in discovered:
        by_type.setdefault(item.record_type, []).append(item.path)

    counts = {"discovered": len(discovered), "loaded": 0, "families_run": 0}
    session = Session(engine, expire_on_commit=False)
    try:
        state_row = _seed(session, state)
        ctx = FamilyContext(
            session=session,
            engine=engine,
            state_id=state_row.id,
            state_code=state_row.code,
            state=state,
        )
        for worker in sorted(FAMILY_WORKERS, key=lambda w: w.priority):
            files_by_type = {rt: by_type[rt] for rt in worker.record_types if rt in by_type}
            if not files_by_type:
                continue
            result = worker.run(files_by_type, ctx)
            counts["loaded"] += int(result.get("loaded", 0))
            counts["families_run"] += 1
            _logger.info(f"[vectorized] family {sorted(worker.record_types)} -> {result}")
    finally:
        session.close()
    return counts
