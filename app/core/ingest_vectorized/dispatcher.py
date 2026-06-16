"""`run_vectorized` — the vectorized ingest entrypoint.

Mirrors `scripts/loaders/production_loader.discover_and_load`: seed reference rows,
discover source files, then dispatch each registered family worker in FK order. The
per-family transforms live in `app/core/ingest_vectorized/families/`; importing this
package registers them.
"""

from __future__ import annotations

from collections.abc import Callable
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
    dry_run: bool = False,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, int]:
    """Vectorized ingest of *state* source files under *fixtures_dir* into *engine*.

    Returns counters: ``{discovered, loaded, families_run}``. Families register
    themselves on import; each is run in ascending ``priority`` (FK order).

    ``dry_run`` discovers files and returns the counts without writing anything (parity
    with the ORM loader's ``--dry-run``). ``should_stop`` is polled before each family so a
    graceful shutdown request stops cleanly between families (the unit of work) — committed
    families persist, like the ORM loader's per-file ``should_stop`` checkpointing. The
    *engine* must already be bootstrapped (tables + dedup indexes); the ``cf load`` wiring
    bootstraps via ``production_loader._get_session`` before calling this.
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
    if dry_run:
        _logger.info(f"[vectorized] dry-run: discovered {len(discovered)} file(s); skipping writes")
        return counts

    _stopped = False
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
            if should_stop is not None and should_stop():
                _logger.info("[vectorized] stop requested; halting before next family")
                _stopped = True
                break
            files_by_type = {rt: by_type[rt] for rt in worker.record_types if rt in by_type}
            if not files_by_type:
                continue
            result = worker.run(files_by_type, ctx)
            counts["loaded"] += int(result.get("loaded", 0))
            counts["families_run"] += 1
            _logger.info(f"[vectorized] family {sorted(worker.record_types)} -> {result}")

        if _stopped:
            counts["stopped"] = 1
            return counts

        # Post-load reconciliation (analogous to the ORM's _link_after_load): assign each
        # PERSON/ORGANIZATION entity ONE representative person deterministically, so the
        # one-to-one unified_entities.person_id constraint holds (the families themselves
        # no longer set it — see finalize.py).
        from app.core.ingest_vectorized.finalize import finalize_entity_representatives

        linked = finalize_entity_representatives(session, ctx.state_id)
        counts["entity_reps_linked"] = linked
        _logger.info(f"[vectorized] finalize_entity_representatives -> {linked}")

        # Build campaigns + campaign_entities last: they need committees, COMMITTEE
        # entities, and transactions all in place (see campaigns.finalize_campaigns).
        from app.core.ingest_vectorized.campaigns import finalize_campaigns

        camp_counts = finalize_campaigns(session, ctx.state_id)
        counts["campaigns"] = camp_counts["campaigns"]
        counts["campaign_entities"] = camp_counts["campaign_entities"]
        _logger.info(f"[vectorized] finalize_campaigns -> {camp_counts}")
    finally:
        session.close()
    return counts
