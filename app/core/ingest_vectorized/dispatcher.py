"""`run_vectorized` — the vectorized ingest entrypoint.

Mirrors `scripts/loaders/production_loader.discover_and_load`: seed reference rows,
discover source files, then dispatch each registered family worker in FK order. The
per-family transforms live in `app/core/ingest_vectorized/families/`; importing this
package registers them.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console

from app.core.ingest_vectorized.progress import family_worker_label, run_with_progress
from app.core.ingest_vectorized.registry import FAMILY_WORKERS, FamilyContext, FamilyWorker
from app.core.loader_bootstrap import ensure_committee_types, ensure_state
from app.logger import Logger

_logger = Logger(__name__)


@dataclass(frozen=True)
class _FamilyStage:
    worker: FamilyWorker
    files_by_type: dict[str, list[Path]]


@dataclass(frozen=True)
class _FinalizeStage:
    label: str
    run: Callable[[], dict[str, int]]


def _seed(session: Any, state: str):
    """Seed committee_types + the State row."""
    ensure_committee_types(session)
    return ensure_state(session, state)


def _apply_stage_result(
    counts: dict[str, int],
    stage: _FamilyStage | _FinalizeStage,
    result: dict[str, int],
) -> None:
    if isinstance(stage, _FamilyStage):
        counts["loaded"] += int(result.get("loaded", 0))
        counts["families_run"] += 1
        return
    if "entity_reps_linked" in result:
        counts["entity_reps_linked"] = int(result["entity_reps_linked"])
    if "campaigns" in result:
        counts["campaigns"] = int(result["campaigns"])
        counts["campaign_entities"] = int(result.get("campaign_entities", 0))


def run_vectorized(
    engine: Any,
    fixtures_dir: Path | str,
    *,
    state: str = "texas",
    dry_run: bool = False,
    should_stop: Callable[[], bool] | None = None,
    show_progress: bool | None = None,
    progress_console: Console | None = None,
) -> dict[str, int]:
    """Vectorized ingest of *state* source files under *fixtures_dir* into *engine*."""
    from sqlmodel import Session

    from app.core.ingest_vectorized import families  # noqa: F401
    from scripts.loaders.file_discovery import discover_state_files

    fixtures_dir = Path(fixtures_dir)
    discovered = discover_state_files(state, base_dir=fixtures_dir)
    by_type: dict[str, list[Path]] = {}
    for item in discovered:
        by_type.setdefault(item.record_type, []).append(item.path)

    counts: dict[str, int] = {"discovered": len(discovered), "loaded": 0, "families_run": 0}
    if dry_run:
        _logger.info(f"[vectorized] dry-run: discovered {len(discovered)} file(s); skipping writes")
        return counts

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

        family_stages: list[_FamilyStage] = []
        for worker in sorted(FAMILY_WORKERS, key=lambda w: w.priority):
            files_by_type = {rt: by_type[rt] for rt in worker.record_types if rt in by_type}
            if files_by_type:
                family_stages.append(_FamilyStage(worker=worker, files_by_type=files_by_type))

        def _run_entity_reps() -> dict[str, int]:
            from app.core.ingest_vectorized.finalize import finalize_entity_representatives

            linked = finalize_entity_representatives(session, ctx.state_id)
            _logger.info(f"[vectorized] finalize_entity_representatives -> {linked}")
            return {"entity_reps_linked": linked}

        def _run_campaigns() -> dict[str, int]:
            from app.core.ingest_vectorized.campaigns import finalize_campaigns

            camp_counts = finalize_campaigns(session, ctx.state_id)
            _logger.info(f"[vectorized] finalize_campaigns -> {camp_counts}")
            return camp_counts

        finalize_stages = [
            _FinalizeStage(label="Link entity representatives", run=_run_entity_reps),
            _FinalizeStage(label="Finalize campaigns", run=_run_campaigns),
        ]
        stage_plan: list[_FamilyStage | _FinalizeStage] = [*family_stages, *finalize_stages]

        def _stage_label(stage: _FamilyStage | _FinalizeStage) -> str:
            if isinstance(stage, _FamilyStage):
                return family_worker_label(stage.worker)
            return stage.label

        def _run_stage(stage: _FamilyStage | _FinalizeStage) -> dict[str, int]:
            if isinstance(stage, _FamilyStage):
                result = stage.worker.run(stage.files_by_type, ctx)
                _logger.info(f"[vectorized] family {sorted(stage.worker.record_types)} -> {result}")
            else:
                result = stage.run()
            _apply_stage_result(counts, stage, result)
            return result

        stage_results = run_with_progress(
            stage_plan,
            label_fn=_stage_label,
            run_fn=_run_stage,
            title="Ingest",
            show_progress=show_progress,
            console=progress_console,
            should_stop=should_stop,
        )

        if len(stage_results) < len(stage_plan):
            _logger.info("[vectorized] stop requested; halting after current stage")
            counts["stopped"] = 1
    finally:
        session.close()
    return counts
