"""ResolutionRun orchestrator and Stage protocol.

``ResolutionRun`` manages the full ``match_run`` lifecycle:
  running → completed
  running → failed

It calls each stage in order, merges their count dicts, and guarantees
that a failed run drops its staging tables so no partial canonical write
survives.

``Stage`` is the protocol every pipeline stage callable must satisfy.
task-1z injects the concrete stage list; this module defines the contract.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel

from app.resolve.models.resolution import MatchRun, PassType, RunStatus

logger = logging.getLogger(__name__)

_ENGINE_VERSION = "1.0.0"

# Resolve-layer tables created by the CLI (canonical + resolution + staging).
_RESOLUTION_SCHEMA_MODELS: tuple[type[SQLModel], ...] = ()


def _resolution_schema_models() -> tuple[type[SQLModel], ...]:
    """Import and return resolve-layer SQLModel classes (lazy, no unified models)."""
    global _RESOLUTION_SCHEMA_MODELS
    if _RESOLUTION_SCHEMA_MODELS:
        return _RESOLUTION_SCHEMA_MODELS

    import app.resolve.models.canonical  # noqa: F401
    import app.resolve.models.resolution  # noqa: F401
    from app.resolve.blocking import CandidatePair
    from app.resolve.models.canonical import (
        CanonicalAddress,
        CanonicalCampaign,
        CanonicalEntity,
        CanonicalNameHistory,
    )
    from app.resolve.models.resolution import (
        AddressCrosswalk,
        CampaignCrosswalk,
        EntityCrosswalk,
        MatchDecision,
        MergeReview,
    )
    from app.resolve.stages.cluster import ClusterAssignment
    from app.resolve.stages.fastpath import MergeEdge
    from app.resolve.stages.score import ScoredPair
    from app.resolve.standardize.staging import ResolutionInput

    _RESOLUTION_SCHEMA_MODELS = (
        CanonicalAddress,
        CanonicalCampaign,
        CanonicalEntity,
        CanonicalNameHistory,
        MatchRun,
        EntityCrosswalk,
        AddressCrosswalk,
        CampaignCrosswalk,
        MatchDecision,
        MergeReview,
        ResolutionInput,
        CandidatePair,
        MergeEdge,
        ScoredPair,
        ClusterAssignment,
    )
    return _RESOLUTION_SCHEMA_MODELS


def resolution_schema_table_names() -> frozenset[str]:
    """Return table names owned by the resolve pipeline schema."""
    return frozenset(model.__tablename__ for model in _resolution_schema_models())  # type: ignore[attr-defined]


def ensure_resolution_schema(engine: Engine) -> None:
    """Create only resolve-layer tables; never the full unified schema."""
    tables = [model.__table__ for model in _resolution_schema_models()]
    SQLModel.metadata.create_all(engine, tables=tables)


# Counter keys written to ``match_run`` by ``ResolutionRun.finish()``.
_COUNTER_COLS = (
    "records_in",
    "pairs_compared",
    "auto_merges",
    "queued",
    "rejected",
    "canonical_out",
)


@runtime_checkable
class Stage(Protocol):
    """Protocol for a resolution pipeline stage.

    A stage is any callable with signature
    ``(session, run_id, config) -> dict``.  The returned dict holds
    **counter snapshots** keyed by :data:`_COUNTER_COLS` names.

    Count merge semantics (``ResolutionRun.run``)
    ---------------------------------------------
    Stage dicts are merged left-to-right via ``dict.update``.  For each
    counter key:

    * **Same key in multiple stages** — the **last** stage wins (overwrite,
      not sum).  Example: stage 1 returns ``{"records_in": 10}`` and stage 2
      returns ``{"records_in": 5}`` → final ``records_in`` is ``5``.
    * **Key absent from a stage dict** — earlier value is preserved.
    * **Empty dict** — valid; the stage contributes no counts.

    Stages should therefore return **final totals** for counters they own,
    not deltas, unless they intentionally replace an earlier stage's value.
    """

    def __call__(
        self,
        session: Session,
        run_id: int,
        config: dict[str, Any],
    ) -> dict[str, Any]: ...


class ResolutionRun:
    """Orchestrates a single resolution pipeline execution.

    Parameters
    ----------
    state_code:
        Two-letter state code (e.g. ``"TX"``).
    config:
        Pipeline configuration dict.  Snapshotted verbatim into
        ``match_run.config_json`` so the run is fully reproducible.
    """

    def __init__(self, state_code: str, config: dict[str, Any]) -> None:
        self.state_code = state_code
        self.config = config
        self._run: MatchRun | None = None

    @property
    def run_id(self) -> int | None:
        """The ``match_run.id`` after ``start()`` is called, else ``None``."""
        if self._run is None:
            return None
        return self._run.id

    def start(self, session: Session) -> MatchRun:
        """Insert a ``match_run`` row with ``status="running"`` and return it.

        ``config_json`` is a deterministic JSON snapshot of ``self.config``
        (keys sorted), ensuring identical configs produce identical JSON.
        """
        run = MatchRun(
            state_code=self.state_code,
            pass_type=PassType.entity,
            engine_version=_ENGINE_VERSION,
            config_json=json.dumps(self.config, sort_keys=True),
            started_at=datetime.now(timezone.utc),
            status=RunStatus.running,
        )
        session.add(run)
        session.commit()
        session.refresh(run)
        self._run = run
        logger.info("Started match_run id=%d state=%s", run.id, self.state_code)
        return run

    def finish(self, session: Session, counts: dict[str, Any]) -> None:
        """Mark the run ``completed``, set ``finished_at``, and write counts.

        Only counter columns present in *counts* are updated; missing keys
        leave the existing column values intact.

        Raises
        ------
        RuntimeError
            If ``start()`` has not been called.
        """
        if self._run is None:
            raise RuntimeError("ResolutionRun.start() must be called before finish()")

        run = session.get(MatchRun, self._run.id)
        if run is None:
            raise RuntimeError(f"MatchRun id={self._run.id} not found")

        run.status = RunStatus.completed
        run.finished_at = datetime.now(timezone.utc)

        for col in _COUNTER_COLS:
            if col in counts:
                setattr(run, col, int(counts[col]))

        session.add(run)
        session.commit()
        session.refresh(run)
        self._run = run
        logger.info("Completed match_run id=%d", run.id)

    def fail(self, session: Session, error: str) -> None:
        """Mark the run ``failed``, set ``finished_at``, and drop staging tables.

        Ensures no partial canonical write survives a failed run.

        Raises
        ------
        RuntimeError
            If ``start()`` has not been called.
        """
        if self._run is None:
            raise RuntimeError("ResolutionRun.start() must be called before fail()")

        session.rollback()

        run = session.get(MatchRun, self._run.id)
        if run is None:
            raise RuntimeError(f"MatchRun id={self._run.id} not found")

        run.status = RunStatus.failed
        run.finished_at = datetime.now(timezone.utc)
        session.add(run)
        session.commit()
        session.refresh(run)
        self._run = run

        try:
            from app.resolve.staging import drop_run_staging

            drop_run_staging(session, run.id)
        except Exception:
            logger.exception("Failed to drop staging tables for run id=%d", run.id)

        logger.error("Failed match_run id=%d: %s", run.id, error)

    def run(
        self,
        session: Session,
        stages: list[Stage],
    ) -> MatchRun:
        """Execute the pipeline: start, call each stage, then finish or fail.

        Each stage receives ``(session, run_id, config)`` and returns a
        counter dict (see :class:`Stage`).  Dicts are merged left-to-right
        with ``dict.update``: duplicate keys are **overwritten** by the later
        stage, not summed.  Only keys present in the merged dict are written
        to ``match_run`` at finish time.

        On any exception the run is marked ``failed`` and the exception is
        re-raised unchanged.  The stage list is injected by ``task-1z``; an
        empty list runs a no-op pipeline and completes cleanly.

        Returns
        -------
        MatchRun
            The completed (or failed, if post-failure return) run row.
        """
        self.start(session)
        merged_counts: dict[str, Any] = {}

        try:
            for stage in stages:
                stage_counts = stage(session, self._run.id, self.config)
                if stage_counts:
                    merged_counts.update(stage_counts)
        except Exception as exc:
            session.rollback()
            try:
                self.fail(session, str(exc))
            except Exception:
                logger.exception(
                    "Failed to record match_run failure for id=%s",
                    self._run.id if self._run else None,
                )
            raise

        self.finish(session, merged_counts)
        return self._run
