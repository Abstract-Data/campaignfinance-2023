"""Phase 1 stage callables conforming to the Stage protocol.

All four callables share the signature::

    (session: Session, run_id: int, config: dict[str, Any]) -> dict[str, Any]

``stage1_build_resolution_input`` wraps the raw ``build_resolution_input``
function (which takes a positional ``state_code`` arg) so it matches the
protocol expected by ``ResolutionRun.run()``.
"""

from __future__ import annotations

from typing import Any

from sqlmodel import Session

from app.resolve.blocking import run_blocking_stage
from app.resolve.stages.fastpath import run_fastpath_stage
from app.resolve.stages.survivorship import run_survivorship_stage
from app.resolve.standardize.stage1 import build_resolution_input


def stage1_build_resolution_input(
    session: Session,
    run_id: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Stage 1 wrapper: standardize source records into resolution_input.

    Extracts ``state_code`` from *config* and delegates to
    :func:`app.resolve.standardize.stage1.build_resolution_input`.

    Returns
    -------
    dict
        ``{"records_in": <n>}`` where *n* is the number of staged rows.
    """
    state_code: str = config.get("state_code", "TX")
    count = build_resolution_input(session, run_id, state_code)
    return {"records_in": count}


__all__ = [
    "run_blocking_stage",
    "run_fastpath_stage",
    "run_survivorship_stage",
    "stage1_build_resolution_input",
]
