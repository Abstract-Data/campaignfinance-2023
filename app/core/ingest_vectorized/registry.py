"""Family-worker registry for the vectorized ingest engine.

Each record-type family registers a worker that transforms its source files into
unified rows. Mirrors `app/core/source_models/__init__.py::RECORD_TYPE_BUILDERS`,
but workers operate on whole frames (Polars) rather than per-row.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@dataclass
class FamilyContext:
    """Shared per-run context handed to every family worker."""

    session: Any
    engine: Any
    state_id: int
    state_code: str
    state: str = "texas"
    # Shared dim id-maps (committees/persons/entities/addresses) populated by the
    # foundation so families resolve FKs without re-deduping. Filled in later phases.
    dims: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class FamilyWorker(Protocol):
    """A vectorized worker for one record-type family."""

    #: Record types this worker handles (e.g. {"CVR1", "FINL"}).
    record_types: frozenset[str]
    #: Lower = earlier (FK ordering), mirroring production_loader._FILE_PRIORITY.
    priority: int

    def run(self, files: list[Path], ctx: FamilyContext) -> dict[str, int]:
        """Transform + persist this family's files. Returns counters."""
        ...


# Populated by `register()` as family modules are imported.
FAMILY_WORKERS: list[FamilyWorker] = []


def register(worker: FamilyWorker) -> FamilyWorker:
    """Register a family worker (idempotent by type identity)."""
    if not any(type(w) is type(worker) for w in FAMILY_WORKERS):
        FAMILY_WORKERS.append(worker)
    return worker
