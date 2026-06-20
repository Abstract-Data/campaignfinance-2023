"""ORM loader stage plan + Rich progress runner."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console

from app.core.ingest_vectorized.progress import run_with_progress


@dataclass(frozen=True)
class FileStage:
    path: Path
    record_type: str | None


def file_stage_label(stage: FileStage) -> str:
    return f"{stage.path.name} ({stage.record_type or 'unknown'})"


def run_file_stages(
    file_stages: list[FileStage],
    *,
    run_file: Callable[[FileStage], None],
    show_progress: bool | None = None,
    progress_console: Console | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> bool:
    """Run *file_stages* with optional Rich progress. Returns True if stopped early."""
    if not file_stages:
        return False

    results = run_with_progress(
        file_stages,
        label_fn=file_stage_label,
        run_fn=run_file,
        title="Load",
        show_progress=show_progress,
        console=progress_console,
        should_stop=should_stop,
    )
    return len(results) < len(file_stages)
