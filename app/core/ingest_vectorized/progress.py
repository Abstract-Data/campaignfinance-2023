"""Rich progress helpers for vectorized ingest stages."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import TypeVar

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from app.core.ingest_vectorized.registry import FamilyWorker

T = TypeVar("T")
R = TypeVar("R")


class IngestStop(Exception):
    """Graceful shutdown between ingest stages."""


def progress_enabled(show_progress: bool | None) -> bool:
    """Return True when progress bars should render (TTY unless forced off/on)."""
    if show_progress is False:
        return False
    if show_progress is True:
        return True
    return sys.stderr.isatty()


def family_worker_label(worker: FamilyWorker) -> str:
    """Human-readable label for a registered family worker."""
    class_name = type(worker).__name__
    if class_name.endswith("Worker"):
        class_name = class_name[: -len("Worker")]
    record_types = worker.record_types
    types_label = ", ".join(sorted(record_types))
    return f"{class_name} ({types_label})" if types_label else class_name


def run_with_progress(
    items: list[T],
    *,
    label_fn: Callable[[T], str],
    run_fn: Callable[[T], R],
    title: str,
    show_progress: bool | None = None,
    console: Console | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> list[R]:
    """Run *run_fn* over *items*, showing one progress step per item when enabled."""
    if not items:
        return []

    def _run_loop() -> list[R]:
        out: list[R] = []
        for item in items:
            if should_stop is not None and should_stop():
                break
            out.append(run_fn(item))
        return out

    if not progress_enabled(show_progress):
        return _run_loop()

    out: list[R] = []
    progress_console = console or Console(stderr=True)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=progress_console,
        transient=False,
    ) as progress:
        task_id = progress.add_task(title, total=len(items))
        for index, item in enumerate(items, start=1):
            if should_stop is not None and should_stop():
                break
            progress.update(
                task_id,
                description=f"{title} {index}/{len(items)}: {label_fn(item)}",
            )
            out.append(run_fn(item))
            progress.advance(task_id)
    return out
