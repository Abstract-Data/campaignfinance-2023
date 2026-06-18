"""cf db — database utility subcommands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(name="db", help="Database utility commands.", no_args_is_help=True)


@app.command(name="size")
def size(
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Path to save the Markdown report."),
    ] = None,
) -> None:
    """Print table sizes, index usage, and total DB size. Save report to docs/."""
    from scripts.db_size_report import _run_report

    _run_report(output=output)
