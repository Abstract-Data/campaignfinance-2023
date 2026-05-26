from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from app.cli.state import State, resolve_state

console = Console()


def run_verify(state: State, *, folder: Path | None = None) -> int:
    ctx = resolve_state(state, data_folder=folder)

    from app.states.texas.texas_coverage import verify_coverage

    report = verify_coverage(ctx.temp_folder)

    table = Table(title=f"{state.value.title()} Coverage Report")
    table.add_column("Record Type", style="cyan")
    table.add_column("Files")
    table.add_column("Rows", justify="right")
    table.add_column("Status")

    for row in report.rows:
        files = ", ".join(path.name for path in row.files) if row.files else "—"
        status_style = {
            "present": "green",
            "missing": "red",
            "empty": "yellow",
        }.get(row.status, "")
        table.add_row(
            row.record_type,
            files,
            str(row.row_count),
            f"[{status_style}]{row.status}[/{status_style}]" if status_style else row.status,
        )

    console.print(table)

    if not report.ok:
        console.print("[red]Coverage verification failed.[/red]")
        return 1

    console.print("[green]Coverage verification passed.[/green]")
    return 0


def verify(
    state: Annotated[State, typer.Argument(help="US state to verify coverage for.")],
) -> None:
    raise typer.Exit(run_verify(state))
