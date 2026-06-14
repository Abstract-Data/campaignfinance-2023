from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Optional

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


def run_field_coverage(
    state: str = "texas",
    *,
    db_url: str | None = None,
    sample_rows: int = 5000,
    populated_threshold_pct: float = 1.0,
) -> int:
    """Run ``audit_field_coverage`` and print a per-state/record-type summary.

    Returns 0 on success, 1 on failure.
    """
    from sqlmodel import Session, SQLModel, create_engine

    from app.core.field_coverage import FieldCoverage, audit_field_coverage

    resolved_url = db_url or os.environ.get("DATABASE_URL", "sqlite://")
    engine = create_engine(resolved_url)
    SQLModel.metadata.create_all(engine, tables=[FieldCoverage.__table__])

    try:
        with Session(engine) as session:
            rows_written = audit_field_coverage(
                session,
                state,
                sample_rows=sample_rows,
                populated_threshold_pct=populated_threshold_pct,
            )
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]field-coverage audit failed: {exc}[/red]")
        return 1
    finally:
        engine.dispose()

    if rows_written == 0:
        console.print(
            f"[yellow]No source files found for state '{state}'. "
            "Run `cf prepare <state>` first.[/yellow]"
        )
        return 0

    # Re-query for the summary table.
    engine2 = create_engine(resolved_url)
    with Session(engine2) as session:
        from sqlmodel import select

        from app.core.field_coverage import FieldCoverage as FC

        all_rows = session.exec(select(FC).where(FC.state_code != "")).all()

    engine2.dispose()

    table = Table(title=f"Field Coverage — {state.title()}")
    table.add_column("Record Type", style="cyan")
    table.add_column("Column")
    table.add_column("Status")
    table.add_column("Fill %", justify="right")
    table.add_column("Unified Field")

    status_styles = {
        "MAPPED": "green",
        "STRUCTURAL": "blue",
        "HANDLED": "blue",
        "UNMAPPED_POPULATED": "yellow",
        "UNMAPPED_EMPTY": "dim",
    }
    for fc_row in all_rows:
        style = status_styles.get(fc_row.status, "")
        table.add_row(
            fc_row.record_type,
            fc_row.source_column,
            f"[{style}]{fc_row.status}[/{style}]" if style else fc_row.status,
            f"{fc_row.source_fill_pct:.1f}",
            fc_row.unified_field or "—",
        )

    console.print(table)
    console.print(f"[green]{rows_written} coverage rows audited.[/green]")
    return 0


def field_coverage(
    state: Annotated[
        Optional[str],
        typer.Argument(help="State name to audit (default: texas)."),
    ] = "texas",
    db_url: Annotated[
        Optional[str],
        typer.Option("--db-url", help="Database URL (defaults to DATABASE_URL env var or sqlite://)."),
    ] = None,
    sample_rows: Annotated[
        int,
        typer.Option("--sample-rows", help="Rows to sample per record type."),
    ] = 5000,
    populated_threshold_pct: Annotated[
        float,
        typer.Option("--threshold", help="Fill %% threshold for UNMAPPED_POPULATED (0-100)."),
    ] = 1.0,
) -> None:
    """Audit field coverage for a state and print a per-record-type summary."""
    raise typer.Exit(
        run_field_coverage(
            state or "texas",
            db_url=db_url,
            sample_rows=sample_rows,
            populated_threshold_pct=populated_threshold_pct,
        )
    )
