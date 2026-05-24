from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.progress import Progress

from app.cli.state import State, resolve_state

console = Console()


def run_convert(
    state: State,
    *,
    overwrite: bool = False,
    keep_csv: bool = True,
) -> int:
    ctx = resolve_state(state)

    from app.states.texas.texas_converter import convert_folder

    with Progress() as progress:
        task = progress.add_task("Converting CSV files to parquet...", total=None)

        def on_progress(path: Path) -> None:
            progress.update(task, description=f"Converting {path.name}")

        result = convert_folder(
            ctx.temp_folder,
            overwrite=overwrite,
            keep_csv=keep_csv,
            on_progress=on_progress,
        )

    console.print(
        f"Converted: {result.converted}, "
        f"Skipped: {result.skipped}, "
        f"Failed: {len(result.failed)}"
    )
    for path, error in result.failed:
        console.print(f"  [red]{path.name}:[/red] {error}")

    if not result.ok:
        console.print("[red]Conversion completed with failures.[/red]")
        return 1

    console.print("[green]Conversion complete.[/green]")
    return 0


def convert(
    state: Annotated[State, typer.Argument(help="US state to convert data for.")],
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="Overwrite existing parquet files."),
    ] = False,
    keep_csv: Annotated[
        bool,
        typer.Option(
            "--keep-csv/--no-keep-csv",
            help="Keep source CSV files after conversion (default: keep).",
        ),
    ] = True,
) -> None:
    raise typer.Exit(run_convert(state, overwrite=overwrite, keep_csv=keep_csv))
