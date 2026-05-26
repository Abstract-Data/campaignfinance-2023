from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from app.cli.convert import run_convert
from app.cli.download import run_download
from app.cli.state import State
from app.cli.verify import run_verify

console = Console()


def run_prepare(
    state: State,
    *,
    overwrite: bool = False,
    headless: bool = False,
    skip_download: bool = False,
    out: Path | None = None,
) -> int:
    ctx_folder = out.expanduser().resolve() if out is not None else None

    if not skip_download:
        if run_download(state, overwrite=overwrite, headless=headless, out=ctx_folder) != 0:
            console.print("[red]Prepare failed at download stage.[/red]")
            return 1

    if run_convert(state, overwrite=overwrite, folder=ctx_folder) != 0:
        console.print("[red]Prepare failed at convert stage.[/red]")
        return 1

    if run_verify(state, folder=ctx_folder) != 0:
        console.print("[red]Prepare failed at verify stage.[/red]")
        return 1

    console.print(f"[green]Prepare complete for {state.value}.[/green]")
    return 0


def prepare(
    state: Annotated[State, typer.Argument(help="US state to prepare data for.")],
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="Overwrite existing files at each stage."),
    ] = False,
    headless: Annotated[
        bool,
        typer.Option(
            "--headless/--no-headless",
            help="Run the download browser headless (default: visible browser).",
        ),
    ] = False,
    skip_download: Annotated[
        bool,
        typer.Option(
            "--skip-download",
            help="Skip the download stage and run convert then verify only.",
        ),
    ] = False,
    out: Annotated[
        Path | None,
        typer.Option("--out", help="Output directory for downloaded and converted files."),
    ] = None,
) -> None:
    raise typer.Exit(
        run_prepare(
            state,
            overwrite=overwrite,
            headless=headless,
            skip_download=skip_download,
            out=out,
        )
    )
