from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from app.cli.state import State, resolve_state

console = Console()


def run_download(
    state: State,
    *,
    overwrite: bool = False,
    headless: bool = False,
    out: Path | None = None,
) -> int:
    ctx = resolve_state(state, data_folder=out)

    from app.states.texas import DownloadError, TECDownloader

    downloader = TECDownloader(config=ctx.config)

    try:
        with console.status(f"[bold green]Downloading {state.value} campaign finance data..."):
            result_path = downloader.download(
                overwrite=overwrite,
                headless=headless,
                output_dir=ctx.temp_folder,
            )
    except DownloadError as exc:
        console.print(f"[red]Download failed:[/red] {exc}")
        return 1

    destination = Path(result_path) if not isinstance(result_path, Path) else result_path
    console.print(f"[green]Download complete.[/green] Files saved to {destination}")
    return 0


def download(
    state: Annotated[State, typer.Argument(help="US state to download data for.")],
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", "-o", help="Re-download even if files already exist."),
    ] = False,
    headless: Annotated[
        bool,
        typer.Option(
            "--headless/--no-headless",
            help="Run the browser headless (default: visible browser).",
        ),
    ] = False,
    out: Annotated[
        Path | None,
        typer.Option("--out", help="Output directory for downloaded files."),
    ] = None,
) -> None:
    raise typer.Exit(run_download(state, overwrite=overwrite, headless=headless, out=out))
