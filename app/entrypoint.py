"""Production ``cf`` CLI — bootstrap, scrape, load, and cadence scheduling."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from app.cli import convert, download, prepare, verify
from app.cli.state import State
from app.scheduler import CadenceScheduler, GracefulShutdown

__version__ = "0.1.0"

app = typer.Typer(
    name="cf",
    help="Campaign finance pipeline: scrape, prepare, load, and schedule.",
    no_args_is_help=True,
)

console = Console()
_shutdown = GracefulShutdown()


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the CLI version and exit.",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging."),
    ] = False,
) -> None:
    if verbose:
        logging.basicConfig(level=logging.DEBUG, force=True)


def run_bootstrap(*, skip_ddl: bool = False, echo: bool = False) -> int:
    from app.core.unified_database import get_db_manager

    manager = get_db_manager(bootstrap=not skip_ddl, echo=echo)
    console.print(f"[green]Database ready.[/green] {manager.database_url}")
    return 0


def bootstrap(
    skip_ddl: Annotated[
        bool,
        typer.Option(
            "--skip-ddl",
            help="Connect only; do not run create_all (schema already exists).",
        ),
    ] = False,
    echo: Annotated[
        bool,
        typer.Option("--echo", help="Echo SQL statements from SQLAlchemy."),
    ] = False,
) -> None:
    """Initialize the database manager and run DDL bootstrap."""
    raise typer.Exit(run_bootstrap(skip_ddl=skip_ddl, echo=echo))


def run_scrape(
    state: State,
    *,
    overwrite: bool = False,
    headless: bool = False,
    out: Path | None = None,
) -> int:
    from app.cli.download import run_download

    return run_download(state, overwrite=overwrite, headless=headless, out=out)


def scrape(
    state: Annotated[State, typer.Argument(help="US state to scrape data for.")],
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", "-o", help="Re-scrape even if files already exist."),
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
        typer.Option("--out", help="Output directory for scraped files."),
    ] = None,
) -> None:
    """Scrape campaign-finance files from the state portal (download stage)."""
    raise typer.Exit(run_scrape(state, overwrite=overwrite, headless=headless, out=out))


def run_load(
    state: str,
    *,
    preset: str = "production",
    dry_run: bool = False,
    should_stop: Callable[[], bool] | None = None,
) -> int:
    from app.core.unified_database import get_db_manager
    from scripts.loaders.loader_config import get_config
    from scripts.loaders.production_loader import discover_and_load

    manager = get_db_manager()
    config = get_config(preset)
    stop_fn = should_stop if should_stop is not None else (lambda: _shutdown.requested)

    try:
        results = discover_and_load(
            state,
            config,
            dry_run=dry_run,
            db_url=manager.database_url,
            should_stop=stop_fn,
        )
    except ValueError as exc:
        console.print(f"[red]Load failed:[/red] {exc}")
        return 1

    if _shutdown.requested:
        console.print(
            "[yellow]Shutdown requested; finished current file batch.[/yellow] "
            f"{results}"
        )
        return 0

    console.print(f"[green]Load complete.[/green] {results}")
    return 0


def load(
    state: Annotated[str, typer.Argument(help="State to load (e.g. texas).")],
    preset: Annotated[
        str,
        typer.Option(
            "--preset",
            "-p",
            help="Loader preset: development | testing | production.",
        ),
    ] = "production",
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Discover files but skip DB writes."),
    ] = False,
) -> None:
    """Discover parquet/CSV under tmp/<state> and load into the database."""
    _shutdown.install()
    try:
        raise typer.Exit(run_load(state, preset=preset, dry_run=dry_run))
    finally:
        _shutdown.restore()


def _scheduled_pipeline(state: State) -> None:
    from app.cli.prepare import run_prepare

    exit_code = run_prepare(state, headless=True)
    if exit_code != 0:
        msg = f"prepare failed for {state.value} with exit {exit_code}"
        raise RuntimeError(msg)
    load_code = run_load(state.value, preset="production")
    if load_code != 0:
        msg = f"load failed for {state.value} with exit {load_code}"
        raise RuntimeError(msg)


def run_schedule(
    state: State,
    *,
    interval_hours: float = 24.0,
) -> int:
    scheduler = CadenceScheduler(_shutdown)
    _shutdown.install()

    def job() -> None:
        console.print(f"[bold]Running scheduled pipeline for {state.value}[/bold]")
        _scheduled_pipeline(state)

    return scheduler.run_periodic(
        job,
        interval_seconds=interval_hours * 3600.0,
    )


def schedule(
    state: Annotated[State, typer.Argument(help="US state for scheduled runs.")],
    interval_hours: Annotated[
        float,
        typer.Option(
            "--interval-hours",
            "-i",
            help="Hours between full scrape→prepare→load cycles.",
        ),
    ] = 24.0,
) -> None:
    """Run scrape→prepare→load on a cadence until SIGTERM (finish current cycle)."""
    raise typer.Exit(run_schedule(state, interval_hours=interval_hours))


app.command(name="bootstrap")(bootstrap)
app.command(name="scrape")(scrape)
app.command(name="load")(load)
app.command(name="schedule")(schedule)

# Data-prep commands (state CLI) remain available on the production entrypoint.
app.command(name="download")(download.download)
app.command(name="convert")(convert.convert)
app.command(name="verify")(verify.verify)
app.command(name="prepare")(prepare.prepare)
