"""Production ``cf`` CLI — bootstrap, scrape, load, and cadence scheduling."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console

from app.cli import convert, download, prepare, resolve_prune, verify
from app.cli import db as db_cli
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


def _run_vectorized_load(
    state: str,
    config: Any,
    db_url: str,
    *,
    dry_run: bool,
    should_stop: Callable[[], bool],
) -> dict[str, int]:
    """Bootstrap the schema (reusing the ORM loader's `_get_session`: create_all + dedup
    indexes + additive columns) then run the vectorized engine against the state's source dir."""
    from app.core.ingest_vectorized import run_vectorized
    from scripts.loaders.loader_config import STATE_GLOB_CONFIGS
    from scripts.loaders.production_loader import _get_session

    if config.max_records is not None:
        console.print(
            "[yellow]Note:[/yellow] the vectorized engine ignores the preset row cap "
            f"(max_records={config.max_records}); it loads every discovered row. "
            "Use --engine orm for a capped subset."
        )
    session = _get_session(db_url)
    engine = session.get_bind()
    session.close()
    fixtures_dir = STATE_GLOB_CONFIGS[state].base_dir
    return run_vectorized(
        engine, fixtures_dir, state=state, dry_run=dry_run, should_stop=should_stop
    )


def run_migrate(*, db_url: str | None = None) -> int:
    """Apply pending Alembic migrations (`alembic upgrade head`) — the deploy step that brings
    an EXISTING database to the latest schema. Fresh DBs get the full schema from the baseline."""
    from app.core.unified_database import get_db_manager
    from app.db_migrate import current_revision, upgrade_head

    url = db_url or get_db_manager(bootstrap=False).database_url
    try:
        before = current_revision(url)
        upgrade_head(url)
        after = current_revision(url)
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]Migration failed:[/red] {exc}")
        return 1
    if before == after:
        console.print(f"[green]Database already at head[/green] ({after}).")
    else:
        console.print(f"[green]Migrated[/green] {before or '(empty)'} -> {after}.")
    return 0


def migrate() -> None:
    """Apply pending database migrations (alembic upgrade head)."""
    raise typer.Exit(run_migrate())


def run_load(
    state: str,
    *,
    preset: str = "production",
    dry_run: bool = False,
    should_stop: Callable[[], bool] | None = None,
    engine: str | None = None,
) -> int:
    import os

    from app.core.unified_database import get_db_manager
    from scripts.loaders.loader_config import get_config

    manager = get_db_manager()
    config = get_config(preset)
    stop_fn = should_stop if should_stop is not None else (lambda: _shutdown.requested)
    # Engine selection: vectorized is the default (P5 flip); `--engine orm` or
    # INGEST_ENGINE=orm falls back to the ORM loader.
    engine_name = (engine or os.environ.get("INGEST_ENGINE", "vectorized")).lower()

    try:
        if engine_name == "orm":
            from scripts.loaders.production_loader import discover_and_load

            results = discover_and_load(
                state,
                config,
                dry_run=dry_run,
                db_url=manager.database_url,
                should_stop=stop_fn,
            )
        else:
            results = _run_vectorized_load(
                state, config, manager.database_url, dry_run=dry_run, should_stop=stop_fn
            )
    except ValueError as exc:
        console.print(f"[red]Load failed:[/red] {exc}")
        return 1

    if _shutdown.requested:
        console.print(
            f"[yellow]Shutdown requested; finished current file batch.[/yellow] {results}"
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
    engine: Annotated[
        str | None,
        typer.Option(
            "--engine",
            help="Ingest engine: vectorized (default, fast COPY path) | orm (row-by-row).",
        ),
    ] = None,
) -> None:
    """Discover parquet/CSV under tmp/<state> and load into the database."""
    _shutdown.install()
    try:
        raise typer.Exit(run_load(state, preset=preset, dry_run=dry_run, engine=engine))
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
app.command(name="migrate")(migrate)
app.command(name="scrape")(scrape)
app.command(name="load")(load)
app.command(name="schedule")(schedule)

# Data-prep commands (state CLI) remain available on the production entrypoint.
app.command(name="download")(download.download)
app.command(name="convert")(convert.convert)
app.command(name="verify")(verify.verify)
app.command(name="prepare")(prepare.prepare)
app.command(name="field-coverage")(verify.field_coverage)

# DB utility commands (Wave 0 of DB Bloat Remediation).
app.add_typer(db_cli.app, name="db")

# Resolve pipeline commands (Wave 3a of DB Bloat Remediation).
_resolve_app = typer.Typer(
    name="resolve",
    help="Resolve pipeline commands.",
    no_args_is_help=True,
)
_resolve_app.command(name="prune")(resolve_prune.prune)
app.add_typer(_resolve_app, name="resolve")
