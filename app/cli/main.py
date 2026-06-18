from __future__ import annotations

import logging
from typing import Annotated

import typer

from app.cli import convert, db, download, prepare, resolve_prune, verify

__version__ = "0.1.0"

app = typer.Typer(
    name="cf",
    help="Prepare state campaign-finance data for the resolution pipeline.",
    no_args_is_help=True,
)


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


app.command(name="download")(download.download)
app.command(name="convert")(convert.convert)
app.command(name="verify")(verify.verify)
app.command(name="prepare")(prepare.prepare)
app.command(name="field-coverage")(verify.field_coverage)
app.add_typer(db.app, name="db")

_resolve_app = typer.Typer(
    name="resolve",
    help="Resolve pipeline commands.",
    no_args_is_help=True,
)
_resolve_app.command(name="prune")(resolve_prune.prune)
app.add_typer(_resolve_app, name="resolve")


if __name__ == "__main__":
    app()
