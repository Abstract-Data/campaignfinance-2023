from __future__ import annotations

import logging
from typing import Annotated

import typer

from app.cli import convert, download, prepare, verify

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
