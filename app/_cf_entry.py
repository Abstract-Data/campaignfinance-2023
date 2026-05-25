"""Console entry point for the ``cf`` CLI."""

from __future__ import annotations


def main() -> None:
    from app.cli.main import app

    app(prog_name="cf")
