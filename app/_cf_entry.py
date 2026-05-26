"""Console entry point for the ``cf`` CLI (backward-compatible shim)."""

from __future__ import annotations


def main() -> None:
    from app.entrypoint import app

    app(prog_name="cf")
