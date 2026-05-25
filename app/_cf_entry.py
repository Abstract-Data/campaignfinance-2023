"""Console entry point: mirror test PYTHONPATH (project root + app/)."""

from __future__ import annotations

from app._path_setup import ensure_legacy_import_paths


def main() -> None:
    ensure_legacy_import_paths()
    from app.cli.main import app

    app(prog_name="cf")
