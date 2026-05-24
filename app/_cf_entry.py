"""Console entry point: mirror test PYTHONPATH (project root + app/)."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_legacy_import_paths() -> None:
    """Match tests/cli/conftest.py so `from abcs` / `from funcs` resolve."""
    app_dir = Path(__file__).resolve().parent
    project_root = app_dir.parent
    for path in (project_root, app_dir):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)


def main() -> None:
    _ensure_legacy_import_paths()
    from app.cli.main import app

    app(prog_name="cf")
