"""Legacy import paths: project root + app/ on sys.path (see tests/cli/conftest.py)."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_legacy_import_paths() -> None:
    """So `from abcs`, `from funcs`, `from logger` resolve without PYTHONPATH."""
    app_dir = Path(__file__).resolve().parent
    project_root = app_dir.parent
    for path in (project_root, app_dir):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)
