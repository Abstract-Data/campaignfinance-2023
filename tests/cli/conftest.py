"""Pytest configuration for CLI tests."""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_APP_DIR = _PROJECT_ROOT / "app"

for path in (_PROJECT_ROOT, _APP_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
