"""Structural guards for detail_children subpackage decomposition."""
from __future__ import annotations

from pathlib import Path

PKG = Path("app/core/ingest_vectorized/families/detail_children")

# Per-file caps from Plan 4 + approved builders exception
_CAPS = {
    "worker.py": 80,
    "transactions.py": 400,
    "dims.py": 400,
    "specs.py": 250,
    "exprs.py": 300,
    "builders.py": 700,  # 6 per-type builders + shared helpers; approved at 654
    "__init__.py": 20,
}


def test_detail_children_modules_under_loc_cap():
    for py in PKG.glob("*.py"):
        loc = len(py.read_text().splitlines())
        cap = _CAPS.get(py.name, 450)
        assert loc <= cap, f"{py.name} has {loc} lines (cap {cap})"
