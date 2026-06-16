from __future__ import annotations

from pathlib import Path

import tomli


def read_toml(file_path: str | Path) -> dict:
    with open(file_path, "rb") as f:
        return tomli.load(f)
