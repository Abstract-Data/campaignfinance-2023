from __future__ import annotations
import tomli
from pathlib import Path


def read_toml(file_path: str | Path) -> dict:
    with open(file_path, 'rb') as f:
        return tomli.load(f)
