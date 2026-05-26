"""Fixtures for ABC tests — isolate Oklahoma validator imports from package init."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_OK_PREFIX = "app.states.oklahoma"


def _exec_validator_module(relative_path: str, module_name: str):
    """Load a validator module without executing ``app.states.oklahoma.__init__``."""
    path = _ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {module_name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _ensure_ok_validator_packages() -> None:
    for pkg in ("app", "app.states", _OK_PREFIX, f"{_OK_PREFIX}.validators"):
        if pkg not in sys.modules:
            mod = types.ModuleType(pkg)
            sys.modules[pkg] = mod
            if pkg == _OK_PREFIX:
                mod.__path__ = [str(_ROOT / "app/states/oklahoma")]
            if pkg == f"{_OK_PREFIX}.validators":
                mod.__path__ = [str(_ROOT / "app/states/oklahoma/validators")]


@pytest.fixture(scope="module")
def ok_expenditure_models():
    """Oklahoma expenditure Create/Read/Table classes (isolated import).

    If the module was already loaded by another test file (e.g. tests/states/)
    we reuse the cached module rather than re-loading it.  Re-loading triggers
    SQLAlchemy's 'Table already defined' error because OklahomaExpenditure
    (table=True, schema='oklahoma') cannot be registered in the same MetaData
    instance twice.
    """
    mod_key = f"{_OK_PREFIX}.validators.ok_expenditure"
    if mod_key in sys.modules and hasattr(sys.modules[mod_key], "OklahomaExpenditureBase"):
        yield sys.modules[mod_key]
        return

    saved = {
        key: sys.modules.pop(key)
        for key in list(sys.modules)
        if key == _OK_PREFIX or key.startswith(f"{_OK_PREFIX}.")
    }
    try:
        _ensure_ok_validator_packages()
        _exec_validator_module(
            "app/states/oklahoma/validators/ok_settings.py",
            f"{_OK_PREFIX}.validators.ok_settings",
        )
        _exec_validator_module(
            "app/states/oklahoma/validators/_helpers.py",
            f"{_OK_PREFIX}.validators._helpers",
        )
        mod = _exec_validator_module(
            "app/states/oklahoma/validators/ok_expenditure.py",
            f"{_OK_PREFIX}.validators.ok_expenditure",
        )
        yield mod
    finally:
        for key in list(sys.modules):
            if key == _OK_PREFIX or key.startswith(f"{_OK_PREFIX}."):
                sys.modules.pop(key, None)
        sys.modules.update(saved)
