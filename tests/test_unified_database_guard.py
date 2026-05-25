"""
Tests for P1-SEC-002 and P1-OPS-001 fixes in unified_database.py.

P1-SEC-002: run_custom_query was deleted (no callers in app/).
P1-OPS-001: db_manager singleton guard narrowed to RuntimeError only.

Design note
-----------
``unified_database`` creates a real PostgreSQL connection at import time.
In the dev / CI environment psycopg2 is not installed, so we must mock
``PostgresConfig.validate_connection`` before reloading the module.  Each
helper pops the module from ``sys.modules`` and reimports it under the patch,
which re-executes the module-level guard code with a controlled exception.
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest

_MODULE = "app.core.unified_database"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_with_validate(
    *,
    side_effect: BaseException | None = None,
    return_value: bool | None = None,
) -> object:
    """Reload unified_database with ``validate_connection`` mocked.

    Parameters
    ----------
    side_effect:
        If given, ``validate_connection()`` raises this exception.
    return_value:
        If given (and ``side_effect`` is None), ``validate_connection()``
        returns this value.  Pass ``False`` to trigger the normal
        RuntimeError path inside ``UnifiedDatabaseManager.__init__``.
    """
    # Ensure the config module is cached so we can patch its class method.
    import app.states.postgres_config  # noqa: F401

    # Force the module to re-execute so the module-level guard runs fresh.
    sys.modules.pop(_MODULE, None)

    mock_vc = MagicMock()
    if side_effect is not None:
        mock_vc.side_effect = side_effect
    elif return_value is not None:
        mock_vc.return_value = return_value

    target = "app.states.postgres_config.PostgresConfig.validate_connection"
    with patch(target, mock_vc):
        mod = importlib.import_module(_MODULE)
    return mod


# ---------------------------------------------------------------------------
# P1-SEC-002 — run_custom_query must not exist (deleted; no app/ callers)
# ---------------------------------------------------------------------------


class TestRunCustomQueryDeleted:
    """run_custom_query was removed because it had no callers in app/ and
    allowed arbitrary SQL execution (DROP TABLE, etc.)."""

    def test_run_custom_query_not_on_class(self) -> None:
        """UnifiedDatabaseManager must not expose run_custom_query."""
        mod = _load_with_validate(return_value=False)
        assert not hasattr(
            mod.UnifiedDatabaseManager, "run_custom_query"
        ), "run_custom_query still exists — P1-SEC-002 not fixed"


# ---------------------------------------------------------------------------
# P1-OPS-001 — singleton guard must propagate non-RuntimeError exceptions
# ---------------------------------------------------------------------------


class TestDbManagerGuard:
    """The module-level db_manager singleton must only swallow RuntimeError
    (genuine connection-unavailable).  ModuleNotFoundError / ImportError and
    any other unexpected exception must propagate so the bug is visible."""

    def test_runtime_error_is_caught_sets_none(self) -> None:
        """RuntimeError (e.g. no PostgreSQL available) → db_manager = None."""
        # validate_connection returning False causes __init__ to raise RuntimeError,
        # which the narrowed guard should catch and set db_manager = None.
        mod = _load_with_validate(return_value=False)
        assert (
            mod.db_manager is None
        ), "db_manager should be None when RuntimeError is raised during init"

    def test_module_not_found_error_propagates(self) -> None:
        """ModuleNotFoundError must propagate — catching it silently hides
        a missing dependency, masking the real bug (P1-OPS-001)."""
        with pytest.raises(ModuleNotFoundError):
            _load_with_validate(side_effect=ModuleNotFoundError("No module named 'missing_dep'"))

    def test_import_error_propagates(self) -> None:
        """ImportError must propagate for the same reason."""
        with pytest.raises(ImportError):
            _load_with_validate(side_effect=ImportError("cannot import name 'X' from 'y'"))

    def test_value_error_propagates(self) -> None:
        """Unexpected ValueError must also propagate — only RuntimeError is
        expected as a 'connection unavailable' sentinel."""
        with pytest.raises(ValueError):
            _load_with_validate(side_effect=ValueError("unexpected config"))
