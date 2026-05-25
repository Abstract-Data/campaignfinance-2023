"""
Tests for P1-SEC-002 (Wave 1) and the Wave-2 successor of P1-OPS-001 in
``unified_database.py``.

P1-SEC-002 — ``run_custom_query`` was deleted (no callers in ``app/``).

P1-OPS-001 — Wave 1 narrowed an import-time ``db_manager = UnifiedDatabaseManager()``
guard to swallow only ``RuntimeError``.  Wave 2's task 2a (P2-ARC-002) removes
the import-time singleton entirely and replaces it with a lazy
``get_db_manager()`` factory.  The fail-loud guarantee now lives on the
factory: a ``RuntimeError`` from construction propagates, as does any other
exception type.  The factory has no silent-swallow behavior, so the original
"guard catches RuntimeError" semantics no longer apply — the relevant tests
have been rewritten to assert the factory's error-propagation contract.

Design note
-----------
The factory module is intentionally inert at import time (P2-ARC-002), so
helpers below patch ``PostgresConfig.validate_connection`` and then invoke
:func:`get_db_manager` directly rather than reloading the module.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import app.core.unified_database as unified_database


def _reset_factory_cache() -> None:
    """Drop any cached manager so each test starts with a clean factory."""
    unified_database.reset_db_manager_cache()


# ---------------------------------------------------------------------------
# P1-SEC-002 — run_custom_query must not exist (deleted; no app/ callers)
# ---------------------------------------------------------------------------


class TestRunCustomQueryDeleted:
    """``run_custom_query`` was removed because it had no callers in
    ``app/`` and allowed arbitrary SQL execution (DROP TABLE, etc.)."""

    def test_run_custom_query_not_on_class(self) -> None:
        assert not hasattr(
            unified_database.UnifiedDatabaseManager, "run_custom_query"
        ), "run_custom_query still exists — P1-SEC-002 not fixed"


# ---------------------------------------------------------------------------
# P2-ARC-002 / P1-OPS-001 successor — factory must propagate errors loudly
# ---------------------------------------------------------------------------


class TestGetDbManagerErrorPropagation:
    """``get_db_manager()`` lazily constructs the manager.  It must not
    silently swallow construction errors: callers need to see the real
    failure so a missing dependency or misconfiguration is never hidden."""

    def setup_method(self) -> None:
        _reset_factory_cache()

    def teardown_method(self) -> None:
        _reset_factory_cache()

    def test_runtime_error_propagates_from_factory(self) -> None:
        """``PostgresConfig.validate_connection`` returning ``False`` makes
        ``UnifiedDatabaseManager.__init__`` raise ``RuntimeError``.  The
        factory propagates it (Wave 1's silent-catch is gone — see
        :func:`app.core.unified_database.get_db_manager`)."""
        target = "app.states.postgres_config.PostgresConfig.validate_connection"
        with patch(target, return_value=False):
            with pytest.raises(RuntimeError):
                unified_database.get_db_manager()

    def test_module_not_found_error_propagates(self) -> None:
        """ModuleNotFoundError must propagate — catching it silently hides
        a missing dependency (P1-OPS-001 carried forward to the factory)."""
        target = "app.states.postgres_config.PostgresConfig.validate_connection"
        with patch(target, side_effect=ModuleNotFoundError("No module named 'missing_dep'")):
            with pytest.raises(ModuleNotFoundError):
                unified_database.get_db_manager()

    def test_import_error_propagates(self) -> None:
        target = "app.states.postgres_config.PostgresConfig.validate_connection"
        with patch(target, side_effect=ImportError("cannot import name 'X' from 'y'")):
            with pytest.raises(ImportError):
                unified_database.get_db_manager()

    def test_value_error_propagates(self) -> None:
        target = "app.states.postgres_config.PostgresConfig.validate_connection"
        with patch(target, side_effect=ValueError("unexpected config")):
            with pytest.raises(ValueError):
                unified_database.get_db_manager()

    def test_db_manager_attribute_is_none_sentinel(self) -> None:
        """The module-level ``db_manager`` symbol still exists for backward
        compatibility (e.g. legacy ``from .unified_database import db_manager``
        imports), but it must be ``None`` at import time — never a constructed
        manager (P2-ARC-002 acceptance criterion)."""
        assert unified_database.db_manager is None
