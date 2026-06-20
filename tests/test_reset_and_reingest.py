"""
Characterization tests for scripts/reset_and_reingest.py CLI surface.

Red phase (Task 1): these tests are written before the refactor and are expected
to fail until Task 2 moves get_db_manager + run_vectorized to module level.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def repo_root(tmp_path, monkeypatch):
    texas = tmp_path / "tmp" / "texas"
    texas.mkdir(parents=True)
    (texas / "sample.parquet").write_bytes(b"PAR1")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_dry_run_truncates_without_vectorized_load(repo_root, monkeypatch):
    # Deferred import: module must be resolved after monkeypatch fixtures are set up
    import scripts.reset_and_reingest as mod  # noqa: PLC0415

    mock_engine = MagicMock()
    mock_manager = MagicMock()
    mock_manager.engine = mock_engine
    monkeypatch.setattr(mod, "get_db_manager", lambda bootstrap=False: mock_manager)

    vectorized = MagicMock()
    monkeypatch.setattr(mod, "run_vectorized", vectorized)

    monkeypatch.setattr(
        "sys.argv",
        ["reset_and_reingest.py", "--dry-run"],
    )
    mod.main()

    # _truncate returns early in dry_run before opening a connection
    mock_engine.connect.assert_not_called()
    vectorized.assert_not_called()


def test_skip_ingest_does_not_call_vectorized(repo_root, monkeypatch):
    # Deferred import: module must be resolved after monkeypatch fixtures are set up
    import scripts.reset_and_reingest as mod  # noqa: PLC0415

    mock_manager = MagicMock()
    monkeypatch.setattr(mod, "get_db_manager", lambda bootstrap=False: mock_manager)
    vectorized = MagicMock()
    monkeypatch.setattr(mod, "run_vectorized", vectorized)

    monkeypatch.setattr("sys.argv", ["reset_and_reingest.py", "--skip-ingest"])
    mod.main()

    vectorized.assert_not_called()
    mock_manager.bootstrap.assert_called_once()


def test_default_ingest_calls_run_vectorized(repo_root, monkeypatch):
    # Deferred import: module must be resolved after monkeypatch fixtures are set up
    import scripts.reset_and_reingest as mod  # noqa: PLC0415

    mock_engine = MagicMock()
    mock_manager = MagicMock()
    mock_manager.engine = mock_engine
    monkeypatch.setattr(mod, "get_db_manager", lambda bootstrap=False: mock_manager)
    # Point ROOT at the tmp repo so parquet_dir existence check passes
    monkeypatch.setattr(mod, "ROOT", repo_root)

    vectorized = MagicMock(return_value={"loaded": 0, "families_run": 0})
    monkeypatch.setattr(mod, "run_vectorized", vectorized)

    monkeypatch.setattr("sys.argv", ["reset_and_reingest.py"])
    mod.main()

    vectorized.assert_called_once()
    assert vectorized.call_args.kwargs["state"] == "texas"
    assert vectorized.call_args.kwargs["dry_run"] is False
