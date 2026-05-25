"""Smoke tests for the production ``cf`` entrypoint (TASK-5c)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from app.entrypoint import app
from app.scheduler import CadenceScheduler, GracefulShutdown

runner = CliRunner()


def test_cf_help_lists_production_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for name in ("bootstrap", "scrape", "load", "schedule", "prepare", "download"):
        assert name in result.stdout


def test_version_exits_zero() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.stdout


def test_bootstrap_uses_get_db_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_manager = MagicMock(database_url="postgresql://test/db")
    import app.core.unified_database as udb

    monkeypatch.setattr(udb, "get_db_manager", lambda **kwargs: fake_manager)

    result = runner.invoke(app, ["bootstrap"])
    assert result.exit_code == 0
    assert "Database ready" in result.stdout


def test_bootstrap_skip_ddl(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bool] = []
    fake_manager = MagicMock(database_url="sqlite:///test.db")

    def _fake_get_db_manager(*, bootstrap: bool = True, echo: bool = False):
        calls.append(bootstrap)
        return fake_manager

    import app.core.unified_database as udb

    monkeypatch.setattr(udb, "get_db_manager", _fake_get_db_manager)

    result = runner.invoke(app, ["bootstrap", "--skip-ddl"])
    assert result.exit_code == 0
    assert calls == [False]


def test_scrape_delegates_to_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.entrypoint.run_scrape", lambda *a, **k: 0)
    result = runner.invoke(app, ["scrape", "texas"])
    assert result.exit_code == 0


def test_scrape_failure_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.entrypoint.run_scrape", lambda *a, **k: 1)
    result = runner.invoke(app, ["scrape", "texas"])
    assert result.exit_code == 1


def test_load_uses_discover_and_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_manager = MagicMock(database_url="postgresql://test/db")
    import app.core.unified_database as udb
    import scripts.loaders.production_loader as pl

    monkeypatch.setattr(udb, "get_db_manager", lambda **kwargs: fake_manager)
    monkeypatch.setattr(
        pl,
        "discover_and_load",
        lambda *a, **k: {"discovered": 2, "loaded": 10, "skipped": 0},
    )

    result = runner.invoke(app, ["load", "texas", "--dry-run"])
    assert result.exit_code == 0
    assert "Load complete" in result.stdout


def test_load_shutdown_after_run_exits_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.entrypoint as ep

    def _load_with_shutdown(*args, **kwargs):
        ep._shutdown.request()
        return 0

    monkeypatch.setattr(ep, "run_load", _load_with_shutdown)
    result = runner.invoke(app, ["load", "texas"])
    assert result.exit_code == 0


def test_graceful_shutdown_finishes_current_job() -> None:
    shutdown = GracefulShutdown()
    ran: list[str] = []

    def job() -> None:
        ran.append("done")
        shutdown.request()

    scheduler = CadenceScheduler(shutdown)
    code = scheduler.run_periodic(job, interval_seconds=60.0)
    assert code == 0
    assert ran == ["done"]


def test_discover_and_load_stops_when_should_stop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from scripts.loaders import production_loader as pl
    from scripts.loaders.loader_config import GlobPattern, LoaderConfig, StateGlobConfig

    base = tmp_path / "texas"
    base.mkdir()
    (base / "a.parquet").write_bytes(b"")
    (base / "b.parquet").write_bytes(b"")

    cfg = LoaderConfig(max_records=0, commit_frequency=1)
    glob_cfg = StateGlobConfig(
        state_name="texas",
        base_dir=base,
        patterns=[GlobPattern("**/*.parquet", "CVR1")],
    )
    monkeypatch.setitem(pl.STATE_GLOB_CONFIGS, "texas", glob_cfg)

    calls = {"n": 0}

    def should_stop() -> bool:
        calls["n"] += 1
        return calls["n"] > 1

    monkeypatch.setattr(pl, "_get_session", lambda db_url=None: MagicMock())
    monkeypatch.setattr(pl, "_ensure_state", lambda s, name: MagicMock(id=1, code="TX"))
    monkeypatch.setattr(pl, "_load_file", lambda *a, **k: 0)
    monkeypatch.setattr(pl, "_link_after_load", lambda s: 0)

    results = pl.discover_and_load("texas", cfg, db_url="sqlite://", should_stop=should_stop)
    assert results["discovered"] == 2
    assert results["loaded"] == 0
