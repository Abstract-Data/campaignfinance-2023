"""Tests for ``app.states.postgres_config.PostgresConfig``."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from app.states.postgres_config import PostgresConfig


def _populate_postgres_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "db.test.local")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "test_campaign")
    monkeypatch.setenv("POSTGRES_USER", "cf_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "super-secret-pass")
    monkeypatch.setenv("POSTGRES_POOL_SIZE", "5")
    monkeypatch.setenv("POSTGRES_MAX_OVERFLOW", "15")
    monkeypatch.setenv("POSTGRES_POOL_TIMEOUT", "25")
    monkeypatch.setenv("POSTGRES_POOL_RECYCLE", "1800")


def test_postgres_config_loads_from_env_and_builds_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _populate_postgres_env(monkeypatch)
    cfg = PostgresConfig()

    url = cfg.database_url
    assert url.startswith("postgresql://")
    assert "db.test.local:5433" in url
    assert "test_campaign" in url
    assert isinstance(cfg.password, SecretStr)

    leaked = repr(cfg) + str(cfg)
    assert "super-secret-pass" not in leaked

    assert cfg.pool_size == 5
    assert cfg.max_overflow == 15
    assert cfg.pool_timeout == 25
    assert cfg.pool_recycle == 1800


def test_validate_connection_true_when_engine_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _populate_postgres_env(monkeypatch)
    cfg = PostgresConfig()

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_conn)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_engine.connect.return_value = mock_ctx

    with patch("app.states.postgres_config.create_engine", return_value=mock_engine):
        assert cfg.validate_connection() is True

    mock_engine.connect.assert_called_once()
    mock_conn.execute.assert_called_once()
    mock_engine.dispose.assert_called_once()


def test_import_postgres_config_succeeds() -> None:
    from app.states.postgres_config import PostgresConfig as Imported

    assert Imported is PostgresConfig
