"""Tests for the shared interactive database-URL resolver."""

from __future__ import annotations

import pytest

from app.core.db_resolve import (
    display_url,
    postgres_target_url,
    resolve_runtime_database_url,
)


def _reachable_true(_url: str) -> bool:
    return True


def _reachable_false(_url: str) -> bool:
    return False


# --------------------------------------------------------------------------- #
# resolve_runtime_database_url
# --------------------------------------------------------------------------- #
def test_force_sqlite_returns_in_memory() -> None:
    # --sqlite never touches Postgres and never writes a file.
    url = resolve_runtime_database_url(force_sqlite=True, reachable=_reachable_false, isatty=True)
    assert url == "sqlite://"


def test_postgres_used_when_reachable() -> None:
    url = resolve_runtime_database_url(
        reachable=lambda u: True, isatty=False, prompt=lambda _p: "s"
    )
    assert url.startswith("postgresql")


def test_prompt_choose_sqlite_returns_file() -> None:
    url = resolve_runtime_database_url(
        reachable=_reachable_false,
        isatty=True,
        prompt=lambda _p: "s",
        sqlite_path="my_dev.db",
    )
    assert url == "sqlite:///my_dev.db"


def test_prompt_choose_postgres_setup_exits() -> None:
    with pytest.raises(SystemExit):
        resolve_runtime_database_url(reachable=_reachable_false, isatty=True, prompt=lambda _p: "p")


def test_prompt_default_blank_is_postgres_setup() -> None:
    # Anything not starting with 's' means "I'll set up Postgres" → exit, not SQLite.
    with pytest.raises(SystemExit):
        resolve_runtime_database_url(reachable=_reachable_false, isatty=True, prompt=lambda _p: "")


def test_non_interactive_unreachable_raises_not_sqlite() -> None:
    with pytest.raises(RuntimeError, match="not reachable"):
        resolve_runtime_database_url(
            reachable=_reachable_false, isatty=False, prompt=lambda _p: "s"
        )


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def test_display_url_redacts_password() -> None:
    shown = display_url("postgresql+psycopg2://user:secret@host:5432/db")
    assert "secret" not in shown
    assert "***" in shown
    assert "user" in shown


def test_postgres_target_url_prefers_database_url(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://u:p@h:5432/d")
    assert postgres_target_url() == "postgresql+psycopg2://u:p@h:5432/d"


def test_postgres_target_url_ignores_sqlite_database_url(monkeypatch) -> None:
    # A sqlite DATABASE_URL must not be treated as the Postgres target.
    monkeypatch.setenv("DATABASE_URL", "sqlite:///x.db")
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    url = postgres_target_url()
    assert url.startswith("postgresql")  # falls through to PostgresConfig default
