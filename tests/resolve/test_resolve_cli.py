"""Task 1d — CLI entry-point tests.

Tests for app/resolve/cli.py covering:

- _resolve_state_code: name/code/invalid inputs
- _load_config: None / valid JSON file / non-dict JSON / bad path
- Database URL resolution: special-char passwords, Postgres fail-loud, SQLite fallback
- main(): argument parsing, good runs, bad state, missing config
- smoke: main(["run", "--state", "texas"]) completes with exit-code 0 using SQLite when no
  ``DATABASE_URL`` is configured.
"""

from __future__ import annotations

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlmodel import create_engine

from app.resolve.cli import (
    _load_config,
    _resolve_state_code,
    build_postgres_url_from_env,
    main,
    postgres_env_configured,
    resolve_database_url,
    resolve_engine_url,
)
from app.resolve.run import ensure_resolution_schema, resolution_schema_table_names

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_postgres_env(monkeypatch):
    """Ensure smoke tests use SQLite unless a test sets Postgres env explicitly."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    for key in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# Database URL resolution
# ---------------------------------------------------------------------------


class TestDatabaseUrlResolution:
    def test_no_postgres_env_uses_sqlite(self):
        assert resolve_database_url() == "sqlite://"

    def test_explicit_sqlite_flag(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://u:p@localhost/db")
        assert resolve_database_url(use_sqlite=True) == "sqlite://"

    def test_postgres_env_configured_from_database_url(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://u:p@localhost/db")
        assert postgres_env_configured() is True

    def test_postgres_env_configured_from_postgres_vars(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.setenv("POSTGRES_DB", "campaign_finance")
        assert postgres_env_configured() is True

    def test_sqlite_database_url_not_postgres_configured(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "sqlite:///tmp.db")
        assert postgres_env_configured() is False

    def test_build_postgres_url_encodes_special_password(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss:word/foo")
        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        monkeypatch.setenv("POSTGRES_DB", "testdb")

        url = build_postgres_url_from_env()
        assert "p%40ss%3Aword%2Ffoo" in url
        assert "postgresql+psycopg2://user:" in url

    def test_build_postgres_url_uses_database_url_when_set(self, monkeypatch):
        monkeypatch.setenv(
            "DATABASE_URL",
            "postgresql+psycopg2://user:secret@db.example.com:5432/prod",
        )
        assert build_postgres_url_from_env() == (
            "postgresql+psycopg2://user:secret@db.example.com:5432/prod"
        )

    def test_postgres_connection_failure_raises(self, monkeypatch):
        monkeypatch.setenv(
            "DATABASE_URL",
            "postgresql+psycopg2://bad:bad@127.0.0.1:59999/nodb",
        )
        # Non-interactive: an unreachable Postgres must raise, never silently fall
        # back to SQLite (and never prompt under `pytest -s`).
        monkeypatch.setattr("sys.stdin.isatty", lambda: False)
        with pytest.raises(RuntimeError, match="not reachable"):
            resolve_engine_url()

    def test_postgres_connection_failure_main_returns_1(self, monkeypatch):
        monkeypatch.setenv(
            "DATABASE_URL",
            "postgresql+psycopg2://bad:bad@127.0.0.1:59999/nodb",
        )
        monkeypatch.setattr("sys.stdin.isatty", lambda: False)
        code = main(["run", "--state", "texas"])
        assert code == 1

    def test_explicit_sqlite_main_succeeds_despite_database_url(self, monkeypatch):
        monkeypatch.setenv(
            "DATABASE_URL",
            "postgresql+psycopg2://bad:bad@127.0.0.1:59999/nodb",
        )
        monkeypatch.setattr(
            "app.resolve.cli._get_run_stages",
            lambda *args, **kwargs: [lambda session, run_id, config: {}],
        )
        code = main(["run", "--state", "texas", "--sqlite"])
        assert code == 0


class TestEnsureResolutionSchema:
    def test_create_all_scoped_to_resolve_tables_only(self):
        import app.core.models  # noqa: F401 — registers unified tables

        engine = create_engine("sqlite:///:memory:", echo=False)
        ensure_resolution_schema(engine)

        created = set(sa_inspect(engine).get_table_names())
        expected = resolution_schema_table_names()
        assert expected <= created
        assert "unified_transactions" not in created
        assert "states" not in created

        engine.dispose()

    def test_adds_missing_columns_to_existing_resolution_input(self):
        """An older resolution_input (no linked_* cols) is migrated in place;
        create_all alone cannot add columns to a pre-existing table."""
        from sqlalchemy import text

        engine = create_engine("sqlite:///:memory:", echo=False)
        # Simulate a pre-existing table that lacks the new deterministic-link columns.
        with engine.begin() as conn:
            conn.execute(
                text("CREATE TABLE resolution_input (id INTEGER PRIMARY KEY, run_id INTEGER)")
            )

        ensure_resolution_schema(engine)

        cols = {c["name"] for c in sa_inspect(engine).get_columns("resolution_input")}
        assert "linked_person_id" in cols
        assert "linked_committee_id" in cols
        engine.dispose()


class TestResolveStateCode:
    def test_texas_name_returns_TX(self):
        assert _resolve_state_code("texas") == "TX"

    def test_texas_mixed_case_returns_TX(self):
        assert _resolve_state_code("Texas") == "TX"

    def test_oklahoma_name_returns_OK(self):
        assert _resolve_state_code("oklahoma") == "OK"

    def test_two_letter_code_uppercase_passthrough(self):
        assert _resolve_state_code("TX") == "TX"

    def test_two_letter_code_lowercase_upcased(self):
        assert _resolve_state_code("tx") == "TX"

    def test_two_letter_unknown_code_is_accepted(self):
        # Any valid 2-alpha code is accepted; validation of supported states
        # is a runtime concern, not a parsing concern.
        assert _resolve_state_code("CA") == "CA"

    def test_invalid_long_code_raises(self):
        with pytest.raises(ValueError, match="Unrecognised state"):
            _resolve_state_code("XYZ")

    def test_numeric_code_raises(self):
        with pytest.raises(ValueError, match="Unrecognised state"):
            _resolve_state_code("12")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _resolve_state_code("")


# ---------------------------------------------------------------------------
# _load_config
# ---------------------------------------------------------------------------


class TestLoadConfig:
    def test_none_returns_empty_dict(self):
        assert _load_config(None) == {}

    def test_valid_json_file_returns_dict(self, tmp_path):
        cfg = tmp_path / "config.json"
        cfg.write_text('{"threshold": 0.85, "seed": 7}')
        result = _load_config(str(cfg))
        assert result == {"threshold": 0.85, "seed": 7}

    def test_empty_json_object_returns_empty_dict(self, tmp_path):
        cfg = tmp_path / "empty.json"
        cfg.write_text("{}")
        assert _load_config(str(cfg)) == {}

    def test_json_array_raises_value_error(self, tmp_path):
        cfg = tmp_path / "array.json"
        cfg.write_text("[1, 2, 3]")
        with pytest.raises(ValueError, match="JSON object"):
            _load_config(str(cfg))

    def test_missing_file_raises_os_error(self, tmp_path):
        with pytest.raises(OSError):
            _load_config(str(tmp_path / "nonexistent.json"))

    def test_invalid_json_raises(self, tmp_path):
        cfg = tmp_path / "bad.json"
        cfg.write_text("not json {")
        with pytest.raises(Exception):
            _load_config(str(cfg))


# ---------------------------------------------------------------------------
# main() — argument parsing
# ---------------------------------------------------------------------------


class TestMainArgParsing:
    def test_missing_state_exits_nonzero(self):
        """--state is required; missing it should fail fast."""
        with pytest.raises(SystemExit) as exc_info:
            main(["run"])
        assert exc_info.value.code != 0

    def test_no_subcommand_exits_nonzero(self):
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code != 0

    def test_help_exits_zero(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_run_help_exits_zero(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["run", "--help"])
        assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# main() — runtime: bad inputs return exit code 1 without crashing
# ---------------------------------------------------------------------------


class TestMainBadInputs:
    def test_unrecognised_state_returns_1(self):
        code = main(["run", "--state", "NOTASTATE123"])
        assert code == 1

    def test_missing_config_file_returns_1(self, tmp_path):
        code = main(["run", "--state", "texas", "--config", str(tmp_path / "no.json")])
        assert code == 1

    def test_non_dict_config_returns_1(self, tmp_path):
        bad_cfg = tmp_path / "bad.json"
        bad_cfg.write_text("[1, 2]")
        code = main(["run", "--state", "texas", "--config", str(bad_cfg)])
        assert code == 1


# ---------------------------------------------------------------------------
# main() — smoke: happy path with an empty stage list
# ---------------------------------------------------------------------------


class TestMainSmoke:
    """End-to-end: the CLI opens a match_run, completes it, and exits 0.

    No ``DATABASE_URL`` in the environment — the CLI uses in-memory SQLite.
    Stages are stubbed to a no-op so smoke tests do not require unified source
    tables (those live in Postgres; see ``test_phase1_integration.py``).
    """

    @pytest.fixture(autouse=True)
    def _noop_stages(self, monkeypatch):
        monkeypatch.setattr(
            "app.resolve.cli._get_run_stages",
            lambda *args, **kwargs: [lambda session, run_id, config: {}],
        )

    def test_run_texas_no_stages_returns_0(self):
        code = main(["run", "--state", "texas"])
        assert code == 0

    def test_run_tx_uppercase_returns_0(self):
        code = main(["run", "--state", "TX"])
        assert code == 0

    def test_run_with_config_file_returns_0(self, tmp_path):
        cfg = tmp_path / "cfg.json"
        cfg.write_text('{"threshold": 0.9, "seed": 42}')
        code = main(["run", "--state", "texas", "--config", str(cfg)])
        assert code == 0

    def test_run_with_pass_type_address_returns_0(self):
        code = main(["run", "--state", "texas", "--pass-type", "address"])
        assert code == 0

    def test_run_verbose_returns_0(self):
        code = main(["run", "--state", "texas", "--verbose"])
        assert code == 0

    def test_pass_type_defaults_written_to_config(self, monkeypatch):
        """pass_type and state_code defaults are injected into the config."""
        captured_configs: list[dict] = []

        import app.resolve.cli as cli_module

        def spy_run_command(args):
            config = cli_module._load_config(args.config)
            config.setdefault("pass_type", args.pass_type)
            config.setdefault("state_code", cli_module._resolve_state_code(args.state))
            captured_configs.append(dict(config))
            # Don't actually open DB — just capture and return 0.
            return 0

        monkeypatch.setattr(cli_module, "_run_command", spy_run_command)
        cli_module.main(["run", "--state", "texas"])

        assert captured_configs[0]["pass_type"] == "entity"
        assert captured_configs[0]["state_code"] == "TX"
