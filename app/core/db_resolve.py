"""Interactive database-URL resolution shared by the loader and resolve CLIs.

Policy
------
PostgreSQL is the default target.  SQLite is a deliberate, opt-in fallback — it
is **never** created or used silently:

- ``--sqlite`` (force_sqlite) → an ephemeral in-memory SQLite DB, the smoke-test
  escape hatch (no file written).
- Postgres reachable → use it.
- Postgres unreachable, interactive terminal → prompt the user to either set
  Postgres up (exit) or **create a local SQLite file** (persistent, so a load and
  a later resolve share it).
- Postgres unreachable, non-interactive (CI/cron/tests, no TTY) → raise, never a
  silent SQLite fallback.

The SQLite file (default ``campaignfinance_dev.db``) is git-ignored via ``*.db``;
it is created only when a user opts into it at the prompt, never committed.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable

SQLITE_FILENAME = "campaignfinance_dev.db"


def postgres_target_url() -> str:
    """The Postgres URL we would use: ``DATABASE_URL`` if set, else PostgresConfig.

    PostgresConfig supplies host/db defaults (localhost/campaign_finance) and reads
    ``POSTGRES_*`` / ``.env``, so this returns a concrete URL even with no env set.
    """
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if database_url and not database_url.startswith("sqlite"):
        return database_url
    from app.states.postgres_config import PostgresConfig

    return PostgresConfig().database_url


def display_url(url: str) -> str:
    """A log-safe rendering of *url* with any password redacted."""
    from sqlalchemy.engine import make_url

    try:
        # hide_password=True renders the password as a clean ``***`` mask.
        return make_url(url).render_as_string(hide_password=True)
    except Exception:  # noqa: BLE001 — display helper must never raise
        return url


def postgres_reachable(url: str) -> bool:
    """Return True if a short-lived connection to *url* succeeds."""
    from sqlalchemy import text
    from sqlmodel import create_engine

    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001 — any connection failure means "not reachable"
        return False
    finally:
        engine.dispose()


_PROMPT = (
    "\nPostgreSQL is not reachable at {url}.\n"
    "How do you want to proceed?\n"
    "  [p] I'll set up PostgreSQL myself  (exit now so you can configure it)\n"
    "  [s] Create / use a local SQLite database instead ({sqlite})\n"
    "Choice [p/s]: "
)


def resolve_runtime_database_url(
    *,
    force_sqlite: bool = False,
    sqlite_path: str = SQLITE_FILENAME,
    prompt: Callable[[str], str] = input,
    isatty: bool | None = None,
    reachable: Callable[[str], bool] = postgres_reachable,
) -> str:
    """Resolve the database URL for a CLI run, prompting when Postgres is absent.

    Parameters are injectable so the decision logic is unit-testable without a
    real terminal or database.  Returns a SQLAlchemy URL string.

    Raises
    ------
    RuntimeError
        Postgres is unreachable and the session is non-interactive (no TTY).
    SystemExit
        The user chose to set Postgres up rather than use SQLite.
    """
    if force_sqlite:
        # Ephemeral in-memory smoke-test DB — never a file on disk.
        return "sqlite://"

    pg_url = postgres_target_url()
    if reachable(pg_url):
        return pg_url

    if isatty is None:
        isatty = sys.stdin.isatty()
    if not isatty:
        raise RuntimeError(
            f"PostgreSQL is not reachable ({display_url(pg_url)}) and there is no "
            "terminal to prompt. Start PostgreSQL and set DATABASE_URL / POSTGRES_*, "
            "or pass --sqlite to use a local SQLite database."
        )

    answer = prompt(_PROMPT.format(url=display_url(pg_url), sqlite=sqlite_path)).strip().lower()
    if answer.startswith("s"):
        print(f"→ Using local SQLite database: {sqlite_path}")
        return f"sqlite:///{sqlite_path}"
    print(
        "→ Set up PostgreSQL (e.g. `createdb campaign_finance`), then set "
        "POSTGRES_DB / POSTGRES_USER / … or DATABASE_URL and re-run."
    )
    raise SystemExit(2)
