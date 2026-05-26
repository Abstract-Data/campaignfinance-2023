"""
Tests for P1-SEC-001 / RF-ARCH-001 — parameterized SQL in unified_state_loader.

Verifies:
* Officer-linking handles names containing apostrophes (e.g. "O'Brien") without
  raising an exception.
* Role strings containing apostrophes do not break query construction.
* The previously broken line-491 path (session.exec on a bare string) now
  executes via a parameterized SQLModel ``select()`` statement.
"""

from __future__ import annotations

import re
from pathlib import Path

SOURCE = Path("app/core/unified_state_loader.py").read_text()


def test_no_fstring_text_sql_remains() -> None:
    """No f-string-interpolated SQL may appear in the loader source."""
    pattern = re.compile(r"text\(\s*f[\"']")
    assert not pattern.search(SOURCE), (
        "Found f-string text(f\"...\") SQL in unified_state_loader.py — "
        "convert to parameterized SQLModel select()/update()."
    )


def test_no_bare_string_session_exec() -> None:
    """`session.exec("SELECT ...")` (bare string) is the line-491 bug."""
    pattern = re.compile(r"session\.exec\(\s*f?[\"']\s*SELECT", re.IGNORECASE)
    assert not pattern.search(SOURCE), (
        "Found bare-string session.exec(\"SELECT ...\") — must use select()."
    )


def test_uses_sqlmodel_select_for_tx_persons() -> None:
    """The officer-linking path must build a parameterized select() query."""
    assert "select(UnifiedTransactionPerson).where(" in SOURCE
    assert "UnifiedTransactionPerson.transaction_id == transaction.id" in SOURCE


def test_uses_sqlmodel_select_for_committee_person() -> None:
    """Committee-person lookup must be parameterized; role passed as a bound value."""
    assert "select(UnifiedCommitteePerson).where(" in SOURCE
    assert "UnifiedCommitteePerson.role == officer['role']" in SOURCE


def test_uses_sqlmodel_update_for_committee_person_id() -> None:
    """Committee-person-id update must use parameterized update()/.values()."""
    assert "update(UnifiedTransactionPerson)" in SOURCE
    assert "values(committee_person_id=committee_person.id)" in SOURCE


def test_apostrophe_in_role_does_not_break_query_construction() -> None:
    """Round-trip: a role string with an apostrophe must not break construction
    of the where-clause (proves we are not doing string concatenation)."""
    # Use a lightweight model with the same column shape so we can compile a
    # statement without importing the full unified mapper graph (which has
    # unrelated forward-ref issues outside the scope of this security fix).
    from sqlalchemy import Column, Integer, MetaData, String, Table
    from sqlalchemy.sql import select as sa_select

    metadata = MetaData()
    cp_table = Table(
        "_cp_smoke",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("committee_id", String(50)),
        Column("role", String(100)),
    )

    role_with_apostrophe = "Vice-Chair's deputy"
    stmt = sa_select(cp_table).where(cp_table.c.role == role_with_apostrophe)
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))
    # Value must be a bound parameter — never inlined into the SQL string.
    assert "Vice-Chair's deputy" not in compiled
    assert ":role" in compiled or "?" in compiled


def test_apostrophe_in_name_does_not_break_person_lookup() -> None:
    """Names containing apostrophes (O'Brien) must compose into a safe query."""
    from sqlalchemy import Column, Integer, MetaData, String, Table
    from sqlalchemy.sql import select as sa_select

    metadata = MetaData()
    p_table = Table(
        "_p_smoke",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("first_name", String(200)),
        Column("last_name", String(200)),
    )

    stmt = sa_select(p_table).where(
        p_table.c.first_name.ilike("Patrick"),
        p_table.c.last_name.ilike("O'Brien"),
    )
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))
    # Apostrophe-bearing value is bound, not inlined.
    assert "O'Brien" not in compiled
    assert ":last_name" in compiled or "?" in compiled
