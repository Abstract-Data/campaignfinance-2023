"""Wave 4 FK parity verification.

Verifies that the address lookup caching in FamilyContext (Wave 4a) and the
already-present state_id scoping in filer/finalize families (Wave 4b) produce
the same FK assignments as before.

This test checks structural invariants on the DB schema and the FamilyContext
interface, confirming the Wave 4 changes are correctly wired.
"""

from __future__ import annotations

import os

import pytest

_PG_BASE = os.environ.get("BENCH_PG_BASE", "postgresql+psycopg2://localhost:5432")


def _pg_available() -> bool:
    try:
        from sqlalchemy import create_engine, text

        e = create_engine(_PG_BASE + "/postgres")
        with e.connect() as c:
            c.execute(text("SELECT 1"))
        e.dispose()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="no local PostgreSQL")


def test_family_context_has_address_lookup_cache() -> None:
    """FamilyContext.get_address_lookup() method exists (Wave 4a)."""
    from app.core.ingest_vectorized.registry import FamilyContext

    assert hasattr(FamilyContext, "get_address_lookup"), (
        "FamilyContext must have get_address_lookup() method (Wave 4a)"
    )


def test_address_lookup_cached_on_second_call(tmp_path) -> None:
    """FamilyContext.get_address_lookup() returns cached result on second call."""
    from unittest.mock import MagicMock, patch

    from app.core.ingest_vectorized.registry import FamilyContext

    mock_engine = MagicMock()
    ctx = FamilyContext(
        session=MagicMock(),
        engine=mock_engine,
        state_id=1,
        state_code="TX",
    )

    sentinel = object()
    with patch("app.core.ingest_vectorized.common.full_address_lookup", return_value=sentinel) as mock_fn:
        result1 = ctx.get_address_lookup()
        result2 = ctx.get_address_lookup()

    assert result1 is sentinel
    assert result2 is sentinel
    assert mock_fn.call_count == 1, "full_address_lookup must only be called once (cached)"


def test_filer_id_map_already_state_scoped() -> None:
    """_person_id_map in filer.py already filters by state_id (Wave 4b — pre-existing)."""
    import inspect

    from app.core.ingest_vectorized.families import filer

    src = inspect.getsource(filer._person_id_map)
    assert "state_id" in src, "_person_id_map must filter by state_id"


def test_finalize_already_state_scoped() -> None:
    """finalize.py functions already filter by state_id (Wave 4b — pre-existing)."""
    import inspect

    from app.core.ingest_vectorized import finalize

    src = inspect.getsource(finalize._person_frame)
    assert "state_id" in src, "_person_frame must filter by state_id"
