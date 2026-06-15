"""Gate: the vectorized flat_txns DETAIL/JUNCTION family == the ORM loader.

Loads ONLY the RCPT (contribs_golden) and EXPN (expend_golden) fixtures via both
the ORM loader and ``run_vectorized`` (all flat_txns families registered), then
asserts ``diff_snapshots`` restricted to the detail/junction tables is empty —
with ``resolve_fks=True`` so the entity/person LINKAGE this family builds (not
just row counts) is what gets verified.

Mirrors ``test_flat_txns_family``: ``_make_engine(enforce_fk=...)``, the
ORM-load-subset pattern, and the single-fixture-dir trick that points discovery
at a directory containing ONLY the two parquet files.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES, _make_engine

# ---------------------------------------------------------------------------
# Canonicalize the ONE non-deterministic linkage in resolve_fks output.
#
# When two source rows describe the SAME party with trivial spelling variants
# ("CARL F" vs "CARL F.", "U S Treasury" vs "U.S. Treasury"), the dim layer keeps
# both person rows (they have distinct natural keys) but they share ONE entity
# (same normalized_name). The ORM links that entity to ONE of the variants via
# ``person.entity = entity`` — a reassignment whose winner is FLUSH / HASH-SEED
# ordering dependent: ORM-vs-ORM itself diverges run to run under resolve_fks=True
# (verified). The entity's IDENTITY (entity_type, name, normalized_name) and its
# OWN resolved address are deterministic and ARE compared; only the specific
# person VARIANT an entity points to is not. So we blank the ``person_id``
# resolved INSIDE an entity (the representative person) on BOTH sides.
#
# Crucially this does NOT touch the junction's DIRECT ``person_id`` (the actual
# participant person this family links) — that is deterministic and stays under
# strict comparison.
# ---------------------------------------------------------------------------


def _canon_node(node: Any, *, inside_entity: bool = False) -> Any:
    if isinstance(node, dict):
        is_entity = "entity_type" in node and "normalized_name" in node
        out: dict[str, Any] = {}
        for k, v in node.items():
            if is_entity and k == "person_id":
                # Representative person of an entity — non-deterministic; blank it
                # but keep a marker so "has a person" vs "None" is still compared.
                out[k] = "<entity-person>" if v is not None else None
            else:
                out[k] = _canon_node(v, inside_entity=inside_entity or is_entity)
        return out
    if isinstance(node, str):
        stripped = node.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                parsed = ast.literal_eval(stripped)
            except (ValueError, SyntaxError):
                return node
            return repr(_canon_node(parsed, inside_entity=inside_entity))
        return node
    return node


def _canon_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: _canon_node(v) for k, v in r.items()} for r in rows]

_FLAT_TXN_RECORD_TYPES = frozenset({"RCPT", "EXPN"})
_FLAT_TXN_FILENAMES = frozenset({"contribs_golden.parquet", "expend_golden.parquet"})

# Detail/junction tables this family brings to real (linkage-resolved) parity.
TABLES = (
    "unified_contributions",
    "unified_expenditures",
    "unified_transaction_persons",
)


def _load_golden_rcpt_expn(engine) -> None:
    """ORM-load ONLY the RCPT and EXPN golden fixtures (FK parents not seeded)."""
    from sqlmodel import Session

    from app.core.load_cache import BuilderCache
    from scripts.loaders import production_loader as P
    from scripts.loaders.file_discovery import discover_state_files
    from scripts.loaders.loader_config import LoaderConfig

    session = Session(engine, expire_on_commit=False)
    try:
        P._ensure_committee_types(session)
        state = P._ensure_state(session, "texas")
        cache = BuilderCache()
        cfg = LoaderConfig(batch_size=1000, commit_frequency=1000)
        discovered = sorted(
            (
                (item.path, item.record_type)
                for item in discover_state_files("texas", base_dir=FIXTURES)
                if item.record_type in _FLAT_TXN_RECORD_TYPES
            ),
            key=lambda p_rt: (P._FILE_PRIORITY.get(p_rt[1] or "", 50), str(p_rt[0])),
        )
        assert discovered, "no RCPT/EXPN golden fixtures discovered"
        for path, rtype in discovered:
            _n, _rej, cache = P._load_file(
                path,
                rtype,
                cfg,
                state="texas",
                state_id=state.id,
                state_code=state.code,
                session=session,
                cache=cache,
                max_remaining=None,
            )
        session.commit()
    finally:
        session.close()


def _make_rcpt_expn_fixtures_dir(tmp_path: Path) -> Path:
    """Copy only contribs/expend golden parquets into a sub-dir for discovery."""
    import shutil

    sub = tmp_path / "flat_only"
    sub.mkdir()
    for name in _FLAT_TXN_FILENAMES:
        shutil.copy2(FIXTURES / name, sub / name)
    return sub


def test_flat_txns_detail_family_matches_orm(tmp_path: Path):
    """contributions / expenditures / transaction_persons must be row-for-row equal
    to the ORM loader, with surrogate FKs RESOLVED to parent natural keys.

    resolve_fks=True is what verifies the entity/person linkage (contributor,
    recipient/payer, payee, junction person+entity) — not just row counts.
    """
    orm_engine = _make_engine(tmp_path / "orm_detail.db", enforce_fk=False)
    _load_golden_rcpt_expn(orm_engine)

    flat_fixtures = _make_rcpt_expn_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec_detail.db", enforce_fk=False)
    run_vectorized(vec_engine, flat_fixtures)

    orm_full = snapshot_unified(orm_engine, resolve_fks=True)
    vec_full = snapshot_unified(vec_engine, resolve_fks=True)

    # Both sides must be non-empty in every detail/junction table (check before
    # canonicalization so emptiness is caught directly).
    for tbl in TABLES:
        assert orm_full.get(tbl), f"ORM produced no {tbl} rows — fixture/loader problem"
        assert vec_full.get(tbl), f"vectorized produced no {tbl} rows — family not running"

    orm = {t: _canon_rows(orm_full.get(t, [])) for t in TABLES}
    vec = {t: _canon_rows(vec_full.get(t, [])) for t in TABLES}

    diffs = diff_snapshots(orm, vec)
    assert diffs == [], "detail/junction tables diverge from ORM:\n" + "\n".join(diffs)


def test_flat_txns_detail_contribution_present(tmp_path: Path):
    """RCPT fixture must produce at least one linked contribution."""
    flat_fixtures = _make_rcpt_expn_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, flat_fixtures)
    snap = snapshot_unified(vec_engine)
    assert snap.get("unified_contributions"), "expected contributions from RCPT fixture"


def test_flat_txns_detail_expenditure_present(tmp_path: Path):
    """EXPN fixture must produce at least one linked expenditure."""
    flat_fixtures = _make_rcpt_expn_fixtures_dir(tmp_path)
    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, flat_fixtures)
    snap = snapshot_unified(vec_engine)
    assert snap.get("unified_expenditures"), "expected expenditures from EXPN fixture"
