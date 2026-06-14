"""Finding #3 fix: CAND rows enrich an existing expenditure with the candidate they
name (UnifiedTransactionPerson role=CANDIDATE) instead of creating a duplicate
EXPENDITURE transaction that collides on the dedup index and double-counts.
"""
from __future__ import annotations

import pytest
from sqlalchemy import event
from sqlmodel import Session, SQLModel, create_engine, select

from app.core import models  # noqa: F401 — register tables
from app.core.enums import PersonRole, TransactionType
from app.core.load_cache import BuilderCache
from app.core.models import (
    State,
    UnifiedTransaction,
    UnifiedTransactionPerson,
)
from scripts.loaders.production_loader import (
    ENRICHMENT_RECORD_TYPES,
    TRANSACTION_RECORD_TYPES,
    _persist_cand_link,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "connect")
    def _fk(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    # Only create non-schema-qualified tables: when the full suite has registered
    # schema-qualified (texas.*) models, a plain create_all on sqlite raises
    # "unknown database texas".  See tests/resolve/conftest.py for the same guard.
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    with Session(engine, expire_on_commit=False) as s:
        s.add(State(id=1, code="TX", name="Texas"))
        s.commit()
        yield s


def _cand_row(expend_id="100000001", last="CHISUM", first="WARREN"):
    return {
        "recordType": "CAND",
        "expendInfoId": expend_id,
        "candidateNameLast": last,
        "candidateNameFirst": first,
    }


def test_cand_is_not_a_transaction_type():
    assert "CAND" not in TRANSACTION_RECORD_TYPES
    assert "CAND" in ENRICHMENT_RECORD_TYPES


def test_cand_links_candidate_to_existing_expenditure(session):
    # An expenditure with the matching natural key already exists.
    session.add(
        UnifiedTransaction(
            transaction_id="100000001",
            transaction_type=TransactionType.EXPENDITURE,
            state_id=1,
        )
    )
    session.commit()

    status = _persist_cand_link(
        session, _cand_row(), state="texas", state_id=1, state_code="TX",
        cache=BuilderCache(),
    )
    session.commit()

    assert status == "linked"
    links = session.exec(
        select(UnifiedTransactionPerson).where(
            UnifiedTransactionPerson.role == PersonRole.CANDIDATE
        )
    ).all()
    assert len(links) == 1
    # No duplicate EXPENDITURE transaction was created.
    txns = session.exec(select(UnifiedTransaction)).all()
    assert len(txns) == 1


def test_cand_without_matching_expenditure_is_unlinked_not_error(session):
    status = _persist_cand_link(
        session, _cand_row(expend_id="999999"), state="texas", state_id=1,
        state_code="TX", cache=BuilderCache(),
    )
    session.commit()
    assert status == "unlinked_no_expenditure"
    assert session.exec(select(UnifiedTransactionPerson)).all() == []


def test_cand_link_is_idempotent(session):
    session.add(
        UnifiedTransaction(
            transaction_id="100000001",
            transaction_type=TransactionType.EXPENDITURE,
            state_id=1,
        )
    )
    session.commit()
    cache = BuilderCache()
    s1 = _persist_cand_link(session, _cand_row(), state="texas", state_id=1, state_code="TX", cache=cache)
    session.commit()
    s2 = _persist_cand_link(session, _cand_row(), state="texas", state_id=1, state_code="TX", cache=cache)
    session.commit()
    assert s1 == s2 == "linked"
    links = session.exec(select(UnifiedTransactionPerson)).all()
    assert len(links) == 1  # no duplicate link


def test_cand_with_no_expend_id_is_skipped(session):
    status = _persist_cand_link(
        session, {"recordType": "CAND", "candidateNameLast": "X"},
        state="texas", state_id=1, state_code="TX", cache=BuilderCache(),
    )
    assert status == "skipped_no_id"


def test_two_candidates_on_same_expenditure_both_link(session):
    """The 62K internal-dup-expendInfoId case: many candidates per expenditure."""
    session.add(
        UnifiedTransaction(
            transaction_id="100000001",
            transaction_type=TransactionType.EXPENDITURE,
            state_id=1,
        )
    )
    session.commit()
    cache = BuilderCache()
    _persist_cand_link(session, _cand_row(last="CHISUM", first="WARREN"), state="texas", state_id=1, state_code="TX", cache=cache)
    _persist_cand_link(session, _cand_row(last="SMITH", first="JANE"), state="texas", state_id=1, state_code="TX", cache=cache)
    session.commit()
    links = session.exec(select(UnifiedTransactionPerson)).all()
    assert len(links) == 2  # two distinct candidates linked to one expenditure
