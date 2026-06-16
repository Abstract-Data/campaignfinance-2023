"""Task 1e tests for Stage-2 blocking."""

from __future__ import annotations

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.blocking import (
    CandidatePair,
    default_blocking_rules,
    generate_candidate_pairs,
    resolve_blocking_backend,
    run_blocking_stage,
)
from app.resolve.blocking_sql import run_blocking_stage_sql
from app.resolve.standardize.staging import ResolutionInput


def _build_session() -> Session:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(
        engine,
        tables=[ResolutionInput.__table__, CandidatePair.__table__],
    )
    return Session(engine)


def _add_input_row(
    session: Session,
    *,
    run_id: int,
    source_type: str,
    source_id: str,
    entity_type: str = "person",
    last_name_phonetic: str | None = None,
    normalized_org: str | None = None,
    zip5: str | None = None,
    line_1: str | None = None,
    first_name: str | None = "John",
    first_name_phonetic: str | None = None,
) -> None:
    session.add(
        ResolutionInput(
            run_id=run_id,
            source_type=source_type,
            source_id=source_id,
            entity_type=entity_type,
            raw_name=f"{source_type}-{source_id}",
            raw_address="raw address",
            first_name=first_name,
            first_name_phonetic=first_name_phonetic,
            last_name_phonetic=last_name_phonetic,
            normalized_org=normalized_org,
            zip5=zip5,
            line_1=line_1,
        )
    )


def test_generate_candidate_pairs_emits_within_shared_block():
    with _build_session() as session:
        _add_input_row(
            session,
            run_id=11,
            source_type="unified_person",
            source_id="P1",
            last_name_phonetic="SM0",
            zip5="78701",
        )
        _add_input_row(
            session,
            run_id=11,
            source_type="unified_person",
            source_id="P2",
            last_name_phonetic="SM0",
            zip5="78702",
        )
        session.commit()

        rules = default_blocking_rules()
        pairs = list(
            generate_candidate_pairs(
                session,
                run_id=11,
                rules=rules,
                max_block_size=100,
            )
        )

        assert len(pairs) == 1
        pair = pairs[0]
        assert (pair.source_a_type, pair.source_a_id) == ("unified_person", "P1")
        assert (pair.source_b_type, pair.source_b_id) == ("unified_person", "P2")


def test_generate_candidate_pairs_does_not_cross_blocks():
    with _build_session() as session:
        _add_input_row(
            session,
            run_id=12,
            source_type="unified_person",
            source_id="P1",
            last_name_phonetic="SM0",
            zip5="78701",
        )
        _add_input_row(
            session,
            run_id=12,
            source_type="unified_person",
            source_id="P2",
            last_name_phonetic="JN0",
            zip5="78701",
        )
        session.commit()

        pairs = list(
            generate_candidate_pairs(
                session,
                run_id=12,
                rules=default_blocking_rules(),
                max_block_size=100,
            )
        )

        assert pairs == []


def test_full_first_phonetic_blocks_same_person_across_zips():
    """Rule 2 (first_name_phonetic + last_name_phonetic) pairs the same person
    across different ZIP3s, where the ZIP3-anchored rule 1 cannot."""
    with _build_session() as session:
        _add_input_row(
            session,
            run_id=20,
            source_type="unified_person",
            source_id="P1",
            first_name="John",
            first_name_phonetic="JN",
            last_name_phonetic="SM0",
            zip5="78701",
        )
        _add_input_row(
            session,
            run_id=20,
            source_type="unified_person",
            source_id="P2",
            first_name="Jon",
            first_name_phonetic="JN",
            last_name_phonetic="SM0",
            zip5="90210",
        )
        session.commit()

        pairs = list(
            generate_candidate_pairs(
                session,
                run_id=20,
                rules=default_blocking_rules(),
                max_block_size=100,
            )
        )

        assert len(pairs) == 1
        assert {p.rule_name for p in pairs} == {"person_first_last_phonetic"}


def test_shared_first_initial_alone_does_not_block_across_zips():
    """Two different first names sharing only an initial (and a phonetic last
    name) must NOT pair across ZIP3s — the explosive first-initial key is gone."""
    with _build_session() as session:
        _add_input_row(
            session,
            run_id=21,
            source_type="unified_person",
            source_id="P1",
            first_name="John",
            first_name_phonetic="JN",
            last_name_phonetic="SM0",
            zip5="78701",
        )
        _add_input_row(
            session,
            run_id=21,
            source_type="unified_person",
            source_id="P2",
            first_name="Jane",
            first_name_phonetic="JN0",
            last_name_phonetic="SM0",
            zip5="90210",
        )
        session.commit()

        pairs = list(
            generate_candidate_pairs(
                session,
                run_id=21,
                rules=default_blocking_rules(),
                max_block_size=100,
            )
        )

        assert pairs == []


def test_generate_candidate_pairs_skips_oversized_blocks(caplog):
    with _build_session() as session:
        for i in range(3):
            _add_input_row(
                session,
                run_id=13,
                source_type="unified_person",
                source_id=f"P{i}",
                last_name_phonetic="SM0",
                zip5="78701",
            )
        session.commit()

        pairs = list(
            generate_candidate_pairs(
                session,
                run_id=13,
                rules=default_blocking_rules(),
                max_block_size=2,
            )
        )

        assert pairs == []
        assert "Skipping oversized block" in caplog.text


def test_generate_candidate_pairs_de_dupes_across_rules():
    with _build_session() as session:
        _add_input_row(
            session,
            run_id=14,
            source_type="unified_person",
            source_id="P1",
            last_name_phonetic="SM0",
            normalized_org="acme",
            zip5="78701",
        )
        _add_input_row(
            session,
            run_id=14,
            source_type="unified_person",
            source_id="P2",
            last_name_phonetic="SM0",
            normalized_org="acme",
            zip5="78703",
        )
        session.commit()

        pairs = list(
            generate_candidate_pairs(
                session,
                run_id=14,
                rules=default_blocking_rules(),
                max_block_size=100,
            )
        )

        assert len(pairs) == 1


def test_run_blocking_stage_persists_pairs_and_returns_count():
    with _build_session() as session:
        _add_input_row(
            session,
            run_id=15,
            source_type="unified_person",
            source_id="P1",
            last_name_phonetic="SM0",
            zip5="78701",
        )
        _add_input_row(
            session,
            run_id=15,
            source_type="unified_person",
            source_id="P2",
            last_name_phonetic="SM0",
            zip5="78702",
        )
        session.commit()

        result = run_blocking_stage(session, run_id=15, config={})
        stored = session.exec(select(CandidatePair).where(CandidatePair.run_id == 15)).all()

        assert result == {"pairs_compared": 1}
        assert len(stored) == 1
        assert stored[0].rule_name


def test_default_rules_do_not_block_on_address_alone():
    for rule in default_blocking_rules():
        assert not rule.is_address_only


def test_default_rules_exclude_lone_phonetic_last_name():
    rule_names = {rule.name for rule in default_blocking_rules()}
    assert "person_last_phonetic" not in rule_names
    assert "org_normalized" not in rule_names
    assert "person_last_phonetic_zip3" in rule_names
    assert "person_first_last_phonetic" in rule_names
    assert "org_normalized_zip3" in rule_names


def test_generate_candidate_pairs_requires_zip3_for_org_blocks():
    with _build_session() as session:
        _add_input_row(
            session,
            run_id=16,
            source_type="unified_entity",
            source_id="O1",
            entity_type="organization",
            normalized_org="acme corp",
            zip5="78701",
            first_name=None,
        )
        _add_input_row(
            session,
            run_id=16,
            source_type="unified_entity",
            source_id="O2",
            entity_type="organization",
            normalized_org="acme corp",
            zip5="90210",
            first_name=None,
        )
        session.commit()

        pairs = list(
            generate_candidate_pairs(
                session,
                run_id=16,
                rules=default_blocking_rules(),
                max_block_size=100,
            )
        )

        assert pairs == []


def test_resolve_blocking_backend_defaults_to_python_on_sqlite():
    with _build_session() as session:
        assert resolve_blocking_backend(session, {}) == "python"


def test_run_blocking_stage_caps_pairs_per_run(caplog):
    with _build_session() as session:
        for i in range(4):
            _add_input_row(
                session,
                run_id=17,
                source_type="unified_person",
                source_id=f"P{i}",
                last_name_phonetic="SM0",
                zip5="78701",
            )
        session.commit()

        result = run_blocking_stage(
            session,
            run_id=17,
            config={"max_pairs_per_run": 2},
        )
        stored = session.exec(select(CandidatePair).where(CandidatePair.run_id == 17)).all()

        assert result == {"pairs_compared": 2}
        assert len(stored) == 2
        assert "Capping candidate pairs" in caplog.text


def test_zip3_blocking_key_lowercases_mixed_case_zip5():
    rule = default_blocking_rules()[0]
    row = ResolutionInput(
        run_id=1,
        source_type="unified_person",
        source_id="p1",
        entity_type="person",
        raw_name="n",
        raw_address="a",
        last_name_phonetic="SM0",
        zip5="78A01",
    )
    assert rule.key_for(row) == "sm0|78a"


def test_sql_backend_rejects_custom_blocking_rules():
    with pytest.raises(ValueError, match="SQL blocking has no static key"):
        run_blocking_stage_sql(
            Session(),
            run_id=1,
            config={"blocking_rules": [{"name": "custom_rule", "fields": ["last_name_phonetic"]}]},
        )
