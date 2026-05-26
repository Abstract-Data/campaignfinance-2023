"""Task 1e tests for Stage-2 blocking."""

from __future__ import annotations

from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.blocking import (
    CandidatePair,
    default_blocking_rules,
    generate_candidate_pairs,
    run_blocking_stage,
)
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
    last_name_phonetic: str | None = None,
    normalized_org: str | None = None,
    zip5: str | None = None,
    line_1: str | None = None,
) -> None:
    session.add(
        ResolutionInput(
            run_id=run_id,
            source_type=source_type,
            source_id=source_id,
            entity_type="person",
            raw_name=f"{source_type}-{source_id}",
            raw_address="raw address",
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
        stored = session.exec(
            select(CandidatePair).where(CandidatePair.run_id == 15)
        ).all()

        assert result == {"pairs_compared": 1}
        assert len(stored) == 1
        assert stored[0].rule_name


def test_default_rules_do_not_block_on_address_alone():
    for rule in default_blocking_rules():
        assert not rule.is_address_only
