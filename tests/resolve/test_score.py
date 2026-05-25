"""Task 2a tests for Stage 4 probabilistic scoring.

Tests cover the acceptance criteria from task-2a-splink-scoring.md:

1. run_score_stage() scores every candidate pair → one scored_pairs row per pair
2. Every scored row has score in [0, 1] and non-empty explanation_json
3. Scoring is deterministic: identical inputs produce identical scores
4. A high-frequency address value contributes near-zero TF-adjusted weight
5. Per-entity-type comparison configs exist and are used (person, organization,
   committee)
6. Stage conforms to the Stage protocol: returns dict with "pairs_compared"

Task: 2a | Branch: resolve/phase-2/task-2a-splink-scoring
"""

from __future__ import annotations

import json

from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.blocking import CandidatePair
from app.resolve.models.resolution import MatchRun, PassType, RunStatus
from app.resolve.stages.score import ScoredPair, run_score_stage
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Engine / table setup
# ---------------------------------------------------------------------------

_SCORE_TABLES = [
    MatchRun.__table__,
    ResolutionInput.__table__,
    CandidatePair.__table__,
    ScoredPair.__table__,
]


def _make_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(engine, tables=_SCORE_TABLES)
    return engine


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _seed_run(session: Session, run_id: int = 1) -> None:
    session.add(
        MatchRun(
            id=run_id,
            state_code="TX",
            pass_type=PassType.entity,
            status=RunStatus.running,
        )
    )
    session.commit()


def _person_row(
    *,
    run_id: int,
    source_id: str,
    first_name: str = "John",
    last_name: str = "Smith",
    line_1: str = "123 Main St",
    city: str = "Austin",
    state: str = "TX",
    zip5: str = "78701",
) -> ResolutionInput:
    return ResolutionInput(
        run_id=run_id,
        source_type="unified_person",
        source_id=source_id,
        entity_type="person",
        first_name=first_name,
        last_name=last_name,
        line_1=line_1,
        city=city,
        state=state,
        zip5=zip5,
        parse_status="parsed",
    )


def _org_row(
    *,
    run_id: int,
    source_id: str,
    normalized_org: str = "Acme Corp",
    line_1: str = "100 Business Park",
    city: str = "Dallas",
    state: str = "TX",
    zip5: str = "75201",
) -> ResolutionInput:
    return ResolutionInput(
        run_id=run_id,
        source_type="unified_entity",
        source_id=source_id,
        entity_type="organization",
        is_organization=True,
        normalized_org=normalized_org,
        line_1=line_1,
        city=city,
        state=state,
        zip5=zip5,
        parse_status="parsed",
    )


def _committee_row(
    *,
    run_id: int,
    source_id: str,
    normalized_org: str = "Friends of Texas",
    line_1: str = "456 Capitol Ave",
    city: str = "Austin",
    state: str = "TX",
    zip5: str = "78702",
) -> ResolutionInput:
    return ResolutionInput(
        run_id=run_id,
        source_type="unified_committee",
        source_id=source_id,
        entity_type="committee",
        is_organization=True,
        normalized_org=normalized_org,
        line_1=line_1,
        city=city,
        state=state,
        zip5=zip5,
        parse_status="parsed",
    )


def _pair(
    *,
    run_id: int,
    source_a_type: str,
    source_a_id: str,
    source_b_type: str,
    source_b_id: str,
    rule_name: str = "test_rule",
) -> CandidatePair:
    return CandidatePair(
        run_id=run_id,
        source_a_type=source_a_type,
        source_a_id=source_a_id,
        source_b_type=source_b_type,
        source_b_id=source_b_id,
        rule_name=rule_name,
    )


# ---------------------------------------------------------------------------
# Core scoring contract
# ---------------------------------------------------------------------------


class TestRunScoreStageContract:
    """Stage-protocol and output-shape tests."""

    def test_returns_pairs_compared_key(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(_person_row(run_id=1, source_id="p-1"))
            session.add(_person_row(run_id=1, source_id="p-2", first_name="Jon"))
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-2",
                )
            )
            session.commit()

            result = run_score_stage(session, run_id=1, config={"seed": 42})

        assert "pairs_compared" in result
        assert result["pairs_compared"] == 1

    def test_one_scored_pair_row_per_candidate_pair(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            for i in range(1, 5):
                session.add(_person_row(run_id=1, source_id=f"p-{i}"))
            # Three candidate pairs
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-2",
                )
            )
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-3",
                )
            )
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-2",
                    source_b_type="unified_person",
                    source_b_id="p-4",
                )
            )
            session.commit()

            result = run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        assert result["pairs_compared"] == 3
        assert len(rows) == 3

    def test_score_is_in_zero_one_range(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(_person_row(run_id=1, source_id="p-1", last_name="Smith"))
            session.add(_person_row(run_id=1, source_id="p-2", last_name="Smyth"))
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-2",
                )
            )
            session.commit()

            run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        assert rows
        for row in rows:
            assert 0.0 <= row.score <= 1.0, f"score={row.score} out of range"

    def test_explanation_json_is_non_empty(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(_person_row(run_id=1, source_id="p-1"))
            session.add(_person_row(run_id=1, source_id="p-2", first_name="Jon"))
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-2",
                )
            )
            session.commit()

            run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        for row in rows:
            assert row.explanation_json, "explanation_json must not be empty"
            payload = json.loads(row.explanation_json)
            assert isinstance(payload, dict)
            assert payload, "explanation dict must not be empty"

    def test_scored_pairs_have_entity_type(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(_person_row(run_id=1, source_id="p-1"))
            session.add(_person_row(run_id=1, source_id="p-2"))
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-2",
                )
            )
            session.commit()

            run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        for row in rows:
            assert row.entity_type == "person"

    def test_empty_candidate_pairs_returns_zero(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            # No candidate pairs at all
            result = run_score_stage(session, run_id=1, config={"seed": 42})
        assert result == {"pairs_compared": 0}


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Scoring the same fixture twice produces identical results."""

    def test_identical_scores_on_repeated_run(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(_person_row(run_id=1, source_id="p-1", last_name="Smith"))
            session.add(_person_row(run_id=1, source_id="p-2", last_name="Smith", first_name="Jon"))
            session.add(
                _person_row(run_id=1, source_id="p-3", last_name="Jones", first_name="Alice")
            )
            session.add(
                _person_row(run_id=1, source_id="p-4", last_name="Jones", first_name="Alicia")
            )
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-2",
                )
            )
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-3",
                    source_b_type="unified_person",
                    source_b_id="p-4",
                )
            )
            session.commit()

            run_score_stage(session, run_id=1, config={"seed": 42})
            first_rows = session.exec(
                select(ScoredPair)
                .where(ScoredPair.run_id == 1)
                .order_by(ScoredPair.source_a_id, ScoredPair.source_b_id)
            ).all()
            first_scores = [(r.source_a_id, r.source_b_id, r.score) for r in first_rows]

            run_score_stage(session, run_id=1, config={"seed": 42})
            second_rows = session.exec(
                select(ScoredPair)
                .where(ScoredPair.run_id == 1)
                .order_by(ScoredPair.source_a_id, ScoredPair.source_b_id)
            ).all()
            second_scores = [(r.source_a_id, r.source_b_id, r.score) for r in second_rows]

        assert first_scores == second_scores


# ---------------------------------------------------------------------------
# Term-frequency adjustment
# ---------------------------------------------------------------------------


class TestTermFrequencyAdjustment:
    """Address TF adjustment is active and reduces weight for common addresses."""

    def _build_tf_fixture(self, session: Session) -> None:
        """Seed 10 persons sharing a common registered-agent address + 2 with unique addrs."""
        _seed_run(session)
        common_addr = "100 Registered Agent Ave"
        # Many people at the common (hub) address — makes it high-frequency.
        for i in range(10):
            session.add(
                _person_row(
                    run_id=1,
                    source_id=f"hub-{i}",
                    first_name=f"Person{i}",
                    last_name=f"Person{i}",
                    line_1=common_addr,
                )
            )
        # Two persons with matching names but at the common address.
        session.add(
            _person_row(
                run_id=1,
                source_id="common-a",
                first_name="Target",
                last_name="Test",
                line_1=common_addr,
            )
        )
        session.add(
            _person_row(
                run_id=1,
                source_id="common-b",
                first_name="Target",
                last_name="Test",
                line_1=common_addr,
            )
        )
        # Two persons with the same names at a unique address.
        session.add(
            _person_row(
                run_id=1,
                source_id="rare-a",
                first_name="Target",
                last_name="Test",
                line_1="999 Unique Private Rd",
            )
        )
        session.add(
            _person_row(
                run_id=1,
                source_id="rare-b",
                first_name="Target",
                last_name="Test",
                line_1="999 Unique Private Rd",
            )
        )
        # Candidate pairs
        session.add(
            _pair(
                run_id=1,
                source_a_type="unified_person",
                source_a_id="common-a",
                source_b_type="unified_person",
                source_b_id="common-b",
                rule_name="common_addr_pair",
            )
        )
        session.add(
            _pair(
                run_id=1,
                source_a_type="unified_person",
                source_a_id="rare-a",
                source_b_type="unified_person",
                source_b_id="rare-b",
                rule_name="rare_addr_pair",
            )
        )
        session.commit()

    def test_tf_adjustment_column_present_in_explanation(self):
        engine = _make_engine()
        with Session(engine) as session:
            self._build_tf_fixture(session)
            run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        # At least the row for the common-address pair should have TF data.
        common_row = next((r for r in rows if r.source_a_id == "common-a"), None)
        assert common_row is not None
        payload = json.loads(common_row.explanation_json)
        # line_1 comparison should have a bf_tf_adj entry showing TF was applied.
        assert "line_1" in payload, f"Expected 'line_1' in explanation; got {list(payload)}"
        line1_entry = payload["line_1"]
        assert (
            "bf_tf_adj" in line1_entry
        ), "Expected bf_tf_adj in line_1 explanation; TF adjustment must be active"

    def test_common_address_has_lower_tf_weight_than_rare_address(self):
        """TF-adjusted BF for address must be lower for high-frequency addresses."""
        engine = _make_engine()
        with Session(engine) as session:
            self._build_tf_fixture(session)
            run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        common_row = next((r for r in rows if r.source_a_id == "common-a"), None)
        rare_row = next((r for r in rows if r.source_a_id == "rare-a"), None)
        assert common_row and rare_row

        common_payload = json.loads(common_row.explanation_json)
        rare_payload = json.loads(rare_row.explanation_json)

        assert "line_1" in common_payload and "line_1" in rare_payload
        common_tf = common_payload["line_1"].get("bf_tf_adj")
        rare_tf = rare_payload["line_1"].get("bf_tf_adj")

        assert common_tf is not None, "Common address must have bf_tf_adj"
        assert rare_tf is not None, "Rare address must have bf_tf_adj"
        assert common_tf < rare_tf, (
            f"Common address TF weight ({common_tf:.4f}) must be less than "
            f"rare address TF weight ({rare_tf:.4f})"
        )


# ---------------------------------------------------------------------------
# Per-entity-type configs
# ---------------------------------------------------------------------------


class TestEntityTypeConfigs:
    """Each entity type uses its own comparison config."""

    def test_organization_pairs_scored(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(_org_row(run_id=1, source_id="org-1", normalized_org="Acme Corp"))
            session.add(_org_row(run_id=1, source_id="org-2", normalized_org="Acme Corporation"))
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_entity",
                    source_a_id="org-1",
                    source_b_type="unified_entity",
                    source_b_id="org-2",
                )
            )
            session.commit()

            result = run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        assert result["pairs_compared"] == 1
        assert len(rows) == 1
        assert rows[0].entity_type == "organization"
        assert 0.0 <= rows[0].score <= 1.0

    def test_committee_pairs_scored(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(
                _committee_row(run_id=1, source_id="CMT-1", normalized_org="Friends of Texas")
            )
            session.add(
                _committee_row(run_id=1, source_id="CMT-2", normalized_org="Friends of Texas PAC")
            )
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_committee",
                    source_a_id="CMT-1",
                    source_b_type="unified_committee",
                    source_b_id="CMT-2",
                )
            )
            session.commit()

            result = run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        assert result["pairs_compared"] == 1
        assert rows[0].entity_type == "committee"
        assert 0.0 <= rows[0].score <= 1.0

    def test_mixed_entity_types_scored_independently(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(_person_row(run_id=1, source_id="p-1"))
            session.add(_person_row(run_id=1, source_id="p-2", first_name="Jon"))
            session.add(_org_row(run_id=1, source_id="o-1", normalized_org="Corp A"))
            session.add(_org_row(run_id=1, source_id="o-2", normalized_org="Corp A Inc"))
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-2",
                )
            )
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_entity",
                    source_a_id="o-1",
                    source_b_type="unified_entity",
                    source_b_id="o-2",
                )
            )
            session.commit()

            result = run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        assert result["pairs_compared"] == 2
        entity_types = {r.entity_type for r in rows}
        assert "person" in entity_types
        assert "organization" in entity_types


# ---------------------------------------------------------------------------
# Idempotency / re-run safety
# ---------------------------------------------------------------------------


class TestIdempotency:
    """Re-running the stage replaces previous scored_pairs rows."""

    def test_rerun_does_not_duplicate_scored_pairs(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            session.add(_person_row(run_id=1, source_id="p-1"))
            session.add(_person_row(run_id=1, source_id="p-2"))
            session.add(
                _pair(
                    run_id=1,
                    source_a_type="unified_person",
                    source_a_id="p-1",
                    source_b_type="unified_person",
                    source_b_id="p-2",
                )
            )
            session.commit()

            run_score_stage(session, run_id=1, config={"seed": 42})
            run_score_stage(session, run_id=1, config={"seed": 42})
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()

        # Should still be exactly 1 row, not 2 duplicates.
        assert len(rows) == 1

    def test_different_run_ids_are_isolated(self):
        engine = _make_engine()
        with Session(engine) as session:
            for run_id in (1, 2):
                _seed_run(session, run_id=run_id)
                session.add(_person_row(run_id=run_id, source_id=f"p-{run_id}-1"))
                session.add(_person_row(run_id=run_id, source_id=f"p-{run_id}-2"))
                session.add(
                    _pair(
                        run_id=run_id,
                        source_a_type="unified_person",
                        source_a_id=f"p-{run_id}-1",
                        source_b_type="unified_person",
                        source_b_id=f"p-{run_id}-2",
                    )
                )
            session.commit()

            run_score_stage(session, run_id=1, config={"seed": 42})
            run_score_stage(session, run_id=2, config={"seed": 42})

            rows_run1 = session.exec(select(ScoredPair).where(ScoredPair.run_id == 1)).all()
            rows_run2 = session.exec(select(ScoredPair).where(ScoredPair.run_id == 2)).all()

        assert len(rows_run1) == 1
        assert len(rows_run2) == 1
        assert all(r.run_id == 1 for r in rows_run1)
        assert all(r.run_id == 2 for r in rows_run2)
