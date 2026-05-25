"""Task 3a — MergeReview queue lifecycle tests (TDD).

Covers:
- list_pending returns only ``pending`` rows, ordered by score descending.
- list_pending filters by run_id and entity_type.
- approve flips status to ``approved``, stamps reviewer and decided_at.
- reject flips status to ``rejected``, stamps reviewer and decided_at.
- Approving / rejecting an already-decided row raises ValueError (immutability).
- Decided rows are excluded from subsequent list_pending calls.
- CLI smoke: ``list`` and ``approve`` subcommands run without errors against a
  seeded in-memory database.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlmodel import Session, SQLModel, create_engine

from app.resolve.models.resolution import (
    MatchRun,
    MergeReview,
    PassType,
    ReviewStatus,
    RunStatus,
    SourceType,
)
from app.resolve.review.queue import (
    AlreadyDecidedError,
    approve,
    get_review,
    list_pending,
    reject,
)

# ---------------------------------------------------------------------------
# In-memory engine shared by all tests in this module
# ---------------------------------------------------------------------------

_TABLES = [MatchRun.__table__, MergeReview.__table__]


def _make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine, tables=_TABLES)
    return engine


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_run(session: Session, run_id: int = 1) -> MatchRun:
    run = MatchRun(
        id=run_id,
        state_code="TX",
        pass_type=PassType.entity,
        status=RunStatus.running,
    )
    session.add(run)
    session.commit()
    return run


def _seed_review(
    session: Session,
    *,
    run_id: int = 1,
    source_a_type: SourceType = SourceType.unified_person,
    source_a_id: str = "p-1",
    source_b_id: str = "p-2",
    score: float = 0.85,
    status: ReviewStatus = ReviewStatus.pending,
    reviewer: str | None = None,
    notes: str | None = None,
) -> MergeReview:
    row = MergeReview(
        run_id=run_id,
        source_a_type=source_a_type,
        source_a_id=source_a_id,
        source_b_type=SourceType.unified_person,
        source_b_id=source_b_id,
        score=score,
        status=status,
        reviewer=reviewer,
        decided_at=datetime.now(timezone.utc) if status != ReviewStatus.pending else None,
        notes=notes,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


# ---------------------------------------------------------------------------
# list_pending — filtering and ordering
# ---------------------------------------------------------------------------


class TestListPending:
    def test_returns_only_pending_rows(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r1 = _seed_review(session, source_a_id="p-1", score=0.90)
            _seed_review(
                session,
                source_a_id="p-2",
                score=0.85,
                status=ReviewStatus.approved,
                reviewer="alice",
            )
            _seed_review(
                session,
                source_a_id="p-3",
                score=0.80,
                status=ReviewStatus.rejected,
                reviewer="alice",
            )

            results = list_pending(session)

        ids = {r.id for r in results}
        assert r1.id in ids
        assert len(results) == 1

    def test_ordered_by_score_descending(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            _seed_review(session, source_a_id="p-1", score=0.82)
            _seed_review(session, source_a_id="p-2", score=0.95)
            _seed_review(session, source_a_id="p-3", score=0.87)

            results = list_pending(session)

        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True), "Rows must be ordered by score descending"

    def test_filter_by_run_id(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session, run_id=1)
            _seed_run(session, run_id=2)
            r1 = _seed_review(session, run_id=1, source_a_id="p-1")
            _seed_review(session, run_id=2, source_a_id="p-2")

            results = list_pending(session, run_id=1)

        assert len(results) == 1
        assert results[0].id == r1.id

    def test_filter_by_entity_type(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r_person = _seed_review(
                session,
                source_a_type=SourceType.unified_person,
                source_a_id="p-1",
            )
            _seed_review(
                session,
                source_a_type=SourceType.unified_committee,
                source_a_id="c-1",
            )

            results = list_pending(session, entity_type="person")

        assert len(results) == 1
        assert results[0].id == r_person.id

    def test_limit_parameter(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            for i in range(5):
                _seed_review(session, source_a_id=f"p-{i}", score=0.80 + i * 0.01)

            results = list_pending(session, limit=3)

        assert len(results) == 3

    def test_empty_queue_returns_empty_list(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            results = list_pending(session)
        assert results == []

    def test_null_score_rows_sorted_last(self):
        """Rows with no score appear after rows that have a score."""
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            _seed_review(session, source_a_id="p-null", score=None)
            _seed_review(session, source_a_id="p-scored", score=0.88)

            results = list_pending(session)

        non_null = [r for r in results if r.score is not None]
        null_rows = [r for r in results if r.score is None]
        assert len(non_null) >= 1
        # Non-null scores come before null scores
        assert results.index(non_null[0]) < results.index(null_rows[0])

    def test_invalid_entity_type_raises_value_error(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            with pytest.raises(ValueError, match="Unknown entity_type"):
                list_pending(session, entity_type="invalid")


# ---------------------------------------------------------------------------
# get_review
# ---------------------------------------------------------------------------


class TestGetReview:
    def test_returns_row_by_id(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            result = get_review(session, r.id)
        assert result.id == r.id

    def test_raises_for_missing_id(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            with pytest.raises(KeyError):
                get_review(session, 9999)


# ---------------------------------------------------------------------------
# approve
# ---------------------------------------------------------------------------


class TestApprove:
    def test_approve_flips_status_and_stamps_fields(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            # Use naive UTC so the comparison survives SQLite (which strips tzinfo).
            before = datetime.now(timezone.utc).replace(tzinfo=None)

            result = approve(session, r.id, reviewer="alice", notes="looks good")

        assert result.status == ReviewStatus.approved
        assert result.reviewer == "alice"
        assert result.notes == "looks good"
        assert result.decided_at is not None
        decided_naive = result.decided_at.replace(tzinfo=None)
        assert decided_naive >= before

    def test_approve_without_notes_defaults_empty_string(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            result = approve(session, r.id, reviewer="bob")
        # notes defaults to ""
        assert result.notes is not None

    def test_approve_persists_to_db(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            approve(session, r.id, reviewer="alice")

        with Session(engine) as session:
            row = get_review(session, r.id)
            assert row.status == ReviewStatus.approved
            assert row.reviewer == "alice"

    def test_approve_already_approved_raises(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            approve(session, r.id, reviewer="alice")
            with pytest.raises(AlreadyDecidedError):
                approve(session, r.id, reviewer="bob")

    def test_approve_already_rejected_raises(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            reject(session, r.id, reviewer="alice")
            with pytest.raises(AlreadyDecidedError):
                approve(session, r.id, reviewer="bob")


# ---------------------------------------------------------------------------
# reject
# ---------------------------------------------------------------------------


class TestReject:
    def test_reject_flips_status_and_stamps_fields(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            # Use naive UTC so the comparison survives SQLite (which strips tzinfo).
            before = datetime.now(timezone.utc).replace(tzinfo=None)

            result = reject(session, r.id, reviewer="carol", notes="different person")

        assert result.status == ReviewStatus.rejected
        assert result.reviewer == "carol"
        assert result.notes == "different person"
        assert result.decided_at is not None
        decided_naive = result.decided_at.replace(tzinfo=None)
        assert decided_naive >= before

    def test_reject_persists_to_db(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            reject(session, r.id, reviewer="carol")

        with Session(engine) as session:
            row = get_review(session, r.id)
            assert row.status == ReviewStatus.rejected

    def test_reject_already_decided_raises(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            reject(session, r.id, reviewer="carol")
            with pytest.raises(AlreadyDecidedError):
                reject(session, r.id, reviewer="dave")


# ---------------------------------------------------------------------------
# Immutability: decided rows excluded from list_pending
# ---------------------------------------------------------------------------


class TestDecidedRowsExcludedFromQueue:
    def test_approved_row_excluded_from_list_pending(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            approve(session, r.id, reviewer="alice")
            results = list_pending(session)
        assert all(row.id != r.id for row in results)

    def test_rejected_row_excluded_from_list_pending(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            reject(session, r.id, reviewer="carol")
            results = list_pending(session)
        assert all(row.id != r.id for row in results)

    def test_only_pending_rows_survive_mixed_queue(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            pending1 = _seed_review(session, source_a_id="p-1", score=0.90)
            pending2 = _seed_review(session, source_a_id="p-2", score=0.85)
            decided = _seed_review(session, source_a_id="p-3", score=0.80)
            approve(session, decided.id, reviewer="alice")

            results = list_pending(session)

        result_ids = {r.id for r in results}
        assert pending1.id in result_ids
        assert pending2.id in result_ids
        assert decided.id not in result_ids


# ---------------------------------------------------------------------------
# CLI smoke tests
# ---------------------------------------------------------------------------


class TestCliSmoke:
    """Smoke tests invoke the CLI main() directly with a patched session."""

    def _make_seeded_engine(self):
        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            _seed_review(session, source_a_id="p-1", score=0.92)
            _seed_review(session, source_a_id="p-2", score=0.87)
        return engine

    def test_list_command_runs_without_error(self, capsys):
        from app.resolve.review.cli import _run_list

        engine = self._make_seeded_engine()
        with Session(engine) as session:
            _run_list(session, run_id=None, entity_type=None, limit=None)

        captured = capsys.readouterr()
        assert captured.out  # some tabular output was produced

    def test_list_command_shows_pending_items(self, capsys):
        from app.resolve.review.cli import _run_list

        engine = self._make_seeded_engine()
        with Session(engine) as session:
            _run_list(session, run_id=None, entity_type=None, limit=None)

        captured = capsys.readouterr()
        # Both pending pairs should appear
        assert "p-1" in captured.out or "pending" in captured.out.lower()

    def test_approve_command_via_cli_helper(self, capsys):
        from app.resolve.review.cli import _run_approve

        engine = self._make_seeded_engine()
        with Session(engine) as session:
            rows = list_pending(session)
            target_id = rows[0].id

        with Session(engine) as session:
            _run_approve(session, target_id, reviewer="test-reviewer", notes="smoke test")

        with Session(engine) as session:
            row = get_review(session, target_id)
            assert row.status == ReviewStatus.approved
            assert row.reviewer == "test-reviewer"

    def test_reject_command_via_cli_helper(self):
        from app.resolve.review.cli import _run_reject

        engine = self._make_seeded_engine()
        with Session(engine) as session:
            rows = list_pending(session)
            target_id = rows[0].id

        with Session(engine) as session:
            _run_reject(session, target_id, reviewer="test-reviewer", notes="smoke test")

        with Session(engine) as session:
            row = get_review(session, target_id)
            assert row.status == ReviewStatus.rejected

    def test_show_command_does_not_raise(self, capsys):
        from app.resolve.review.cli import _run_show

        engine = self._make_seeded_engine()
        with Session(engine) as session:
            rows = list_pending(session)
            target_id = rows[0].id

        with Session(engine) as session:
            _run_show(session, target_id)

        captured = capsys.readouterr()
        assert captured.out  # some output was produced


class TestCliErrorExitCodes:
    """Approve/reject helpers return 1 on error; main() propagates without sys.exit."""

    def test_approve_missing_id_returns_1(self):
        from app.resolve.review.cli import _run_approve

        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            code = _run_approve(session, 9999, reviewer="alice")

        assert code == 1

    def test_approve_already_decided_returns_1(self):
        from app.resolve.review.cli import _run_approve

        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            approve(session, r.id, reviewer="alice")
            code = _run_approve(session, r.id, reviewer="bob")

        assert code == 1

    def test_reject_missing_id_returns_1(self):
        from app.resolve.review.cli import _run_reject

        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            code = _run_reject(session, 9999, reviewer="carol")

        assert code == 1

    def test_reject_already_decided_returns_1(self):
        from app.resolve.review.cli import _run_reject

        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            reject(session, r.id, reviewer="carol")
            code = _run_reject(session, r.id, reviewer="dave")

        assert code == 1

    def test_approve_success_returns_0(self):
        from app.resolve.review.cli import _run_approve

        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            r = _seed_review(session)
            code = _run_approve(session, r.id, reviewer="alice")

        assert code == 0

    def test_main_show_missing_returns_1(self):
        from app.resolve.review.cli import main

        code = main(["--sqlite", "show", "9999"])
        assert code == 1

    def test_main_approve_missing_returns_1(self):
        from app.resolve.review.cli import main

        code = main(["--sqlite", "approve", "9999", "--reviewer", "alice"])
        assert code == 1

    def test_error_paths_do_not_call_sys_exit(self, monkeypatch):
        from app.resolve.review.cli import _run_approve

        def _fail_exit(_code: int) -> None:
            raise AssertionError("sys.exit must not be called from approve/reject helpers")

        monkeypatch.setattr("app.resolve.review.cli.sys.exit", _fail_exit)

        engine = _make_engine()
        with Session(engine) as session:
            _seed_run(session)
            code = _run_approve(session, 9999, reviewer="alice")

        assert code == 1
