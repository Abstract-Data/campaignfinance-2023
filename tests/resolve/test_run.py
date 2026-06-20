"""Task 1d — ResolutionRun orchestrator tests.

TDD Steps
---------
Step 1: .start() writes a running match_run row.
Step 2: .finish() flips to completed with merged counts.
Step 3: .fail() flips to failed.
Step 4: .run() with a raising stage leaves the run failed and re-raises;
        .run() with two trivial stages merges their count dicts.
Step 5: staging swap module deleted; fail() works without staging tables.
"""

from __future__ import annotations

import json

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlmodel import Session, SQLModel, create_engine

from app.resolve.models.resolution import MatchRun, RunStatus
from app.resolve.run import ResolutionRun, Stage

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_engine():
    """Return a fresh in-memory SQLite engine with match_run created."""
    eng = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(eng, tables=[MatchRun.__table__])
    return eng


@pytest.fixture()
def engine():
    eng = _make_engine()
    yield eng
    eng.dispose()


@pytest.fixture()
def session(engine):
    with Session(engine) as s:
        yield s


@pytest.fixture()
def basic_run():
    return ResolutionRun(state_code="TX", config={"seed": 42})


# ---------------------------------------------------------------------------
# Step 1 — .start() writes a running match_run
# ---------------------------------------------------------------------------


class TestStart:
    def test_start_inserts_running_row(self, session, basic_run):
        run = basic_run.start(session)
        assert run.id is not None
        assert run.status == RunStatus.running

    def test_start_sets_state_code(self, session, basic_run):
        run = basic_run.start(session)
        assert run.state_code == "TX"

    def test_start_captures_config_json(self, session, basic_run):
        run = basic_run.start(session)
        assert run.config_json is not None
        decoded = json.loads(run.config_json)
        assert decoded["seed"] == 42

    def test_start_sets_started_at(self, session, basic_run):
        run = basic_run.start(session)
        assert run.started_at is not None

    def test_start_row_persisted_in_db(self, engine, basic_run):
        """Row must survive outside the session it was inserted in."""
        with Session(engine) as s:
            basic_run.start(s)
        with Session(engine) as s:
            rows = s.exec(SQLModel.metadata.tables["match_run"].select()).all()
            assert len(rows) == 1
            assert rows[0].status == "running"


# ---------------------------------------------------------------------------
# Step 2 — .finish() flips to completed with counts
# ---------------------------------------------------------------------------


class TestFinish:
    def test_finish_sets_completed_status(self, session, basic_run):
        basic_run.start(session)
        basic_run.finish(session, {})
        row = session.get(MatchRun, basic_run.run_id)
        assert row.status == RunStatus.completed

    def test_finish_sets_finished_at(self, session, basic_run):
        basic_run.start(session)
        basic_run.finish(session, {})
        row = session.get(MatchRun, basic_run.run_id)
        assert row.finished_at is not None

    def test_finish_merges_records_in(self, session, basic_run):
        basic_run.start(session)
        basic_run.finish(session, {"records_in": 150})
        row = session.get(MatchRun, basic_run.run_id)
        assert row.records_in == 150

    def test_finish_merges_all_counter_columns(self, session, basic_run):
        counts = {
            "records_in": 100,
            "pairs_compared": 200,
            "auto_merges": 50,
            "queued": 10,
            "rejected": 30,
            "canonical_out": 60,
        }
        basic_run.start(session)
        basic_run.finish(session, counts)
        row = session.get(MatchRun, basic_run.run_id)
        assert row.records_in == 100
        assert row.pairs_compared == 200
        assert row.auto_merges == 50
        assert row.queued == 10
        assert row.rejected == 30
        assert row.canonical_out == 60

    def test_finish_without_start_raises(self, session):
        run = ResolutionRun(state_code="TX", config={})
        with pytest.raises(RuntimeError, match="start"):
            run.finish(session, {})


# ---------------------------------------------------------------------------
# Step 3 — .fail() flips to failed
# ---------------------------------------------------------------------------


class TestFail:
    def test_fail_sets_failed_status(self, session, basic_run):
        basic_run.start(session)
        basic_run.fail(session, "boom")
        row = session.get(MatchRun, basic_run.run_id)
        assert row.status == RunStatus.failed

    def test_fail_sets_finished_at(self, session, basic_run):
        basic_run.start(session)
        basic_run.fail(session, "boom")
        row = session.get(MatchRun, basic_run.run_id)
        assert row.finished_at is not None

    def test_fail_without_start_raises(self, session):
        run = ResolutionRun(state_code="TX", config={})
        with pytest.raises(RuntimeError, match="start"):
            run.fail(session, "error")


# ---------------------------------------------------------------------------
# Step 4 — .run() orchestration
# ---------------------------------------------------------------------------


class TestRun:
    @staticmethod
    def _make_stage(name: str, counts: dict, call_log: list) -> Stage:
        """Build a minimal stage callable that records its invocation."""

        def stage(session, run_id, config):
            call_log.append(name)
            return counts

        return stage  # type: ignore[return-value]

    @staticmethod
    def _make_failing_stage(exc: Exception) -> Stage:
        def stage(session, run_id, config):
            raise exc

        return stage  # type: ignore[return-value]

    def test_run_with_no_stages_completes(self, engine, basic_run):
        with Session(engine) as session:
            result = basic_run.run(session, [])
        assert result.status == RunStatus.completed

    def test_run_calls_stages_in_order(self, engine):
        call_log: list[str] = []
        run = ResolutionRun(state_code="TX", config={})
        stages = [
            self._make_stage("first", {}, call_log),
            self._make_stage("second", {}, call_log),
        ]
        with Session(engine) as session:
            run.run(session, stages)
        assert call_log == ["first", "second"]

    def test_run_later_stage_overwrites_same_counter_key(self, engine):
        """Duplicate counter keys use last-stage overwrite, not summation."""
        run = ResolutionRun(state_code="TX", config={})
        stages = [
            self._make_stage("s1", {"records_in": 10, "pairs_compared": 3}, []),
            self._make_stage("s2", {"records_in": 5, "pairs_compared": 8}, []),
        ]
        with Session(engine) as session:
            result = run.run(session, stages)
        assert result.records_in == 5
        assert result.pairs_compared == 8

    def test_run_merges_stage_count_dicts(self, engine):
        run = ResolutionRun(state_code="TX", config={})
        stages = [
            self._make_stage("s1", {"records_in": 10, "auto_merges": 2}, []),
            self._make_stage("s2", {"records_in": 5, "canonical_out": 7}, []),
        ]
        with Session(engine) as session:
            result = run.run(session, stages)
        # Second stage overrides first for the same key (dict.update semantics).
        assert result.records_in == 5
        assert result.auto_merges == 2
        assert result.canonical_out == 7

    def test_run_fails_run_on_stage_exception(self, engine):
        run = ResolutionRun(state_code="TX", config={})
        boom = ValueError("stage exploded")
        with Session(engine) as session:
            with pytest.raises(ValueError, match="stage exploded"):
                run.run(session, [self._make_failing_stage(boom)])

        with Session(engine) as fresh:
            row = fresh.get(MatchRun, run.run_id)
        assert row.status == RunStatus.failed

    def test_run_reraises_stage_exception(self, engine):
        run = ResolutionRun(state_code="TX", config={})
        boom = RuntimeError("must propagate")
        with Session(engine) as session:
            with pytest.raises(RuntimeError, match="must propagate"):
                run.run(session, [self._make_failing_stage(boom)])

    def test_run_passes_run_id_to_stages(self, engine):
        """Stages receive the match_run.id of this run."""
        captured: list[int] = []

        def capture(session, run_id, config):
            captured.append(run_id)
            return {}

        run = ResolutionRun(state_code="TX", config={})
        with Session(engine) as session:
            result = run.run(session, [capture])

        assert len(captured) == 1
        assert captured[0] == result.id

    def test_run_passes_config_to_stages(self, engine):
        """Stages receive the config dict that was passed to ResolutionRun."""
        captured: list[dict] = []

        def capture(session, run_id, config):
            captured.append(config)
            return {}

        cfg = {"threshold": 0.99, "seed": 7}
        run = ResolutionRun(state_code="TX", config=cfg)
        with Session(engine) as session:
            run.run(session, [capture])

        assert captured[0] == cfg


# ---------------------------------------------------------------------------
# Step 5 replacement — staging module removal + fail() without staging
# ---------------------------------------------------------------------------


def test_staging_swap_module_removed():
    import importlib

    try:
        importlib.import_module("app.resolve.staging")
    except ModuleNotFoundError:
        return
    raise AssertionError("app.resolve.staging should be deleted (Option 1)")


def test_resolution_run_fail_without_staging_tables():
    """fail() must succeed even when no staging_run_* tables exist."""
    engine = _make_engine()
    run = ResolutionRun(state_code="TX", config={})
    with Session(engine) as session:
        run.start(session)
        run.fail(session, "simulated stage error")
    refreshed: MatchRun | None
    with Session(engine) as s:
        refreshed = s.get(MatchRun, run.run_id)
    assert refreshed is not None
    assert refreshed.status == RunStatus.failed


# ---------------------------------------------------------------------------
# ensure_resolution_schema — scoped DDL
# ---------------------------------------------------------------------------


def test_ensure_resolution_schema_creates_match_run_only_from_resolve_models():
    """ensure_resolution_schema must not create unified source tables."""
    import app.core.models  # noqa: F401
    from app.resolve.run import ensure_resolution_schema, resolution_schema_table_names

    engine = create_engine("sqlite:///:memory:", echo=False)
    ensure_resolution_schema(engine)

    created = set(sa_inspect(engine).get_table_names())
    assert "match_run" in created
    assert resolution_schema_table_names() <= created
    assert "unified_persons" not in created

    engine.dispose()


def test_ensure_resolution_schema_creates_scored_pairs_and_clusters():
    """ensure_resolution_schema must create the Phase 2 staging tables
    scored_pairs and clusters so that stages 4 and 6 can write to them."""
    import app.core.models  # noqa: F401
    from app.resolve.run import ensure_resolution_schema

    engine = create_engine("sqlite:///:memory:", echo=False)
    ensure_resolution_schema(engine)

    created = set(sa_inspect(engine).get_table_names())
    assert "scored_pairs" in created, "scored_pairs table not created by ensure_resolution_schema"
    assert "clusters" in created, "clusters table not created by ensure_resolution_schema"

    engine.dispose()
