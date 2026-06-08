"""Task 3c — Employer survivorship + history.

Tests for:
- CanonicalEntity.employer scalar: most-recent row (by activity/created date) with
  a non-null, non-blank employer.
- provenance_json["employer_history"]: list of {value, first_seen, last_seen} per
  distinct employer value, ordered by (first_seen, value).
- Cluster with no employer → employer is None and no employer_history key.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

from app.resolve.stages.survivorship import (
    Cluster,
    build_golden_record,
)
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dt(d: str) -> datetime:
    """Parse an ISO date string into a UTC datetime (midnight)."""
    return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)


def _make_row(
    source_id: str,
    *,
    employer: str | None = None,
    last_activity_date: date | None = None,
    first_activity_date: date | None = None,
    created_at: datetime | None = None,
    first_name: str = "John",
    last_name: str = "Doe",
    source_type: str = "unified_person",
    entity_type: str = "person",
) -> ResolutionInput:
    """Create an in-memory (unpersisted) ResolutionInput with just the fields under test."""
    return ResolutionInput(
        run_id=1,
        source_type=source_type,
        source_id=source_id,
        entity_type=entity_type,
        first_name=first_name,
        last_name=last_name,
        raw_name=f"{first_name} {last_name}",
        is_organization=False,
        parse_status="unparsed",
        employer=employer,
        first_activity_date=first_activity_date,
        last_activity_date=last_activity_date,
        created_at=created_at or _dt("2024-01-01"),
    )


# ---------------------------------------------------------------------------
# Tests: scalar employer selection
# ---------------------------------------------------------------------------


class TestEmployerScalar:
    def test_most_recent_by_last_activity_date_wins(self):
        """When rows have distinct last_activity_dates, the most recent wins."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            _make_row(
                "1",
                employer="Acme Corp",
                last_activity_date=date(2020, 1, 1),
                created_at=_dt("2020-01-01"),
            ),
            _make_row(
                "2",
                employer="Globex",
                last_activity_date=date(2023, 6, 15),
                created_at=_dt("2020-01-01"),
            ),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer == "Globex"

    def test_fallback_to_created_at_when_no_activity_dates(self):
        """When no activity dates exist, most-recent created_at determines employer."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            _make_row("1", employer="OldCo", created_at=_dt("2019-03-01")),
            _make_row("2", employer="NewCo", created_at=_dt("2024-07-04")),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer == "NewCo"

    def test_null_employer_rows_excluded_from_selection(self):
        """Rows with null employer do not compete for the scalar employer."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            # Newer row but no employer
            _make_row("1", employer=None, last_activity_date=date(2025, 1, 1)),
            # Older row with employer
            _make_row("2", employer="OnlyCo", last_activity_date=date(2018, 1, 1)),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer == "OnlyCo"

    def test_blank_employer_rows_excluded_from_selection(self):
        """Rows with blank/whitespace-only employer are treated as absent."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            _make_row("1", employer="   ", last_activity_date=date(2025, 1, 1)),
            _make_row("2", employer="RealCo", last_activity_date=date(2018, 1, 1)),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer == "RealCo"

    def test_no_employer_in_cluster_returns_none(self):
        """A cluster where no row has a non-null employer → entity.employer is None."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            _make_row("1", employer=None),
            _make_row("2", employer=""),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer is None

    def test_singleton_cluster_with_employer(self):
        """A single-member cluster with an employer propagates it correctly."""
        cluster = Cluster(members=[("unified_person", "1")])
        rows = [_make_row("1", employer="SoloCo", last_activity_date=date(2022, 5, 1))]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer == "SoloCo"

    def test_first_activity_date_used_when_no_last_activity(self):
        """When last_activity_date is absent, first_activity_date is used as tiebreaker."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            _make_row("1", employer="EarlyCo", first_activity_date=date(2019, 1, 1)),
            _make_row("2", employer="LateCo", first_activity_date=date(2022, 1, 1)),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer == "LateCo"


# ---------------------------------------------------------------------------
# Tests: employer_history in provenance_json
# ---------------------------------------------------------------------------


class TestEmployerHistory:
    def test_two_distinct_employers_produce_two_history_entries(self):
        """Two distinct employer values across the cluster → two employer_history entries."""
        cluster = Cluster(
            members=[("unified_person", "1"), ("unified_person", "2"), ("unified_person", "3")]
        )
        rows = [
            _make_row(
                "1",
                employer="Acme Corp",
                last_activity_date=date(2018, 1, 1),
                created_at=_dt("2018-01-01"),
            ),
            _make_row(
                "2",
                employer="Acme Corp",
                last_activity_date=date(2019, 6, 30),
                created_at=_dt("2019-06-30"),
            ),
            _make_row(
                "3",
                employer="Globex",
                last_activity_date=date(2021, 3, 15),
                created_at=_dt("2021-03-15"),
            ),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        prov = json.loads(entity.provenance_json)
        assert "employer_history" in prov
        history = prov["employer_history"]
        assert len(history) == 2

    def test_employer_history_values_are_correct(self):
        """employer_history entries have correct value, first_seen, last_seen."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            _make_row(
                "1",
                employer="Acme Corp",
                last_activity_date=date(2018, 3, 1),
                created_at=_dt("2018-03-01"),
            ),
            _make_row(
                "2",
                employer="Globex",
                last_activity_date=date(2022, 9, 10),
                created_at=_dt("2022-09-10"),
            ),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        prov = json.loads(entity.provenance_json)
        history = prov["employer_history"]
        history_by_value = {e["value"]: e for e in history}

        assert "Acme Corp" in history_by_value
        assert history_by_value["Acme Corp"]["first_seen"] == "2018-03-01"
        assert history_by_value["Acme Corp"]["last_seen"] == "2018-03-01"

        assert "Globex" in history_by_value
        assert history_by_value["Globex"]["first_seen"] == "2022-09-10"
        assert history_by_value["Globex"]["last_seen"] == "2022-09-10"

    def test_employer_history_spans_correct_dates_for_repeated_employer(self):
        """When multiple rows share the same employer, first_seen=min and last_seen=max."""
        cluster = Cluster(
            members=[("unified_person", "1"), ("unified_person", "2"), ("unified_person", "3")]
        )
        rows = [
            _make_row(
                "1",
                employer="Acme Corp",
                last_activity_date=date(2015, 1, 1),
            ),
            _make_row(
                "2",
                employer="Acme Corp",
                last_activity_date=date(2020, 12, 31),
            ),
            _make_row(
                "3",
                employer="Acme Corp",
                last_activity_date=date(2018, 6, 15),
            ),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        prov = json.loads(entity.provenance_json)
        history = prov["employer_history"]
        assert len(history) == 1
        entry = history[0]
        assert entry["value"] == "Acme Corp"
        assert entry["first_seen"] == "2015-01-01"
        assert entry["last_seen"] == "2020-12-31"

    def test_employer_history_ordered_by_first_seen_then_value(self):
        """employer_history is sorted by (first_seen, value) for determinism."""
        cluster = Cluster(
            members=[
                ("unified_person", "1"),
                ("unified_person", "2"),
                ("unified_person", "3"),
            ]
        )
        rows = [
            _make_row(
                "1",
                employer="Zebra Inc",
                last_activity_date=date(2015, 1, 1),
            ),
            _make_row(
                "2",
                employer="Apple LLC",
                last_activity_date=date(2015, 1, 1),
            ),
            _make_row(
                "3",
                employer="Globex",
                last_activity_date=date(2022, 1, 1),
            ),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        prov = json.loads(entity.provenance_json)
        history = prov["employer_history"]
        values_in_order = [e["value"] for e in history]
        # "Apple LLC" and "Zebra Inc" both have first_seen 2015-01-01;
        # alphabetically Apple comes before Zebra.
        assert values_in_order == ["Apple LLC", "Zebra Inc", "Globex"]

    def test_no_employer_produces_no_employer_history_key(self):
        """A cluster with no employer rows → provenance_json has no employer_history key."""
        cluster = Cluster(members=[("unified_person", "1")])
        rows = [_make_row("1", employer=None)]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer is None
        prov = json.loads(entity.provenance_json)
        assert "employer_history" not in prov

    def test_employer_history_dates_fallback_to_created_at(self):
        """When activity dates are absent, history first/last_seen derive from created_at."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            _make_row("1", employer="WorkCo", created_at=_dt("2017-04-10")),
            _make_row("2", employer="WorkCo", created_at=_dt("2023-11-05")),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        prov = json.loads(entity.provenance_json)
        history = prov["employer_history"]
        assert len(history) == 1
        assert history[0]["first_seen"] == "2017-04-10"
        assert history[0]["last_seen"] == "2023-11-05"

    def test_employer_scalar_matches_most_recent_history_entry(self):
        """entity.employer matches the employer of the row with the latest activity date."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = [
            _make_row(
                "1",
                employer="OldCo",
                last_activity_date=date(2015, 6, 1),
            ),
            _make_row(
                "2",
                employer="NewCo",
                last_activity_date=date(2023, 3, 20),
            ),
        ]
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.employer == "NewCo"
        prov = json.loads(entity.provenance_json)
        history = prov["employer_history"]
        history_by_value = {e["value"]: e for e in history}
        # NewCo is the most recent (last_seen 2023)
        assert "NewCo" in history_by_value

    def test_existing_provenance_keys_preserved_with_employer(self):
        """Adding employer_history must not clobber canonical_name or other provenance keys."""
        cluster = Cluster(members=[("unified_person", "1")])
        rows = [_make_row("1", employer="MyCo", last_activity_date=date(2022, 1, 1))]
        entity = build_golden_record(cluster, rows, "TX")

        prov = json.loads(entity.provenance_json)
        # Pre-existing keys must still be present
        assert "canonical_name" in prov
        assert "first_seen_date" in prov
        assert "last_seen_date" in prov
        # And new key
        assert "employer_history" in prov
