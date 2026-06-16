"""Tests for employer as a Splink comparison signal (Task 3a).

Verifies:
(a) person.py COMPARISONS contains an employer comparison and neither
    blocking-rule constant references "employer".
(b) _compute_features normalizes employer from source person rows and
    leaves it None when absent.
"""

from __future__ import annotations

from sqlmodel import SQLModel, create_engine

import app.resolve.models  # noqa: F401 — register UnifiedReport before ORM use
from app.resolve.splink_config.person import (
    COMPARISONS,
    PREDICTION_BLOCKING_RULES,
    TRAINING_BLOCKING_RULE,
)
from app.resolve.standardize.stage1 import _compute_features
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _blocking_rule_col_names(rule) -> set[str]:
    """Recursively collect all column output_column_name values from a blocking rule.

    block_on() returns an And object with a .blocking_rules list of
    ExactMatchRule objects, each with a .col_expression attribute.
    """
    names: set[str] = []
    sub_rules = getattr(rule, "blocking_rules", None)
    if sub_rules:
        for sub in sub_rules:
            names.extend(_blocking_rule_col_names(sub))
    else:
        ce = getattr(rule, "col_expression", None)
        if ce is not None:
            names.append(ce.output_column_name)
    return set(names)


# ---------------------------------------------------------------------------
# (a) Config-level assertions
# ---------------------------------------------------------------------------


def test_comparisons_contains_employer():
    """COMPARISONS must include exactly one employer comparison."""
    employer_comps = [c for c in COMPARISONS if c.create_output_column_name() == "employer"]
    assert len(employer_comps) == 1, (
        f"Expected 1 employer comparison in COMPARISONS, found {len(employer_comps)}"
    )


def test_training_blocking_rule_does_not_reference_employer():
    """TRAINING_BLOCKING_RULE must not reference the employer column."""
    cols = _blocking_rule_col_names(TRAINING_BLOCKING_RULE)
    assert "employer" not in cols, (
        f"TRAINING_BLOCKING_RULE unexpectedly references employer; columns found: {cols}"
    )


def test_prediction_blocking_rules_do_not_reference_employer():
    """PREDICTION_BLOCKING_RULES must not reference the employer column."""
    for rule in PREDICTION_BLOCKING_RULES:
        cols = _blocking_rule_col_names(rule)
        assert "employer" not in cols, (
            f"PREDICTION_BLOCKING_RULES entry unexpectedly references employer; columns: {cols}"
        )


# ---------------------------------------------------------------------------
# (b) Feature-computation assertions
# ---------------------------------------------------------------------------


def _make_engine():
    """Return a fresh in-memory SQLite engine with only the schema-less tables."""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=tables)
    return engine


def test_compute_features_normalizes_employer():
    """A person source dict with an employer produces a normalized employer on ResolutionInput."""
    rows = [
        {
            "source_type": "unified_person",
            "source_id": "1",
            "entity_type": "person",
            "raw_name": "Jane Doe",
            "raw_address": "100 Main St, Austin, TX 78701",
            "employer": "Acme Corp., LLC",
        }
    ]
    results = _compute_features(rows, run_id=1)
    assert len(results) == 1
    ri = results[0]
    # normalize_org_name lowercases, strips legal suffixes, strips punctuation
    assert ri.employer is not None
    assert "acme" in ri.employer
    # punctuation and legal suffix stripped
    assert "." not in ri.employer
    assert "llc" not in ri.employer


def test_compute_features_employer_none_when_absent():
    """A person source dict without an employer key yields employer=None."""
    rows = [
        {
            "source_type": "unified_person",
            "source_id": "2",
            "entity_type": "person",
            "raw_name": "Bob Smith",
            "raw_address": "200 Oak Ave, Dallas, TX 75201",
            # No 'employer' key — simulates committee or entity rows
        }
    ]
    results = _compute_features(rows, run_id=1)
    assert len(results) == 1
    assert results[0].employer is None


def test_compute_features_employer_none_when_blank():
    """A person source dict with empty-string employer yields employer=None."""
    rows = [
        {
            "source_type": "unified_person",
            "source_id": "3",
            "entity_type": "person",
            "raw_name": "Alice Jones",
            "raw_address": "300 Elm St, Houston, TX 77001",
            "employer": "",
        }
    ]
    results = _compute_features(rows, run_id=1)
    assert len(results) == 1
    assert results[0].employer is None


def test_compute_features_mixed_employer_rows():
    """Mixed rows (person with employer, person without) produce correct per-row employer values."""
    rows = [
        {
            "source_type": "unified_person",
            "source_id": "10",
            "entity_type": "person",
            "raw_name": "Carol White",
            "raw_address": "10 Maple Dr, San Antonio, TX 78201",
            "employer": "Texas Oil & Gas Inc",
        },
        {
            "source_type": "unified_person",
            "source_id": "11",
            "entity_type": "person",
            "raw_name": "Dave Brown",
            "raw_address": "20 Pine St, Austin, TX 78702",
        },
        {
            "source_type": "unified_committee",
            "source_id": "CMT-99",
            "entity_type": "committee",
            "raw_name": "Friends of Carol",
            "raw_address": "",
        },
    ]
    results = _compute_features(rows, run_id=2)
    assert len(results) == 3

    carol = results[0]
    dave = results[1]
    committee = results[2]

    # Carol has an employer — normalized (lowercased, & expanded, suffix stripped)
    assert carol.employer is not None
    assert "texas oil" in carol.employer

    # Dave has no employer key
    assert dave.employer is None

    # Committee has no employer
    assert committee.employer is None


def test_resolution_input_has_employer_column():
    """ResolutionInput.__table__ must declare an 'employer' column of length 500."""
    col = ResolutionInput.__table__.c.get("employer")
    assert col is not None, "employer column missing from ResolutionInput"
    assert col.type.length == 500
