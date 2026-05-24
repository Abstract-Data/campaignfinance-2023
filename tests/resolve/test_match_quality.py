"""Task 2e — Golden-set precision/recall regression harness.

Loads hand-labeled golden fixtures and asserts that the matching path achieves
at least PRECISION_FLOOR precision.  Recall is computed and reported but not
gated; it is expected to rise as Phase 2 probabilistic stages land.

Phase 2 Splink path
-------------------
``app.resolve.stages.splink_scorer`` seeds golden CSV pairs into an in-memory
SQLite session and delegates to ``run_score_stage``.  Baseline exact-match
tests remain for harness smoke coverage when Splink is unavailable.

No live database required — all operations run in-memory on plain CSV data.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PRECISION_FLOOR: float = 0.95

GOLDEN_DIR: Path = Path(__file__).parent / "golden"

# ---------------------------------------------------------------------------
# Splink scorer availability (task-2a + golden-set adapter)
# ---------------------------------------------------------------------------

_SPLINK_AVAILABLE: bool = False

try:
    from app.resolve.stages import splink_scorer as _splink_scorer  # noqa: F401

    _SPLINK_AVAILABLE = True
except ImportError:
    pass

# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------


def _load_golden(filename: str) -> list[dict[str, Any]]:
    """Load a golden CSV and return a list of row dicts."""
    path = GOLDEN_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Golden fixture not found: {path}. "
            "Run from the repo root or check GOLDEN_DIR."
        )
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


# ---------------------------------------------------------------------------
# Precision / recall helper
# ---------------------------------------------------------------------------


def compute_precision_recall(
    predictions: list[str],
    labels: list[str],
    *,
    positive_label: str = "match",
) -> tuple[float, float]:
    """Return (precision, recall) for binary ``positive_label`` classification.

    - If there are no positive predictions, precision is defined as 1.0
      (a perfectly selective classifier that never fires has no false positives).
    - If there are no positive labels, recall is defined as 0.0.
    """
    if len(predictions) != len(labels):
        raise ValueError(
            f"Length mismatch: {len(predictions)} predictions vs {len(labels)} labels"
        )

    tp = sum(
        1
        for p, lbl in zip(predictions, labels)
        if p == positive_label and lbl == positive_label
    )
    fp = sum(
        1
        for p, lbl in zip(predictions, labels)
        if p == positive_label and lbl != positive_label
    )
    fn = sum(
        1
        for p, lbl in zip(predictions, labels)
        if p != positive_label and lbl == positive_label
    )

    precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    return precision, recall


# ---------------------------------------------------------------------------
# Baseline scorers (exact-match stubs; active until task-2a merges)
# ---------------------------------------------------------------------------


def _norm(value: str | None) -> str:
    """Lowercase-strip a nullable field; return empty string for None/blank."""
    return (value or "").strip().lower()


def _baseline_person_scorer(row: dict[str, Any]) -> str:
    """Predict 'match' only when first_name AND last_name are identical.

    Conservative: high precision, lower recall.  Name variants (Jim/James)
    and typos will be false-negatives until the Splink scorer lands.
    """
    if _norm(row.get("first_name_a")) and _norm(row.get("last_name_a")):
        if _norm(row.get("first_name_a")) == _norm(row.get("first_name_b")) and _norm(
            row.get("last_name_a")
        ) == _norm(row.get("last_name_b")):
            return "match"
    return "no_match"


def _baseline_org_scorer(row: dict[str, Any]) -> str:
    """Predict 'match' when normalized_org is identical.

    ``normalize_org_name`` already strips legal suffixes (Corp/Inc/LLC/…) so
    suffix variants are caught here.  Abbreviation variants and typos require
    the probabilistic scorer.
    """
    norm_a = _norm(row.get("normalized_org_a"))
    norm_b = _norm(row.get("normalized_org_b"))
    if norm_a and norm_b and norm_a == norm_b:
        return "match"
    return "no_match"


def _baseline_committee_scorer(row: dict[str, Any]) -> str:
    """Predict 'match' when filer_id is identical, or when normalized_org matches.

    Same-filer-ID is certain; same-normalized-name handles committees that
    re-registered under a new ID with the same name.
    """
    fid_a = _norm(row.get("filer_id_a"))
    fid_b = _norm(row.get("filer_id_b"))
    if fid_a and fid_b and fid_a == fid_b:
        return "match"

    norm_a = _norm(row.get("normalized_org_a"))
    norm_b = _norm(row.get("normalized_org_b"))
    if norm_a and norm_b and norm_a == norm_b:
        return "match"

    return "no_match"


# ---------------------------------------------------------------------------
# Unit test: precision/recall helper
# ---------------------------------------------------------------------------


class TestComputePrecisionRecall:
    """Verify the precision/recall helper against known TP/FP/FN/TN counts."""

    def test_perfect_precision_and_recall(self) -> None:
        preds = ["match", "match", "no_match", "no_match"]
        labels = ["match", "match", "no_match", "no_match"]
        precision, recall = compute_precision_recall(preds, labels)
        assert precision == pytest.approx(1.0)
        assert recall == pytest.approx(1.0)

    def test_no_positive_predictions_precision_is_one(self) -> None:
        preds = ["no_match", "no_match", "no_match"]
        labels = ["match", "no_match", "no_match"]
        precision, recall = compute_precision_recall(preds, labels)
        assert precision == pytest.approx(1.0)
        assert recall == pytest.approx(0.0)

    def test_all_positive_predictions_no_true_positives(self) -> None:
        # All predicted match, none are actually match → precision = 0
        preds = ["match", "match", "match"]
        labels = ["no_match", "no_match", "no_match"]
        precision, recall = compute_precision_recall(preds, labels)
        assert precision == pytest.approx(0.0)
        # recall: no positive labels → 0.0
        assert recall == pytest.approx(0.0)

    def test_mixed_results(self) -> None:
        # TP=3, FP=1, FN=2  → precision=3/4=0.75, recall=3/5=0.60
        preds = ["match", "match", "match", "match", "no_match", "no_match", "no_match"]
        labels = ["match", "match", "match", "no_match", "match", "match", "no_match"]
        precision, recall = compute_precision_recall(preds, labels)
        assert precision == pytest.approx(3 / 4)
        assert recall == pytest.approx(3 / 5)

    def test_length_mismatch_raises(self) -> None:
        with pytest.raises(ValueError, match="Length mismatch"):
            compute_precision_recall(["match", "no_match"], ["match"])

    def test_all_true_negatives(self) -> None:
        preds = ["no_match", "no_match"]
        labels = ["no_match", "no_match"]
        precision, recall = compute_precision_recall(preds, labels)
        assert precision == pytest.approx(1.0)
        assert recall == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Baseline precision/recall tests (always run — Phase 2 not required)
# ---------------------------------------------------------------------------


def _assert_precision_floor(
    entity_type: str,
    predictions: list[str],
    labels: list[str],
    scorer_name: str,
) -> None:
    """Compute metrics, print them, and enforce PRECISION_FLOOR."""
    precision, recall = compute_precision_recall(predictions, labels)
    tp = sum(1 for p, lbl in zip(predictions, labels) if p == "match" and lbl == "match")
    fp = sum(1 for p, lbl in zip(predictions, labels) if p == "match" and lbl != "match")
    fn = sum(1 for p, lbl in zip(predictions, labels) if p != "match" and lbl == "match")
    tn = sum(1 for p, lbl in zip(predictions, labels) if p != "match" and lbl != "match")
    print(
        f"\n[{entity_type} / {scorer_name}] "
        f"precision={precision:.3f}  recall={recall:.3f}  "
        f"TP={tp} FP={fp} FN={fn} TN={tn}  n={len(labels)}"
    )
    assert precision >= PRECISION_FLOOR, (
        f"{entity_type} {scorer_name} precision {precision:.3f} < floor "
        f"{PRECISION_FLOOR}. TP={tp} FP={fp} (false positives above)."
    )


def test_person_match_quality_baseline() -> None:
    """Baseline exact-name scorer must meet the precision floor on person pairs."""
    pairs = _load_golden("person_pairs.csv")
    predictions = [_baseline_person_scorer(row) for row in pairs]
    labels = [row["label"] for row in pairs]
    _assert_precision_floor("person", predictions, labels, "baseline")


def test_organization_match_quality_baseline() -> None:
    """Baseline normalized-org scorer must meet precision floor on org pairs."""
    pairs = _load_golden("organization_pairs.csv")
    predictions = [_baseline_org_scorer(row) for row in pairs]
    labels = [row["label"] for row in pairs]
    _assert_precision_floor("organization", predictions, labels, "baseline")


def test_committee_match_quality_baseline() -> None:
    """Baseline filer-id/normalized-org scorer must meet precision floor on committee pairs."""
    pairs = _load_golden("committee_pairs.csv")
    predictions = [_baseline_committee_scorer(row) for row in pairs]
    labels = [row["label"] for row in pairs]
    _assert_precision_floor("committee", predictions, labels, "baseline")


# ---------------------------------------------------------------------------
# Splink-wired tests (live when splink_scorer adapter is importable)
# ---------------------------------------------------------------------------

_SKIP_SPLINK = pytest.mark.skipif(
    not _SPLINK_AVAILABLE,
    reason="app.resolve.stages.splink_scorer is not importable",
)


@_SKIP_SPLINK
def test_person_match_quality_splink() -> None:
    """Splink probabilistic scorer must meet the precision floor on person pairs.

    Unlike the baseline, this scorer should also achieve meaningfully higher
    recall by catching name variants and typos.
    """
    from app.resolve.stages import splink_scorer

    pairs = _load_golden("person_pairs.csv")
    predictions = splink_scorer.score_person_pairs(pairs)
    labels = [row["label"] for row in pairs]
    _assert_precision_floor("person", predictions, labels, "splink")


@_SKIP_SPLINK
def test_organization_match_quality_splink() -> None:
    """Splink probabilistic scorer must meet the precision floor on org pairs."""
    from app.resolve.stages import splink_scorer

    pairs = _load_golden("organization_pairs.csv")
    predictions = splink_scorer.score_organization_pairs(pairs)
    labels = [row["label"] for row in pairs]
    _assert_precision_floor("organization", predictions, labels, "splink")


@_SKIP_SPLINK
def test_committee_match_quality_splink() -> None:
    """Splink probabilistic scorer must meet the precision floor on committee pairs."""
    from app.resolve.stages import splink_scorer

    pairs = _load_golden("committee_pairs.csv")
    predictions = splink_scorer.score_committee_pairs(pairs)
    labels = [row["label"] for row in pairs]
    _assert_precision_floor("committee", predictions, labels, "splink")


# ---------------------------------------------------------------------------
# Sanity checks: golden CSV schema / coverage
# ---------------------------------------------------------------------------


class TestGoldenFixtureSchema:
    """Lightweight structural checks on the golden CSVs."""

    def test_person_pairs_has_required_columns(self) -> None:
        pairs = _load_golden("person_pairs.csv")
        assert len(pairs) >= 50, f"Expected ≥50 person pairs, got {len(pairs)}"
        required = {
            "pair_id", "label", "notes",
            "first_name_a", "last_name_a", "line_1_a", "city_a", "state_a", "zip5_a",
            "first_name_b", "last_name_b", "line_1_b", "city_b", "state_b", "zip5_b",
        }
        cols = set(pairs[0].keys())
        missing = required - cols
        assert not missing, f"person_pairs.csv missing columns: {missing}"

    def test_person_pairs_has_both_labels(self) -> None:
        pairs = _load_golden("person_pairs.csv")
        labels = {row["label"] for row in pairs}
        assert "match" in labels
        assert "no_match" in labels

    def test_person_pairs_hard_cases_present(self) -> None:
        pairs = _load_golden("person_pairs.csv")
        notes_values = {row["notes"] for row in pairs}
        hard_tags = {n for n in notes_values if "variant" in n or "typo" in n}
        assert hard_tags, (
            "person_pairs.csv must contain at least one name_variant or typo pair. "
            f"Found notes: {notes_values}"
        )

    def test_org_pairs_has_required_columns(self) -> None:
        pairs = _load_golden("organization_pairs.csv")
        assert len(pairs) >= 50, f"Expected ≥50 org pairs, got {len(pairs)}"
        required = {
            "pair_id", "label", "notes",
            "raw_name_a", "normalized_org_a", "line_1_a", "city_a", "state_a", "zip5_a",
            "raw_name_b", "normalized_org_b", "line_1_b", "city_b", "state_b", "zip5_b",
        }
        cols = set(pairs[0].keys())
        missing = required - cols
        assert not missing, f"organization_pairs.csv missing columns: {missing}"

    def test_org_pairs_has_both_labels(self) -> None:
        pairs = _load_golden("organization_pairs.csv")
        labels = {row["label"] for row in pairs}
        assert "match" in labels
        assert "no_match" in labels

    def test_org_pairs_has_suffix_variant_cases(self) -> None:
        pairs = _load_golden("organization_pairs.csv")
        suffix_pairs = [row for row in pairs if "suffix_variant" in row.get("notes", "")]
        assert suffix_pairs, "organization_pairs.csv must contain suffix_variant pairs"

    def test_committee_pairs_has_required_columns(self) -> None:
        pairs = _load_golden("committee_pairs.csv")
        assert len(pairs) >= 50, f"Expected ≥50 committee pairs, got {len(pairs)}"
        required = {
            "pair_id", "label", "notes",
            "filer_id_a", "raw_name_a", "normalized_org_a",
            "filer_id_b", "raw_name_b", "normalized_org_b",
        }
        cols = set(pairs[0].keys())
        missing = required - cols
        assert not missing, f"committee_pairs.csv missing columns: {missing}"

    def test_committee_pairs_has_both_labels(self) -> None:
        pairs = _load_golden("committee_pairs.csv")
        labels = {row["label"] for row in pairs}
        assert "match" in labels
        assert "no_match" in labels

    def test_committee_pairs_has_shared_address_diff_person_in_persons(self) -> None:
        """person_pairs must include the shared-address-different-person hard case."""
        pairs = _load_golden("person_pairs.csv")
        shared_addr = [
            row
            for row in pairs
            if "shared_address" in row.get("notes", "") and row["label"] == "no_match"
        ]
        assert shared_addr, (
            "person_pairs.csv must include no_match pairs with 'shared_address' in notes"
        )

    def test_no_duplicate_pair_ids(self) -> None:
        for filename in ("person_pairs.csv", "organization_pairs.csv", "committee_pairs.csv"):
            pairs = _load_golden(filename)
            ids = [row["pair_id"] for row in pairs]
            assert len(ids) == len(set(ids)), f"Duplicate pair_ids in {filename}"

    def test_all_labels_are_valid(self) -> None:
        for filename in ("person_pairs.csv", "organization_pairs.csv", "committee_pairs.csv"):
            pairs = _load_golden(filename)
            for row in pairs:
                assert row["label"] in {"match", "no_match"}, (
                    f"Invalid label '{row['label']}' in {filename} row {row.get('pair_id')}"
                )
