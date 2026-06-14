"""Splink comparison settings for *person* entity-type scoring (Stage 4).

Persons are compared on name components and address. Address comparisons use
term-frequency (TF) adjustment so that shared-hub addresses (e.g. a
registered-agent address used by hundreds of donors) contribute little Bayes
weight, per the spec's address-as-shared-hub section.

Task: 2a | Branch: resolve/phase-2/task-2a-splink-scoring
"""

from __future__ import annotations

import splink.comparison_library as cl
from splink import block_on

# ---------------------------------------------------------------------------
# Comparisons
# ---------------------------------------------------------------------------

COMPARISONS = [
    # Name components — Jaro-Winkler is appropriate for name typos / nicknames.
    cl.JaroWinklerAtThresholds("first_name", [0.92, 0.7]),
    cl.JaroWinklerAtThresholds("last_name", [0.92, 0.7]),
    # Address — TF-adjusted so high-frequency addresses (shared hubs) get
    # near-zero Bayes weight.
    cl.ExactMatch("line_1").configure(term_frequency_adjustments=True),
    cl.ExactMatch("city"),
    cl.ExactMatch("zip5"),
    # Employer — used as a soft comparison signal only, never a blocking key.
    # Employers change over time; blocking on employer would split the same
    # person across filing years.  This field is null-heavy: only
    # contribution-sourced persons carry it; Splink handles nulls natively via
    # its null-level gamma bucket.  EM training must re-estimate m/u weights
    # after this comparison is added.
    cl.JaroWinklerAtThresholds("employer", [0.88]),
]

# ---------------------------------------------------------------------------
# Blocking rules
# ---------------------------------------------------------------------------

# Used during EM training to estimate m-probabilities for non-blocked fields.
TRAINING_BLOCKING_RULE = block_on("last_name_phonetic", "zip3")

# Mirrors Phase-1 default blocking rules for bulk DuckDB prediction. Must stay
# in lock-step with app.resolve.blocking.default_blocking_rules — Splink's bulk
# predict re-blocks on these, so a broader rule here (e.g. first_initial) would
# regenerate the >100M-pair explosion the external blocking was tuned to avoid.
PREDICTION_BLOCKING_RULES = [
    block_on("last_name_phonetic", "zip3"),
    block_on("first_name_phonetic", "last_name_phonetic"),
]
