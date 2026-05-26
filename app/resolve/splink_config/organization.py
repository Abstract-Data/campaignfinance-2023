"""Splink comparison settings for *organization* entity-type scoring (Stage 4).

Organizations are compared on normalized org name and address. Address
comparisons carry TF adjustment to down-weight shared registered-agent or
PO Box addresses.

Task: 2a | Branch: resolve/phase-2/task-2a-splink-scoring
"""

from __future__ import annotations

import splink.comparison_library as cl
from splink import block_on

# ---------------------------------------------------------------------------
# Comparisons
# ---------------------------------------------------------------------------

COMPARISONS = [
    # Normalized org name — Jaro-Winkler handles abbreviations and punctuation
    # differences (e.g. "Corp" vs "Corporation").
    cl.JaroWinklerAtThresholds("normalized_org", [0.92, 0.8, 0.7]),
    # Address — TF-adjusted for shared-hub addresses.
    cl.ExactMatch("line_1").configure(term_frequency_adjustments=True),
    cl.ExactMatch("city"),
    cl.ExactMatch("zip5"),
]

# ---------------------------------------------------------------------------
# Blocking rules
# ---------------------------------------------------------------------------

# Blocks on normalized org name token for EM training.
TRAINING_BLOCKING_RULE = block_on("normalized_org")

# Prediction blocking mirrors Phase-1 org_normalized blocking rule.
PREDICTION_BLOCKING_RULES = [
    block_on("normalized_org"),
    block_on("zip5"),
]
