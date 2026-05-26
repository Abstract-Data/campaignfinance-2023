"""Splink comparison settings for *committee* entity-type scoring (Stage 4).

Committees lean heavily on normalized org name and address. When source_id
(filer_id) fields are available and equal, an exact-match comparison carries
high Bayes weight; fuzzy name comparisons catch re-registrations and
spelling variants.

Task: 2a | Branch: resolve/phase-2/task-2a-splink-scoring
"""

from __future__ import annotations

import splink.comparison_library as cl
from splink import block_on

# ---------------------------------------------------------------------------
# Comparisons
# ---------------------------------------------------------------------------

COMPARISONS = [
    # Filer ID exact match — strong signal when available.
    cl.ExactMatch("source_id"),
    # Committee name — JaroWinkler handles abbreviations and reregistrations.
    cl.JaroWinklerAtThresholds("normalized_org", [0.92, 0.8, 0.7]),
    # Address — TF-adjusted for shared registered-agent addresses.
    cl.ExactMatch("line_1").configure(term_frequency_adjustments=True),
    cl.ExactMatch("zip5"),
]

# ---------------------------------------------------------------------------
# Blocking rules
# ---------------------------------------------------------------------------

# EM training blocks on normalized org name.
TRAINING_BLOCKING_RULE = block_on("normalized_org")

# Prediction blocking: by name (mirrors Phase-1) or by zip for fallback.
PREDICTION_BLOCKING_RULES = [
    block_on("normalized_org"),
    block_on("zip5"),
]
