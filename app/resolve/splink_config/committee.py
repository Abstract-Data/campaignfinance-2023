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

# EM training blocks on normalized org name + ZIP3.
TRAINING_BLOCKING_RULE = block_on("normalized_org", "zip3")

# Prediction blocking mirrors Phase-1 org blocking rules.  The SQL blocking now
# scopes each rule by entity_type (see blocking_sql._RULE_BLOCK_KEY_SQL), so
# committee candidate pairs come only from these rules and Splink's bulk
# predict reproduces them — no per-pair fallback.
#
# LOCK-STEP WARNING: every rule here must have a matching entry in BOTH
# blocking.default_blocking_rules() and blocking_sql._RULE_BLOCK_KEY_SQL.
# Adding or removing a rule in one place without updating the other two will
# cause the SQL blocker to generate pairs that Splink cannot reproduce in bulk
# predict, forcing a slow per-pair fallback.
# The second rule connects the SAME normalized org across different addresses
# (different ZIPs) within a state — the D-org cross-role/cross-address case the
# zip3 rule misses. It blocks on EXACT normalized_org (not a first-word phonetic):
# measured on real spike data, (org_name_phonetic, state) exploded candidate
# pairs ~9,200x (34.1M vs 3.7k) because org_name_phonetic is only the phonetic of
# the first name token and state is TX-dominated; (normalized_org, state) is ~1.2x
# (4.4k). Fuzzy name variants are still caught by the JaroWinkler comparison at
# scoring time.
PREDICTION_BLOCKING_RULES = [
    block_on("normalized_org", "zip3"),
    block_on("normalized_org", "state"),
]
