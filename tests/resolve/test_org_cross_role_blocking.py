"""Tests for Task 3b — org cross-role blocking on (normalized_org, state).

The original spec proposed (org_name_phonetic, state); measured on real spike
data that exploded candidate pairs ~9,200x (org_name_phonetic is only the
first-token phonetic and state is TX-dominated), so per the pack's >5x guardrail
the rule was corrected to EXACT (normalized_org, state) (~1.2x). Fuzzy name
variants are still caught by the org JaroWinkler comparison at scoring time.

Verifies the rule is registered consistently across all three lock-step
locations:
  1. organization.PREDICTION_BLOCKING_RULES (Splink bulk-predict)
  2. blocking.default_blocking_rules()       (Python + SQL stage-2 blocking)
  3. blocking_sql._RULE_BLOCK_KEY_SQL        (static SQL key expressions)
Also checks the functional key-derivation behaviour of the BlockingRule itself.
"""

from __future__ import annotations

from app.resolve.blocking import BlockingRule, default_blocking_rules
from app.resolve.blocking_sql import _RULE_BLOCK_KEY_SQL
from app.resolve.splink_config import organization as org_config
from app.resolve.standardize.staging import ResolutionInput

_RULE_NAME = "org_normalized_state"
_RULE_FIELDS = ("normalized_org", "state")

# ---------------------------------------------------------------------------
# (a) PREDICTION_BLOCKING_RULES has 2 rules and one blocks on the new pair
# ---------------------------------------------------------------------------


def test_prediction_blocking_rules_count():
    """organization.PREDICTION_BLOCKING_RULES must now contain exactly 2 rules."""
    assert len(org_config.PREDICTION_BLOCKING_RULES) == 2


def _splink_rule_column_names(rule) -> list[str]:
    """Extract column names from a Splink block_on() (And) object."""
    sub_rules = getattr(rule, "blocking_rules", [])
    return [getattr(br.col_expression, "raw_sql_expression", "") for br in sub_rules]


def test_prediction_blocking_rules_includes_normalized_org_state():
    """One rule must block on normalized_org + state (and NOT org_name_phonetic)."""
    found = any(
        "normalized_org" in _splink_rule_column_names(rule)
        and "state" in _splink_rule_column_names(rule)
        for rule in org_config.PREDICTION_BLOCKING_RULES
    )
    assert found, (
        "No rule references both normalized_org and state. Rules present: "
        + repr(org_config.PREDICTION_BLOCKING_RULES)
    )
    # The coarse first-token phonetic rule must NOT be present (it exploded pairs).
    for rule in org_config.PREDICTION_BLOCKING_RULES:
        assert "org_name_phonetic" not in _splink_rule_column_names(rule)


# ---------------------------------------------------------------------------
# (b) default_blocking_rules() includes the new rule with the correct fields
# ---------------------------------------------------------------------------


def test_default_blocking_rules_includes_org_normalized_state():
    rules = default_blocking_rules()
    names = [r.name for r in rules]
    assert _RULE_NAME in names, f"{_RULE_NAME!r} not in default_blocking_rules(): {names}"


def test_default_blocking_rules_org_normalized_state_fields():
    rules = default_blocking_rules()
    rule = next(r for r in rules if r.name == _RULE_NAME)
    assert rule.fields == _RULE_FIELDS, f"Expected {_RULE_FIELDS}, got {rule.fields!r}"


# ---------------------------------------------------------------------------
# (c) blocking_sql._RULE_BLOCK_KEY_SQL has an entry scoped to org/committee
# ---------------------------------------------------------------------------


def test_block_key_sql_has_org_normalized_state():
    assert _RULE_NAME in _RULE_BLOCK_KEY_SQL, (
        f"{_RULE_NAME!r} missing from _RULE_BLOCK_KEY_SQL: {list(_RULE_BLOCK_KEY_SQL)}"
    )


def test_block_key_sql_scoped_to_organization_and_committee():
    sql = _RULE_BLOCK_KEY_SQL[_RULE_NAME]
    assert "organization" in sql
    assert "committee" in sql


def test_block_key_sql_references_normalized_org_and_state():
    sql = _RULE_BLOCK_KEY_SQL[_RULE_NAME]
    assert "normalized_org" in sql
    assert "ri.state" in sql


# ---------------------------------------------------------------------------
# (d) Functional key_for() checks on BlockingRule
# ---------------------------------------------------------------------------


def _make_row(**kwargs) -> ResolutionInput:
    defaults = dict(
        run_id=1,
        source_type="unified_committee",
        source_id="C1",
        entity_type="committee",
        raw_name="Acme PAC",
        raw_address="123 Main St",
    )
    defaults.update(kwargs)
    return ResolutionInput(**defaults)


def test_key_for_returns_normalized_pipe_state_for_org_row():
    rule = BlockingRule(name=_RULE_NAME, fields=_RULE_FIELDS)
    row = _make_row(normalized_org="ACME PAC", state="TX")
    assert rule.key_for(row) == "acme pac|tx"


def test_key_for_returns_none_when_normalized_org_missing():
    rule = BlockingRule(name=_RULE_NAME, fields=_RULE_FIELDS)
    assert rule.key_for(_make_row(normalized_org=None, state="TX")) is None


def test_key_for_returns_none_when_state_missing():
    rule = BlockingRule(name=_RULE_NAME, fields=_RULE_FIELDS)
    assert rule.key_for(_make_row(normalized_org="ACME PAC", state=None)) is None


def test_key_for_returns_none_when_normalized_org_blank():
    rule = BlockingRule(name=_RULE_NAME, fields=_RULE_FIELDS)
    assert rule.key_for(_make_row(normalized_org="   ", state="TX")) is None


def test_key_for_returns_none_when_state_blank():
    rule = BlockingRule(name=_RULE_NAME, fields=_RULE_FIELDS)
    assert rule.key_for(_make_row(normalized_org="ACME PAC", state="  ")) is None


def test_key_for_normalizes_to_lowercase():
    rule = BlockingRule(name=_RULE_NAME, fields=_RULE_FIELDS)
    key = rule.key_for(_make_row(normalized_org="ACME PAC", state="TX"))
    assert key == key.lower()
