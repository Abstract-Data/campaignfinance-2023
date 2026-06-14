"""Finding #4 fix: the default entity pass now ends with an address-link stage so
canonical_entity.canonical_address_id (and the address_occupancy view) is populated
by a single `run --pass-type entity`, instead of requiring a separate address pass.
"""
from __future__ import annotations

from app.resolve.cli import (
    _get_run_stages,
    _run_address_link_stage,
    _run_address_stage,
)


def test_entity_pass_appends_address_link_stage():
    stages = _get_run_stages("entity")
    assert stages[-1] is _run_address_link_stage, (
        "entity pass must end with the address-link stage so occupancy is populated"
    )
    # 7 resolution stages + 1 address-link tail.
    assert len(stages) == 8


def test_address_link_stage_does_not_emit_canonical_out():
    """The chained tail must NOT return `canonical_out` (a _COUNTER_COLS key), or it
    would overwrite survivorship's entity count in match_run via the counter merge."""
    import inspect as _inspect

    # Inspect the return statement specifically (the docstring legitimately
    # mentions canonical_out when explaining why it is avoided).
    return_lines = [
        ln.strip()
        for ln in _inspect.getsource(_run_address_link_stage).splitlines()
        if ln.strip().startswith("return")
    ]
    assert return_lines, "stage must return a counter dict"
    assert all("canonical_out" not in ln for ln in return_lines)
    assert any("addresses_out" in ln for ln in return_lines)


def test_standalone_address_pass_unchanged():
    """`--pass-type address` still runs exactly the deterministic address stage."""
    assert _get_run_stages("address") == [_run_address_stage]
