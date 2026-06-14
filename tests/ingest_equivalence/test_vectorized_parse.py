"""Parity proof for the vectorized parse expressions vs the ORM parse helpers.

These are the hardest primitives of the vectorized engine — if a Polars expression
diverges from the ORM helper on any input, the row-for-row equivalence gate fails.
We assert exact equality across a battery of inputs (incl. TEC edge cases).
"""

from __future__ import annotations

from decimal import Decimal

import polars as pl

from app.core.builders import UnifiedSQLModelBuilder
from app.core.ingest_vectorized import common
from app.core.source_models import reports_ingest as ri

# Shared ORM builder for the builders.py-dialect helpers.
_B = UnifiedSQLModelBuilder("texas", 1, "TX")

AMOUNT_INPUTS = [
    None,
    "",
    "  ",
    "0",
    "90",
    "90.00",
    "1000.00",
    "1,000.00",
    "$1,000.00",
    "$1,234,567.89",
    "-50.5",
    "abc",
    "Prime Rate",
    "NONE",
    ".",
    "-",
    "12.5%",
]
# Clean, well-defined dates for the tec_* dialect (its ORM helper RAISES on
# malformed 8-digit input like "20001305", so such values can't be parity-tested
# here — they reject the row at ingest, an edge absent from clean TEC data).
TEC_DATE_INPUTS = [
    None,
    "",
    "  ",
    "20000705",
    "20120229",
    "2000-07-05",
    "07/05/2000",
    "2000/07/05",
    "07-05-2000",
    "garbage",
]
# builders._parse_date is total (returns None on bad input) so it can take more.
DATE_INPUTS = TEC_DATE_INPUTS + ["00041110", "20001305"]
BOOL_INPUTS = [None, "", "Y", "y", "N", "true", "TRUE", "false", "1", "0", "t", "x"]


def _apply(expr_fn, inputs) -> list:
    df = pl.DataFrame({"v": [None if x is None else str(x) for x in inputs]}, schema={"v": pl.Utf8})
    return df.select(expr_fn("v").alias("out"))["out"].to_list()


def _eq_amount(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return Decimal(str(a)) == Decimal(str(b))


def test_tec_amount_matches_reports_ingest():
    got = _apply(common.tec_amount, AMOUNT_INPUTS)
    want = [ri._parse_amount(x) for x in AMOUNT_INPUTS]
    for inp, g, w in zip(AMOUNT_INPUTS, got, want):
        assert _eq_amount(g, w), f"tec_amount({inp!r}): got {g!r} want {w!r}"


def test_tec_date_matches_reports_ingest():
    got = _apply(common.tec_date, TEC_DATE_INPUTS)
    want = [ri._parse_date(x) for x in TEC_DATE_INPUTS]
    for inp, g, w in zip(TEC_DATE_INPUTS, got, want):
        assert g == w, f"tec_date({inp!r}): got {g!r} want {w!r}"


def test_builder_amount_matches_builders():
    got = _apply(common.builder_amount, AMOUNT_INPUTS)
    want = [_B._parse_amount(x) for x in AMOUNT_INPUTS]
    for inp, g, w in zip(AMOUNT_INPUTS, got, want):
        assert _eq_amount(g, w), f"builder_amount({inp!r}): got {g!r} want {w!r}"


def test_builder_date_matches_builders():
    got = _apply(common.builder_date, DATE_INPUTS)
    want = [_B._parse_date(x) for x in DATE_INPUTS]
    for inp, g, w in zip(DATE_INPUTS, got, want):
        assert g == w, f"builder_date({inp!r}): got {g!r} want {w!r}"


def test_bool_expr_matches_builders():
    got = _apply(common.bool_expr, BOOL_INPUTS)
    want = [_B._parse_boolean(x) for x in BOOL_INPUTS]
    for inp, g, w in zip(BOOL_INPUTS, got, want):
        assert bool(g) == bool(w), f"bool_expr({inp!r}): got {g!r} want {w!r}"


def test_clean_str_strips_and_nulls_empty():
    got = _apply(common.clean_str, [None, "", "  ", " x ", "abc"])
    assert got == [None, None, None, "x", "abc"]
