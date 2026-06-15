"""Parity proof for the dim-normalization expressions vs the ORM value objects.

These are the dedup keys (entity normalized_name, person full_name) every remaining
vectorized family relies on, so they must match the ORM exactly.
"""

from __future__ import annotations

import polars as pl

from app.core.ingest_vectorized import common
from app.core.value_objects import PersonName, normalize_entity_name

ENTITY_NAMES = [
    None,
    "",
    "  ",
    "Acme, Inc.",
    "ACME   INC",
    "O'Brien & Sons",
    "José  García",
    "Smith-Jones LLC",
    "  Multiple   Spaces  ",
    "123 Main!!!",
    "...",
    "a",
]

NAME_TUPLES = [
    # (first, middle, last, suffix, organization)
    (None, None, None, None, None),
    ("John", None, "Doe", None, None),
    ("John", "Q", "Public", "Jr", None),
    (None, None, None, None, "Acme PAC"),
    ("ignored", None, "ignored", None, "Org Wins"),  # org takes precedence
    ("  spaced  ", None, "  name  ", None, None),
    (None, None, "Last", None, None),
    ("", "", "", "", ""),  # all blank -> ""
]


def test_normalize_entity_name_matches_value_objects():
    df = pl.DataFrame({"v": ENTITY_NAMES}, schema={"v": pl.Utf8})
    got = df.select(common.normalize_entity_name("v").alias("o"))["o"].to_list()
    want = [normalize_entity_name(x) for x in ENTITY_NAMES]
    for inp, g, w in zip(ENTITY_NAMES, got, want):
        assert g == w, f"normalize_entity_name({inp!r}): got {g!r} want {w!r}"


def test_full_name_expr_matches_person_name():
    cols = ["first", "middle", "last", "suffix", "org"]
    df = pl.DataFrame(
        {c: [t[i] for t in NAME_TUPLES] for i, c in enumerate(cols)},
        schema={c: pl.Utf8 for c in cols},
    )
    got = df.select(common.full_name_expr("first", "middle", "last", "suffix", "org").alias("o"))[
        "o"
    ].to_list()
    want = [PersonName(*t).full_name for t in NAME_TUPLES]
    for t, g, w in zip(NAME_TUPLES, got, want):
        assert g == w, f"full_name{t}: got {g!r} want {w!r}"


def test_upper_str_strips_and_uppers():
    got = (
        pl.DataFrame({"v": [None, "", " tx ", "Ca"]}, schema={"v": pl.Utf8})
        .select(common.upper_str("v").alias("o"))["o"]
        .to_list()
    )
    assert got == [None, None, "TX", "CA"]
