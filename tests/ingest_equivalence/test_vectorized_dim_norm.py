"""Parity proof for the dim-normalization expressions vs the ORM value objects.

These are the dedup keys (entity normalized_name, person full_name) every remaining
vectorized family relies on, so they must match the ORM exactly.
"""

from __future__ import annotations

import polars as pl

from app.core.ingest_vectorized import common
from app.core.load_cache import BuilderCache
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


# (street_1, city, state, zip) — exercises the (name + address) dedup-key denormalizer.
# The address state is stored upper-cased by the ORM (AddressParts.normalized()), so the
# vec input mirrors that with upper_str before person_addr_key_expr lowercases it.
ADDR_TUPLES = [
    (None, None, None, None),  # 0 fields -> None
    ("Main St", None, None, None),  # 1 field  -> None (degrades to name-only)
    (None, "Austin", None, None),  # 1 field  -> None
    (None, "Austin", "TX", None),  # 2 fields -> key
    (None, "Austin", "tx", "78701"),  # RCPT-style (no street), mixed case
    ("PO Box 619", "Evans", "GA", "30809"),
    ("MAIN ST", "AUSTIN", "tx", "78701"),  # mixed case lowers consistently
    ("", "  ", "  ", ""),  # blanks -> all null -> None
    (None, None, "TX", "78701"),  # gap in the middle keeps separators
]


def test_person_addr_key_expr_matches_builder_cache():
    """The vec denormalizer must produce the SAME dedup_addr_key string the ORM stores,
    or the harness's row-for-row person comparison (and the PG unique index) diverges.

    The ORM feeds ``address_key_str`` the output of
    ``AddressParts.normalized()`` (blanks already stripped to None, state uppercased);
    the vec ``person_addr_key_expr`` cleans/uppercases inline. We mirror that
    normalization on the ORM side so the two see the same inputs.
    """

    def _norm(v):
        return v.strip() or None if isinstance(v, str) else v

    cols = ["s1", "city", "state", "zip"]
    df = pl.DataFrame(
        {c: [t[i] for t in ADDR_TUPLES] for i, c in enumerate(cols)},
        schema={c: pl.Utf8 for c in cols},
    )
    got = df.select(
        common.person_addr_key_expr("s1", "city", common.upper_str("state"), "zip").alias("o")
    )["o"].to_list()
    want = []
    for t in ADDR_TUPLES:
        state = _norm(t[2])
        want.append(
            BuilderCache.address_key_str(
                {
                    "street_1": _norm(t[0]),
                    "city": _norm(t[1]),
                    "state": state.upper() if state else state,
                    "zip_code": _norm(t[3]),
                }
            )
        )
    for t, g, w in zip(ADDR_TUPLES, got, want):
        assert g == w, f"person_addr_key_expr{t}: got {g!r} want {w!r}"
