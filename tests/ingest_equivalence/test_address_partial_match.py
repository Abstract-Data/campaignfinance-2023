"""Unit tests for the omit-null partial-address match (resolve_partial_address /
full_address_lookup) — the vectorized equivalent of builders._find_address_by_fields.

A no-street contributor address (city+state+zip) must resolve to an existing fuller address
sharing that (city, state, zip), inheriting its street — so the contributor person's
dedup_addr_key matches the ORM's and the resolver does not over-split.
"""

from __future__ import annotations

import polars as pl
from sqlalchemy import create_engine, text

from app.core.ingest_vectorized import common

_ADDR = list(common._ADDR_COLS)


def _frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={c: pl.Utf8 for c in _ADDR})


def _lookup(rows: list[dict]) -> pl.DataFrame:
    """Hand-build a lookup like full_address_lookup emits."""
    data = {
        "_lk_city": [r["city"].lower() for r in rows],
        "_lk_state": [r["state"].lower() for r in rows],
        "_lk_zip": [r["zip_code"] for r in rows],
        **{f"a_{c}": [r.get(c) for r in rows] for c in _ADDR},
    }
    return pl.DataFrame(data, schema={
        "_lk_city": pl.Utf8, "_lk_state": pl.Utf8, "_lk_zip": pl.Utf8,
        **{f"a_{c}": pl.Utf8 for c in _ADDR},
    })


def test_no_street_inherits_matched_full_address():
    df = _frame([{"street_1": None, "street_2": None, "city": "CONROE", "state": "TX",
                  "zip_code": "77304", "country": "USA", "county": None}])
    lk = _lookup([{"street_1": "3115 Wilson Rd.", "street_2": None, "city": "Conroe",
                   "state": "TX", "zip_code": "77304", "country": None, "county": "Montgomery"}])
    out = common.resolve_partial_address(df, lk).to_dicts()[0]
    assert out["street_1"] == "3115 Wilson Rd."        # inherited street
    assert out["city"] == "Conroe"                      # inherits matched casing
    assert out["county"] == "Montgomery"                # inherits matched county
    assert set(out) == set(_ADDR)                        # temp/a_ cols dropped


def test_row_with_own_street_is_unchanged():
    df = _frame([{"street_1": "999 Payee St.", "street_2": None, "city": "CONROE",
                  "state": "TX", "zip_code": "77304", "country": None, "county": None}])
    lk = _lookup([{"street_1": "3115 Wilson Rd.", "street_2": None, "city": "Conroe",
                   "state": "TX", "zip_code": "77304", "country": None, "county": None}])
    out = common.resolve_partial_address(df, lk).to_dicts()[0]
    assert out["street_1"] == "999 Payee St."            # keeps its own street


def test_no_match_leaves_partial_unchanged():
    df = _frame([{"street_1": None, "street_2": None, "city": "AUSTIN", "state": "TX",
                  "zip_code": "78701", "country": None, "county": None}])
    lk = _lookup([{"street_1": "3115 Wilson Rd.", "street_2": None, "city": "Conroe",
                   "state": "TX", "zip_code": "77304", "country": None, "county": None}])
    out = common.resolve_partial_address(df, lk).to_dicts()[0]
    assert out["street_1"] is None                       # no Austin match -> unchanged


def test_match_winner_without_street_does_not_enrich():
    # If the first-created (lookup) address for the city/state/zip is itself street-less,
    # nothing is inherited (mirrors the ORM .first() returning a no-street row).
    df = _frame([{"street_1": None, "street_2": None, "city": "CONROE", "state": "TX",
                  "zip_code": "77304", "country": None, "county": None}])
    lk = _lookup([{"street_1": None, "street_2": None, "city": "Conroe", "state": "TX",
                   "zip_code": "77304", "country": None, "county": None}])
    out = common.resolve_partial_address(df, lk).to_dicts()[0]
    assert out["street_1"] is None


def test_full_address_lookup_lowest_id_wins():
    eng = create_engine("sqlite://")
    with eng.begin() as c:
        c.execute(text(
            "CREATE TABLE unified_addresses (id INTEGER PRIMARY KEY, street_1 TEXT, "
            "street_2 TEXT, city TEXT, state TEXT, zip_code TEXT, country TEXT, county TEXT)"
        ))
        # id 1 = FILER full address (created first); id 2 = a later no-street row, same c/s/z.
        c.execute(text(
            "INSERT INTO unified_addresses VALUES "
            "(1,'3115 Wilson Rd.',NULL,'Conroe','TX','77304',NULL,NULL),"
            "(2,NULL,NULL,'Conroe','TX','77304',NULL,NULL),"
            "(3,NULL,NULL,NULL,'TX','78701',NULL,NULL)"  # missing city -> excluded
        ))
    lk = common.full_address_lookup(eng)
    assert lk.height == 1                                 # only the complete c/s/z key
    row = lk.to_dicts()[0]
    assert (row["_lk_city"], row["_lk_state"], row["_lk_zip"]) == ("conroe", "tx", "77304")
    assert row["a_street_1"] == "3115 Wilson Rd."         # lowest id (1) won
