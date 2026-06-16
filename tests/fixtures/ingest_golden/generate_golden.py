"""Generate the FK-coherent golden ingest fixtures from tmp/texas.

Committed output (`*_golden.parquet`) is what the equivalence harness loads; this
generator documents how it was produced and is re-runnable only where the raw TEC
data exists under tmp/texas/.

Strategy: derive a committee set from the first contributions, then keep only rows
referencing those committees so transaction->committee->report FKs resolve, and keep
only CAND rows whose expenditure is present so the enrichment links.  Filenames keep
the TEC prefixes so file_discovery routes them to the right record type.

Run:  uv run python tests/fixtures/ingest_golden/generate_golden.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "tmp" / "texas"
OUT = Path(__file__).resolve().parent

N_CONTRIB = 400  # seed contributions -> defines the committee set
CAP_TXN = 250  # per transaction file
CAP_REF = 600  # per reference/lookup file


def _read(name: str) -> pl.DataFrame | None:
    # Real TEC files carry a date suffix (e.g. contribs_01_20260524.parquet); match
    # the exact stem or the prefix form, preferring the shortest match.
    matches = sorted(SRC.glob(f"{name}.parquet")) + sorted(SRC.glob(f"{name}_*.parquet"))
    return pl.read_parquet(matches[0]) if matches else None


def _by_filer(df: pl.DataFrame | None, filers: set[str], cap: int) -> pl.DataFrame | None:
    if df is None or "filerIdent" not in df.columns:
        return df.head(cap) if df is not None else None
    sel = df.filter(pl.col("filerIdent").is_in(list(filers))).head(cap)
    return sel if sel.height else df.head(min(cap, 50))  # fallback: keep some rows


def main() -> None:
    if not SRC.is_dir():
        print(f"ERROR: {SRC} not found (raw TEC data required to regenerate)")
        sys.exit(1)

    contribs_all = _read("contribs_01")
    # contribs files are filer-sorted, so derive a DIVERSE committee set from a wide
    # scan, then keep this slice's transactions scoped to those committees.
    N_COMMITTEES = 25
    filer_set = set(
        contribs_all["filerIdent"].drop_nulls().head(50_000).unique().to_list()[:N_COMMITTEES]
    )
    contribs = contribs_all.filter(pl.col("filerIdent").is_in(list(filer_set))).head(N_CONTRIB)
    print(f"seed committees: {len(filer_set)}; seed contributions: {contribs.height}")

    written: list[tuple[str, int]] = []

    def write(name: str, df: pl.DataFrame | None) -> None:
        if df is None or df.height == 0:
            print(f"  skip {name} (no rows)")
            return
        df.write_parquet(OUT / f"{name}_golden.parquet")
        written.append((name, df.height))

    # Reference / lookup (load fuller so FKs resolve).
    write("filers", _by_filer(_read("filers"), filer_set, CAP_REF))
    write("cover", _by_filer(_read("cover"), filer_set, CAP_REF))
    write("finals", _by_filer(_read("finals"), filer_set, CAP_REF))
    write("spacs", _by_filer(_read("spacs"), filer_set, CAP_REF))
    write("notices", _by_filer(_read("notices"), filer_set, CAP_REF))
    write("purpose", _by_filer(_read("purpose"), filer_set, CAP_REF))
    excat = _read("expn_catg")
    write("expn_catg", excat.head(CAP_REF) if excat is not None else None)

    # Transactions (committee-scoped).
    write("contribs", contribs)
    expend = _by_filer(_read("expend_01"), filer_set, CAP_TXN)
    write("expend", expend)
    write("loans", _by_filer(_read("loans"), filer_set, CAP_TXN))
    write("debts", _by_filer(_read("debts"), filer_set, CAP_TXN))
    write("credits", _by_filer(_read("credits"), filer_set, CAP_TXN))
    write("travel", _by_filer(_read("travel"), filer_set, CAP_TXN))
    write("assets", _by_filer(_read("assets"), filer_set, CAP_TXN))
    write("pledges", _by_filer(_read("pledges"), filer_set, CAP_TXN))

    # CAND: keep rows whose expenditure is in the slice (so enrichment links),
    # plus a few that aren't (covers the unlinked path).
    cand = _read("cand")
    if cand is not None and expend is not None and "expendInfoId" in expend.columns:
        x = set(expend["expendInfoId"].drop_nulls().to_list())
        linked = cand.filter(pl.col("expendInfoId").is_in(list(x))).head(CAP_TXN)
        unlinked = cand.filter(~pl.col("expendInfoId").is_in(list(x))).head(25)
        write("cand", pl.concat([linked, unlinked]) if linked.height else cand.head(50))

    print("written fixtures:")
    for name, n in written:
        print(f"  {name}_golden.parquet: {n:,} rows")


if __name__ == "__main__":
    main()
