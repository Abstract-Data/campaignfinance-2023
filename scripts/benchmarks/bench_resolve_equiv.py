"""Resolve-equivalence gate: does running RESOLVE on the vectorized ingest produce canonical
entities equivalent to running it on the ORM ingest?

Ingest row-for-row parity is NOT the gate (the ORM has genuine defects — duplicate
transaction_persons, flush-order campaign subset, cross-file address merge — and the Splink
resolver re-merges genuine person dups anyway). What matters for the product is the CANONICAL
output. This script compares the two canonical partitions by NATURAL identity (surrogate ids
differ between independent loads):

  * committee member  -> ("C", filer_id)
  * entity member     -> ("E", entity_type, normalized_name)
  * person member     -> ("P", lower(first)|lower(last)|lower(org))   [vec splits collapse here]

Each canonical entity becomes a frozenset of member natural keys. Restricted to the universe of
natural keys present in BOTH loads, the two clusterings are compared: identical clusters, and
clusters that one side splits or merges differently. A high identical-cluster ratio = the vec
ingest resolves to the same real-world entities as the ORM ingest (the go/no-go that matters).

Assumes resolve has ALREADY been run (entity pass) on both DBs (writes canonical_entity +
entity_crosswalk). See tmp/resolve_one.py for the runner. Usage:
    uv run python -m scripts.benchmarks.bench_resolve_equiv --orm bench_orm --vec bench_vec_copy
"""

from __future__ import annotations

import argparse
from collections import Counter

from sqlalchemy import create_engine, text

PG = "postgresql+psycopg2://localhost:5432"

# Natural-key maps. source_id in entity_crosswalk is unified_*.id (str) — or filer_id for
# committees. We map it to a stable cross-load natural key.
_Q_ENTITY = "SELECT id, entity_type, normalized_name FROM unified_entities"
_Q_PERSON = "SELECT id, first_name, last_name, organization FROM unified_persons"
_Q_XWALK = "SELECT source_type, source_id, canonical_entity_id FROM entity_crosswalk"


def _low(v) -> str:
    return "" if v is None else str(v).strip().lower()


def natural_maps(db: str) -> tuple[dict, dict]:
    """Return ({natural_key: canonical_id}, {canonical_id: frozenset(natural_keys)}) for *db*."""
    eng = create_engine(f"{PG}/{db}")
    with eng.connect() as c:
        ent = {str(r[0]): ("E", _low(getattr(r[1], "name", r[1])), _low(r[2]))
               for r in c.execute(text(_Q_ENTITY))}
        per = {str(r[0]): ("P", f"{_low(r[1])}|{_low(r[2])}|{_low(r[3])}")
               for r in c.execute(text(_Q_PERSON))}
        xwalk = c.execute(text(_Q_XWALK)).fetchall()
    eng.dispose()

    key_to_canon: dict = {}
    canon_to_keys: dict = {}
    for source_type, source_id, canon_id in xwalk:
        st = str(source_type)
        if st.endswith("unified_committee") or st == "unified_committee":
            nk = ("C", _low(source_id))
        elif st.endswith("unified_entity") or st == "unified_entity":
            nk = ent.get(str(source_id))
        elif st.endswith("unified_person") or st == "unified_person":
            nk = per.get(str(source_id))
        else:
            nk = None
        if nk is None:
            continue
        key_to_canon[nk] = canon_id
        canon_to_keys.setdefault(canon_id, set()).add(nk)
    return key_to_canon, canon_to_keys


def clusters_over(shared: set, canon_to_keys: dict) -> set[frozenset]:
    """Set of clusters (frozensets of natural keys) restricted to the shared universe."""
    out = set()
    for keys in canon_to_keys.values():
        r = frozenset(keys & shared)
        if r:
            out.add(r)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--orm", default="bench_orm")
    ap.add_argument("--vec", default="bench_vec_copy")
    ap.add_argument("--examples", type=int, default=12)
    args = ap.parse_args()

    print(f"== resolve-equivalence: {args.orm} (ORM) vs {args.vec} (vec) ==\n")
    o_k2c, o_c2k = natural_maps(args.orm)
    v_k2c, v_c2k = natural_maps(args.vec)

    print(f"canonical entities:  ORM={len(o_c2k):,}  vec={len(v_c2k):,}")
    print(f"member natural keys: ORM={len(o_k2c):,}  vec={len(v_k2c):,}")

    # Coverage: which natural keys exist in each / both.
    o_keys, v_keys = set(o_k2c), set(v_k2c)
    shared = o_keys & v_keys
    print(f"\nnatural-key universe:  shared={len(shared):,}  "
          f"ORM-only={len(o_keys - v_keys):,}  vec-only={len(v_keys - o_keys):,}")
    for tag, ks in (("shared", shared), ("ORM-only", o_keys - v_keys), ("vec-only", v_keys - o_keys)):
        by_type = Counter(k[0] for k in ks)
        print(f"  {tag:9} by type: {dict(by_type)}")

    # Partition agreement over the shared universe.
    o_cl = clusters_over(shared, o_c2k)
    v_cl = clusters_over(shared, v_c2k)
    identical = o_cl & v_cl
    print(f"\nclusters over shared universe:  ORM={len(o_cl):,}  vec={len(v_cl):,}  "
          f"identical={len(identical):,}")
    denom = max(len(o_cl), 1)
    print(f"  identical-cluster ratio (vs ORM): {len(identical) / denom:.1%}")

    # Pairwise disagreement: for each shared key, the set of OTHER shared keys it clusters with.
    def sig(k2c, c2k):
        return {k: frozenset((c2k[k2c[k]] & shared) - {k}) for k in shared}

    o_sig, v_sig = sig(o_k2c, o_c2k), sig(v_k2c, v_c2k)
    over_split = [k for k in shared if v_sig[k] < o_sig[k] and v_sig[k] != o_sig[k]]  # vec apart
    over_merge = [k for k in shared if v_sig[k] > o_sig[k] and v_sig[k] != o_sig[k]]  # vec together
    other = [k for k in shared if v_sig[k] != o_sig[k]
             and not (v_sig[k] < o_sig[k]) and not (v_sig[k] > o_sig[k])]
    agree = len(shared) - len(over_split) - len(over_merge) - len(other)
    print(f"\nper-key clustering agreement over shared universe ({len(shared):,} keys):")
    print(f"  agree:            {agree:,} ({agree / max(len(shared),1):.1%})")
    print(f"  vec over-SPLIT:   {len(over_split):,}  (ORM groups them, vec separates)")
    print(f"  vec over-MERGE:   {len(over_merge):,}  (vec groups them, ORM separates)")
    print(f"  cross-different:  {len(other):,}  (neither subset)")

    print("\n--- example vec over-MERGE (vec groups keys ORM keeps apart) ---")
    for k in over_merge[: args.examples]:
        extra = sorted(v_sig[k] - o_sig[k])[:4]
        print(f"  {k}  +{len(v_sig[k] - o_sig[k])} extra e.g. {extra}")
    print("\n--- example vec over-SPLIT (ORM groups keys vec keeps apart) ---")
    for k in over_split[: args.examples]:
        missing = sorted(o_sig[k] - v_sig[k])[:4]
        print(f"  {k}  -{len(o_sig[k] - v_sig[k])} missing e.g. {missing}")


if __name__ == "__main__":
    main()
