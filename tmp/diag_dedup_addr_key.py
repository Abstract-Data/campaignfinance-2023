"""Localize the dedup_addr_key divergence: is it key CONSTRUCTION or which-address-wins?

For individual persons (organization NULL), compare ORM vs vec on the stored
dedup_addr_key, grouped by name identity (first,last). Categorize and sample.
"""

from __future__ import annotations

from collections import Counter, defaultdict

from sqlalchemy import create_engine, text

PG = "postgresql+psycopg2://localhost:5432"


def load(db: str):
    eng = create_engine(f"{PG}/{db}")
    with eng.connect() as c:
        rows = c.execute(text(
            "SELECT p.first_name, p.last_name, p.dedup_addr_key, "
            "       a.street_1, a.city, a.state, a.zip_code "
            "FROM unified_persons p LEFT JOIN unified_addresses a ON p.address_id = a.id "
            "WHERE p.organization IS NULL AND p.first_name IS NOT NULL AND p.last_name IS NOT NULL"
        )).fetchall()
    eng.dispose()
    return rows


orm = load("bench_orm")
vec = load("bench_vec_copy")
print(f"ORM individuals: {len(orm)}   VEC individuals: {len(vec)}")


def by_name(rows):
    d = defaultdict(list)
    for r in rows:
        d[(r.first_name.lower(), r.last_name.lower())].append(r)
    return d


io, iv = by_name(orm), by_name(vec)
names = set(io) | set(iv)

cats = Counter()
examples = defaultdict(list)
for nm in names:
    o, v = io.get(nm, []), iv.get(nm, [])
    okeys = {r.dedup_addr_key for r in o}
    vkeys = {r.dedup_addr_key for r in v}
    if okeys == vkeys:
        cats["key-sets-match"] += 1
        continue
    # categorize the mismatch
    o_has_null = None in okeys
    v_has_null = None in vkeys
    if okeys - {None} == vkeys - {None} and (o_has_null != v_has_null):
        cat = "null-vs-set (same non-null keys, one side adds a NULL key)"
    elif (okeys - {None}) and (vkeys - {None}) and (okeys - {None}) != (vkeys - {None}):
        cat = "non-null keys DIFFER (construction or which-address)"
    elif not (okeys - {None}) and (vkeys - {None}):
        cat = "ORM all-null, VEC has key"
    elif (okeys - {None}) and not (vkeys - {None}):
        cat = "VEC all-null, ORM has key"
    else:
        cat = "other"
    cats[cat] += 1
    if len(examples[cat]) < 5:
        examples[cat].append((nm, sorted(map(str, okeys)), sorted(map(str, vkeys)),
                              [(r.street_1, r.city, r.state, r.zip_code) for r in o][:2],
                              [(r.street_1, r.city, r.state, r.zip_code) for r in v][:2]))

print("\n=== categories (by name identity) ===")
for c, n in cats.most_common():
    print(f"  {n:>6}  {c}")

print("\n=== examples ===")
for c, exs in examples.items():
    print(f"\n--- {c} ---")
    for nm, ok, vk, oa, va in exs:
        print(f"  name={nm}")
        print(f"    ORM keys={ok}  addrs={oa}")
        print(f"    VEC keys={vk}  addrs={va}")
