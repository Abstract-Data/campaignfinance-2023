"""Categorize the residual vec over-splits: genuine Splink residual (no inheritable full
address) vs a fixable enrichment miss, and name-variant vs same-identity."""

from __future__ import annotations

from collections import Counter

from sqlalchemy import create_engine, text

from scripts.benchmarks.bench_resolve_equiv import natural_maps

PG = "postgresql+psycopg2://localhost:5432"

o_k2c, o_c2k = natural_maps("bench_orm")
v_k2c, v_c2k = natural_maps("bench_vec_copy")
shared = set(o_k2c) & set(v_k2c)


def sig(k2c, c2k):
    return {k: frozenset((c2k[k2c[k]] & shared) - {k}) for k in shared}


o_sig, v_sig = sig(o_k2c, o_c2k), sig(v_k2c, v_c2k)
osplit = [k for k in shared if v_sig[k] < o_sig[k] and v_sig[k] != o_sig[k]]
print(f"over-splits: {len(osplit)}  by type: {dict(Counter(k[0] for k in osplit))}")

# For person over-splits, look up the vec person's dedup_addr_key + whether a full address
# exists for their (city,state,zip). Build helper maps from vec DB.
eng = create_engine(f"{PG}/bench_vec_copy")
with eng.connect() as c:
    persons = c.execute(text(
        "SELECT lower(first_name)||'|'||lower(last_name)||'|'||coalesce(lower(organization),'') k, "
        "dedup_addr_key, address_id FROM unified_persons WHERE organization IS NULL"
    )).fetchall()
    # set of (lower city, lower state, zip) that HAVE a street-bearing address
    full = c.execute(text(
        "SELECT DISTINCT lower(city)||'|'||lower(state)||'|'||coalesce(zip_code,'') FROM "
        "unified_addresses WHERE street_1 IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL "
        "AND zip_code IS NOT NULL"
    )).fetchall()
eng.dispose()
full_csz = {r[0] for r in full}
# person key 'fn|ln|' -> list of dedup_addr_key
from collections import defaultdict  # noqa: E402
pk_to_keys = defaultdict(list)
for k, dak, aid in persons:
    pk_to_keys[k].append(dak)

cats = Counter()
for key in osplit:
    if key[0] != "P":
        cats["entity (E) over-split"] += 1
        continue
    name = key[1]  # 'fn|ln|org'
    daks = pk_to_keys.get(name, [])
    # does this person have a street-bearing dedup key already?
    has_street_key = any(d and not d.startswith("|") for d in daks)
    # does a full address exist for any of their no-street (city,state,zip)?
    nostreet_csz = {d[1:] for d in daks if d and d.startswith("|")}  # 'city|state|zip'
    inheritable = any(csz in full_csz for csz in nostreet_csz)
    if has_street_key:
        cats["P: already street-keyed (Splink residual)"] += 1
    elif inheritable:
        cats["P: no-street BUT full addr exists (enrichment MISS — fixable?)"] += 1
    elif nostreet_csz:
        cats["P: no-street, no full addr to inherit (genuine residual)"] += 1
    else:
        cats["P: name-only / no address (genuine residual)"] += 1

print("\n=== residual over-split categories ===")
for c, n in cats.most_common():
    print(f"  {n:>5}  {c}")
