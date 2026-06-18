"""Diagnostic: why do ORM vs vec unified_committee_persons diverge on real data?

Snapshots both bench DBs with resolve_fks=True (the harness's authoritative comparison),
aligns committee_persons rows on (committee_id, role), and reports which resolved field
differs — to decide if it's the officer-address gap or something else.
"""

from __future__ import annotations

from collections import Counter, defaultdict

from sqlalchemy import create_engine

from app.core.ingest_equivalence import snapshot_unified

ORM = "postgresql+psycopg2://localhost:5432/bench_orm"
VEC = "postgresql+psycopg2://localhost:5432/bench_vec_copy"

orm = snapshot_unified(create_engine(ORM), resolve_fks=True)["unified_committee_persons"]
vec = snapshot_unified(create_engine(VEC), resolve_fks=True)["unified_committee_persons"]

print(f"ORM rows: {len(orm)}   VEC rows: {len(vec)}")
print(f"sample resolved ORM row keys: {sorted(orm[0].keys()) if orm else '(none)'}")
print()
print("--- one sample ORM committee_person (resolved) ---")
if orm:
    for k, v in sorted(orm[0].items()):
        print(f"   {k} = {v!r}")
print()

# Multiset diff on the full resolved row.
ck = lambda r: tuple(sorted((k, None if v is None else str(v)) for k, v in r.items()))
co, cv = Counter(ck(r) for r in orm), Counter(ck(r) for r in vec)
only_orm = co - cv
only_vec = cv - co
print(f"left-only (ORM): {sum(only_orm.values())}   right-only (VEC): {sum(only_vec.values())}")
print()

# Align on (committee_id, role) to find which field flips.
def idx(rows):
    d = defaultdict(list)
    for r in rows:
        d[(r.get("committee_id"), r.get("role"))].append(r)
    return d

io, iv = idx(orm), idx(vec)
all_keys = set(io) | set(iv)
print(f"distinct (committee_id, role) groups: ORM={len(io)} VEC={len(iv)} union={len(all_keys)}")

field_flips = Counter()
orm_only_groups = 0
vec_only_groups = 0
matched_groups = 0
examples = []
for key in all_keys:
    o, v = io.get(key, []), iv.get(key, [])
    if o and not v:
        orm_only_groups += 1
        continue
    if v and not o:
        vec_only_groups += 1
        continue
    # both present — compare first row of each (usually 1 per group)
    or_, vr_ = o[0], v[0]
    diffs = [c for c in set(or_) | set(vr_) if or_.get(c) != vr_.get(c)]
    if diffs:
        for c in diffs:
            field_flips[c] += 1
        if len(examples) < 8:
            examples.append((key, {c: (or_.get(c), vr_.get(c)) for c in diffs}))
    else:
        matched_groups += 1

print(f"(committee_id,role) groups: matched={matched_groups} "
      f"ORM-only={orm_only_groups} VEC-only={vec_only_groups} "
      f"differing-field={sum(1 for _ in examples) if False else len(all_keys)-matched_groups-orm_only_groups-vec_only_groups}")
print()
print("field-flip counts among matched (committee,role) groups that differ:")
for c, n in field_flips.most_common():
    print(f"   {c}: {n}")
print()
print("--- example differing groups (committee_id, role) -> {field: (ORM, VEC)} ---")
for key, d in examples:
    print(f"   {key}: {d}")
