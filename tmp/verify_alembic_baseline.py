"""Gate: `alembic upgrade head` on a fresh DB must produce the SAME schema (tables + columns +
indexes) as the pre-Alembic bootstrap (production_loader._get_session = create_all + dedup indexes).
"""

from __future__ import annotations

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

import scripts.benchmarks.bench_ingest as B

BASE = "postgresql+psycopg2://localhost:5432"


def fingerprint(url: str) -> dict[str, tuple[frozenset, frozenset]]:
    eng = create_engine(url)
    insp = inspect(eng)
    out: dict[str, tuple[frozenset, frozenset]] = {}
    for t in insp.get_table_names():
        cols = frozenset(c["name"] for c in insp.get_columns(t))
        idx = frozenset(i["name"] for i in insp.get_indexes(t))
        out[t] = (cols, idx)
    eng.dispose()
    return out


with B._fresh_database(BASE, "cf_alembic_a") as url_a:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", url_a)
    command.upgrade(cfg, "head")
    fa = fingerprint(url_a)

with B._fresh_database(BASE, "cf_alembic_b") as url_b:
    from scripts.loaders.production_loader import _get_session

    _get_session(url_b).close()
    fb = fingerprint(url_b)

print(f"tables: alembic={len(fa)}  bootstrap={len(fb)}")
only_a, only_b = set(fa) - set(fb), set(fb) - set(fa)
print(f"tables only-in-alembic: {sorted(only_a)}")
print(f"tables only-in-bootstrap: {sorted(only_b)}")
mism = {t: (fa[t], fb[t]) for t in set(fa) & set(fb) if fa[t] != fb[t]}
print(f"col/index mismatches: {len(mism)}")
for t, (a, b) in list(mism.items())[:15]:
    print(f"  {t}: cols +alembic={set(a[0] - b[0])} +boot={set(b[0] - a[0])} "
          f"idx +alembic={set(a[1] - b[1])} +boot={set(b[1] - a[1])}")
print("\nRESULT:", "MATCH ✓" if not only_a and not only_b and not mism else "DIVERGES ✗")
