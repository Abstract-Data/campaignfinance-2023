"""Re-add FK constraints NOT VALID to the bench DBs (so the equivalence harness can
resolve surrogate FKs to natural keys again), then re-run the ORM-vs-vec parity diff.

The benchmark drops FK constraints to load a capped (referentially incomplete) slice;
that also blinds snapshot_unified(resolve_fks=True), which reflects FKs from the live DB.
Re-adding them NOT VALID restores the metadata WITHOUT validating dangling rows.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text

from app.core.ingest_equivalence import diff_snapshots, snapshot_unified

PG = "postgresql+psycopg2://localhost:5432"
ORM = f"{PG}/bench_orm"
VEC = f"{PG}/bench_vec_copy"
FRESH = f"{PG}/bench_fkdefs_tmp"


def bootstrap_fresh() -> list[tuple[str, str]]:
    """Create a fresh bootstrapped DB (has all FKs) and return [(table, constraintdef)]."""
    from scripts.loaders.production_loader import _get_session

    # fresh db
    admin = create_engine(f"{PG}/postgres")
    raw = admin.raw_connection()
    pg = raw.driver_connection
    pg.autocommit = True
    cur = pg.cursor()
    cur.execute("DROP DATABASE IF EXISTS bench_fkdefs_tmp WITH (FORCE)")
    cur.execute("CREATE DATABASE bench_fkdefs_tmp")
    cur.close()
    raw.close()
    admin.dispose()

    s = _get_session(FRESH)
    s.close()
    eng = create_engine(FRESH)
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT conrelid::regclass::text AS tbl, conname, pg_get_constraintdef(oid) AS def "
            "FROM pg_constraint WHERE contype='f' AND connamespace='public'::regnamespace"
        )).fetchall()
    eng.dispose()
    return [(r.tbl, r.conname, r.def_ if hasattr(r, "def_") else r[2]) for r in rows]


def readd(url: str, fkdefs) -> None:
    eng = create_engine(url)
    with eng.connect() as conn:
        for tbl, conname, cdef in fkdefs:
            conn.execute(text(
                f'ALTER TABLE {tbl} ADD CONSTRAINT "{conname}" {cdef} NOT VALID'
            ))
        conn.commit()
    eng.dispose()


fkdefs = bootstrap_fresh()
print(f"collected {len(fkdefs)} FK constraint defs from fresh schema")
for url in (ORM, VEC):
    try:
        readd(url, fkdefs)
        print(f"re-added FKs NOT VALID to {url.rsplit('/',1)[-1]}")
    except Exception as e:  # noqa: BLE001
        print(f"  (some already present?) {url.rsplit('/',1)[-1]}: {type(e).__name__}: {str(e)[:120]}")

print("\n== TRUE parity (FKs visible, resolve_fks=True) ==")
a = snapshot_unified(create_engine(ORM), resolve_fks=True)
b = snapshot_unified(create_engine(VEC), resolve_fks=True)
diff = diff_snapshots(a, b)
if not diff:
    print("EQUAL ✓")
else:
    print(f"{len(diff)} diff line(s):")
    for line in diff:
        print("  " + line)
