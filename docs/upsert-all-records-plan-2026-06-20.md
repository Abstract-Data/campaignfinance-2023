# Plan — Make Every Record Upsert (first-write-wins) — 2026-06-20

**Goal:** every ingest write becomes idempotent. Re-loading the same source adds **zero**
rows and updates **zero** rows (first-write-wins / `DO NOTHING`). This caps the database at
one copy of the data and removes the append-driven bloat.

**Chosen semantics (confirmed):** skip on conflict — existing rows are never touched; only
genuinely new natural keys are inserted.

> GitNexus note: the GitNexus MCP isn't connected in this session, so the impact analysis
> below is derived by reading the call graph directly. Before applying, run
> `gitnexus_impact({target: "write_frame", direction: "upstream"})` and
> `gitnexus_impact({target: "_write_frame_postgres", direction: "upstream"})` — these are
> the hot-path symbols every family writer funnels through (expect HIGH blast radius).

---

## Why we can't just set `conflict_cols=[...]` everywhere

The dedup unique indexes (`app/core/unified_database.py:186` `_DEDUP_INDEXES`) are **partial**
(have a `WHERE`) and several are **functional** (`lower(...)`). Postgres `ON CONFLICT`
column inference only matches a *full, non-expression* unique index unless you also supply
the **expressions** and the **`WHERE` predicate**. The current writer emits a bare
`ON CONFLICT ({cols})`, so pointing it at these indexes raises *"no unique or exclusion
constraint matching the ON CONFLICT specification."* Two tables (persons, addresses) even
have **two** partial indexes each, so one statement can't cover a mixed batch.

So idempotency is delivered per-table by the cheapest mechanism its key allows. Three
mechanisms, one shared rule (first-write-wins).

---

## Key matrix — every write call site

Bucket A = full unique key → atomic `ON CONFLICT DO NOTHING` (writer already supports).
Bucket B = partial, **non-functional** key → `ON CONFLICT (cols) WHERE pred DO NOTHING`
(small writer extension, atomic, scales to the big tables).
Bucket C = partial **+ functional** / double-index key → **anti-join** pre-filter, then plain insert.
Bucket D = **no unique key today** → add a unique index (then it becomes A/B) + anti-join fallback.

| Table | Call sites (file:line) | Natural key | Bucket | Mechanism |
|---|---|---|---|---|
| `unified_transactions` | `flat_txns.py:306,321`; `detail_children/transactions.py:94` | `(state_id, transaction_type, transaction_id)` WHERE `transaction_id IS NOT NULL` | **B** | `ON CONFLICT (state_id,transaction_type,transaction_id) WHERE transaction_id IS NOT NULL DO NOTHING` |
| `unified_entities` | `filer.py:597`; `cand.py:545`; `detail_children/dims.py:345`; `flat_txns_dims.py:~937` | `(entity_type, normalized_name, state_id)` WHERE `state_id IS NOT NULL` | **B** | `ON CONFLICT (entity_type,normalized_name,state_id) WHERE state_id IS NOT NULL DO NOTHING` |
| `unified_contributions` | `flat_txns_detail.py:871` | `transaction_id` (full unique `_transaction_id_key`) | **A** | `conflict_cols=["transaction_id"], update_cols=[]` |
| `unified_expenditures` | `flat_txns_detail.py:900` | `transaction_id` (full) | **A** | same |
| `unified_loans/debts/credits/travel/assets/pledges` | `detail_children/builders.py:234,293,335,400,452,502` | `transaction_id` (full) | **A** | same |
| `unified_transaction_persons` | `flat_txns_detail.py:936`; `cand.py:585` | `(transaction_id, person_id, role)` (full `uix_txperson_txid_personid_role`) | **A** | `conflict_cols=["transaction_id","person_id","role"], update_cols=[]` |
| `unified_persons` | `filer.py:548`; `cand.py:497`; `detail_children/dims.py:274`; `flat_txns_dims.py:~911` | `lower(first),lower(last),state_id,dedup_addr_key` (WHERE org NULL…) **and** `lower(org),state_id` (WHERE org NOT NULL) | **C** | anti-join via `person_id_map` |
| `unified_addresses` | `filer.py:463`; `detail_children/dims.py:230`; `flat_txns_dims.py:~901` | `lower(street_1),lower(city),lower(state),zip` (WHERE street NOT NULL) **and** `lower(city),lower(state),zip` (WHERE street NULL) | **C** | anti-join via `address_id_map` |
| `unified_committees` | `filer.py:480`; `detail_children/dims.py:78`; `flat_txns_dims.py:961` | `filer_id` (full) | **A** | already done ✓ |
| `unified_reports` | `reports.py:145` | `report_ident` (full) | **A** | already done ✓ |
| `unified_campaigns` | `campaigns.py:214` | `(normalized_name, primary_committee_id, election_year)` (builder key; **no DB index**) | **D** | add `uix_campaigns_identity` → Bucket B/anti-join |
| `unified_campaign_entities` | `campaigns.py:233` | `(campaign_id, entity_id, role)` (**no index**) | **D** | add unique → Bucket A |
| `unified_committee_persons` | `filer.py:663` | `(committee_id, person_id, role)` (**no index**) | **D** | add unique → Bucket A |
| `loan_guarantors` | `detail_children/builders.py:612` | `(loan_id/debt_id, lower(last),lower(first),lower(org))` (**no index**) | **D** | anti-join (composite, NULL-tolerant) |

---

## Diff 1 — extend the writer to emit a conflict predicate (Bucket B)

Small, additive, backward-compatible (`conflict_where=None` = today's behavior).

`app/core/ingest_vectorized/common.py` — `write_frame`:

```diff
 def write_frame(
     session: Any,
     model: type,
     frame: pl.DataFrame,
     *,
     conflict_cols: list[str] | None,
     update_cols: list[str] | None = None,
+    conflict_where: str | None = None,
     error_ctx: dict[str, Any] | None = None,
 ) -> int:
```

Thread `conflict_where` down into `_attempt_write` and `_write_frame_postgres`, then in
`_write_frame_postgres` (the staging + `INSERT…ON CONFLICT` branch):

```diff
-            cur.execute(
-                sql.SQL(
-                    "INSERT INTO {tbl} ({cols}) SELECT {cols} FROM {stg} ON CONFLICT ({conf}) {act}"
-                ).format(
-                    tbl=sql.Identifier(table_name),
-                    cols=col_idents,
-                    stg=sql.Identifier(stg),
-                    conf=sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols),
-                    act=action,
-                )
-            )
+            where_sql = (
+                sql.SQL(" WHERE ") + sql.SQL(conflict_where)  # trusted, code-defined predicate
+                if conflict_where
+                else sql.SQL("")
+            )
+            cur.execute(
+                sql.SQL(
+                    "INSERT INTO {tbl} ({cols}) SELECT {cols} FROM {stg} "
+                    "ON CONFLICT ({conf}){where} {act}"
+                ).format(
+                    tbl=sql.Identifier(table_name),
+                    cols=col_idents,
+                    stg=sql.Identifier(stg),
+                    conf=sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols),
+                    where=where_sql,
+                    act=action,
+                )
+            )
```

`conflict_where` is a constant string defined in code (never user data) → injection-safe.
Mirror the same `WHERE` in the sqlite/`bulk_upsert` fallback (`app/core/upsert.py`) using
`postgresql_where`/index-element inference, or gate `conflict_where` to Postgres and let
sqlite use plain `DO NOTHING` (sqlite's partial-index inference differs; tests run on
sqlite, so verify there).

**Bucket B call-site change (transactions example), `flat_txns.py:306`:**

```diff
-        return common.write_frame(ctx.session, UnifiedTransaction, out, conflict_cols=None)
+        return common.write_frame(
+            ctx.session, UnifiedTransaction, out,
+            conflict_cols=["state_id", "transaction_type", "transaction_id"],
+            update_cols=[],
+            conflict_where="transaction_id IS NOT NULL",
+        )
```

Apply identically at `flat_txns.py:321` and `detail_children/transactions.py:94`. Entities
get `conflict_cols=["entity_type","normalized_name","state_id"], update_cols=[],
conflict_where="state_id IS NOT NULL"` at all four entity sites.

---

## Diff 2 — anti-join pre-filter for functional/double-index tables (Bucket C)

New shared helper. It reuses the existing id-map reads, so persons/addresses are filtered
against the exact same normalized key the dedup index uses.

`app/core/ingest_vectorized/common.py` (new function):

```python
def filter_new_rows(
    frame: "pl.DataFrame",
    existing_keys: "pl.DataFrame",
    *,
    key_cols: list[str],
    normalize_lower: list[str] | None = None,
) -> "pl.DataFrame":
    """First-write-wins pre-filter for tables whose unique key is functional/partial or
    split across multiple partial indexes (so ON CONFLICT inference can't be used).

    1. Build a normalized key on *frame* (lower() the columns the functional index lowers).
    2. Drop in-batch duplicates on that key (keep first — deterministic with a prior sort).
    3. Anti-join against *existing_keys* (the natural keys already in the DB, from the
       id-map read) and return only rows whose key is new.

    The caller then writes the result with conflict_cols=None; because the frame is now
    duplicate-free against both the batch and the DB, the partial unique indexes are never
    violated and a re-load inserts nothing.
    """
    import polars as pl

    norm = set(normalize_lower or [])
    key_exprs = [
        (pl.col(c).str.to_lowercase() if c in norm else pl.col(c)).alias(f"_k_{c}")
        for c in key_cols
    ]
    kcols = [f"_k_{c}" for c in key_cols]
    f = frame.with_columns(key_exprs).unique(subset=kcols, keep="first")
    e = existing_keys.with_columns(key_exprs).select(kcols).unique()
    return f.join(e, on=kcols, how="anti").drop(kcols)
```

**Bucket C call-site change (persons example), `detail_children/dims.py:274`** — the
`person_id_map` read already exists in this module; reuse it to get existing keys:

```diff
-    n_persons = common.write_frame(ctx.session, UnifiedPerson, persons_out, conflict_cols=None)
+    existing = id_maps.person_key_frame(ctx.engine, ctx.state_id)  # (first_name,last_name,state_id,dedup_addr_key,organization)
+    persons_new = common.filter_new_rows(
+        persons_out, existing,
+        key_cols=["first_name", "last_name", "state_id", "dedup_addr_key", "organization"],
+        normalize_lower=["first_name", "last_name", "organization"],
+    )
+    n_persons = common.write_frame(ctx.session, UnifiedPerson, persons_new, conflict_cols=None)
```

`person_key_frame` is a thin sibling of the existing `person_id_map` that returns the key
columns (it already SELECTs `lower(first_name), lower(last_name), state_id, dedup_addr_key,
organization` per `_person_id_map`, `flat_txns_detail.py:229`). Note we key on the union of
both partial indexes' columns (name-key for individuals, org for orgs); a single anti-join
on the combined tuple is correct because the two predicates are mutually exclusive
(`organization IS NULL` vs `NOT NULL`). Addresses follow the same pattern via
`address_id_map`, keying `(street_1, city, state, zip_code)` lowercased.

---

## Diff 3 — add unique indexes for no-key tables (Bucket D)

New Alembic revision (`uix_*` partial uniques, mirrored into `_DEDUP_INDEXES` so the
bootstrap path stays consistent):

```sql
CREATE UNIQUE INDEX IF NOT EXISTS uix_campaigns_identity
  ON unified_campaigns (normalized_name, primary_committee_id, election_year);

CREATE UNIQUE INDEX IF NOT EXISTS uix_committee_person_role
  ON unified_committee_persons (committee_id, person_id, role);

CREATE UNIQUE INDEX IF NOT EXISTS uix_campaign_entity_role
  ON unified_campaign_entities (campaign_id, entity_id, role);
```

Then move those call sites to Bucket A (`conflict_cols=[...], update_cols=[]`). `campaigns`
needs `conflict_where` if any key column is nullable (e.g. `WHERE primary_committee_id IS
NOT NULL`). `loan_guarantors` has no clean scalar key → keep it on the Bucket C anti-join
keyed on `(loan_id, debt_id, last_name, first_name, organization)` lowercased.
**Before creating each index, dedup existing rows** (see rollout) or the `CREATE UNIQUE`
fails — same situation migration `0002` handled for transactions.

---

## Edge cases

- **Rows outside every partial predicate** (e.g. a transaction with `transaction_id IS
  NULL`, a person with no name *and* no org): no unique key exists, so neither ON CONFLICT
  nor the index-backed anti-join dedups them. Route them through the anti-join on the full
  natural tuple (NULL-tolerant) so re-loads don't re-append; if even that is ambiguous,
  send to `ingest_errors` rather than silently duplicating. Confirm volume with triage
  step 5 — for TEC RCPT/EXPN these are rare.
- **In-batch duplicates:** a single load spans 134 files and can carry the same key twice.
  `filter_new_rows` dedups within the batch (`.unique(keep="first")`); the Bucket A/B
  staging path must also dedup the staging table before `INSERT…SELECT` (add
  `SELECT DISTINCT ON (conflict_cols)` or a pre-`unique()` on the frame) or two in-batch
  duplicates both attempt insert and the second hits the index.
- **Concurrency:** anti-join (read-then-insert) isn't atomic; two concurrent loads could
  both pass. The partial unique index is the backstop for covered rows (the second load's
  COPY raises and the existing bisection isolation routes it). Single-process loads (the
  norm) are unaffected. ON CONFLICT (Buckets A/B) is atomic regardless.
- **`update_cols=[]` vs `None`:** keep `[]` everywhere here — `None` would `DO UPDATE` and
  reintroduce write churn, which contradicts first-write-wins.

---

## One-time rollout (recover the space the old appends left)

1. **Quantify:** `psql … -f scripts/db_bloat_triage.sql` (step 5 shows surplus duplicate rows).
2. **Dedup existing duplicates** so the new unique indexes can be created: extend the
   `scripts/dedup_unified_transactions.py` pattern to the dimension + child tables (keep
   `min(id)` per natural key, repoint FKs, delete the rest). Run with `--dry-run` first.
3. **Apply migration** (`uv run cf migrate`) — creates the Bucket D indexes.
4. **Reclaim disk:** `VACUUM (FULL, ANALYZE)` the large tables (or `pg_repack` to avoid the
   exclusive lock). This is what returns the bytes; dedup alone only marks tuples dead.
5. **Truncate resolution scratch:** `TRUNCATE resolution_input, candidate_pairs, scored_pairs;`
   (rebuilt on next resolve run).
6. **Verify idempotency:** load a state twice; row counts must be identical after the 2nd run.

---

## Tests to add

- `test_ingest_idempotent.py` (Postgres-gated + sqlite): ingest a fixture, snapshot
  `count(*)` per table, ingest the **same** fixture again, assert every count is unchanged
  and no new `ingest_errors`.
- Per-bucket unit tests: Bucket B emits the `WHERE` predicate and dedups (assert no
  IntegrityError, second insert is a no-op); `filter_new_rows` drops in-batch and
  cross-run duplicates and lower-cases the functional key columns.
- Extend the existing equivalence harness so the COPY path with `conflict_where` stays
  row-identical to `bulk_upsert`.
- Keep `tests/core/test_dedup_migration.py` green; add equivalents for the new dedup of
  campaigns/committee_persons/campaign_entities.

---

## Suggested order of work

1. Diff 1 (writer `conflict_where`) + Bucket A/B call sites (transactions, entities,
   children, transaction_persons) — this alone stops the largest tables from duplicating.
2. Diff 2 (anti-join) for persons/addresses.
3. Diff 3 (new indexes + dedup) for campaigns/committee_persons/campaign_entities/guarantors.
4. Idempotency tests, then the one-time rollout on the full DB.
