# Upsert All Records (First-Write-Wins) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use power-skills-bundle:subagent-driven-development (recommended) or power-skills-bundle:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every ingest write idempotent so re-loading the same source adds zero rows and updates zero rows (first-write-wins), eliminating the append-driven Postgres bloat.

**Architecture:** The vectorized loader writes Polars frames through one funnel, `common.write_frame`. Idempotency is delivered per-table by the cheapest mechanism its unique key allows: full-unique keys use `ON CONFLICT … DO NOTHING`; partial non-functional keys use a new `conflict_where` predicate on the same path; partial+functional/double-index keys (persons, addresses) use an anti-join pre-filter against the natural-key id-maps the pipeline already reads back; tables with no unique key get a new unique index plus a one-time dedup. All four mechanisms are first-write-wins (`update_cols=[]`).

**Tech Stack:** Python 3.12, Polars, SQLModel/SQLAlchemy, psycopg2 (Postgres COPY fast-path), Alembic, pytest, uv.

---

## Prerequisites & Guardrails

- **Spec / design source:** `docs/upsert-all-records-plan-2026-06-20.md` (key matrix + rationale) and `docs/db-bloat-diagnosis-and-architecture-2026-06-20.md` (why this is needed).
- **GitNexus impact analysis is mandatory (per `CLAUDE.md`).** The GitNexus MCP may not be connected; if it is, before editing the writer run:
  - `gitnexus_impact({target: "write_frame", direction: "upstream"})`
  - `gitnexus_impact({target: "_write_frame_postgres", direction: "upstream"})`
  - Report the blast radius; warn the user if HIGH/CRITICAL (it will be — every family writer funnels through these). If the MCP is unavailable, state that and rely on the call-site list in the key matrix.
- **Sub-skills:** @power-skills-bundle:test-driven-development, @power-skills-bundle:verification-before-completion.
- **Run tests with:** `uv run pytest`. Postgres-gated tests require a live local DB (`POSTGRES_*` in `.env`); sqlite-backed tests run anywhere.
- **CRITICAL — sqlite does not get the dedup indexes by default.** The baseline migration and `bootstrap` skip `_DEDUP_INDEXES` off Postgres (`migrations/versions/20260615_0000_0001_baseline_schema.py:42`; the `0002` migration docstring confirms "sqlite databases (tests) never created the index"). So an `ON CONFLICT (...) WHERE ...` against a partial index raises *"ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"* on sqlite unless the test fixture creates that index first. **Every Bucket B/C/D test must either (a) call `UnifiedDatabaseManager._apply_dedup_indexes()` (or run the specific `CREATE UNIQUE INDEX … WHERE …` DDL) on the sqlite test DB in its fixture, or (b) be Postgres-gated.** State which, per test.
- **In-batch duplicates (Bucket A/B):** a single load spans 134 files and can carry the same key twice. The staging `INSERT … SELECT … ON CONFLICT DO NOTHING` path absorbs *within-statement* duplicates fine on Postgres (DO NOTHING swallows the second), so no `SELECT DISTINCT` is strictly required — but add a test that deliberately feeds a duplicated batch and asserts one row. Bucket C's `filter_new_rows` already dedups in-batch via `.unique(keep="first")`.
- **DRY / YAGNI / TDD / frequent commits.** One behavior per commit.

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `app/core/ingest_vectorized/common.py` | Bulk-write funnel | Add `conflict_where` param to `write_frame` / `_attempt_write` / `_write_frame_postgres`; add `filter_new_rows` anti-join helper |
| `app/core/upsert.py` | sqlite/non-PG upsert fallback | Thread `conflict_where` (or Postgres-gate it) |
| `app/core/ingest_vectorized/families/flat_txns.py` | Transaction writes | Bucket B conflict key + predicate |
| `app/core/ingest_vectorized/families/detail_children/transactions.py` | Transaction writes (detail path) | Bucket B |
| `app/core/ingest_vectorized/families/flat_txns_detail.py` | Contribution/Expenditure/TxnPerson writes | Bucket A keys |
| `app/core/ingest_vectorized/families/detail_children/builders.py` | Loan/Debt/Credit/Travel/Asset/Pledge/Guarantor writes | Bucket A keys + Bucket C guarantors |
| `app/core/ingest_vectorized/families/filer.py` | Address/Person/Entity/CommitteePerson writes | Bucket C + B + D |
| `app/core/ingest_vectorized/families/cand.py` | Person/Entity/TxnPerson writes | Bucket C + B + A |
| `app/core/ingest_vectorized/families/flat_txns_dims.py` | Address/Person/Entity dim writes | Bucket C + B |
| `app/core/ingest_vectorized/campaigns.py` | Campaign/CampaignEntity writes | Bucket D |
| `app/core/ingest_vectorized/id_maps.py` | Natural-key → id-map reads | Add `*_key_frame` siblings returning key columns for anti-join |
| `migrations/versions/*.py` | Schema | New revision: dedup + `uix_campaigns_identity`, `uix_committee_person_role`, `uix_campaign_entity_role` |
| `app/core/unified_database.py:186` | `_DEDUP_INDEXES` bootstrap list | Mirror the new indexes |
| `tests/core/test_ingest_idempotent.py` | Idempotency contract | New |
| `tests/core/test_write_frame_conflict_where.py` | Writer unit tests | New |
| `scripts/dedup_dimensions.py` | One-time dedup for D tables | New (mirrors `scripts/dedup_unified_transactions.py`) |

---

## Task 1: Add `conflict_where` to the write funnel

**Files:**
- Modify: `app/core/ingest_vectorized/common.py` (`write_frame`, `_attempt_write`, `_write_frame_postgres`)
- Modify: `app/core/upsert.py` (`bulk_upsert`)
- Test: `tests/core/test_write_frame_conflict_where.py`

- [ ] **Step 1: Write the failing test (sqlite-safe)**

```python
# tests/core/test_write_frame_conflict_where.py
import polars as pl
from sqlalchemy import text
from app.core.ingest_vectorized import common
from app.core.models.tables import UnifiedTransaction

def test_conflict_where_dedups_on_repeat(session_with_dedup_indexes):
    # IMPORTANT: this fixture must build the DB AND apply the partial index, e.g.:
    #   SQLModel.metadata.create_all(engine)
    #   UnifiedDatabaseManager(engine=engine)._apply_dedup_indexes()   # or the one CREATE … WHERE …
    # Without the partial index, sqlite raises "ON CONFLICT clause does not match …".
    session = session_with_dedup_indexes
    rows = pl.DataFrame([{
        "state_id": 1, "transaction_type": "CONTRIBUTION", "transaction_id": "X1", "amount": 1,
    }])
    kw = dict(conflict_cols=["state_id", "transaction_type", "transaction_id"],
              update_cols=[], conflict_where="transaction_id IS NOT NULL")
    common.write_frame(session, UnifiedTransaction, rows, **kw)
    common.write_frame(session, UnifiedTransaction, rows, **kw)  # repeat
    n = session.execute(text("SELECT count(*) FROM unified_transactions")).scalar()
    assert n == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_write_frame_conflict_where.py -v`
Expected: FAIL — `write_frame()` got an unexpected keyword argument `conflict_where` (or unique-violation on the 2nd write).

- [ ] **Step 3: Implement — thread the param through `common.py`**

Add `conflict_where: str | None = None` to `write_frame(...)` and pass it into `_attempt_write(...)` and `_write_frame_postgres(...)`. In `_write_frame_postgres`, build the predicate and inject it into the `INSERT … ON CONFLICT` (staging branch only):

```python
where_sql = (
    sql.SQL(" WHERE ") + sql.SQL(conflict_where)  # code-defined constant, never user data
    if conflict_where else sql.SQL("")
)
cur.execute(
    sql.SQL(
        "INSERT INTO {tbl} ({cols}) SELECT {cols} FROM {stg} "
        "ON CONFLICT ({conf}){where} {act}"
    ).format(
        tbl=sql.Identifier(table_name), cols=col_idents, stg=sql.Identifier(stg),
        conf=sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols),
        where=where_sql, act=action,
    )
)
```

Then make the non-Postgres fallback honor it too. `app/core/upsert.py::bulk_upsert`:

```diff
 def bulk_upsert(
     session,
     model,
     rows,
     *,
     conflict_cols,
     update_cols=None,
+    conflict_where=None,
 ):
@@
-        insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=list(conflict_cols))
+        do_nothing_kwargs = {"index_elements": list(conflict_cols)}
+        if conflict_where:
+            from sqlalchemy import text as _text
+            do_nothing_kwargs["index_where"] = _text(conflict_where)
+        insert_stmt = insert_stmt.on_conflict_do_nothing(**do_nothing_kwargs)
```

Both the SQLAlchemy sqlite and postgresql dialects accept `index_where` for partial-index
inference, so the DO-NOTHING branch matches the same partial index in both. (For the
`DO UPDATE` branch — only reachable with `update_cols`, which this plan never uses — thread
`index_where` identically if you choose to support it.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core/test_write_frame_conflict_where.py -v`
Expected: PASS (count == 1).

- [ ] **Step 5: Verify no regression on the equivalence harness**

Run: `uv run pytest -k "equivalence or write_frame" -v`
Expected: PASS — COPY path stays row-identical to `bulk_upsert`.

- [ ] **Step 6: Commit**

```bash
git add app/core/ingest_vectorized/common.py app/core/upsert.py tests/core/test_write_frame_conflict_where.py
git commit -m "feat(ingest): support partial-index ON CONFLICT predicate in write_frame"
```

---

## Task 2: Bucket B — transactions write idempotently

**Files:**
- Modify: `app/core/ingest_vectorized/families/flat_txns.py:306,321`
- Modify: `app/core/ingest_vectorized/families/detail_children/transactions.py:94`
- Test: `tests/core/test_ingest_idempotent.py` (transaction case)

- [ ] **Step 1: Write the failing test**

```python
# tests/core/test_ingest_idempotent.py
def test_transactions_idempotent(loaded_fixture_session):
    s = loaded_fixture_session
    from sqlalchemy import text
    before = s.execute(text("SELECT count(*) FROM unified_transactions")).scalar()
    reingest_same_fixture(s)            # helper: run the same family ingest again
    after = s.execute(text("SELECT count(*) FROM unified_transactions")).scalar()
    assert after == before
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_ingest_idempotent.py::test_transactions_idempotent -v`
Expected: FAIL — `after == 2 * before` (current append behavior).

- [ ] **Step 3: Implement — set the conflict key at all three sites**

Replace each `conflict_cols=None` transaction write with:

```python
return common.write_frame(
    ctx.session, UnifiedTransaction, out,
    conflict_cols=["state_id", "transaction_type", "transaction_id"],
    update_cols=[],
    conflict_where="transaction_id IS NOT NULL",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core/test_ingest_idempotent.py::test_transactions_idempotent -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/core/ingest_vectorized/families/flat_txns.py app/core/ingest_vectorized/families/detail_children/transactions.py tests/core/test_ingest_idempotent.py
git commit -m "feat(ingest): idempotent unified_transactions writes (first-write-wins)"
```

---

## Task 3: Bucket A — subtype children + transaction_persons write idempotently

**Files:**
- Modify: `app/core/ingest_vectorized/families/flat_txns_detail.py:871,900,936`
- Modify: `app/core/ingest_vectorized/families/detail_children/builders.py:234,293,335,400,452,502`
- Modify: `app/core/ingest_vectorized/families/cand.py:585`
- Test: `tests/core/test_ingest_idempotent.py` (children case)

- [ ] **Step 1: Write the failing test**

```python
def test_children_idempotent(loaded_fixture_session):
    s = loaded_fixture_session
    from sqlalchemy import text
    tables = ["unified_contributions","unified_expenditures","unified_loans",
              "unified_transaction_persons"]
    before = {t: s.execute(text(f"SELECT count(*) FROM {t}")).scalar() for t in tables}
    reingest_same_fixture(s)
    after = {t: s.execute(text(f"SELECT count(*) FROM {t}")).scalar() for t in tables}
    assert after == before
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_ingest_idempotent.py::test_children_idempotent -v`
Expected: FAIL — counts doubled.

- [ ] **Step 3: Implement**

Subtype children (full unique on `transaction_id`):

```python
return common.write_frame(ctx.session, UnifiedContribution, rows,
                          conflict_cols=["transaction_id"], update_cols=[])
```

Apply the same `conflict_cols=["transaction_id"], update_cols=[]` to Expenditure, Loan, Debt,
Credit, Travel, Asset, Pledge. For `UnifiedTransactionPerson` (both `flat_txns_detail.py:936`
and `cand.py:585`):

```python
return common.write_frame(ctx.session, UnifiedTransactionPerson, rows,
                          conflict_cols=["transaction_id", "person_id", "role"], update_cols=[])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core/test_ingest_idempotent.py::test_children_idempotent -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/core/ingest_vectorized/families/flat_txns_detail.py app/core/ingest_vectorized/families/detail_children/builders.py app/core/ingest_vectorized/families/cand.py tests/core/test_ingest_idempotent.py
git commit -m "feat(ingest): idempotent subtype-child and transaction_person writes"
```

---

## Task 4: Bucket B — entities write idempotently

**Files:**
- Modify: `app/core/ingest_vectorized/families/filer.py:597`, `cand.py:545`, `detail_children/dims.py:345`, `flat_txns_dims.py` (entity write ~937)
- Test: `tests/core/test_ingest_idempotent.py` (entities case)

- [ ] **Step 1: Write the failing test** — same pattern, table `unified_entities`.
- [ ] **Step 2: Run — Expected: FAIL (doubled).**
- [ ] **Step 3: Implement at all four sites:**

```python
return common.write_frame(
    ctx.session, UnifiedEntity, out,
    conflict_cols=["entity_type", "normalized_name", "state_id"],
    update_cols=[],
    conflict_where="state_id IS NOT NULL",
)
```

- [ ] **Step 4: Run — Expected: PASS.**
- [ ] **Step 5: Commit**

```bash
git add app/core/ingest_vectorized/families/filer.py app/core/ingest_vectorized/families/cand.py app/core/ingest_vectorized/families/detail_children/dims.py app/core/ingest_vectorized/families/flat_txns_dims.py tests/core/test_ingest_idempotent.py
git commit -m "feat(ingest): idempotent unified_entities writes (first-write-wins)"
```

---

## Task 5: Add the `filter_new_rows` anti-join helper

**Files:**
- Modify: `app/core/ingest_vectorized/common.py` (add `filter_new_rows`)
- Test: `tests/core/test_filter_new_rows.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/core/test_filter_new_rows.py
import polars as pl
from app.core.ingest_vectorized import common

def test_filter_new_rows_drops_existing_and_inbatch_dups():
    frame = pl.DataFrame([
        {"first_name": "Jane", "last_name": "Doe", "state_id": 1},   # existing (case-diff)
        {"first_name": "JANE", "last_name": "DOE", "state_id": 1},   # in-batch dup of above
        {"first_name": "Ann",  "last_name": "Lee", "state_id": 1},   # new
    ])
    existing = pl.DataFrame([{"first_name": "jane", "last_name": "doe", "state_id": 1}])
    out = common.filter_new_rows(
        frame, existing,
        key_cols=["first_name", "last_name", "state_id"],
        normalize_lower=["first_name", "last_name"],
    )
    assert out.height == 1
    assert out["last_name"].to_list() == ["Lee"]
```

- [ ] **Step 2: Run — Expected: FAIL (`filter_new_rows` undefined).**

Run: `uv run pytest tests/core/test_filter_new_rows.py -v`

- [ ] **Step 3: Implement in `common.py`**

```python
def filter_new_rows(frame, existing_keys, *, key_cols, normalize_lower=None):
    """First-write-wins pre-filter for tables whose unique key is functional/partial or
    split across multiple partial indexes (ON CONFLICT inference can't target them).
    Lower-cases the functional key columns, drops in-batch dups, anti-joins against keys
    already in the DB, returns only new rows (written with conflict_cols=None)."""
    import polars as pl
    norm = set(normalize_lower or [])
    exprs = [(pl.col(c).str.to_lowercase() if c in norm else pl.col(c)).alias(f"_k_{c}")
             for c in key_cols]
    kcols = [f"_k_{c}" for c in key_cols]
    f = frame.with_columns(exprs).unique(subset=kcols, keep="first")
    e = existing_keys.with_columns(exprs).select(kcols).unique()
    return f.join(e, on=kcols, how="anti").drop(kcols)
```

- [ ] **Step 4: Run — Expected: PASS.**
- [ ] **Step 5: Commit**

```bash
git add app/core/ingest_vectorized/common.py tests/core/test_filter_new_rows.py
git commit -m "feat(ingest): add filter_new_rows anti-join helper for functional-key tables"
```

---

## Task 6: Bucket C — persons write idempotently (anti-join)

**Files:**
- Modify: `app/core/ingest_vectorized/id_maps.py` (add `person_key_frame`)
- Modify: `app/core/ingest_vectorized/families/filer.py:548`, `cand.py:497`, `detail_children/dims.py:274`, `flat_txns_dims.py` (person write ~911)
- Test: `tests/core/test_ingest_idempotent.py` (persons case)

- [ ] **Step 1: Write the failing test** — table `unified_persons`, same idempotency assertion.
- [ ] **Step 2: Run — Expected: FAIL (doubled).**
- [ ] **Step 3: Implement**

**Reuse the existing key construction — do NOT re-derive it as a raw column tuple.** The
person dedup key is org-collapsed: `person_id_map` / `_person_id_map`
(`id_maps.py:81`, `flat_txns_detail.py:229`) build the key and end with
`common.collapse_org_person_key(...)`, which NULLs the name/addr key parts whenever
`organization` is set — because `uix_persons_org_state` keys org-persons on
`(lower(organization), state_id)` **alone**, while `uix_persons_name_state` keys individuals
on `(lower(first), lower(last), state_id, dedup_addr_key) WHERE organization IS NULL`. A flat
5-tuple key would mis-dedup org rows that carry incidental name values.

Add `person_key_frame(engine, state_id)` to `id_maps.py` as a sibling of `_person_id_map`
that returns the **already-collapsed** key columns (`_pk_org, _pk_fn, _pk_ln, _pk_addr`,
`state_id`). Build the candidate-frame key with the **same** `collapse_org_person_key`, then
anti-join on those collapsed columns:

```python
existing = id_maps.person_key_frame(ctx.engine, ctx.state_id)   # collapsed key cols + state_id
persons_keyed = common.collapse_org_person_key(persons_out)      # same collapse the id-map uses
persons_new = common.filter_new_rows(
    persons_keyed, existing,
    key_cols=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr", "state_id"],
    normalize_lower=[],   # collapse_org_person_key already lower-cased the key parts
)
return common.write_frame(ctx.session, UnifiedPerson, persons_new.drop(
    ["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"]), conflict_cols=None)
```

Verify the exact collapsed column names against `common.collapse_org_person_key` and align
`filter_new_rows`/`drop` to them.

- [ ] **Step 4: Run — Expected: PASS.**
- [ ] **Step 5: Commit**

```bash
git add app/core/ingest_vectorized/id_maps.py app/core/ingest_vectorized/families/filer.py app/core/ingest_vectorized/families/cand.py app/core/ingest_vectorized/families/detail_children/dims.py app/core/ingest_vectorized/families/flat_txns_dims.py tests/core/test_ingest_idempotent.py
git commit -m "feat(ingest): idempotent unified_persons writes via anti-join"
```

---

## Task 7: Bucket C — addresses write idempotently (anti-join)

**Files:**
- Modify: `app/core/ingest_vectorized/id_maps.py` (add `address_key_frame`)
- Modify: `app/core/ingest_vectorized/families/filer.py:463`, `detail_children/dims.py:230`, `flat_txns_dims.py` (address write ~901)
- Test: `tests/core/test_ingest_idempotent.py` (addresses case)

- [ ] **Step 1: Write the failing test** — table `unified_addresses`.
- [ ] **Step 2: Run — Expected: FAIL.**
- [ ] **Step 3: Implement** — `address_key_frame(engine)` returning `(street_1, city, state, zip_code)`; at each site:

```python
existing = id_maps.address_key_frame(ctx.engine)
addr_new = common.filter_new_rows(
    addr_out, existing,
    key_cols=["street_1", "city", "state", "zip_code"],
    normalize_lower=["street_1", "city", "state"],
)
common.write_frame(ctx.session, UnifiedAddress, addr_new, conflict_cols=None)
```

- [ ] **Step 4: Run — Expected: PASS.**
- [ ] **Step 5: Commit**

```bash
git add app/core/ingest_vectorized/id_maps.py app/core/ingest_vectorized/families/filer.py app/core/ingest_vectorized/families/detail_children/dims.py app/core/ingest_vectorized/families/flat_txns_dims.py tests/core/test_ingest_idempotent.py
git commit -m "feat(ingest): idempotent unified_addresses writes via anti-join"
```

---

## Task 8: Bucket D — add unique indexes for no-key tables (+ dedup migration)

**Files:**
- Create: `migrations/versions/<rev>_upsert_dimension_unique_indexes.py`
- Modify: `app/core/unified_database.py:186` (`_DEDUP_INDEXES`)
- Create: `scripts/dedup_dimensions.py`
- Test: `tests/core/test_dedup_dimensions_migration.py`

- [ ] **Step 1: Write the failing test** — assert `alembic upgrade head` creates the three indexes and that a pre-seeded duplicate is collapsed (mirror `tests/core/test_dedup_migration.py`).
- [ ] **Step 2: Run — Expected: FAIL (indexes absent / duplicate remains).**
- [ ] **Step 3: Implement**

`scripts/dedup_dimensions.py` (mirror `scripts/dedup_unified_transactions.py`): for each of
`unified_campaigns`, `unified_committee_persons`, `unified_campaign_entities`, keep `min(id)`
per natural key, repoint FK children, delete the rest; support `--dry-run` and `--db-url`.

Migration `upgrade()`: run the dedup, then:

```python
# Partial WHERE matches Task 9's conflict_where exactly — a bare full unique index would NOT
# be matched by ON CONFLICT (...) WHERE primary_committee_id IS NOT NULL, and would also fail
# to dedup NULL-bearing rows (Postgres treats NULLs as distinct in a full unique index).
op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uix_campaigns_identity "
           "ON unified_campaigns (normalized_name, primary_committee_id, election_year) "
           "WHERE primary_committee_id IS NOT NULL")
op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uix_committee_person_role "
           "ON unified_committee_persons (committee_id, person_id, role)")
op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uix_campaign_entity_role "
           "ON unified_campaign_entities (campaign_id, entity_id, role)")
```

> Decide whether `state_id` belongs in `uix_campaigns_identity`: the campaign id-map read
> (`campaigns.py:248`) scopes by `state_id`, so if `normalized_name` can repeat across states
> the key should include it. If you add it, add it to the `conflict_cols` in Task 9 too.

Mirror the three `CREATE UNIQUE INDEX IF NOT EXISTS` into `_DEDUP_INDEXES` so the
`create_all` bootstrap path stays consistent (see `MIGRATIONS.md` "Relationship to the app
bootstrap"). `downgrade()` drops the three indexes only.

- [ ] **Step 4: Run — Expected: PASS.**

Run: `uv run pytest tests/core/test_dedup_dimensions_migration.py -v`

- [ ] **Step 5: Commit**

```bash
git add migrations/versions/*upsert_dimension_unique_indexes*.py app/core/unified_database.py scripts/dedup_dimensions.py tests/core/test_dedup_dimensions_migration.py
git commit -m "feat(db): unique indexes + dedup for campaigns/committee_persons/campaign_entities"
```

---

## Task 9: Bucket D — campaigns / committee_persons / campaign_entities / guarantors write idempotently

> **Depends on Task 8** — these `ON CONFLICT` writes target the indexes created there. If
> Task 8 hasn't run, they raise "no unique or exclusion constraint matching the ON CONFLICT
> specification."

**Files:**
- Modify: `app/core/ingest_vectorized/campaigns.py:214,233`
- Modify: `app/core/ingest_vectorized/families/filer.py:663` (committee_persons)
- Modify: `app/core/ingest_vectorized/families/detail_children/builders.py:612` (guarantors)
- Modify: `app/core/ingest_vectorized/id_maps.py` (add `guarantor_key_frame`)
- Test: `tests/core/test_ingest_idempotent.py` (campaigns/committee_persons/guarantors)

- [ ] **Step 1: Write the failing test** — tables `unified_campaigns`, `unified_committee_persons`, `unified_campaign_entities`, `loan_guarantors`.
- [ ] **Step 2: Run — Expected: FAIL.**
- [ ] **Step 3: Implement**

Now that the indexes exist, use Bucket A keys:

```python
# campaigns.py:214
common.write_frame(session, UnifiedCampaign, rows,
    conflict_cols=["normalized_name", "primary_committee_id", "election_year"],
    update_cols=[], conflict_where="primary_committee_id IS NOT NULL")
# campaigns.py:233
common.write_frame(session, UnifiedCampaignEntity, links,
    conflict_cols=["campaign_id", "entity_id", "role"], update_cols=[])
# filer.py:663
common.write_frame(ctx.session, UnifiedCommitteePerson, out,
    conflict_cols=["committee_id", "person_id", "role"], update_cols=[])
```

`loan_guarantors` has no clean scalar key → use the anti-join (Bucket C) keyed on
`(loan_id, debt_id, last_name, first_name, organization)` lowercased, reading existing keys
with a small `guarantor_key_frame`.

- [ ] **Step 4: Run — Expected: PASS.**
- [ ] **Step 5: Commit**

```bash
git add app/core/ingest_vectorized/campaigns.py app/core/ingest_vectorized/families/filer.py app/core/ingest_vectorized/families/detail_children/builders.py app/core/ingest_vectorized/id_maps.py tests/core/test_ingest_idempotent.py
git commit -m "feat(ingest): idempotent campaigns/committee_persons/campaign_entities/guarantors"
```

---

## Task 10: End-to-end idempotency contract test

**Files:**
- Modify: `tests/core/test_ingest_idempotent.py` (full-pipeline case)

- [ ] **Step 1: Write the test** — ingest a multi-record fixture covering every family, snapshot `count(*)` for all `unified_*` tables + `ingest_errors`, run the **whole** ingest a second time, assert all counts unchanged and zero new `ingest_errors`.

```python
def test_full_pipeline_idempotent(fixture_dir, fresh_db_session):
    s = fresh_db_session
    run_full_ingest(s, fixture_dir)
    snap1 = table_counts(s)
    run_full_ingest(s, fixture_dir)          # identical second run
    snap2 = table_counts(s)
    assert snap2 == snap1
```

- [ ] **Step 2: Run the whole suite**

Run: `uv run pytest tests/core/test_ingest_idempotent.py -v`
Expected: PASS (all tables stable across re-load).

- [ ] **Step 3: Full regression + lint**

Run: `uv run pytest && uv run ruff check .`
Expected: PASS / no errors.

- [ ] **Step 4: Commit**

```bash
git add tests/core/test_ingest_idempotent.py
git commit -m "test(ingest): end-to-end first-write-wins idempotency contract"
```

---

## Task 11: One-time rollout runbook (recover existing bloat)

> Operational, run once against the full local DB **after** Tasks 1–10 merge. Not part of the test suite. Use @power-skills-bundle:verification-before-completion — capture before/after sizes.

- [ ] **Step 1: Quantify current duplication**

Run: `psql "$DATABASE_URL" -f scripts/db_bloat_triage.sql | tee docs/db-bloat-before-rollout.txt`
Expected: step 5 shows `surplus_rows > 0`; note total DB size.

- [ ] **Step 2: Dry-run the dedups**

Run: `uv run python scripts/dedup_unified_transactions.py --db-url "$DATABASE_URL" --dry-run`
and `uv run python scripts/dedup_dimensions.py --db-url "$DATABASE_URL" --dry-run`
Expected: previewed delete counts look sane.

- [ ] **Step 3: Apply migrations (runs dedup + creates indexes)**

Run: `uv run cf migrate`
Expected: revision applied; unique indexes created without error.

- [ ] **Step 4: Reclaim disk**

Run: `psql "$DATABASE_URL" -c "VACUUM (FULL, ANALYZE) unified_transactions;"` (repeat for the
large tables; or `pg_repack` to avoid the exclusive lock).
Expected: on-disk size drops.

- [ ] **Step 5: Drop resolution scratch**

Run: `psql "$DATABASE_URL" -c "TRUNCATE resolution_input, candidate_pairs, scored_pairs;"`

- [ ] **Step 6: Verify**

Run: `psql "$DATABASE_URL" -f scripts/db_bloat_triage.sql | tee docs/db-bloat-after-rollout.txt`
Expected: `surplus_rows == 0`; total DB size materially smaller. Re-run a state load twice and
confirm counts are identical.

---

## Pre-Completion Self-Check (per `CLAUDE.md`)

- [ ] `gitnexus_impact` run for `write_frame` / `_write_frame_postgres` (or unavailability noted), HIGH risk surfaced to user.
- [ ] `gitnexus_detect_changes()` confirms only the expected files/flows changed.
- [ ] All d=1 (WILL BREAK) dependents of the writer updated.
- [ ] Full `uv run pytest` green; idempotency test proves a double-load is a no-op.
- [ ] After committing, refresh the index: `npx gitnexus analyze` (add `--embeddings` if `.gitnexus/meta.json` shows `stats.embeddings > 0`).

## Plan Review

After saving, dispatch one `plan-document-reviewer` subagent with the plan path
(`prompts/upsert-all-records/2026-06-20-upsert-all-records.md`) and the spec path
(`docs/upsert-all-records-plan-2026-06-20.md`). Fix any ❌ issues and re-review (max 3 loops).
