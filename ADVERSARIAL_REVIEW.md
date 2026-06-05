# Adversarial Architecture Review — Campaign Finance Pipeline
*Full-Stack Review · Template 1 + Blind Spots · 2026-05-28*
*Stack: SQLModel + Pydantic v2 + sync SQLAlchemy + no Alembic*

---

## RANKED RISK LIST

---

### RISK-01 · CRITICAL · Migration Safety — No Alembic; indexes never applied to DB
**Failure mode:** Schema changes silently never reach the database; all performance indexes are dead code.
**Trigger:** `bootstrap()` calls `SQLModel.metadata.create_all(engine)`. Once tables exist, this is a no-op — it never alters a column, adds a missing FK, or creates an index. The `UnifiedTransactionIndexes` class instantiates ~30 SQLAlchemy `Index()` objects but they are **never passed to `metadata.create_all()`** and are never applied to the database. They exist only in Python memory.
**Impact:** Correctness + Performance. Every query against `unified_transactions` by `state_id`, `committee_id`, `transaction_date`, `amount`, or `transaction_type` does a full sequential scan. At 170K rows today this is ~200ms per query; at 10M rows (multi-state ingest) every query will time out. More critically: any column added to a model (e.g., `report_ident` was recently added) will not exist in the running DB after a restart unless someone manually ran `create_all` on a fresh DB.
**Likelihood:** HIGH — index class is defined but never wired up; confirmed no `alembic.ini` in project root.
**Fix:** Add Alembic. Wire `UnifiedTransactionIndexes` via `event.listen(SQLModel.metadata, 'after_create', ...)` or explicit `Index.create(bind=engine)` calls in `bootstrap()` until Alembic is in place.

---

### RISK-02 · CRITICAL · Session Lifecycle — `load_data_from_file` + `save_transactions` cross-session contamination
**Failure mode:** `DetachedInstanceError` at runtime or silent re-insertion of objects with new PKs.
**Trigger:** `unified_database.py`, `load_data_from_file()` opens Session A, builds fully-relationships-loaded `UnifiedTransaction` objects (committee, persons, entities all pending in Session A), then closes Session A and returns the detached objects. `save_transactions()` opens Session B and calls `session.add(tx)` on each detached object. SQLAlchemy treats a detached object with a `None` PK as a new insert — it re-inserts all related rows (persons, committees, entities) with new IDs, duplicating everything. If PKs are non-null (after a prior flush in Session A), `session.add()` may raise `DetachedInstanceError` when SQLAlchemy tries to reconcile the identity map.
**Impact:** Correctness — duplicate rows or runtime crash on any call to `load_and_save_file()`.
**Likelihood:** HIGH — `load_and_save_file()` is a public method on `UnifiedDatabaseManager` and calls both in sequence.
**Fix:** Either process and commit in a single session, or use `session.expunge_all()` + `session.merge()` pattern. The cleaner fix is to delete `load_data_from_file()` and `save_transactions()` entirely in favour of `UnifiedStateLoader.process_records_batch()`, which already manages a single session per batch.

---

### RISK-03 · CRITICAL · Field Exposure — `raw_data` serializes PII on any API response
**Failure mode:** Full source record JSON (contributor names, addresses, employer, occupation) leaks in any API response.
**Trigger:** `UnifiedTransaction.raw_data` stores `json.dumps(raw_data.copy())` — the entire original source row including every TEC column. `UnifiedDatabaseManager.get_transactions()` returns `list[UnifiedTransaction]`. If any FastAPI route passes this directly to a `response_model` or `model.model_dump()`, every contributor's PII is exposed in the response payload.
**Impact:** Data leak — PII (names, addresses, employer). HIGH severity in any regulatory or public-facing context.
**Likelihood:** MEDIUM — no API layer is present yet, but `get_transactions()` is a public method and the pattern is clearly set up for future use.
**Fix:** Define a `TransactionRead` Pydantic schema that explicitly excludes `raw_data`, `last_modified_by`, `change_reason`, `amendment_details`, `download_date`. Use it as `response_model` everywhere.

---

### RISK-04 · CRITICAL · Schema Collapse — All CRUD operations use the same `table=True` model
**Failure mode:** PATCH updates silently NULL-out fields not included in the update payload; internal tracking fields are writable by callers.
**Trigger:** Every entity (`UnifiedTransaction`, `UnifiedPerson`, `UnifiedCommittee`, `UnifiedAddress`) uses a single `table=True` SQLModel class for: DB schema, inserts, reads, partial updates, and version snapshots. There are no separate Create/Update/Read schemas.
**Consequences:**
1. `update_transaction(entity_id, updates)` in `repository.py` uses `setattr(entity, k, v)` for keys in `updates`. If a caller passes `{"amount": 500}` and the entity has 30 other optional fields, those 30 fields remain as-is — this part is fine. But if any caller constructs a `UnifiedTransaction(**partial_data)` and passes it to `session.merge()`, all unset optional fields default to `None` and overwrite existing DB values.
2. `raw_data`, `last_modified_by`, `change_reason` are on the same model as `amount` and `transaction_date`. A future API can't simply use `UnifiedTransaction` as a request body schema without exposing write access to audit fields.
**Impact:** Correctness + Maintainability. High impact once an API layer is added.
**Likelihood:** MEDIUM now, HIGH once API layer exists.
**Fix:** Introduce `TransactionCreate`, `TransactionUpdate`, `TransactionRead` Pydantic models. Keep `table=True` only for ORM/DB work.

---

### RISK-05 · HIGH · Session Lifecycle — `entity_snapshot()` silently omits `sa_column` fields from version history
**Failure mode:** Version snapshots in `*_versions` tables are silently incomplete — `raw_data`, `description`, `notes`, `metadata_json`, `amendment_details` are never snapshotted.
**Trigger:** `repository.py`, `entity_snapshot()` iterates `model_fields.keys()` (Pydantic's field registry). Fields defined via `sa_column=Column(Text)` with no explicit Pydantic `Field()` annotation are invisible to Pydantic's `model_fields`. Affected fields across all models: `raw_data` (Text), `description` (Text), `notes` (Text), `metadata_json` (Text), `amendment_details` (Text), `change_reason` (String via sa_column), `data` on version tables. None of these appear in version snapshots. `UnifiedTransactionVersion.data` will never contain the transaction's description or raw source data.
**Impact:** Correctness — version history is unreliable for any field that uses `sa_column`.
**Likelihood:** HIGH — `sa_column` is used on ~15 fields across models.
**Fix:** Replace `model_fields.keys()` iteration with SQLAlchemy's `inspect(entity).attrs.keys()` which reflects all mapped columns regardless of Pydantic registration.

---

### RISK-06 · HIGH · Session Lifecycle — `__init__` normalization bypassed on DB reads
**Failure mode:** `UnifiedAddress.state` can be lowercase ("tx") after loading from DB; `UnifiedPerson` fields can have leading/trailing whitespace. Dedup queries using equality comparisons will fail to match.
**Trigger:** SQLAlchemy bypasses `__init__` when loading objects from the database — it sets column values directly via the descriptor/instrumentation layer. The whitespace-stripping and `state.upper()` logic in `UnifiedAddress.__init__`, `UnifiedPerson.__init__`, and `UnifiedCommittee.__init__` (the RF-SMELL-001 fix) only runs when Python code explicitly calls the constructor. Any row loaded from the DB that was inserted before the normalization code existed will have raw values that bypass the guards on subsequent reads. The address dedup query `UnifiedAddress.state == person.address.state` will miss rows where the DB has "TX" but the new object has "TX" (fine), but will also miss rows where DB has " TX " with whitespace (inserted before normalization).
**Impact:** Correctness — dedup queries silently fail for pre-normalization rows.
**Likelihood:** MEDIUM — affects rows inserted before the RF-SMELL-001 fix.
**Fix:** Add SQLAlchemy `@validates` decorators (which fire on ORM attribute sets, including loads) as a belt-and-suspenders complement to `__init__` normalization. Or use a Alembic migration to normalize existing rows.

---

### RISK-07 · HIGH · Migration Safety — `UnifiedReport` table only registered via side-effect import
**Failure mode:** `unified_reports` table not created by `create_all`; FK from `unified_transactions.report_id` violates referential integrity at first insert.
**Trigger:** `UnifiedReport` is defined in `app/core/source_models/reports.py`. It is imported in exactly one place: the last line of `processor.py` as `from app.core.source_models.reports import UnifiedReport as _UnifiedReport`. SQLModel registers models in `metadata` at class-definition time. If `processor.py` is never imported before `bootstrap()` calls `create_all`, `unified_reports` is not in `metadata` and the table is never created. The FK on `unified_transactions.report_id → unified_reports.id` will then fail at first INSERT that has a non-NULL `report_id`.
**Impact:** Correctness — silent boot failure if import order changes.
**Likelihood:** MEDIUM — currently the import chain happens to work, but it's fragile.
**Fix:** Import `UnifiedReport` explicitly in `app/core/models/__init__.py` alongside the other table models, not as a side-effect in `processor.py`.

---

### RISK-08 · HIGH · Async Correctness — Blocking I/O in ingest pipeline will deadlock async workers
**Failure mode:** Event loop blocks for minutes during file ingest if ever called from an async context.
**Trigger:** `UnifiedStateLoader.load_state_data()` → `_process_data_file()` → `FileReader.read()` calls `pd.read_parquet(file_path)` (blocking pandas I/O). `process_records_batch()` calls synchronous SQLAlchemy `create_engine` (not `create_async_engine`). The entire ingest pipeline is synchronous. If any future FastAPI background task or Celery worker using asyncio calls into this, the event loop blocks for the full duration of a file read + DB commit (potentially minutes per file).
**Impact:** Performance + Reliability — complete event loop stall in async context.
**Likelihood:** LOW now (CLI-only), HIGH if ingest is ever exposed via API endpoint or async task queue.
**Fix:** Wrap `load_state_data()` with `asyncio.run_in_executor()` if called from async context, or explicitly document it as sync-only and never call from async code.

---

### RISK-09 · HIGH · Schema Drift — `UnifiedFieldLibrary.field_mappings` dict silently drops last-wins duplicates
**Failure mode:** For a given record type, the wrong field wins when multiple state columns map to the same unified field name.
**Trigger:** `UnifiedSQLModelBuilder.__init__` builds `self.field_mappings = {state_field: unified_field for mapping in ...}`. Since the dict key is `state_field` (which is unique), this is fine for the lookup direction. But `_get_field_value` iterates all mappings looking for any `mapped_field == unified_field and state_field in raw_data`. With 100+ Texas mappings and multiple state fields mapping to `transaction_date` (`contributionDt`, `expendDt`, `loanDt`, `pledgeDt`, `creditDt`, `parentDt`), the first one found in iteration order wins. Python dicts are insertion-ordered since 3.7, so `contributionDt` wins for all record types — including expenditure rows (which have `expendDt` but not `contributionDt`). In practice `contributionDt` won't be in an expenditure row's keys, so this resolves safely. But `loanDt` is defined TWICE in the Texas mappings (once at line ~455 and again as part of the new lender fields at line ~545), which means the second entry for `loanAmount` → `amount` and `loanDt` → `transaction_date` will silently override the first entry in `field_mappings`. Since `field_mappings` is `{state_field: unified_field}` and `loanDt` appears twice, the second assignment wins — which happens to map to the same value, so there's no visible bug today. But this is a latent correctness trap.
**Impact:** Correctness — non-deterministic field resolution if duplicate keys are added carelessly.
**Likelihood:** MEDIUM — two duplicate `loanDt` entries already exist.
**Fix:** Assert no duplicate `state_field` values within a state's mappings in `_initialize_state_mappings`. Raise `ValueError` on duplicate. Add a test.

---

### RISK-10 · HIGH · Pydantic Over-use — `UnifiedSQLModelBuilder` reconstructed 170K times per ingest
**Failure mode:** Performance degradation — O(n) object construction for work that is O(1).
**Trigger:** `unified_sql_processor.get_builder()` is called inside `process_record()`, which is called for every record. Each `get_builder()` call constructs a new `UnifiedSQLModelBuilder`, which in `__init__` calls `field_library.get_state_mappings(state)` and builds `self.field_mappings` from a list comprehension over all ~100 Texas mappings. All 170K records for a given state produce an identical `field_mappings` dict. The builder should be constructed once per state per batch, not once per record.
**Impact:** Performance — wasted CPU and GC pressure. Measurable on large files (1M+ rows for future states).
**Likelihood:** HIGH — confirmed by code path: `process_record()` → `get_builder()` → `UnifiedSQLModelBuilder.__init__()` → `field_library.get_state_mappings(state)`.
**Fix:** In `process_records_batch()`, construct one builder per batch and pass it into record processing, or cache the builder in `UnifiedSQLDataProcessor` keyed by `(state, state_id)`.

---

### RISK-11 · MEDIUM · Dependency Injection — `get_db_manager()` is a process-level global singleton; unsafe for tests
**Failure mode:** Test isolation breaks — tests sharing the process contaminate each other's DB state.
**Trigger:** `get_db_manager()` caches `_db_manager_cached` at module level. `reset_db_manager_cache()` exists but must be explicitly called. Any test that imports and calls `get_db_manager()` without resetting will share the engine and session factory with all other tests. If tests run in parallel (e.g., `pytest-xdist`), two tests can open sessions on the same engine concurrently, producing race conditions in dedup queries.
**Impact:** Maintainability + correctness in test suite.
**Likelihood:** MEDIUM — `conftest.py` exists and presumably handles this, but the global singleton is an architectural smell.
**Fix:** Pass `db_manager` via constructor injection everywhere (already done in `UnifiedStateLoader.__init__`). Reserve the global for CLI entrypoints only.

---

### RISK-12 · MEDIUM · Field Exposure — `UnifiedTransactionVersion.data` is a raw JSON dump of full model
**Failure mode:** Version audit trail exposes PII and internal state if queried via API.
**Trigger:** `entity_snapshot()` dumps all model fields to JSON. `UnifiedTransactionVersion.data` will contain `raw_data` (full TEC source row), contributor names, addresses. If a "view history" API endpoint exposes version records, this is a significant PII exposure risk.
**Impact:** Data leak.
**Likelihood:** LOW — no API endpoint for versions visible, but the data is present.
**Fix:** Exclude `raw_data` from `entity_snapshot()`. Add an explicit allowlist of fields to snapshot rather than snapshotting everything.

---

### RISK-13 · MEDIUM · Session Lifecycle — Outer `except SQLAlchemyError` swallows all DB errors and returns partial stats
**Failure mode:** Callers cannot distinguish a complete DB failure from partial success.
**Trigger:** `process_records_batch()`: the inner `except SQLAlchemyError` calls `session.rollback()` and returns `stats` immediately (early return). The outer `except SQLAlchemyError` at the end of the `try` block also returns stats. If a DB error occurs on record 50,000 of 100,000, the stats show 49,999 successes and 1 DB error — but the caller (e.g., CLI progress display) has no indication that only half the batch was committed. The file-origin dedup guard was also bypassed since no `FileOrigin` row was created.
**Impact:** Correctness — partial batch treated as success by caller.
**Likelihood:** MEDIUM — any transient DB connection drop triggers this.
**Fix:** Re-raise after rolling back, or set a `stats.fatal = True` flag and check it in callers.

---

### RISK-14 · LOW · Pydantic v2 — `model_dump(exclude_unset=True)` broken for ORM-loaded objects
**Failure mode:** PATCH-semantics code using `exclude_unset` will return an empty dict or only the fields explicitly set at Python construction time.
**Trigger:** SQLAlchemy sets column attributes via descriptor protocol, not `__init__` kwargs. Pydantic v2's `model_fields_set` tracks only fields passed to `__init__`. An ORM-loaded `UnifiedTransaction` has `model_fields_set = set()` even though all 25 columns are populated. `tx.model_dump(exclude_unset=True)` returns `{}`. `entity_snapshot()` in `repository.py` avoids this by using `getattr(entity, k)` directly — good. But any future partial-update logic that relies on `exclude_unset` will silently discard all DB-loaded values.
**Impact:** Correctness — future PATCH endpoints will overwrite all fields with defaults.
**Likelihood:** LOW now, HIGH once PATCH endpoints are added.
**Fix:** Document this limitation in `tables.py`. In future PATCH handlers, load the existing object and apply updates via `setattr`, never via model reconstruction.

---

### RISK-15 · LOW · Migration Safety — `UnifiedCommittee.filer_id` is a string PK; migration to int PK is catastrophically expensive
**Failure mode:** If business requirements change to a surrogate integer PK, migrating `unified_committees`, `unified_transactions.committee_id`, `unified_reports.committee_id`, `unified_assets.committee_id`, `unified_committee_persons.committee_id`, `unified_entity_associations` (via entity → committee), and `unified_campaigns.primary_committee_id` is an O(n) full-table rewrite.
**Impact:** Maintainability — string PKs are joined against in every transaction query.
**Likelihood:** LOW — PK type is stable for now.
**Note:** Not a fix-now item, but a design debt to document.

---

## HARDENING CROSS-CHECK

| Category | Gap Found? | Detail |
|---|---|---|
| **Security** | YES | `raw_data` PII in table model (RISK-03) |
| **Observability** | PARTIAL | `Logger` is used throughout; no structured `/health` endpoint visible; no metrics |
| **Reliability** | YES | Cross-session object contamination (RISK-02); partial batch on DB error (RISK-13) |
| **Performance** | YES | Indexes never applied (RISK-01); builder rebuilt per record (RISK-10) |

---

## BLIND SPOTS FOLLOW-UP

| Blind Spot | Finding |
|---|---|
| `exclude_unset` broken with `table=True` | CONFIRMED (RISK-14). `entity_snapshot()` works around it but future PATCH code will not. |
| Alembic missing; `create_all` misses changes | CONFIRMED (RISK-01). No Alembic at all. `UnifiedTransactionIndexes` is dead code. |
| Pydantic v2 validator order | `__init__` override fires on construction but NOT on ORM load (RISK-06). `@validates` needed. |
| SQLModel abstraction breaks for `sa_column` fields | CONFIRMED (RISK-05). `sa_column=Column(Text)` fields invisible to `model_fields`; absent from version snapshots. Affected: `raw_data`, `description`, `notes`, `metadata_json`, `amendment_details`. |
| Response schema exposes internal fields | CONFIRMED (RISK-03, RISK-04). `raw_data` (PII), audit trail fields, `uuid` internal IDs all on response-capable model. |
| Sync SDK in async context | CONFIRMED for `pd.read_parquet` (RISK-08). Not a current issue (CLI-only) but architectural trap. |
