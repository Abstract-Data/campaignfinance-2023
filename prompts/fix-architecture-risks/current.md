# Prompt: Remediate Adversarial Architecture Review Findings
# Version: 1.0.0
# Model: claude-sonnet-4-6
# Last Updated: 2026-06-06
# Maintainer: John Eakin / Abstract Data

## Context

You are working on a Python campaign finance data pipeline. Stack: SQLModel + Pydantic v2 + synchronous SQLAlchemy + no Alembic (yet). The project root is the working directory. Key files:

- `app/core/models/tables.py` — all `table=True` SQLModel classes
- `app/core/unified_database.py` — `UnifiedDatabaseManager`, engine, sessions, `get_db_manager()`
- `app/core/repository.py` — `UnifiedVersionedRepository`, `entity_snapshot()`, version recording
- `app/core/processor.py` — `UnifiedSQLDataProcessor`, `_build_participants()`, `DETAIL_BUILDERS`
- `app/core/unified_state_loader.py` — `UnifiedStateLoader`, `process_records_batch()`
- `app/core/builders.py` — `UnifiedSQLModelBuilder`
- `app/core/source_models/reports.py` — `UnifiedReport` table

A detailed adversarial risk analysis is in `ADVERSARIAL_REVIEW.md`. Read it before starting. A separate data-quality fix prompt is in `prompts/fix_ingest_pipeline.md` — the two prompts are complementary and may be worked in parallel or sequentially.

Before modifying any symbol, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` per `CLAUDE.md`. Run `gitnexus_detect_changes()` before every commit.

---

## Objective

Remediate all 15 architectural risks from `ADVERSARIAL_REVIEW.md`. Changes fall into five waves:

1. **Wave A — Migration infrastructure** (RISK-01, RISK-07): Add Alembic; wire indexes; fix `UnifiedReport` import.
2. **Wave B — Session safety** (RISK-02, RISK-06, RISK-13): Eliminate cross-session contamination; fix `__init__`-bypass normalization; fix partial-batch error handling.
3. **Wave C — Schema separation** (RISK-03, RISK-04, RISK-12): Add `Read`/`Create`/`Update` schemas; exclude PII fields from responses.
4. **Wave D — Version snapshot integrity** (RISK-05): Fix `entity_snapshot()` to capture `sa_column` fields.
5. **Wave E — Performance + correctness** (RISK-09, RISK-10, RISK-11, RISK-14, RISK-15): Builder caching; duplicate mapping guard; DI cleanup; `exclude_unset` documentation.

---

## Wave A — Migration Infrastructure

### A1. Install and configure Alembic (RISK-01)

```bash
uv add alembic
alembic init alembic
```

Edit `alembic/env.py` to import SQLModel metadata and all table models before `run_migrations`:

```python
# alembic/env.py — key additions
from sqlmodel import SQLModel
from app.core.models.tables import *                          # all table=True models
from app.core.source_models.reports import UnifiedReport      # must be explicit (see A2)
from app.core.source_models.pledges import *
from app.core.source_models.spac import *
from app.core.source_models.notices import *
from app.core.source_models.lookups import *
from app.states.postgres_config import PostgresConfig

config_obj = PostgresConfig()
config.set_main_option("sqlalchemy.url", config_obj.database_url)
target_metadata = SQLModel.metadata
```

Set `compare_type = True` in `alembic/env.py`'s `context.configure()` call so column type changes are detected.

Edit `alembic.ini`:
```ini
script_location = alembic
file_template = %%(year)d%%(month).2d%%(day).2d_%%(rev)s_%%(slug)s
```

Generate the initial migration from the current live DB state:
```bash
alembic stamp head        # mark current DB as baseline (tables already exist)
alembic revision --autogenerate -m "baseline_with_indexes"
```

In the generated migration's `upgrade()`, manually add all index creation statements from `UnifiedTransactionIndexes` (they will NOT be auto-detected since they were never in the DB):

```python
def upgrade() -> None:
    # Indexes from UnifiedTransactionIndexes — never previously applied
    op.create_index("idx_transactions_state",       "unified_transactions",      ["state_id"])
    op.create_index("idx_transactions_type",        "unified_transactions",      ["transaction_type"])
    op.create_index("idx_transactions_date",        "unified_transactions",      ["transaction_date"])
    op.create_index("idx_transactions_amount",      "unified_transactions",      ["amount"])
    op.create_index("idx_transactions_committee",   "unified_transactions",      ["committee_id"])
    op.create_index("idx_transactions_id",          "unified_transactions",      ["transaction_id"])
    op.create_index("idx_transactions_file_origin", "unified_transactions",      ["file_origin_id"])
    op.create_index("idx_persons_name",             "unified_persons",           ["last_name", "first_name"])
    op.create_index("idx_persons_organization",     "unified_persons",           ["organization"])
    op.create_index("idx_persons_type",             "unified_persons",           ["person_type"])
    op.create_index("idx_transaction_persons_role",        "unified_transaction_persons", ["role"])
    op.create_index("idx_transaction_persons_transaction", "unified_transaction_persons", ["transaction_id"])
    op.create_index("idx_transaction_persons_person",      "unified_transaction_persons", ["person_id"])
    op.create_index("idx_addresses_state",          "unified_addresses",         ["state"])
    op.create_index("idx_addresses_city",           "unified_addresses",         ["city"])
    op.create_index("idx_committees_name",          "unified_committees",        ["name"])
    op.create_index("idx_committees_type",          "unified_committees",        ["committee_type"])
    op.create_index("idx_entities_type",            "unified_entities",          ["entity_type"])
    op.create_index("idx_entities_name",            "unified_entities",          ["normalized_name"])
    op.create_index("idx_campaigns_year",           "unified_campaigns",         ["election_year"])
    op.create_index("idx_campaigns_office",         "unified_campaigns",         ["office_sought"])
    op.create_index("idx_campaigns_name",           "unified_campaigns",         ["normalized_name"])
    op.create_index("idx_campaign_entity_role",     "unified_campaign_entities", ["role"])
    op.create_index("idx_contributions_date",       "unified_contributions",     ["receipt_date"])
    op.create_index("idx_contributions_amount",     "unified_contributions",     ["amount"])
    op.create_index("idx_loans_date",               "unified_loans",             ["loan_date"])
    op.create_index("idx_loans_due_date",           "unified_loans",             ["due_date"])
    op.create_index("idx_debts_date",               "unified_debts",             ["debt_date"])
    op.create_index("idx_debts_amount",             "unified_debts",             ["amount"])
    op.create_index("idx_credits_date",             "unified_credits",           ["credit_date"])
    op.create_index("idx_travel_date",              "unified_travel",            ["travel_date"])
    op.create_index("idx_assets_acquisition_date",  "unified_assets",            ["acquisition_date"])
```

Apply:
```bash
alembic upgrade head
```

Update `UnifiedDatabaseManager.bootstrap()` to call Alembic instead of `create_all`:

```python
def bootstrap(self) -> None:
    """Run all pending Alembic migrations (replaces create_all)."""
    from alembic.config import Config
    from alembic import command
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")
```

Keep `SQLModel.metadata.create_all(self.engine)` as a fallback for in-memory SQLite tests only.

### A2. Fix `UnifiedReport` import to be explicit (RISK-07)

**File:** `app/core/models/__init__.py`

Add the explicit import alongside the other table models:

```python
"""Unified SQLModel table classes."""

from app.core.models.tables import *
from app.core.source_models.reports import UnifiedReport          # ← add this
from app.core.source_models.pledges import UnifiedPledge          # if table=True
from app.core.source_models.spac import *
from app.core.source_models.notices import *
from app.core.source_models.lookups import *
```

Check each `source_models` file for `table=True` classes and import them all here. Remove the side-effect import from the bottom of `processor.py`:

```python
# REMOVE from processor.py:
from app.core.source_models.reports import UnifiedReport as _UnifiedReport  # noqa
```

Verify: `python -c "from app.core.models import UnifiedReport; print('OK')"` should succeed without importing `processor`.

---

## Wave B — Session Safety

### B1. Delete the cross-session `load_data_from_file` + `save_transactions` pair (RISK-02)

**File:** `app/core/unified_database.py`

The methods `load_data_from_file()`, `save_transactions()`, and `load_and_save_file()` build objects in Session A and commit in Session B. This is a `DetachedInstanceError` waiting to happen. Remove all three:

```python
# DELETE these methods entirely from UnifiedDatabaseManager:
# - load_data_from_file()
# - save_transactions()
# - load_and_save_file()
```

All callers should use `UnifiedStateLoader.process_records_batch()` directly, which manages a single session per batch. If any CLI command calls `load_and_save_file()`, update it to:

```python
loader = UnifiedStateLoader(state, data_directory, db_manager=db_manager)
loader._process_data_file(file_path, auto_link_officers=False)
```

### B2. Add `@validates` decorators for normalization on ORM loads (RISK-06)

**File:** `app/core/models/tables.py`

SQLAlchemy's `@validates` fires on attribute assignment including ORM loads, unlike `__init__`. Add validators to complement the existing `__init__` overrides:

```python
from sqlalchemy.orm import validates

class UnifiedAddress(SQLModel, table=True):
    # ... existing fields ...

    @validates("state")
    def _normalize_state(self, key: str, value: str | None) -> str | None:
        return value.strip().upper() if isinstance(value, str) else value

    @validates("city", "street_1", "street_2", "zip_code")
    def _strip_text(self, key: str, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value


class UnifiedPerson(SQLModel, table=True):
    # ... existing fields ...

    @validates("first_name", "last_name", "middle_name", "suffix",
               "organization", "employer", "occupation", "job_title")
    def _strip_text(self, key: str, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value


class UnifiedCommittee(SQLModel, table=True):
    # ... existing fields ...

    @validates("name", "committee_type")
    def _strip_text(self, key: str, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value
```

The existing `__init__` overrides can remain as a construction-time guard; `@validates` adds the ORM-load path.

### B3. Fix partial-batch error handling — don't silently swallow DB failures (RISK-13)

**File:** `app/core/unified_state_loader.py`, `process_records_batch()`

Replace the current silent-return pattern with a distinguishable fatal flag:

```python
@dataclass
class ProcessStats:
    success: int = 0
    failures: int = 0
    db_errors: int = 0
    skipped: int = 0
    fatal: bool = False          # ← add this field

    @property
    def total(self) -> int:
        return self.success + self.failures + self.db_errors + self.skipped
```

In `process_records_batch()`, change the inner SQLAlchemy error handler:

```python
except SQLAlchemyError as exc:
    logger.error(f"DB error on record: {exc}")
    session.rollback()
    stats.db_errors += 1
    stats.fatal = True           # ← mark as fatal
    return stats                 # still early-return, but caller can check stats.fatal
```

In `_process_data_file()`, check the fatal flag:

```python
batch_stats = self.process_records_batch(records, file_path=file_path, ...)
if batch_stats.fatal:
    error_msg = f"Fatal DB error processing {file_path.name} — batch rolled back"
    file_stats["errors"].append(error_msg)
    self.stats["errors"].append(error_msg)
    logger.error(error_msg)
```

---

## Wave C — Schema Separation

### C1. Add Read/Create/Update Pydantic schemas (RISK-03, RISK-04)

Create `app/core/schemas.py` (new file). These are pure Pydantic models — NOT `table=True` — used for API boundaries and serialization:

```python
"""Pydantic read/write schemas — separate from table=True SQLModel classes."""

from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel


class AddressRead(BaseModel):
    id: int
    street_1: Optional[str] = None
    street_2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    country: Optional[str] = None

    model_config = {"from_attributes": True}


class PersonRead(BaseModel):
    id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    middle_name: Optional[str] = None
    suffix: Optional[str] = None
    organization: Optional[str] = None
    employer: Optional[str] = None
    occupation: Optional[str] = None
    person_type: str
    address: Optional[AddressRead] = None

    model_config = {"from_attributes": True}


class CommitteeRead(BaseModel):
    filer_id: str
    name: Optional[str] = None
    committee_type: Optional[str] = None
    state_id: Optional[int] = None

    model_config = {"from_attributes": True}


class TransactionRead(BaseModel):
    """Safe read schema — excludes raw_data and all audit/internal fields."""
    id: int
    uuid: str
    transaction_id: Optional[str] = None
    amount: Optional[Decimal] = None
    transaction_date: Optional[date] = None
    description: Optional[str] = None
    transaction_type: str
    committee_id: Optional[str] = None
    state_id: Optional[int] = None
    filed_date: Optional[date] = None
    amended: bool
    report_ident: Optional[str] = None
    created_at: datetime
    # Intentionally excluded: raw_data, last_modified_by, last_modified_at,
    # change_reason, amendment_details, download_date

    model_config = {"from_attributes": True}


class TransactionCreate(BaseModel):
    """Fields required/allowed when creating a transaction via API."""
    amount: Optional[Decimal] = None
    transaction_date: Optional[date] = None
    description: Optional[str] = None
    transaction_type: str
    committee_id: Optional[str] = None
    # No: id, uuid, raw_data, audit fields


class TransactionUpdate(BaseModel):
    """Fields that may be updated. All optional — PATCH semantics."""
    amount: Optional[Decimal] = None
    transaction_date: Optional[date] = None
    description: Optional[str] = None
    amended: Optional[bool] = None
    change_reason: Optional[str] = None
    amendment_details: Optional[str] = None
    # No: id, uuid, raw_data, state_id, committee_id (structural fields are immutable)
```

### C2. Update `get_transactions()` to return `TransactionRead` (RISK-03)

**File:** `app/core/unified_database.py`

```python
from app.core.schemas import TransactionRead

def get_transactions(
    self,
    state: str | None = None,
    transaction_type: TransactionType | None = None,
    limit: int | None = None,
    load_relationships: bool = True,
) -> list[TransactionRead]:           # ← return schema, not table model
    with self.get_session() as session:
        # ... existing query logic ...
        rows = session.exec(query).all()
    return [TransactionRead.model_validate(row) for row in rows]
```

Do the same for `get_transaction_by_id()`, `get_transactions_by_amount_range()`, `get_transactions_by_date_range()`.

### C3. Exclude `data` field from version table API exposure (RISK-12)

Add a `VersionRead` schema to `app/core/schemas.py`:

```python
class TransactionVersionRead(BaseModel):
    id: int
    transaction_id: int
    version_number: int
    changed_at: datetime
    changed_by: Optional[str] = None
    change_reason: Optional[str] = None
    # Intentionally excluded: data (JSON snapshot — may contain PII)

    model_config = {"from_attributes": True}
```

If a "view version history" endpoint is ever added, it must use this schema, not the raw `UnifiedTransactionVersion`.

---

## Wave D — Version Snapshot Integrity

### D1. Fix `entity_snapshot()` to capture `sa_column` fields (RISK-05)

**File:** `app/core/repository.py`, `entity_snapshot()`

Replace the Pydantic `model_fields` iteration with SQLAlchemy's `inspect()` which reflects all mapped columns:

```python
from sqlalchemy import inspect as sa_inspect

def entity_snapshot(entity: Any) -> dict[str, Any]:
    """Snapshot all mapped columns, including sa_column=Column(...) fields
    that are invisible to Pydantic's model_fields."""
    try:
        mapper = sa_inspect(type(entity))
        column_keys = [col.key for col in mapper.column_attrs]
    except Exception:
        # Fallback to Pydantic for non-ORM objects
        field_names = getattr(entity, "model_fields", None) or entity.__fields__
        column_keys = list(field_names.keys())

    return {k: to_json_safe(getattr(entity, k, None)) for k in column_keys}
```

After this change, version snapshots will include `raw_data`, `description`, `notes`, `metadata_json`, and `amendment_details`. This increases the size of version snapshot rows — consider excluding `raw_data` explicitly since it's already stored on the parent transaction:

```python
_SNAPSHOT_EXCLUDE = frozenset({"raw_data"})   # large field already on parent row

def entity_snapshot(entity: Any) -> dict[str, Any]:
    try:
        mapper = sa_inspect(type(entity))
        column_keys = [col.key for col in mapper.column_attrs
                       if col.key not in _SNAPSHOT_EXCLUDE]
    except Exception:
        field_names = getattr(entity, "model_fields", None) or entity.__fields__
        column_keys = [k for k in field_names.keys() if k not in _SNAPSHOT_EXCLUDE]
    return {k: to_json_safe(getattr(entity, k, None)) for k in column_keys}
```

---

## Wave E — Performance and Correctness

### E1. Guard against duplicate field mappings (RISK-09)

**File:** `app/core/unified_field_library.py`, `_initialize_state_mappings()`

Add a duplicate-key assertion at the end of each state's mapping list initialization:

```python
def _initialize_state_mappings(self):
    # ... build mappings as before ...
    self._validate_state_mappings()

def _validate_state_mappings(self) -> None:
    """Raise ValueError if any state has duplicate state_field entries."""
    for state, mappings in self.state_mappings.items():
        seen: set[str] = set()
        for m in mappings:
            if m.state_field in seen:
                raise ValueError(
                    f"Duplicate state_field {m.state_field!r} in mappings for state {state!r}. "
                    "Each source column must map to exactly one unified field."
                )
            seen.add(m.state_field)
```

Fix the existing duplicate `loanDt` and `loanAmount` entries in the Texas mappings (lines ~455 and ~545 both define them). Keep only the version in the `# Loan / lender fields` block and remove the earlier duplicate in `# Transaction fields`.

Add a unit test:

```python
# tests/test_field_library.py
def test_no_duplicate_state_fields():
    lib = UnifiedFieldLibrary()
    # Constructor calls _validate_state_mappings — if it raises, test fails
    assert lib is not None
```

### E2. Cache `UnifiedSQLModelBuilder` per batch, not per record (RISK-10)

**File:** `app/core/processor.py`, `UnifiedSQLDataProcessor`

The builder is stateless within a (state, state_id) context. Build it once per batch:

```python
class UnifiedSQLDataProcessor:

    def process_record(
        self,
        raw_data: dict[str, Any],
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
        builder: UnifiedSQLModelBuilder | None = None,   # ← accept pre-built builder
    ) -> UnifiedTransaction:
        if builder is None:
            builder = self.get_builder(state, state_id=state_id,
                                       state_code=state_code, session=session)
        # ... rest of processing unchanged ...
```

In `UnifiedStateLoader.process_records_batch()`, build the builder once before the loop:

```python
def process_records_batch(self, records, *, file_path=None, auto_link_officers=False):
    stats = ProcessStats()
    if not records:
        return stats

    file_name = file_path.name if file_path else "batch"

    with self._db_manager.get_session() as session:
        _committees, _persons, state_id, state_code = self._load_batch_indexes(session)
        # ... dedup guard ...

        # Build once per batch instead of once per record
        from app.core.builders import UnifiedSQLModelBuilder
        batch_builder = UnifiedSQLModelBuilder(
            self.state, state_id, state_code, session=session
        )

        for record in records:
            record = dict(record)
            record["state"] = self.state
            record["file_origin"] = file_name
            try:
                transaction = self._persist_transaction_from_record(
                    record, session,
                    state_id=state_id,
                    state_code=state_code,
                    builder=batch_builder,   # ← pass pre-built builder
                )
                # ...
```

Update `_persist_transaction_from_record()` to accept and pass through the `builder` parameter to `unified_sql_processor.process_record()`.

### E3. Scope `get_db_manager()` singleton to CLI entrypoints only (RISK-11)

**File:** `app/core/unified_database.py`

Add a docstring that explicitly warns against test use:

```python
def get_db_manager(
    database_url: str | None = None,
    *,
    echo: bool = False,
    bootstrap: bool = True,
) -> UnifiedDatabaseManager:
    """Return a process-wide cached UnifiedDatabaseManager.

    FOR CLI ENTRYPOINTS ONLY. Tests and application code must inject
    a UnifiedDatabaseManager instance directly to avoid shared state.
    Use reset_db_manager_cache() in test teardown if this is called.
    """
    global _db_manager_cached
    if _db_manager_cached is None:
        _db_manager_cached = UnifiedDatabaseManager(database_url, echo=echo)
        if bootstrap:
            _db_manager_cached.bootstrap()
    return _db_manager_cached
```

In `conftest.py`, ensure `reset_db_manager_cache()` is called in teardown:

```python
@pytest.fixture(autouse=True)
def reset_db_manager():
    yield
    from app.core.unified_database import reset_db_manager_cache
    reset_db_manager_cache()
```

### E4. Document `exclude_unset` limitation for future PATCH handlers (RISK-14)

Add a comment block to `app/core/models/tables.py` at the top of the file:

```python
# PATCH SEMANTICS NOTE (Pydantic v2 + SQLModel table=True):
# SQLAlchemy loads ORM objects by setting attributes via the descriptor protocol,
# bypassing __init__. As a result, model_fields_set is EMPTY for DB-loaded objects.
# model.model_dump(exclude_unset=True) returns {} on any object loaded from the DB.
#
# Correct PATCH pattern:
#   entity = session.get(Model, entity_id)   # load existing
#   for field, value in updates.items():
#       setattr(entity, field, value)         # apply only changed fields
#   session.flush()
#
# INCORRECT pattern (will overwrite all unset fields with defaults):
#   entity = Model(**partial_data)
#   session.merge(entity)
```

---

## Implementation Checklist

Work through waves in order. Run `gitnexus_impact` on each symbol before modifying it. Run `gitnexus_detect_changes()` before each commit.

### Wave A
- [ ] Alembic installed (`uv add alembic`); `alembic init alembic` run
- [ ] `alembic/env.py` imports all table models explicitly
- [ ] `alembic stamp head` run against live DB to baseline
- [ ] `alembic revision --autogenerate -m "baseline_with_indexes"` generated and reviewed
- [ ] All 30 index CREATE statements added to migration manually
- [ ] `alembic upgrade head` applied; verify with `\d unified_transactions` in psql — indexes present
- [ ] `UnifiedReport` imported in `app/core/models/__init__.py`
- [ ] Side-effect import removed from bottom of `processor.py`
- [ ] `bootstrap()` updated to call Alembic

### Wave B
- [ ] `load_data_from_file()`, `save_transactions()`, `load_and_save_file()` deleted from `UnifiedDatabaseManager`
- [ ] Any callers updated to use `UnifiedStateLoader`
- [ ] `@validates` decorators added to `UnifiedAddress`, `UnifiedPerson`, `UnifiedCommittee`
- [ ] `ProcessStats.fatal` field added; fatal DB errors set it to True
- [ ] `_process_data_file()` checks `batch_stats.fatal` and logs appropriately

### Wave C
- [ ] `app/core/schemas.py` created with `TransactionRead`, `TransactionCreate`, `TransactionUpdate`, `PersonRead`, `AddressRead`, `CommitteeRead`, `TransactionVersionRead`
- [ ] `get_transactions()` and related methods return `TransactionRead` not `UnifiedTransaction`
- [ ] `TransactionVersionRead` excludes `data` field

### Wave D
- [ ] `entity_snapshot()` replaced with `sa_inspect(type(entity)).column_attrs` iteration
- [ ] `raw_data` excluded from snapshots via `_SNAPSHOT_EXCLUDE`
- [ ] Spot-check: insert a transaction and update it; confirm `unified_transaction_versions.data` now includes `description`

### Wave E
- [ ] Duplicate `loanDt` / `loanAmount` entries removed from Texas field_library mappings
- [ ] `_validate_state_mappings()` method added and called in constructor
- [ ] `UnifiedSQLModelBuilder` built once per batch in `process_records_batch()`
- [ ] `process_record()` accepts optional `builder` param
- [ ] `get_db_manager()` docstring updated with CLI-only warning
- [ ] `conftest.py` has `reset_db_manager` fixture with `reset_db_manager_cache()` teardown
- [ ] PATCH semantics warning comment added to top of `tables.py`

### Final
- [ ] Run full test suite: `uv run pytest tests/ -v`
- [ ] Run `alembic check` — confirms no unapplied migrations
- [ ] Run `python -c "from app.core.models import UnifiedReport; print('OK')"` without importing processor
- [ ] Run a smoke ingest of 100 Texas contribution rows; confirm `\d unified_transactions` indexes are used (`EXPLAIN ANALYZE SELECT * FROM unified_transactions WHERE state_id = 1 LIMIT 10` should show index scan, not seq scan)
- [ ] Run `gitnexus_detect_changes({scope: "all"})` — confirm only expected files changed
- [ ] Run `npx gitnexus analyze` (or `npx gitnexus analyze --embeddings` if `.gitnexus/meta.json` shows embeddings > 0) to refresh index after commit
