# `app/core/` — unified cross-state model

The unified core normalizes state-specific campaign-finance records into shared
SQLModel tables. Wave 3 split the former monolithic `unified_sqlmodels.py` into
focused modules; this README is the onboarding index.

## Data flow

```
State validator record (dict)
        │
        ▼
unified_field_library  ── maps state field names
        │
        ▼
UnifiedSQLModelBuilder (builders.py)  ── creates Address, Person, Entity, Transaction
        │
        ▼
processor.py  ── DETAIL_BUILDERS registry attaches contribution/loan/debt/… detail rows
        │
        ▼
unified_database.py  ── persist with dedup caches + optional versioning
```

## Module guide

| File / package | When to read it |
|----------------|-----------------|
| `enums.py` | Adding a transaction type, person role, or entity type |
| `constants.py` | Record-type codes, amount buckets, shared SQLAlchemy types |
| `models/tables.py` | Schema changes — every unified table lives here |
| `builders.py` | How a state row becomes unified entities and transactions |
| `processor.py` | Batch/stream entry points (`process_record_stream`, `ProcessStats`) |
| `value_objects.py` | Parsing helpers (`PersonName`, `AddressParts`) used by builders |
| `unified_field_library.py` | Registering a new state field mapping |
| `unified_state_loader.py` | Full load orchestration (committees, relationships) |
| `unified_database.py` | Session management, analysis queries, version snapshots |
| `source_models/` | Secondary record types (reports, pledges, lookups, notices, SPAC) |

## Adding a new state (checklist)

1. Implement validators under `app/states/{state}/` inheriting ABC patterns.
2. Register field mappings in `unified_field_library.py`.
3. Extend `builders.py` only if the state needs custom entity logic (prefer config).
4. Add characterization tests under `tests/` mirroring Texas/Oklahoma patterns.
5. Document state quirks in `docs/STATES.md`.

## Related docs

- `docs/ARCHITECTURE-DIAGRAM.md` — pipeline and ERD diagrams
- `docs/DATA_RELATIONSHIPS.md` — full schema relationships
- `docs/DATA_DICTIONARY.md` — field definitions
