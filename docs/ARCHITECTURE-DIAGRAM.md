# Architecture diagram

Visual reference for the campaignfinance pipeline and the post–Wave-3 unified
core layout. For prose module descriptions see `docs/ARCHITECTURE.md`; for the
full multi-layer ERD (unified → resolution → canonical → publish views) see
`docs/DATA_RELATIONSHIPS.md`.

**Last updated:** 2026-05-25 (TASK-5d)

---

## End-to-end pipeline

```mermaid
flowchart TD
  subgraph acquire [Acquisition — app/states, app/workflows]
    PORT[State ethics portals]
    DL[FileDownloader / Selenium]
    PORT -->|CSV zip| DL
  end

  subgraph cli [Operator CLI — app/cli cf]
    CV[convert — CSV to parquet]
    VF[verify — coverage gate]
    PR[prepare — download convert verify]
    PR --> CV --> VF
  end

  subgraph ingest [Ingest and validate — app/ingest, app/abcs]
    GR[GenericFileReader]
    VAL[StateFileValidation SQLModel]
    GR --> VAL
  end

  subgraph unify [Unified core — app/core]
    FL[unified_field_library]
    BLD[builders.UnifiedSQLModelBuilder]
    PRC[processor — process_record_stream]
    FL --> BLD --> PRC
  end

  subgraph source [Source layer — app/core/source_models]
    RPT[reports pledges lookups notices spac ingest]
  end

  subgraph load [Load — scripts/loaders]
    PL[production_loader — Polars scan batches]
    DB[(PostgreSQL / SQLite)]
    PL --> DB
  end

  subgraph resolve [Entity resolution — app/resolve]
    STD[standardize]
    BLK[blocking]
    FP[fastpath deterministic]
    SC[Splink score]
    CL[classify]
    CLU[cluster]
    SUR[survivorship publish]
    REV[merge_review CLI]
    STD --> BLK --> FP
    BLK --> SC --> CL --> CLU --> SUR
    SC --> REV
  end

  DL --> PR
  VF --> GR
  VAL --> BLD
  PRC --> PL
  RPT --> DB
  DB --> STD
  SUR --> DB
```

### Stage summary

| Stage | Entry point | Output |
|-------|-------------|--------|
| Download | `uv run cf download texas` | Raw CSV under `tmp/{state}/` |
| Convert | `uv run cf convert texas` | Parquet (all-string schema, `infer_schema_length=0`) |
| Verify | `uv run cf verify texas` | Coverage table; non-zero exit if required types missing |
| Validate | State validators + `GenericFileReader` | Typed dicts / SQLModel rows per record |
| Unify | `UnifiedSQLModelBuilder` + `processor` | `UnifiedTransaction`, detail rows, entities |
| Load | `scripts/loaders/production_loader.py` | Rows in unified + source tables |
| Resolve | `uv run python -m app.resolve` | Canonical entities, crosswalks, publish views |

---

## Unified schema ERD (core tables)

Field names match `app/core/models/tables.py` (Wave-3 split). Detail tables
(`unified_loans`, `unified_debts`, …) hang off `unified_transactions` 1:1;
version tables omitted for clarity.

```mermaid
erDiagram
  states {
    int id PK
    string code UK
    string name UK
  }

  file_origins {
    string id PK
    int state_id FK
    string filename
    datetime created_at
  }

  unified_addresses {
    int id PK
    string uuid UK
    string street_1
    string street_2
    string city
    string state
    string zip_code
  }

  unified_persons {
    int id PK
    string uuid UK
    string first_name
    string last_name
    int address_id FK
    int state_id FK
  }

  unified_committees {
    string filer_id PK
    string uuid UK
    string name
    int address_id FK
    int state_id FK
  }

  unified_entities {
    int id PK
    string uuid UK
    string entity_type
    int person_id FK
    string committee_id FK
    int address_id FK
  }

  unified_transactions {
    int id PK
    string uuid UK
    string transaction_id
    decimal amount
    date transaction_date
    string transaction_type
    string committee_id FK
    int state_id FK
    string file_origin_id FK
  }

  unified_contributions {
    int id PK
    int transaction_id FK
    int contributor_entity_id FK
    int recipient_entity_id FK
  }

  unified_transaction_persons {
    int id PK
    int transaction_id FK
    int person_id FK
    int entity_id FK
    string role
  }

  states ||--o{ file_origins : has
  states ||--o{ unified_transactions : has
  states ||--o{ unified_persons : has
  states ||--o{ unified_committees : has
  unified_addresses ||--o{ unified_persons : located
  unified_addresses ||--o{ unified_entities : located
  unified_committees ||--o{ unified_transactions : files
  unified_persons ||--o| unified_entities : represents
  unified_committees ||--o| unified_entities : represents
  unified_transactions ||--o| unified_contributions : detail
  unified_transactions ||--o{ unified_transaction_persons : involves
  unified_entities ||--o{ unified_contributions : contributor
  unified_entities ||--o{ unified_contributions : recipient
```

---

## Module map (`app/core/`)

| Module | Purpose |
|--------|---------|
| `enums.py` | Domain enumerations — `TransactionType`, `PersonRole`, `EntityType`, … |
| `constants.py` | `RECORD_TYPE_CODES`, `PLACEHOLDER_NAMES`, `AMOUNT_BUCKETS`, `MONEY_TYPE` |
| `models/tables.py` | SQLModel table definitions (unified + reference tables) |
| `builders.py` | State record → unified entity builders (`UnifiedSQLModelBuilder`) |
| `processor.py` | `DETAIL_BUILDERS` registry, `process_record` / `process_record_stream` |
| `value_objects.py` | Pure types — `PersonName`, `AddressParts`, `Officer` |
| `unified_field_library.py` | Cross-state field name → unified field mapping |
| `unified_database.py` | Sessions, persistence, versioning, analysis queries |
| `unified_state_loader.py` | State-record → unified-record orchestration |
| `source_models/` | Immutable source-layer ingest for reports, pledges, lookups, notices, SPAC |

See `app/core/README.md` for onboarding notes on the Wave-3 split modules.

---

## Related documents

- `docs/ARCHITECTURE.md` — narrative architecture and cross-cutting patterns
- `docs/DATA_RELATIONSHIPS.md` — full ERD including resolution and canonical layers
- `docs/adr/0002-data-classification-and-retention.md` — PII classification (R12)
- `docs/adr/0003-ai-governance-entity-resolution.md` — Splink governance (R3)
