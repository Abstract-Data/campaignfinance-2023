# Architecture diagram (post Wave 3 split)

## Pipeline component flow

```mermaid
flowchart LR
  subgraph acquire [Acquisition]
    DL[State downloaders]
    CV[Texas CSV to Parquet]
  end
  subgraph ingest [Ingest]
    GR[GenericFileReader]
    VAL[State validators ABC]
  end
  subgraph unify [Unified core]
    BLD[builders.py]
    PRC[processor.py]
    DB[(PostgreSQL / SQLite)]
  end
  subgraph resolve [Entity resolution]
    STD[standardize stages]
    PUB[publish / crosswalk]
  end
  DL --> CV --> GR --> VAL --> BLD --> PRC --> DB
  DB --> STD --> PUB
```

## Unified schema (core tables)

```mermaid
erDiagram
  states ||--o{ unified_committees : hosts
  unified_committees ||--o{ unified_transactions : files
  unified_transactions ||--o| unified_contributions : detail
  unified_transactions ||--o| unified_loans : detail
  unified_persons ||--o{ unified_transaction_persons : role
  unified_transactions ||--o{ unified_transaction_persons : links
  unified_addresses ||--o{ unified_persons : located
```

## Module map (`app/core/`)

| Module | Purpose |
|--------|---------|
| `enums.py` | Shared enumerations (`TransactionType`, `PersonRole`, …) |
| `constants.py` | Record-type codes, amount buckets, placeholder names |
| `models/tables.py` | SQLModel table definitions |
| `builders.py` | State record → unified entity builders |
| `processor.py` | Orchestrates builders + detail registry |
| `value_objects.py` | Pure `PersonName`, `AddressParts`, `Officer` types |
| `unified_database.py` | Persistence, versioning, analysis queries |
