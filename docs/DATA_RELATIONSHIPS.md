# Campaign Finance Data Relationships

This document visualizes how all campaign finance data entities connect to each other, including primary keys, foreign keys, and the flow from raw data to unified models.

## Core Entity Relationship Diagram

```mermaid
erDiagram
    %% ===== REFERENCE TABLES =====
    State {
        int id PK "Auto-increment"
        string code UK "2-char state code (TX, OK)"
        string name "State name"
    }

    FileOrigin {
        string id PK "SHA256(state_id:filename)"
        int state_id FK
        string filename
        datetime created_at
    }

    %% ===== CORE ENTITIES =====
    UnifiedAddress {
        int id PK "Auto-increment"
        string uuid UK "Indexed UUID"
        string street_1
        string street_2
        string city
        string state
        string zip_code
        string county
    }

    UnifiedPerson {
        int id PK "Auto-increment"
        string uuid UK "Indexed UUID"
        int address_id FK
        int state_id FK
        string first_name
        string last_name
        string organization
        string employer
        string occupation
        enum person_type "INDIVIDUAL/ENTITY"
    }

    UnifiedCommittee {
        string filer_id PK "State-assigned ID (stable)"
        string uuid UK "Indexed UUID"
        int address_id FK
        int state_id FK
        string name "Can change over time"
        string committee_type
    }

    UnifiedEntity {
        int id PK "Auto-increment"
        string uuid UK "Indexed UUID"
        int person_id FK "Unique - one-to-one"
        string committee_id FK "Unique - one-to-one"
        int address_id FK
        int state_id FK
        enum entity_type "PERSON/ORG/COMMITTEE/CAMPAIGN"
        string name
        string normalized_name "For matching"
    }

    %% ===== CAMPAIGNS =====
    UnifiedCampaign {
        int id PK "Auto-increment (stable)"
        string uuid UK "Indexed UUID"
        int candidate_person_id FK
        string primary_committee_id FK
        int state_id FK
        string name "Can change over time"
        string normalized_name "For matching"
        int election_year
        string office_sought
        string district
    }

    UnifiedCampaignEntity {
        int id PK
        string uuid UK
        int campaign_id FK
        int entity_id FK
        int state_id FK
        enum role "CANDIDATE/TREASURER/CHAIR/DONOR/VENDOR"
        bool is_primary
        date start_date
        date end_date
    }

    %% ===== TRANSACTIONS =====
    UnifiedTransaction {
        int id PK "Auto-increment"
        string uuid UK "Indexed UUID"
        string committee_id FK "Links to filer"
        int campaign_id FK "Links to campaign"
        int state_id FK
        string file_origin_id FK "Source file tracking"
        string transaction_id "State-assigned ID"
        decimal amount
        date transaction_date
        date filed_date "Report filing date"
        enum transaction_type "CONTRIBUTION/EXPENDITURE/LOAN"
        bool amended
        json raw_data "Original record"
    }

    UnifiedTransactionPerson {
        int id PK
        string uuid UK
        int transaction_id FK
        int person_id FK
        int entity_id FK
        int committee_person_id FK
        int state_id FK
        enum role "CONTRIBUTOR/RECIPIENT/PAYEE"
        decimal amount
    }

    %% ===== CONTRIBUTION & LOAN DETAILS =====
    UnifiedContribution {
        int id PK
        string uuid UK
        int transaction_id FK "Unique - one-to-one"
        int contributor_entity_id FK
        int recipient_entity_id FK
        int state_id FK
        decimal amount
        date receipt_date
        string contribution_type
    }

    UnifiedLoan {
        int id PK
        string uuid UK
        int transaction_id FK "Unique - one-to-one"
        int lender_entity_id FK
        int borrower_entity_id FK
        int state_id FK
        decimal amount
        date loan_date
        date due_date
        decimal interest_rate
    }

    %% ===== COMMITTEE-PERSON RELATIONSHIPS =====
    UnifiedCommitteePerson {
        int id PK
        string uuid UK
        string committee_id FK
        int person_id FK
        int entity_id FK
        int state_id FK
        enum role "TREASURER/CHAIR/CANDIDATE"
        date start_date
        date end_date
        bool is_active
    }

    UnifiedEntityAssociation {
        int id PK
        string uuid UK
        int source_entity_id FK
        int target_entity_id FK
        enum association_type "TREASURER_OF/DONOR_TO/VENDOR_FOR"
        date start_date
        date end_date
    }

    %% ===== RELATIONSHIPS =====
    State ||--o{ FileOrigin : "has files from"
    State ||--o{ UnifiedPerson : "state_id"
    State ||--o{ UnifiedCommittee : "state_id"
    State ||--o{ UnifiedEntity : "state_id"
    State ||--o{ UnifiedCampaign : "state_id"
    State ||--o{ UnifiedTransaction : "state_id"

    FileOrigin ||--o{ UnifiedTransaction : "file_origin_id"

    UnifiedAddress ||--o{ UnifiedPerson : "address_id"
    UnifiedAddress ||--o{ UnifiedCommittee : "address_id"
    UnifiedAddress ||--o{ UnifiedEntity : "address_id"

    UnifiedPerson ||--o| UnifiedEntity : "person_id (one-to-one)"
    UnifiedCommittee ||--o| UnifiedEntity : "committee_id (one-to-one)"

    UnifiedPerson ||--o{ UnifiedCampaign : "candidate_person_id"
    UnifiedCommittee ||--o{ UnifiedCampaign : "primary_committee_id"

    UnifiedCampaign ||--o{ UnifiedCampaignEntity : "campaign_id"
    UnifiedEntity ||--o{ UnifiedCampaignEntity : "entity_id"

    UnifiedCommittee ||--o{ UnifiedTransaction : "committee_id"
    UnifiedCampaign ||--o{ UnifiedTransaction : "campaign_id"

    UnifiedTransaction ||--o| UnifiedContribution : "transaction_id (one-to-one)"
    UnifiedTransaction ||--o| UnifiedLoan : "transaction_id (one-to-one)"
    UnifiedTransaction ||--o{ UnifiedTransactionPerson : "transaction_id"

    UnifiedPerson ||--o{ UnifiedTransactionPerson : "person_id"
    UnifiedEntity ||--o{ UnifiedTransactionPerson : "entity_id"

    UnifiedCommittee ||--o{ UnifiedCommitteePerson : "committee_id"
    UnifiedPerson ||--o{ UnifiedCommitteePerson : "person_id"
    UnifiedEntity ||--o{ UnifiedCommitteePerson : "entity_id"

    UnifiedEntity ||--o{ UnifiedContribution : "contributor_entity_id"
    UnifiedEntity ||--o{ UnifiedContribution : "recipient_entity_id"
    UnifiedEntity ||--o{ UnifiedLoan : "lender_entity_id"
    UnifiedEntity ||--o{ UnifiedLoan : "borrower_entity_id"

    UnifiedEntity ||--o{ UnifiedEntityAssociation : "source_entity_id"
    UnifiedEntity ||--o{ UnifiedEntityAssociation : "target_entity_id"
```

## Data Flow: From Raw State Data to Unified Models

```mermaid
flowchart TB
    subgraph StateData["State-Specific Data (Texas Example)"]
        direction TB
        TECFiler["TECFiler<br/>filerIdent (PK)"]
        TECCover["TECCoverSheet1<br/>report_info_ident (PK)<br/>filerIdent (FK)"]
        TECContrib["TECContribution<br/>contribution_info_id (PK)<br/>report_info_ident (FK)"]
        TECExpend["TECExpenditure<br/>expend_info_id (PK)<br/>report_info_ident (FK)"]
        TECLoan["TECLoan<br/>loan_info_id (PK)<br/>report_info_ident (FK)"]
    end

    subgraph UnifiedData["Unified Data Layer"]
        direction TB
        UComm["UnifiedCommittee<br/>filer_id = TECFiler.filerIdent"]
        UCampaign["UnifiedCampaign<br/>id (stable PK)<br/>primary_committee_id (FK)"]
        UTrans["UnifiedTransaction<br/>committee_id (FK)<br/>campaign_id (FK)<br/>file_origin_id (FK)"]
        UContrib["UnifiedContribution<br/>transaction_id (FK)"]
        ULoan["UnifiedLoan<br/>transaction_id (FK)"]
    end

    TECFiler -->|"filerIdent maps to"| UComm
    TECCover -->|"Links filer to report period"| UTrans
    TECContrib -->|"Normalized to"| UTrans
    TECContrib -->|"Details stored in"| UContrib
    TECExpend -->|"Normalized to"| UTrans
    TECLoan -->|"Normalized to"| UTrans
    TECLoan -->|"Details stored in"| ULoan
    UComm -->|"primary_committee_id"| UCampaign
    UCampaign -->|"campaign_id"| UTrans
```

## Report → Campaign → Committee Connection

This diagram shows how reports connect to campaigns and how campaign names can change while IDs remain stable.

```mermaid
flowchart LR
    subgraph Reports["Reports/Filings"]
        FileOrigin["FileOrigin<br/>id: SHA256(state:filename)<br/>filename: contributions_2024.csv"]
        FiledDate["filed_date on Transaction<br/>(from state cover sheet)"]
    end

    subgraph Transactions["Transactions"]
        Trans1["UnifiedTransaction<br/>id: 12345<br/>file_origin_id → FileOrigin<br/>committee_id → Committee<br/>campaign_id → Campaign"]
    end

    subgraph Campaign["Campaign (Stable ID)"]
        Camp["UnifiedCampaign<br/>id: 100 (STABLE)<br/>name: 'Smith for Senate 2024'<br/>normalized_name: 'smith_senate'<br/>election_year: 2024<br/>office_sought: 'Senate'"]
        CampV1["Previous Name:<br/>'John Smith Campaign'"]
        CampV2["Current Name:<br/>'Smith for Senate 2024'"]
    end

    subgraph Committee["Committee (Stable filer_id)"]
        Comm["UnifiedCommittee<br/>filer_id: 'TX-12345' (STABLE)<br/>name: 'Friends of Smith'"]
        CommV1["Previous Name:<br/>'Smith Election Fund'"]
        CommV2["Current Name:<br/>'Friends of Smith'"]
    end

    FileOrigin -->|"file_origin_id"| Trans1
    Trans1 -->|"committee_id"| Comm
    Trans1 -->|"campaign_id"| Camp
    Comm -->|"primary_committee_id"| Camp
    
    CampV1 -.->|"Name changed but id=100 stays same"| CampV2
    CommV1 -.->|"Name changed but filer_id stays same"| CommV2
```

## Key Identifier Stability

| Entity | Stable Identifier | Mutable Fields | How Names Change |
|--------|------------------|----------------|------------------|
| **State** | `id` (int) | - | States don't change |
| **Committee** | `filer_id` (string from state) | `name`, `committee_type`, `address_id` | Versioned in `UnifiedCommitteeVersion` |
| **Campaign** | `id` (auto-increment) | `name`, `office_sought`, `district` | Lookup uses `normalized_name` + `primary_committee_id` + `election_year` |
| **Person** | `id` (auto-increment) | `first_name`, `last_name`, `employer`, `occupation` | Versioned in `UnifiedPersonVersion` |
| **Transaction** | `id` (auto-increment) + `transaction_id` (state) | `amount`, `description`, `amended` | Versioned in `UnifiedTransactionVersion` |
| **Address** | `id` (auto-increment) | All fields | Versioned in `UnifiedAddressVersion` |

## Campaign Lookup Logic

When matching records to campaigns, the system uses this priority:

```mermaid
flowchart TD
    Start["New Transaction Record"] --> Extract["Extract campaign info:<br/>- committee filer_id<br/>- candidate name<br/>- election year<br/>- office sought"]
    
    Extract --> Normalize["Normalize name to<br/>normalized_name"]
    
    Normalize --> Search["Search existing campaigns by:<br/>1. normalized_name<br/>2. primary_committee_id<br/>3. candidate_person_id<br/>4. election_year"]
    
    Search --> Found{Campaign<br/>Found?}
    
    Found -->|Yes| Reuse["Reuse existing<br/>campaign.id<br/>(stable)"]
    Found -->|No| Create["Create new campaign<br/>with new id"]
    
    Reuse --> Link["Link transaction to<br/>campaign_id"]
    Create --> Link
    
    Link --> Update["Update campaign.name<br/>if changed<br/>(id stays same)"]
```

## Entity Deduplication Layer

The `UnifiedEntity` table serves as a deduplication layer connecting persons and committees to transactions:

```mermaid
flowchart TD
    subgraph Sources["Source Records"]
        Contrib1["Contribution from<br/>'JOHN SMITH'<br/>123 Main St"]
        Contrib2["Contribution from<br/>'John Smith'<br/>123 Main Street"]
        Contrib3["Contribution from<br/>'J. Smith'<br/>123 Main St."]
    end

    subgraph Normalization["Normalization"]
        NormName["Normalized Name:<br/>'john_smith'"]
        NormAddr["Normalized Address:<br/>'123 main st'"]
    end

    subgraph UnifiedEntities["Unified Entity Layer"]
        Person["UnifiedPerson<br/>id: 500<br/>first_name: 'John'<br/>last_name: 'Smith'"]
        Entity["UnifiedEntity<br/>id: 800<br/>person_id: 500<br/>normalized_name: 'john_smith'"]
    end

    subgraph Transactions["Transactions"]
        Trans1["Transaction 1<br/>contributor → Entity 800"]
        Trans2["Transaction 2<br/>contributor → Entity 800"]
        Trans3["Transaction 3<br/>contributor → Entity 800"]
    end

    Contrib1 --> NormName
    Contrib2 --> NormName
    Contrib3 --> NormName
    Contrib1 --> NormAddr
    Contrib2 --> NormAddr
    Contrib3 --> NormAddr
    
    NormName --> Person
    NormAddr --> Person
    Person --> Entity
    
    Entity --> Trans1
    Entity --> Trans2
    Entity --> Trans3
```

## Texas-Specific → Unified Mapping

```mermaid
flowchart LR
    subgraph Texas["Texas Data (TEC)"]
        direction TB
        TXFiler["TECFiler<br/>filerIdent: 12345<br/>filerName: 'Smith PAC'"]
        TXCover["TECCoverSheet1<br/>report_info_ident: R001<br/>filerIdent: 12345<br/>filed_dt: 2024-01-15<br/>period_start_dt: 2024-01-01<br/>period_end_dt: 2024-03-31"]
        TXContrib["TECContribution<br/>contribution_info_id: C001<br/>report_info_ident: R001<br/>contributorNameLast: JONES<br/>contributionAmount: 500.00"]
    end

    subgraph Unified["Unified Data"]
        direction TB
        UComm["UnifiedCommittee<br/>filer_id: 'TX-12345'<br/>name: 'Smith PAC'"]
        UTrans["UnifiedTransaction<br/>committee_id: 'TX-12345'<br/>transaction_id: 'TX-C001'<br/>amount: 500.00<br/>filed_date: 2024-01-15"]
        UPerson["UnifiedPerson<br/>last_name: 'JONES'"]
        UEntity["UnifiedEntity<br/>person_id → UPerson"]
        UContrib["UnifiedContribution<br/>transaction_id → UTrans<br/>contributor_entity_id → UEntity"]
    end

    TXFiler -->|"Field mapping:<br/>filerIdent → filer_id<br/>filerName → name"| UComm
    TXCover -->|"Filed date metadata"| UTrans
    TXContrib -->|"Field mapping:<br/>contributionAmount → amount<br/>contributorNameLast → last_name"| UTrans
    TXContrib --> UPerson
    UPerson --> UEntity
    UEntity --> UContrib
    UTrans --> UContrib
```

## Version History Tracking

Name changes and other modifications are tracked via version tables:

```mermaid
flowchart TB
    subgraph Current["Current State"]
        Comm["UnifiedCommittee<br/>filer_id: 'TX-12345'<br/>name: 'Friends of Smith 2024'"]
    end

    subgraph History["Version History"]
        V1["UnifiedCommitteeVersion<br/>version: 1<br/>data: {name: 'Smith PAC'}<br/>changed_at: 2022-01-01"]
        V2["UnifiedCommitteeVersion<br/>version: 2<br/>data: {name: 'Smith for Texas'}<br/>changed_at: 2023-06-15"]
        V3["UnifiedCommitteeVersion<br/>version: 3<br/>data: {name: 'Friends of Smith 2024'}<br/>changed_at: 2024-01-01"]
    end

    V1 --> V2 --> V3
    V3 -.->|"Current version"| Comm
```

## Texas Record Types: Coverage Analysis

This section shows what Texas data files exist vs. what gets processed into the unified layer.

### Texas File Types (from TEC_CF_CSV.zip)

| File Prefix | Record Type | Description |
|-------------|-------------|-------------|
| `contribs` | RCPT | Monetary and in-kind contributions |
| `expend` | EXPN | Expenditures and disbursements |
| `loans` | LOAN | Loans received by campaigns |
| `pledges` | PLDG | Pledges (promised contributions) |
| `debts` | DEBT | Outstanding debts |
| `credits` | CRED | Credits (refunds/returns) |
| `travel` | TRVL | Travel expense details |
| `cand` | CAND | Candidate-related expenditures |
| `assets` | ASSET | Campaign assets |
| `filers` | FILER | Committee/filer registration |
| `finals` | FINL | Final reports |
| `spacs` | SPAC | Specific-purpose PACs |
| `covr1` | CVR1 | Cover sheet (report metadata) |

### Processing Status: Texas → Unified

```mermaid
flowchart LR
    subgraph Texas["Texas State Models<br/>(Fully Defined)"]
        direction TB
        TXContrib["TECContribution<br/>RCPT"]
        TXExpend["TECExpenditure<br/>EXPN"]
        TXLoan["TECLoan<br/>LOAN"]
        TXPledge["TECPledge<br/>PLDG"]
        TXDebt["TECDebt<br/>DEBT"]
        TXCredit["TECCredit<br/>CRED"]
        TXTravel["TECTravel<br/>TRVL"]
        TXCand["TECCandidate<br/>CAND"]
        TXAsset["TECAsset<br/>ASSET"]
        TXFiler["TECFiler<br/>FILER"]
        TXCover["TECCoverSheet1<br/>CVR1"]
    end

    subgraph Unified["Unified Layer"]
        direction TB
        UTrans["UnifiedTransaction"]
        UContrib["UnifiedContribution"]
        ULoan["UnifiedLoan"]
        UDebt["UnifiedDebt"]
        UCredit["UnifiedCredit"]
        UTravel["UnifiedTravel"]
        UAsset["UnifiedAsset"]
        UComm["UnifiedCommittee"]
        UPerson["UnifiedPerson"]
    end

    TXContrib -->|"✅"| UTrans
    TXContrib -->|"✅"| UContrib
    TXExpend -->|"✅"| UTrans
    TXLoan -->|"✅"| UTrans
    TXLoan -->|"✅"| ULoan
    TXPledge -->|"✅"| UTrans
    TXDebt -->|"✅"| UTrans
    TXDebt -->|"✅"| UDebt
    TXCredit -->|"✅"| UTrans
    TXCredit -->|"✅"| UCredit
    TXTravel -->|"✅"| UTrans
    TXTravel -->|"✅"| UTravel
    TXCand -->|"⚠️"| UTrans
    TXAsset -->|"✅"| UTrans
    TXAsset -->|"✅"| UAsset
    TXFiler -->|"✅"| UComm
    TXCover -->|"⚠️"| UTrans
```

### Detailed Status Table

| Texas Model | Code | Unified TransactionType | Unified Detail Table | Status | Notes |
|-------------|------|------------------------|---------------------|--------|-------|
| `TECContribution` | RCPT | `CONTRIBUTION` | `UnifiedContribution` | ✅ **FULL** | Contributor, recipient, amount tracked |
| `TECExpenditure` | EXPN | `EXPENDITURE` | - | ✅ **FULL** | Payee, purpose, amount tracked |
| `TECLoan` | LOAN | `LOAN` | `UnifiedLoan` | ✅ **FULL** | Lender, borrower, terms tracked |
| `TECPledge` | PLDG | `PLEDGE` | - | ✅ **FULL** | Type detected, maps to unified transaction |
| `TECDebt` | DEBT | `DEBT` | `UnifiedDebt` | ✅ **FULL** | Creditor, debtor, guarantor info tracked |
| `TECCredit` | CRED | `CREDIT` | `UnifiedCredit` | ✅ **FULL** | Payor, recipient, credit type tracked |
| `TECTravel` | TRVL | `TRAVEL` | `UnifiedTravel` | ✅ **FULL** | Traveler, itinerary, purpose tracked |
| `TECCandidate` | CAND | `EXPENDITURE` | - | ⚠️ **PARTIAL** | Processed as expenditures |
| `TECAsset` | ASSET | `ASSET` | `UnifiedAsset` | ✅ **FULL** | Asset type, valuation, disposition tracked |
| `TECFiler` | FILER | - | `UnifiedCommittee` | ✅ **FULL** | Maps to committee |
| `TECCoverSheet1` | CVR1 | - | - | ⚠️ **PARTIAL** | Report dates → transaction.filed_date |
| `SPAC` | SPAC | - | - | ❌ **NO MODEL** | File exists, no model defined |

### Texas-Specific Data Flow

```mermaid
flowchart TB
    subgraph Download["Downloaded Data (tmp/texas/)"]
        direction LR
        ZIP["TEC_CF_CSV.zip"]
    end

    subgraph Files["Extracted Files"]
        direction TB
        F1["contribs_01.csv..."]
        F2["expend_01.csv..."]
        F3["loans_01.csv..."]
        F4["pledges_01.csv..."]
        F5["debts_01.csv..."]
        F6["credits_01.csv..."]
        F7["travel_01.csv..."]
        F8["cand_01.csv..."]
        F9["assets_01.csv..."]
        F10["filers_01.csv..."]
        F11["covr1_01.csv..."]
        F12["finals_01.csv..."]
        F13["spacs_01.csv..."]
    end

    subgraph TexasSchema["Texas Schema (tx_*)"]
        direction TB
        TX1["tx_contributions"]
        TX2["tx_expenditures"]
        TX3["tx_loans"]
        TX4["tx_pledges"]
        TX5["tx_debts"]
        TX6["tx_credits"]
        TX7["tx_travel"]
        TX8["tx_candidates"]
        TX9["tx_assets"]
        TX10["tx_filers"]
        TX11["tx_cover_sheet1"]
    end

    subgraph UnifiedSchema["Unified Schema"]
        direction TB
        U1["unified_transactions"]
        U2["unified_contributions"]
        U3["unified_loans"]
        U4["unified_committees"]
        U5["unified_persons"]
        U6["unified_addresses"]
    end

    ZIP --> Files
    F1 --> TX1 --> U1
    TX1 --> U2
    F2 --> TX2 --> U1
    F3 --> TX3 --> U1
    TX3 --> U3
    F4 --> TX4 --> U1
    F5 --> TX5
    F6 --> TX6
    F7 --> TX7
    F8 --> TX8
    F9 --> TX9
    F10 --> TX10 --> U4
    F11 --> TX11
    F12 --> TX11
    F13 -.->|"No model"| Files

    TX5 -.->|"❌ Not unified"| TexasSchema
    TX6 -.->|"❌ Not unified"| TexasSchema
    TX7 -.->|"❌ Not unified"| TexasSchema
    TX8 -.->|"❌ Not unified"| TexasSchema
    TX9 -.->|"❌ Not unified"| TexasSchema
```

### Unified Layer Coverage

The following unified models are now available for cross-state analysis:

#### ✅ **Fully Supported Record Types**

| Record Type | Unified Model | Key Features |
|-------------|---------------|--------------|
| **Travel** (TRVL) | `UnifiedTravel` | Traveler, itinerary (departure/arrival cities), transportation type, purpose |
| **Debt** (DEBT) | `UnifiedDebt` | Creditor, debtor, amount, due date, guarantor info, payment status |
| **Credit** (CRED) | `UnifiedCredit` | Payor, recipient, credit type, related transaction |
| **Asset** (ASSET) | `UnifiedAsset` | Asset type, description, acquisition/valuation info, disposition |

#### ⚠️ **Partially Supported**

| Record Type | Status | Notes |
|-------------|--------|-------|
| **Candidate Data** (CAND) | Processed as expenditures | Candidate-specific office details may be lost |
| **Cover Sheets** (CVR1) | Report metadata only | Only `filed_date` extracted to transactions |

#### ❌ **No Model Defined**

| Record Type | Issue |
|-------------|-------|
| **SPAC Records** | File `spacs_*.csv` exists but no model defined (neither Texas nor Unified) |

### Cover Sheet (Report) Relationships

The `TECCoverSheet1` model contains critical report-level data that links everything together:

```mermaid
flowchart TB
    subgraph CoverSheet["TECCoverSheet1 (Report)"]
        direction TB
        CVR["report_info_ident (PK)<br/>filer_ident (FK)<br/>filed_dt<br/>period_start_dt<br/>period_end_dt<br/>total_contrib_amount<br/>total_expend_amount<br/>loan_balance_amount"]
    end

    subgraph Transactions["All Transaction Types"]
        direction TB
        RCPT["TECContribution<br/>report_info_ident (FK)"]
        EXPN["TECExpenditure<br/>report_info_ident (FK)"]
        LOAN["TECLoan<br/>report_info_ident (FK)"]
        PLDG["TECPledge<br/>report_info_ident (FK)"]
        DEBT["TECDebt<br/>report_info_ident (FK)"]
        CRED["TECCredit<br/>report_info_ident (FK)"]
        TRVL["TECTravel<br/>report_info_ident (FK)"]
        CAND["TECCandidate<br/>report_info_ident (FK)"]
        ASSET["TECAsset<br/>report_info_ident (FK)"]
    end

    subgraph Filer["TECFiler"]
        FLR["filer_ident (PK)<br/>filer_name<br/>filer_type_cd<br/>treasurer_person<br/>chair_person"]
    end

    FLR --> CVR
    CVR --> RCPT
    CVR --> EXPN
    CVR --> LOAN
    CVR --> PLDG
    CVR --> DEBT
    CVR --> CRED
    CVR --> TRVL
    CVR --> CAND
    CVR --> ASSET
```

## Summary

### Primary Keys (Stable Identifiers)
- **State**: `id` (int)
- **Committee**: `filer_id` (string from state systems - e.g., "TX-12345")
- **Campaign**: `id` (auto-increment int)
- **Person**: `id` (auto-increment int)
- **Entity**: `id` (auto-increment int)
- **Transaction**: `id` (auto-increment int)
- **Address**: `id` (auto-increment int)

### Key Foreign Key Relationships
1. **Transaction → Committee**: `committee_id` → `UnifiedCommittee.filer_id`
2. **Transaction → Campaign**: `campaign_id` → `UnifiedCampaign.id`
3. **Transaction → FileOrigin**: `file_origin_id` → `FileOrigin.id`
4. **Campaign → Committee**: `primary_committee_id` → `UnifiedCommittee.filer_id`
5. **Campaign → Person**: `candidate_person_id` → `UnifiedPerson.id`
6. **Entity → Person/Committee**: `person_id`/`committee_id` (deduplication layer)
7. **Contribution/Loan → Transaction**: `transaction_id` (one-to-one)

### How Names Stay Connected to IDs
1. **Stable IDs**: `filer_id` (committees) and `id` (campaigns, persons) never change
2. **Normalized names**: Used for matching/lookup (e.g., `normalized_name`)
3. **Version tables**: Store JSON snapshots of historical changes
4. **Lookup logic**: Matches on multiple criteria (committee + candidate + year) to find existing records

### Coverage Summary

| Fully Unified | Partially Unified | No Model |
|--------------|-------------------|----------|
| Contributions | Candidates | SPACs |
| Expenditures | Cover Sheets | |
| Loans | | |
| Debts | | |
| Credits | | |
| Travel | | |
| Assets | | |
| Pledges | | |
| Filers/Committees | | |

### New Unified Detail Tables (Added)

The following new unified detail tables were added to support cross-state analysis:

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `unified_debts` | Track outstanding campaign debts | creditor, debtor, amount, due_date, guarantor info |
| `unified_credits` | Track credits/refunds | payor, recipient, credit_type, related_transaction |
| `unified_travel` | Track travel expenses | traveler, itinerary, transportation, purpose |
| `unified_assets` | Track campaign assets | asset_type, valuation, disposition info |
