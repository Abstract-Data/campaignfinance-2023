# Model: claude-opus-4-6 (separate PR)

# Task 5b: UUID String → Native uuid Type (SEPARATE PR)

## Phase

Wave 5 — optional, larger scope. SEPARATE PR from task 5a.
Only if context permits (>200K token task per spec).

## Branch

`db-bloat/wave-5/task-5b-uuid-native`

## IMPORTANT

This task is a **separate PR** — do NOT include in the same commit chain as 5a or
any other wave. It is the highest-risk change in the plan.

## Objective

19 models have `uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True)`
— 36-char text with a unique index each. Converting to Postgres native `uuid` (16 bytes)
roughly halves those 19 unique indexes and their heap footprint.

## Mandatory Pre-work

Run `gitnexus_impact` on **every affected model** before starting.
**STOP and report if any returns HIGH or CRITICAL.** Do not proceed without confirming
the blast radius with the user.

Consult Context7 for current SQLModel, SQLAlchemy, and Alembic docs on UUID type migration.

## Implementation

For each model with a `uuid: str` field:
1. Change field type from `str` to `uuid.UUID`
2. Change `default_factory` to `uuid.uuid4` (not wrapped in `str()`)
3. Update all joins/FK references to the field
4. Generate migration with `ALTER COLUMN ... TYPE uuid USING uuid::uuid`

Migration pattern:
```sql
-- upgrade
ALTER TABLE <table> ALTER COLUMN uuid TYPE uuid USING uuid::uuid;

-- downgrade
ALTER TABLE <table> ALTER COLUMN uuid TYPE varchar(36) USING uuid::text;
```

Where an integer PK already provides identity and the surrogate `uuid` is unused,
**prefer dropping the column outright** over converting it.

## Scope Warning

This is a wide change. Before starting:
1. List all 19 affected models
2. Run `gitnexus_impact` on each
3. Report total blast radius to user
4. Get explicit approval before proceeding

## Commit

```
feat: migrate uuid columns from varchar(36) to native uuid type (5b)
```

(in separate PR)

## Checklist

- [ ] All waves 0–4 `*-complete` tags confirmed
- [ ] Task 5a NOT in this branch
- [ ] `gitnexus_impact` run on all 19 affected models
- [ ] User approved blast radius before proceeding
- [ ] Context7 consulted for SQLModel + Alembic uuid migration docs
- [ ] All 19 models converted
- [ ] All FK/join references updated
- [ ] Alembic migration with `USING uuid::uuid` in upgrade
- [ ] Working `downgrade()` with `USING uuid::text`
- [ ] Columns dropped outright where integer PK makes uuid redundant
- [ ] Separate PR opened
