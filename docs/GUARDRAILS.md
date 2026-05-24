# GUARDRAILS.md

Persistent safety constraints for the campaignfinance pipeline. Load and apply
this document for any schema, data-loading, scraping, or validation work.
`AGENTS.md` references this file; respect every Sign below.

## Privilege Boundaries

What an agent may do autonomously vs. what requires a human:

| Action | Autonomy |
|--------|----------|
| Read source, docs, fixture data | Autonomous |
| Run tests, lint, type checks | Autonomous |
| Edit `app/`, `scripts/`, `tests/`, `docs/` on a feature branch | Autonomous |
| Add a dependency (`uv add`) | Ask first |
| Change the database schema or a SQLModel definition | Ask first |
| Modify a unified field mapping (affects all states) | Ask first |
| Alter an ABC interface | Ask first |
| Run a loader against **production PostgreSQL** | Human only |
| Write to, drop, or truncate any production table | Human only |
| Commit secrets, `.env`, or `texas.env` | Never |

## Rate Limiting — State Portal Scraping

Selenium downloads hit live state campaign-finance portals.

- Respect each portal's implied rate limits — do not parallelize requests
  against a single state portal.
- Use the existing exponential-backoff download-wait logic; do not replace it
  with tight retry loops.
- Cache downloaded files in `tmp/{state}/` and reuse them — never re-download
  data already present for the same period.
- If a portal returns repeated errors or CAPTCHAs, **stop** and escalate; do not
  hammer the endpoint.

## Error Classification

| Class | Examples | Response |
|-------|----------|----------|
| **Retryable** | Network timeout, portal 5xx, transient DB connection loss | Retry with backoff (bounded attempts), then escalate |
| **Fatal** | Schema mismatch, unmapped field, validation failure, malformed file | Stop, log row counts, surface to a human — do not "fix" by dropping data |
| **Silent-danger** | Row count shrinks across a stage, dedup collapses distinct records | Treat as fatal: halt and report; never proceed on suspected data loss |

## PII Handling

Campaign-finance records contain personal data — donor and filer **names,
home addresses, phone numbers, employers**.

- Never log full PII. The `Logger` class output (PaperTrail + local) must not
  contain donor names/addresses — log record IDs and counts instead.
- Never write PII to non-gitignored files. Generated `*.csv` / `*.txt` /
  `*.parquet` are gitignored — keep it that way.
- PII belongs only in the database and in `tmp/` source files. Do not paste it
  into docs, ADRs, commit messages, or Notion.
- When sharing sample data in tests, use the existing fixtures — do not add new
  real records as fixtures.

## Escalation Rules

Stop and request human help when:

- The same error occurs 3+ times in a row, or tool calls loop without progress.
- A stage's output row count differs materially from its input without an
  explained filter.
- A change would touch production data, an ABC interface, or a cross-state
  field mapping.
- A state portal's file format appears to have changed (new/renamed columns).
- You are about to proceed despite an unresolved data-integrity warning.

When escalating, append a Sign below describing the pattern, then stop.

## Signs Architecture

A **Sign** is a recorded failure pattern. Each has:

- **Trigger** — the observable condition.
- **Instruction** — what to do when the trigger fires.
- **Reason** — why this matters.
- **Provenance** — where the Sign came from.

## Initial Signs

### Sign 1 — Unmapped state column
- **Trigger:** A state file contains a header with no entry in the unified field library.
- **Instruction:** Stop. Do not drop the column. Add the mapping to the unified field library (ask first — it affects all states) or escalate.
- **Reason:** Silently dropping a column loses campaign-finance data and corrupts cross-state analysis.
- **Provenance:** Initial — derived from the schema-driven ingestion design.

### Sign 2 — String-interpolated SQL
- **Trigger:** A query is built with an f-string, `.format()`, `%`, or `+` concatenation.
- **Instruction:** Replace with a parameterized SQLModel/SQLAlchemy query before proceeding. The `sql-injection-check.sh` hook blocks this.
- **Reason:** SQL injection risk; security overrides convenience.
- **Provenance:** Initial — Abstract Data Python playbook.

### Sign 3 — Row count drop without explanation
- **Trigger:** A pipeline stage outputs fewer rows than it received, with no logged filter explaining the difference.
- **Instruction:** Halt the pipeline. Log input/output counts at the stage. Identify the cause before continuing.
- **Reason:** Unexplained row loss is silent data corruption.
- **Provenance:** Initial — data-integrity guardrail for ETL pipelines.

### Sign 4 — Deduplication collision
- **Trigger:** Address/committee/entity dedup merges records that represent distinct real-world entities.
- **Instruction:** Stop. Re-examine the dedup key. Do not loosen or tighten it without review — it affects loaded data integrity.
- **Reason:** Over-merging destroys distinct records; under-merging inflates entity counts.
- **Provenance:** Initial — derived from address/committee caching design.

### Sign 5 — Secret resolved outside the SDK
- **Trigger:** Code reads a secret via `os.environ`, a hardcoded value, or the `op read` CLI.
- **Instruction:** Route the secret through `OnePasswordSettings` in `app/op.py`. The `block-op-read.sh` and `env-leak-check.sh` hooks flag this.
- **Reason:** Centralized secret resolution prevents leaks and keeps credentials out of code.
- **Provenance:** Initial — 1Password Environments SDK mode.

## Agent-Learned Signs

_Append-only. When you detect a new failure pattern (3+ identical errors,
circular tool loops, context pollution), add a Sign here using the format above
with `Provenance: Agent-learned — {brief description}`, then escalate._
