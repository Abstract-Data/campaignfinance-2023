# Review Response Remediation + Upsert — 2026-06-07
# Version: 1.0.0
# Model: claude-sonnet-4-6
# Last Updated: 2026-06-07
# Maintainer: John Eakin / Abstract Data

> **For agentic workers:** This is a complete, self-contained remediation prompt
> pack. Each task below is a work order for **one agent**. Steps use `- [ ]`
> checkboxes. The pack runs in **4 waves**; within a wave, the `task-<N><letter>`
> work orders run **concurrently** (each owns a disjoint set of files), then the
> `task-<N>z` integration step runs alone before the next wave starts.

**Goal:** Implement every *confirmed* finding from
`docs/review-response-2026-06-07.md` (the verified rewrite of the Jun 7 code
review — read it first; it corrects four false positives in the original
review), **plus** add real upsert-from-file functionality to the ingest path.

**Do NOT implement** the original review's Gaps 4 (as written), 6, 7, or 8 —
they were verified false against the live TEC CSV headers in `tmp/texas/` and
against `app/core/source_models/lookups.py` / `spac.py`. The corrected versions
are what's specified below.

**Source documents:**

- `docs/review-response-2026-06-07.md` — verified findings + implementation plan (authoritative)
- `tmp/texas/CFS-ReadMe_20260524.txt` + live CSV headers — ground truth for TEC columns
- Original review (for context only): `code-review-2026-06-07.md` upload

**Tech stack:** Python 3.12, `uv`, SQLModel / SQLAlchemy 2.0, Pydantic v2,
Polars, Splink 4.0.16 (pinned), PostgreSQL / SQLite, Typer, pytest + Hypothesis.

---

## How this pack runs — waves of parallel agents

```
Wave 1  Data-loss bugs + upsert primitive       3 parallel tasks → 1z integration
Wave 2  Unified-layer idempotency & columns     3 parallel tasks → 2z integration
Wave 3  Resolution quality (Splink/survivorship) 3 parallel tasks → 3z integration
Wave 4  Hygiene, QA checks & docs               1 task (solo)

Wave 1 ─▶ Wave 2 ─▶ Wave 3 ─▶ Wave 4
```

**Orchestration.** For each wave: dispatch every `task-<wave><letter>`
concurrently; wait for all to land; run `task-<wave>z` alone (cross-cutting
wiring + full quality/test gates); only then start the next wave.

## Collision protocol

1. A task edits **only the files in its "Files" list.** Two same-wave tasks
   touching one file is a bug in this plan — stop and flag it.
2. Branch per task: `remediation/2026-06-07/wave-<N>/task-<NX>-<slug>`.
3. Import rewiring, `__init__.py`, and registry edits belong to the wave's `z`
   step, never a parallel peer.
4. Every task ends with `uv run ruff check --fix` on its own files and a green
   `uv run pytest` for any suite it touched.

## GitNexus guardrails (from `CLAUDE.md`)

- Before editing any symbol: `gitnexus_impact({target: "<symbol>", direction:
  "upstream"})`; report blast radius; **warn on HIGH/CRITICAL before
  proceeding.** Hottest symbols in this pack:
  `add_with_limits`, `check_if_records_exist`, `process_records_batch`,
  `_persist_transaction_from_record`, `build_report`,
  `build_canonical_campaigns`, `run_survivorship_stage`.
- Before each commit: `gitnexus_detect_changes()` must match the task's file list.
- Renames via `gitnexus_rename(..., dry_run: true)` first. Never find-and-replace.
- After the final wave merges: `npx gitnexus analyze` (check
  `.gitnexus/meta.json` `stats.embeddings` first; add `--embeddings` if > 0).

---

## Wave 1 — Data-loss bugs + the upsert primitive

### task-1a — Rebuild `DebtData` in camelCase with guarantor capture
**Files:** `app/states/texas/validators/texas_debtdata.py`, new test file
`app/tests/test_debt_validator.py`

Context: every DEBT row fails validation today. The CSV headers are camelCase
(`reportInfoIdent`, `loanInfoId`, `lenderPersentTypeCd`, …); `DebtData` declares
snake_case required fields with no alias generator → instant 100% data loss.
Verify against `head -1 tmp/texas/debts_*.csv` (89 columns) before writing code.

- [ ] Rename every field to match the CSV header exactly (camelCase), mirroring
      the conventions in `texas_traveldata.py` / `texas_contributions.py`.
- [ ] Add all five guarantor blocks (`guarantorPersentTypeCd{1..5}`, name fields,
      address fields — generate programmatically if cleaner, but the model must
      expose every column). All Optional.
- [ ] Keep `loanInfoId` as primary key via **`sqlmodel.Field`**; keep `table=True`.
- [ ] Update the three model validators to read camelCase keys.
- [ ] `texas.tx_debt_data` is empty (everything failed) — drop & recreate the
      table; do not write a column-rename migration.
- [ ] Tests: a real header row from `debts_*.csv` validates; INDIVIDUAL/ENTITY
      lender rules still enforced; a guarantor block round-trips.

### task-1b — Make `TECTravelData` a real table
**Files:** `app/states/texas/validators/texas_traveldata.py`, new test file
`app/tests/test_travel_validator.py`

- [ ] Change import: `from sqlmodel import Field` (currently `pydantic.Field` —
      `primary_key=True` is inert there).
- [ ] `class TECTravelData(TECSettings, table=True):` — keep
      `__tablename__`/`__table_args__` as-is.
- [ ] Ensure `texas.tx_travel_data` is created by the same metadata/DDL path the
      other staging tables use.
- [ ] Tests: a real `travel_*.csv` header row validates AND persists to the
      table (in-memory SQLite is fine); `travelInfoId` is the PK.

### task-1c — Bulk upsert primitive for staging loads
**Files:** `app/abcs/abc_db_loader.py`, new `app/core/upsert.py`, new test file
`app/tests/test_upsert.py`

Context: staging is insert-only. `check_if_records_exist()` loads **all existing
PKs into a Python set** (won't scale; never updates), so amended TEC records —
which reuse natural ids (`contributionInfoId`, `loanInfoId`, `travelInfoId`) —
are silently dropped on re-download.

- [ ] New `app/core/upsert.py`: `bulk_upsert(session, model, rows, *,
      conflict_cols, update_cols=None, chunk_size=5000)` built on
      `sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_update(...)`.
      **Dialect-aware:** dispatch on `session.bind.dialect.name` — the
      `sqlite` dialect has the same `on_conflict_do_update` API; raise a clear
      error for anything else. `update_cols=None` ⇒ update all non-PK columns.
      Default excludes `created_at`-style columns if present.
- [ ] Rewire `DatabaseLoaderABC.add` / `add_with_limits` to call `bulk_upsert`
      (model instances → dicts via `model_dump()`), keyed on each validator's
      natural PK. `DO UPDATE`, not `DO NOTHING` — amendments must overwrite.
- [ ] Deprecate `check_if_records_exist` (leave a shim that logs a deprecation
      warning and returns the iterator unchanged; the z-step removes callers).
- [ ] Tests: same PK loaded twice with a changed amount ⇒ one row, new value;
      chunking boundary (chunk_size=2, 5 rows); runs green on SQLite.

### task-1z — Wave 1 integration
- [ ] Remove remaining `check_if_records_exist` call sites; fix imports.
- [ ] Smoke load: one `debts_*` and one `travel_*` parquet file through the CLI
      load path; assert non-zero rows in `texas.tx_debt_data` and
      `texas.tx_travel_data`; re-run the same file and assert row count is
      unchanged (upsert, not duplicate).
- [ ] Full gates: `uv run ruff check`, `uv run pytest`, `gitnexus_detect_changes()`.

---

## Wave 2 — Unified-layer idempotency & at-filing columns

### task-2a — `UnifiedReport` at-filing columns + JSON backfill
**Files:** `app/core/source_models/reports.py`,
`app/core/source_models/reports_ingest.py`, tests for both

- [ ] Add `committee_name_at_filing: str | None` and
      `treasurer_name_at_filing: str | None` to `UnifiedReport`.
- [ ] Populate in `build_report()`: `raw.get("filerName")`; treasurer from the
      `treas*` name parts, branching on `treasPersentTypeCd`
      (INDIVIDUAL → "First Last", ENTITY → `treasNameOrganization`).
- [ ] Backfill function (callable from CLI): one set-based UPDATE extracting
      from the existing `raw_data` JSON — **no re-ingest**. Dialect-aware JSON
      extraction (`->>'filerName'` on PG, `json_extract` on SQLite).
- [ ] Add query helper `treasurer_for_report(session, report)` implementing the
      documented date-range join against `TECTreasurer`
      (`treasEffStartDt ≤ filed_date ≤ treasEffStopDt`). **No treasurer FK** —
      the review-response doc rejects it deliberately.

### task-2b — Natural-key uniqueness + upsert on `unified_transactions`
**Files:** `app/core/unified_database.py`, `app/core/unified_state_loader.py`,
tests

Context: `unified_transactions.transaction_id` (TEC source id) has no unique
constraint; the `FileOrigin` whole-file guard keys on **date-stamped filenames**
so every fresh TEC download re-loads everything as duplicates.

- [ ] Add to `_DEDUP_INDEXES`: unique partial index on
      `(state_id, transaction_type, transaction_id)
      WHERE transaction_id IS NOT NULL`. Before creating it, the integration
      step must check for and report existing violations (dup cleanup query
      provided in the z-step).
- [ ] In `_persist_transaction_from_record`: on natural-key hit, **update** the
      existing transaction (amount, dates, description, report_ident) instead of
      inserting — reuse `bulk_upsert` semantics or a select-then-mutate
      consistent with `BuilderCache`.
- [ ] Demote the `FileOrigin` guard to informational logging (keep the table —
      it's still the provenance record); idempotency now rests on the index.
- [ ] Tests: same TEC record loaded from two differently-named files ⇒ one
      transaction row; amended amount overwrites.

### task-2c — Canonical campaigns with NULL election year
**Files:** `app/resolve/publish/campaigns.py`, its tests

- [ ] Drop `AND election_year IS NOT NULL` from the source query.
- [ ] Identity tuple uses sentinel cycle `0` for NULL years (simpler than
      `NULLS NOT DISTINCT`, and the builder is delete-and-rebuild anyway);
      `CanonicalCampaign.election_cycle` stores 0 ⇒ document the sentinel in the
      module docstring and any read paths in this file.
- [ ] Tests: officeholder committee with no election year gets exactly one
      canonical campaign across two consecutive runs.

### task-2z — Wave 2 integration
- [ ] Run the duplicate-detection query for 2b's index
      (`GROUP BY state_id, transaction_type, transaction_id HAVING COUNT(*)>1`);
      if dups exist in dev data, delete keeping lowest `id`, then create index.
- [ ] Run the 2a backfill against dev data; spot-check 5 reports against
      `raw_data`.
- [ ] Full gates + `gitnexus_detect_changes()`.

---

## Wave 3 — Resolution quality

### task-3a — Employer as a Splink comparison signal (never blocking)
**Files:** `app/resolve/standardize/staging.py`,
`app/resolve/standardize/stage1.py`, `app/resolve/splink_config/person.py`,
tests

- [ ] Add `employer: str | None` (String(500)) to `ResolutionInput`.
- [ ] Stage1: populate from `UnifiedPerson.employer`, normalized with the org
      helpers in `standardize/orgs.py` (upper, punctuation-stripped).
- [ ] `person.py` COMPARISONS: append
      `cl.JaroWinklerAtThresholds("employer", [0.88])`. **Do NOT add employer
      to any blocking rule** — employers change; blocking on one splits the same
      person across years.
- [ ] EM training runs after this change must re-estimate m/u — confirm the
      training path picks up the new comparison; note expected null-heaviness
      (only contribution-sourced persons carry employer; Splink handles nulls).

### task-3b — Org cross-role blocking rule
**Files:** `app/resolve/splink_config/organization.py`,
`app/resolve/blocking.py`, tests

Context: persons already cross-role-block via
`(first_name_phonetic, last_name_phonetic)`; orgs only block on
`(normalized_org, zip3)`, so a vendor/donor org with two addresses resolves as
two entities. The staging column is **`state`** (not `state_code` — the
original review's suggested column doesn't exist).

- [ ] Add `block_on("org_name_phonetic", "state")` to org
      `PREDICTION_BLOCKING_RULES`.
- [ ] Mirror the rule in `app.resolve.blocking` default rules — the lock-step
      warning in the config comments is binding; Splink re-blocks on these.
- [ ] **Measure pair counts before/after** on dev data and record both numbers
      in the PR description. Orgs are few, explosion risk is low — verify, don't
      assume. If candidate pairs grow >5×, stop and flag.

### task-3c — Employer survivorship + history
**Files:** `app/resolve/stages/survivorship.py`, its tests

- [ ] Scalar employer on `CanonicalEntity` comes from the **most recent** record
      (by `last_activity_date`, fallback `created_at`) — not the best-name row.
- [ ] Aggregate per-cluster `(employer, first_activity_date,
      last_activity_date)` into `provenance_json["employer_history"]`, the shape
      mirroring `CanonicalNameHistory` semantics:
      `[{"value": ..., "first_seen": ..., "last_seen": ...}]`.
- [ ] Do **not** build a `CanonicalEmployerHistory` table in this pass —
      promote only when something queries it relationally.
- [ ] Tests: cluster with two employers across date ranges → ordered history,
      scalar = most recent.

### task-3z — Wave 3 integration
- [ ] Full resolve run on dev data; compare cluster counts to the previous run
      (golden tests in `tests/resolve/golden/` must pass or be re-baselined with
      justification in the commit message).
- [ ] Full gates + `gitnexus_detect_changes()`.

---

## Wave 4 — Hygiene, QA & docs (single agent)

### task-4a
**Files:** `app/states/texas/validators/texas_expenses.py`,
`app/resolve/publish/views.py` (or review CLI), `app/tests/`, docs

- [ ] Delete the commented `TECExpenseCategory` block in `texas_expenses.py`
      (EXCAT is live in `app/core/source_models/lookups.py` — verify, don't move).
- [ ] Add the cross-role consolidation diagnostic as a QA query (entities with
      both contribution and expenditure activity, ordered by volume) exposed via
      the review CLI or `publish/views.py`.
- [ ] Integration test for travel dedup (closes original Gap 10): a TRVL row
      flows staging → unified traveller entity → `resolution_input`.
- [ ] Document in `docs/` (data-notes or ADR): TEC public extracts contain **no
      contributor street lines** — person `line_1` comparisons only fire for
      filer/treasurer/lender-sourced entities. This is a source limitation, not
      a bug.
- [ ] Field-coverage check script: diff each validator's fields against
      `head -1` of its CSV in `tmp/texas/`; wire into the verify CLI so future
      reviews can't propose phantom columns.
- [ ] Final: full test suite, `npx gitnexus analyze`, update
      `docs/review-response-2026-06-07.md` status column if one was added.

---

## Deferrals (explicit, with rationale)

1. **Set-based texas→unified rewrite** (review-response §3, items 1–2): larger
   refactor; this pack keeps the row path but adds the unique indexes and
   upsert primitives that rewrite will need. Schedule separately.
2. **Treasurer FK on `UnifiedReport`:** rejected by design — date-range join +
   helper (task-2a) is the correct relational model.
3. **`CanonicalEmployerHistory` table:** JSON provenance first (task-3c);
   promote on demonstrated query need.
