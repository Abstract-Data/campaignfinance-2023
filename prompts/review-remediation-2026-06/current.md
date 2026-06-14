# Review Remediation — 2026-06 Hardening Pass
# Version: 1.0.0
# Model: claude-sonnet-4-6
# Last Updated: 2026-06-06
# Maintainer: John Eakin / Abstract Data

> **For agentic workers:** This is a complete, self-contained remediation prompt
> pack. Each task below is a work order for **one agent**. Steps use `- [ ]`
> checkboxes. The pack runs in **3 waves**; within a wave, the `task-<N><letter>`
> work orders run **concurrently** (each owns a disjoint set of files), then the
> `task-<N>z` integration step runs alone before the next wave starts.

**Goal:** Implement **every** finding from the Jun 4, 2026 review reports —
defer nothing except the three items explicitly listed under "Deferrals" (each
with a rationale). This run is a *hardening pass on an already-mature codebase*:
the reports found **0 critical refactors**, a strong architecture, exemplary SQL
and secrets hygiene, and ~752 tests. The work here closes a small set of
correctness, portability, DRY, and governance gaps — it does not rebuild
anything.

**Source reports (read for full per-finding detail — current code, recommended
code, exact `file:line`, and grounding citations):**

- Code Review Report — https://app.notion.com/p/3767d7f562988141810ef5398c3c1e2e
- Refactoring & Code Smell Report — https://app.notion.com/p/3767d7f5629881b7a2cac20f9b7f5014
- Developer Assessment Report — https://app.notion.com/p/3767d7f56298817ea21eccd1d70eef58

**Tech stack:** Python 3.12, `uv`, SQLModel / SQLAlchemy 2.0, Pydantic v2,
Polars, Splink 4.0.16 (pinned), PostgreSQL / SQLite, Typer, pytest + Hypothesis.

**Grounding playbook:** Abstract Data *Python Design Principles Playbook*
(P7, P12, P15, P16, P17 + False-Positives guidance) —
https://app.notion.com/p/3727d7f56298819cac31c4245e0870a9

---

## How this pack runs — waves of parallel agents

```
Wave 1  Correctness, security & portability   4 parallel tasks → 1z integration
Wave 2  DRY consolidation & dedup unification  4 parallel tasks → 2z integration
Wave 3  Hardening, polish & governance         4 parallel tasks → 3z integration

Wave 1 ─▶ Wave 2 ─▶ Wave 3
```

**Orchestration (how to drive it).** For each wave, in order:

1. Dispatch every `task-<wave><letter>` in that wave to its own agent — they run
   **concurrently**.
2. Wait for all of them to land on the wave branch.
3. Run the wave's `task-<wave>z` integration step (a single agent): it does the
   cross-cutting cleanup, runs the full quality + test gates, and verifies the
   wave.
4. Only then start the next wave.

## Collision protocol

1. A task edits **only the files in its "Files" list.** If two tasks in the same
   wave would touch one file, that is a bug in this plan — stop and flag it.
2. Each task works on its own branch:
   `remediation/2026-06/wave-<N>/task-<NX>-<slug>`.
3. Cross-file import rewiring, registry edits, and `__init__.py` changes are done
   by the wave's `z` integration step, never by a parallel peer.
4. Every task ends with `uv run ruff check --fix` on its own files and a green
   `uv run pytest` for any suite it touched.

## GitNexus guardrails (from `CLAUDE.md`)

This repo is indexed by GitNexus. Before editing **any** function/class/method:

- Run `gitnexus_impact({target: "<symbol>", direction: "upstream"})` and report
  the blast radius. **Warn the user on HIGH/CRITICAL risk before proceeding.**
- The hottest symbols in this pack are `_persist_transaction_from_record`,
  `process_records_batch`, `run_survivorship_stage`, and `_commit_pending` —
  run impact analysis on these first (Wave 2/3).
- Before committing, run `gitnexus_detect_changes()` to confirm the change set
  matches the task's file list.
- Renames go through `gitnexus_rename(..., dry_run: true)` first, never
  find-and-replace.

---

## Full backlog — every finding, deduplicated across the three reports

Severity key: **P1/P2/P3** = Code Review priority · **RF-*** = Refactoring issue ·
**R#** = Developer-Assessment risk register.

<!-- Wave 1 -->
| ID(s) | Finding | Wave · Task |
|-------|---------|-------------|
| P1-SEC-001 | Raw PII logged at ERROR (`unified_state_loader.py:447-449`) violates ADR-0002 → log non-PII id, route full row to `ingest_errors` | 1a |
| R-port, DA §2/§17 | Arch-specific 1Password import `onepassword.lib.aarch64.op_uniffi_core` (`app/op.py:8`) breaks x86 CI/hosts | 1b |
| DA §1.3 | Latent type-alias bug `PassedFailedIndividualRecord = PassedRecord or FailedRecord` (`app/abcs/abc_validation.py:20`) + legacy `typing.*` imports in that file | 1b |
| P1-OPS-001 | `mypy` referenced in docs but never configured or run — add `[tool.mypy]` + CI step | 1c |
| P3-DOC-001 | Align docs (CLAUDE.md / AGENTS.md / context) to actual tooling once mypy is wired | 1c |
| RF-DEAD-002 | Delete large commented-out method bodies (`app/abcs/abc_category.py:58-78`, `abc_state_config.py:51+`) | 1d |
| RF-SMELL-003 | Narrow broad `except Exception` around `map_unified_to_canonical_entity_type` (`survivorship.py:260`) | 1d |
<!-- Wave 2 -->
| RF-DRY-002, R-loaders, DA §2 | Find-or-create dedup reimplemented in loader (`unified_state_loader.py:506-555`) diverges (case-sensitivity) from the builder (`builders.py:708-812`) — route all dedup through the builder; resolve the dual-loader divergence | 2a |
| RF-DRY-001 / RF-DEAD-001 | Duplicated Typer CLI scaffolding across `app/entrypoint.py` vs `app/cli/main.py` — extract `build_base_app()` or delete the legacy `python -m app.cli` surface | 2b |
| RF-DRY-003 | `clear_blank_strings` redefined 5× + duplicated `check_*_type` discriminator — apply `AddressValidatedModel` + `validate_individual_entity_discriminator` across TEC validators | 2c |
| RF-DRY-004 | Repeated officer contribution/expenditure query blocks (`officer_repository.py`) — extract `_officer_txns()` + `_sum_amounts()` | 2d |
<!-- Wave 3 -->
| P2-MAINT-001, RF-CPLX-003 | Oversized functions: `run_survivorship_stage` (~127), `build_golden_record` (~107), `_commit_pending` (~107, 8 params) — extract named helpers | 3a |
| P2-SEC-002 | Validator base uses `extra='ignore'` (`base_models.py:29-33`) — switch to `forbid` where the source schema is stable, else emit a non-PII drift signal | 3b |
| R-splink | Splink private `_settings_obj` access (`score.py:114-126`) — wrap in a typed adapter + version-pinned test | 3b |
| R-drift | ADR-0003 drift signal specified but not automated — schedule golden-set + score-distribution check in CI | 3c |
| RF-MAGIC-001 | Repeated column-length literals (`max_length=30/100/20/500`) → named `Field` length constants | 3d |
| RF-MAGIC-002 | Residual `"texas"`/`"TX"` literals → route through the existing `State` enum / `DEFAULT_STATE` | 3d |
| RF-SMELL-002 | `typing.Dict/List/Optional/Set` in 20+ files → PEP 585/604 (`ruff --select UP`) | 3d |
| RF-SMELL-001 | 19 functions with >5 params; recurring `(state, state_id, state_code, session, cache)` clump → `LoadContext` parameter object | 3d |
| RF-CPLX-001 | `build_person` (`builders.py:176`, 100L/CC24) — extract person-type classification + extra-field backfill | 3d |
| RF-CPLX-002 | `_initialize_unified_fields` (358L) / `_initialize_state_mappings` (219L) — move field/mapping data to a declarative file loaded at import | 3d |
| P2-PERF-001 | Per-row `raw_data` JSON blob (`builders.py:83-85`) inflates storage/writes — make retention a conscious decision | 3d |
| P3-QUAL-001 | Expand the minimal Ruff rule set (`.ruff.toml:18` → add `B, C4, UP, N`) incrementally | 3d |

Cross-cutting hygiene (`ic()` residue, `datetime.utcnow`, unused imports, bare
`except`) is fixed **in whichever file-owning task touches the file**, and each
`z` integration step greps to confirm none remain in scope.

---

# Wave 1 — Correctness, security & portability

## task-1a — Close the ADR-0002 PII logging leak (P1-SEC-001)

**Files:** `app/core/unified_state_loader.py`

- [ ] `gitnexus_impact({target: "process_records_batch", direction: "upstream"})`; report blast radius.
- [ ] At `app/core/unified_state_loader.py:447-449`, stop logging the full record (`{record!r}` contains donor/filer names, addresses, employers — PII). Log only a non-PII identifier: record type + primary key (e.g. `contributionInfoId`) + source file + the exception.
- [ ] Route the full failing row to the existing `ingest_errors` table (the pattern `production_loader.py` already uses), **not** the log stream.
- [ ] Audit the rest of this file for any other `logger.*` call that interpolates a raw record or `raw_data`; apply the same treatment.
- [ ] Fix any `ic()` calls and bare `except` in this file as you go.

**Acceptance:** No raw record / `raw_data` reaches any log handler at any level.
A test asserts a forced validation failure logs a non-PII line and writes one
`ingest_errors` row. `grep -rn "record!r\|raw_data" app/core/unified_state_loader.py`
shows no logging use. Reference: `docs/adr/0002-data-classification-and-retention.md`,
OWASP Logging Cheat Sheet.

## task-1b — Portability + latent type bugs (op.py, abc_validation.py)

**Files:** `app/op.py`, `app/abcs/abc_validation.py`

- [ ] `app/op.py:8`: replace the architecture-pinned import
  `onepassword.lib.aarch64.op_uniffi_core` with the platform-agnostic
  `onepassword` error/type import so x86_64 hosts and CI runners work. Verify
  the symbol used is importable on both arches (check the installed SDK surface).
- [ ] `app/abcs/abc_validation.py:20`: fix
  `PassedFailedIndividualRecord = PassedRecord or FailedRecord` — the `or` on two
  types evaluates eagerly to `PassedRecord`. Replace with a proper
  `PassedRecord | FailedRecord` (or `Union[...]`) type alias.
- [ ] Modernize the legacy `typing.Dict/Tuple/Type/Generator` imports in this
  file to PEP 585/604 forms.

**Acceptance:** `python -c "import app.op"` resolves the 1Password error type on
both arches (or behind a runtime-agnostic guard). `mypy app/abcs/abc_validation.py`
(once wired in 1c) is clean on the alias. No behavior change to callers of the alias.

## task-1c — Wire up mypy for real (P1-OPS-001, P3-DOC-001)

**Files:** `pyproject.toml`, `.github/workflows/ci-quality.yml`,
`.pre-commit-config.yaml` (optional)

- [ ] Add a `[tool.mypy]` section to `pyproject.toml`. Start lenient
  (`ignore_missing_imports = true`), then **target `app/core` and `app/resolve`
  first** with stricter settings so the data-integrity-critical paths are checked.
- [ ] Add a CI step to `ci-quality.yml`: `uvx mypy app` (or scoped to the strict
  packages initially) so type hints are machine-verified on every PR.
- [ ] (Optional) Add a `mypy` hook to `.pre-commit-config.yaml`.
- [ ] Align docs to reality: anywhere `CLAUDE.md` / `AGENTS.md` / project context
  claims mypy is part of the toolchain, ensure it now is (or correct the wording).
  **Note:** `CLAUDE.md` is protected for the file-edit tools — edit it via shell
  if a change is needed.

**Acceptance:** `uvx mypy app/core app/resolve` runs in CI and passes (fix or
`# type: ignore[code]` with a reason for any legitimate gaps). Docs no longer
reference tooling that does not run.

## task-1d — Dead code + broad-except quick wins (RF-DEAD-002, RF-SMELL-003)

**Files:** `app/abcs/abc_category.py`, `app/abcs/abc_state_config.py`,
`app/resolve/stages/survivorship.py` (single-line except narrowing only)

- [ ] Delete the large commented-out method bodies at
  `app/abcs/abc_category.py:58-78` and `app/abcs/abc_state_config.py:51+`
  (history is in git).
- [ ] `app/resolve/stages/survivorship.py:260`: narrow the
  `except Exception` around `map_unified_to_canonical_entity_type` to the specific
  mapping error so real bugs are not silently coerced to `EntityType.organization`.
  **Touch only this line/handler** — the larger survivorship decomposition is
  task-3a in a later wave.

**Acceptance:** No commented-out code blocks remain in the two `abc_*` files; the
narrowed except has a test (or existing test still green) proving the happy path
and that an unexpected error now propagates.

## task-1z — Wave 1 integration

- [ ] Merge 1a–1d to the wave branch.
- [ ] `uv run ruff check` (touched files) — clean.
- [ ] `uvx mypy app/core app/resolve` — clean.
- [ ] `uv run pytest tests app/tests --ignore=tests/resolve` and
  `uv run pytest tests/resolve -m "not integration"` — green.
- [ ] Repo greps clean in touched scope: `ic(`, `record!r` in logging,
  `aarch64`, commented-out `def` bodies.

---

# Wave 2 — DRY consolidation & dedup unification

## task-2a — Unify dedup through the builder; resolve the dual-loader divergence (RF-DRY-002, R-loaders)

**Files:** `app/core/unified_state_loader.py`, `tests/core/` (new test);
read-only: `app/core/builders.py`, `scripts/loaders/production_loader.py`

- [ ] `gitnexus_impact` on `_persist_transaction_from_record` **and**
  `process_records_batch` (both on the load critical path) — report and heed risk.
- [ ] First decide the end state: confirm whether `production_loader.py`
  (modern path, already uses the builder via `process_record_stream`) and
  `UnifiedStateLoader` are both active for the same state. If `UnifiedStateLoader`
  is legacy, prefer retiring its inline dedup (and consider deprecating the path)
  over maintaining two implementations.
- [ ] Route all person/address dedup through the builder's
  `_find_person_by_name_state` (`builders.py:708-757`) and `_find_address_by_fields`
  (`builders.py:759-812`), passing the same `BuilderCache` + `Session`. Delete the
  loader's inline re-lookup at `unified_state_loader.py:506-555`.
- [ ] This removes the **case-sensitivity divergence** (builder is case-insensitive
  to match the `lower()` unique indexes; loader was case-sensitive) — a latent
  duplicate-row bug.
- [ ] Add a characterization test: load the same contributor twice with different
  casing → assert exactly one `UnifiedPerson` row.

**Acceptance:** Single dedup definition that provably matches the DB unique
indexes; the ~92-line CC-14 method shrinks; casing-collision test passes;
existing loader/core suites green. Grounding: SQLModel `where` tutorial;
Playbook P15/P16.

## task-2b — De-duplicate the Typer CLI scaffolding (RF-DRY-001 / RF-DEAD-001)

**Files:** `app/entrypoint.py`, `app/cli/main.py`, `app/cli/__main__.py`,
`tests/` (one CLI smoke test)

- [ ] Confirm whether `python -m app.cli` is still a supported surface.
  - If **legacy:** delete `app/cli/main.py` + `app/cli/__main__.py` and re-point
    any callers (resolves RF-DRY-001 outright).
  - If **supported:** extract the shared scaffolding (the `typer.Typer(name="cf")`
    construction, `_version_callback`, the `--version`/`--verbose` callback, and
    the four `download/convert/verify/prepare` registrations) into
    `app/cli/_app_factory.py::build_base_app() -> typer.Typer`. `entrypoint.py`
    calls it and adds the pipeline commands; `cli/main.py` returns it as-is.
- [ ] Define `__version__` **once** (read from package metadata), not in two files.
- [ ] Add a smoke test asserting the active app(s) expose the four prep commands
  and `--version`.

**Acceptance:** No duplicated version flag / command registration; one
`__version__` source; CLI smoke test green.

## task-2c — Finish adopting the validator mixin (RF-DRY-003)

**Files:** `app/states/texas/validators/` (`_mixins.py` + the ~5 validators that
hand-roll helpers, e.g. `texas_traveldata.py`, `texas_debtdata.py`,
`texas_pledgedata.py`)

- [ ] Make every TEC SQLModel validator inherit `AddressValidatedModel`
  (`_mixins.py:132-138`) or compose the shared
  `model_validator(mode="before")(funcs.clear_blank_strings)`; delete the 5 local
  `clear_blank_strings` redefinitions.
- [ ] Replace the per-file `check_*_type` INDIVIDUAL/ENTITY validators with calls
  to the existing `validate_individual_entity_discriminator`
  (`_mixins.py:73-110`).
- [ ] Add one parametrized test asserting blank-string clearing on a model that
  previously hand-rolled it.

**Acceptance:** One blank-string + one discriminator definition; existing
validator tests pass unchanged; new TEC record types inherit the chain for free.
Grounding: Pydantic v2 reusable validators; Playbook P7/P17.

## task-2d — Extract the officer query helper (RF-DRY-004)

**Files:** `app/core/officer_repository.py`

- [ ] `gitnexus_impact` on the four query sites (`get_committee_officer_activities`
  `:243-269`, `get_person_committee_financial_summary` `:302-331`,
  `get_officer_contributions`/`get_officer_expenditures` `:187-217`).
- [ ] Extract `_officer_txns(session, *, committee_person_id, role,
  with_relations=True) -> list[UnifiedTransactionPerson]` (with the single
  `selectinload` option set) and call it with `PersonRole.CONTRIBUTOR` /
  `PersonRole.PAYEE`.
- [ ] Extract a `_sum_amounts(txns)` helper for the repeated amount-summing loops.

**Acceptance:** One eager-load policy (no future N+1 if a copy forgets
`selectinload`); ~40 lines removed; existing repository tests green. Grounding:
Playbook P12.

## task-2z — Wave 2 integration

- [ ] Merge 2a–2d.
- [ ] `gitnexus_detect_changes()` — change set matches the four file lists.
- [ ] `uv run ruff check` + `uvx mypy app/core app/resolve` — clean.
- [ ] Full suite green: `uv run pytest tests app/tests --ignore=tests/resolve`
  and `uv run pytest tests/resolve -m "not integration"`.
- [ ] Confirm a single dedup path and a single `__version__` source remain.

---

# Wave 3 — Hardening, polish & governance

## task-3a — Decompose the longest functions (P2-MAINT-001, RF-CPLX-003)

**Files:** `app/resolve/stages/survivorship.py`,
`scripts/loaders/production_loader.py`

- [ ] `gitnexus_impact` on `run_survivorship_stage`, `build_golden_record`,
  `_commit_pending`.
- [ ] `survivorship.py`: extract cohesive sub-steps from `run_survivorship_stage`
  (~127L) and `build_golden_record` (~107L) — e.g. name-history assembly,
  field-survivorship selection, edge clustering — into named, individually tested
  helpers. Survivorship is data-integrity critical; keep changes behavior-preserving.
- [ ] `production_loader.py:636-742`: split `_commit_pending` (~107L, 8 params)
  into its three recovery phases (bulk commit → orphan partition/route → cleaned
  retry → row-by-row isolation) and pass a small dataclass instead of 8 positional
  args.

**Acceptance:** No function in these files exceeds ~60 lines without justification;
golden-set / reversibility / loader-recovery tests stay green; behavior unchanged.

## task-3b — Schema-drift safety + Splink adapter (P2-SEC-002, R-splink)

**Files:** `app/abcs/base_models.py`, `app/resolve/stages/score.py`,
`tests/resolve/`

- [ ] `app/abcs/base_models.py:29-33`: where the source schema is contractually
  stable, switch `BaseValidatorModel`/Create models to `extra='forbid'`; otherwise
  emit a **non-PII** drift metric/log when unexpected keys appear (pairs with the
  existing `scrapers/drift_detector.py`). Do not silently drop columns.
- [ ] `app/resolve/stages/score.py:114-126`: wrap the Splink private
  `_settings_obj` access in a typed adapter so a minor Splink bump fails loudly in
  one place; add a Splink-version test asserting the explanation metadata shape.
  Consult `context7` for the pinned Splink 4.0.16 API before touching internals.

**Acceptance:** Unexpected source columns are surfaced (not dropped silently);
Splink internals are accessed through one adapter with a guarding test.

## task-3c — Automate drift detection (R-drift)

**Files:** `.github/workflows/` (new scheduled job), `tests/resolve/golden/` (wire-up)

- [ ] Add a scheduled CI job that re-runs the golden-set and checks the
  score-distribution against ADR-0003's drift signal, alerting on regression.
- [ ] Reuse the existing golden-set / `test_match_quality.py` machinery rather
  than building new harness.

**Acceptance:** A scheduled workflow runs the golden-set drift check and fails /
alerts on match-quality regression. Reference: `docs/adr/0003-ai-governance-entity-resolution.md`.

## task-3d — Polish (magic constants, typing, params, complexity, raw_data, ruff)

**Files (disjoint sub-areas — may be split into 3d-1…3d-7 if parallelized):**

- [ ] **RF-MAGIC-001** — promote recurring column-length literals
  (`max_length=30/100/20/500`) to named `Field` length constants
  (`NAME_LEN`, `DESC_LEN`, …) in a shared module.
- [ ] **RF-MAGIC-002** — route the residual `"texas"`/`"TX"` literals through the
  existing `DEFAULT_STATE` / `State` enum rather than bare strings.
- [ ] **RF-SMELL-002** — modernize `typing.Dict/List/Optional/Set` (20+ files) to
  PEP 585/604; enable via `ruff check --select UP --fix`.
- [ ] **RF-SMELL-001** — introduce a `LoadContext` parameter object for the
  recurring `(state, state_id, state_code, session, cache)` clump (>5-param
  functions in `repository.py`, `production_loader.py`, `processor.py`).
- [ ] **RF-CPLX-001** — `build_person` (`builders.py:176`): extract person-type
  classification (`:236-247`) and extra-field backfill (`:208-222`) into helpers.
- [ ] **RF-CPLX-002** — move `_initialize_unified_fields` (358L) /
  `_initialize_state_mappings` (219L) data into a declarative JSON/TOML file
  loaded at import (the module already imports `json`/`Path`).
- [ ] **P2-PERF-001** — decide consciously on `raw_data` retention
  (`builders.py:83-85`): keep online, or move to an append-only/compressed table,
  or retain only for rejected rows. Document the decision (ADR addendum).
- [ ] **P3-QUAL-001** — expand `.ruff.toml` rule set to add `B, C4, UP, N`
  incrementally with per-file ignores to keep CI green.

**Acceptance:** Each sub-item lands behind a green `ruff` + `mypy` + relevant test
run. Run `gitnexus_impact` before touching `build_person` and any shared-constant
symbol. Expanded ruff rules pass on the whole repo (with documented ignores).

## task-3z — Wave 3 integration & final verification

- [ ] Merge 3a–3d.
- [ ] `gitnexus_detect_changes({scope: "all"})` — only expected files changed.
- [ ] `uv run ruff check` (full, with the expanded rule set) — clean.
- [ ] `uvx mypy app` — clean.
- [ ] Full gates green:
  - `uv run pytest tests/ app/tests/ --cov=app --cov-fail-under=80 --ignore=tests/resolve`
  - `uv run pytest tests/resolve -m "not integration"`
- [ ] Re-run `npx gitnexus analyze --embeddings` to refresh the index (preserve
  embeddings — see `CLAUDE.md`).
- [ ] Update `prompts/review-remediation-2026-06/` with a short `COMPLETION.md`
  (gate results, per-task status, any deferrals taken).

---

## Deferrals (with rationale)

| Item | Rationale |
|------|-----------|
| **Bus factor = 1 / add a second maintainer to the resolve internals** (DA §11, §17) | Organizational, not a code change. Documentation already mitigates it. Track outside this pack. |
| **GDPR-style erasure workflow / PII retention minimization** (DA §13, P2-PERF-001 retention) | ADR-0002 explicitly requires legal review *before* designing an erasure path. Keep the legal-review gate; do not build the workflow speculatively. The `raw_data` *storage* decision (3d) is in scope; the *erasure* workflow is not. |
| **Full Base/Create/Read/Table validator migration for every remaining state validator** | The four-level split is intentionally incremental (`app/abcs/base_models.py:5-12`). RF-DRY-003 (task-2c) advances it for the TEC validators; finishing every state is a separate, larger refactor with its own ROI check. |

## What was deliberately NOT turned into tasks (per the Playbook False-Positives guidance)

- The repeated `*_ingest` source models vs. the unified `tables.py` models are
  **intentional boundary isolation**, not DRY debt.
- The near-identical `*Version` audit-history table classes (`tables.py:462-525`)
  are an audit pattern; collapsing them is optional polish only.
- Direct `session` use in `scripts/` loaders and debug scripts is acceptable
  (one-off, not a request handler).

## Verification commands (copy/paste)

```bash
uv run ruff check
uvx mypy app
uv run pytest tests app/tests --ignore=tests/resolve
uv run pytest tests/resolve -m "not integration"
uv run pytest tests/ app/tests/ --cov=app --cov-fail-under=80 --ignore=tests/resolve
# residue greps (should be clean in touched scope):
grep -rn "record!r" app/core/unified_state_loader.py
grep -rn "aarch64" app/op.py
grep -rn " or FailedRecord" app/abcs/abc_validation.py
```
