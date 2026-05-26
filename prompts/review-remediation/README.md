# Review Remediation — Implementation Prompt Pack

> **For agentic workers:** Each `task-*.md` is a complete, self-contained work
> order for **one agent**. Steps use `- [ ]` checkboxes. This pack runs in
> **5 waves**; within a wave, all `task-<wave><letter>` files run **concurrently**
> as parallel agents, then the `task-<wave>z-integration.md` runs alone.

**Goal:** Implement **every** finding and recommendation from the three May 24,
2026 review reports — defer nothing. Priority 1/2/3 code-review items, all 18
`RF-*` refactoring issues, and all 12 risk-register items.

**Source reports (read for full per-finding detail — current code, recommended
code, exact file:line):**

- Code Review Report — `https://www.notion.so/36b7d7f5629881c6a392fec179ea252f`
- Refactoring & Code Smell Report — `https://www.notion.so/36b7d7f56298813f94eaf46ff0a63286`
- Developer Assessment Report — `https://www.notion.so/36b7d7f56298811895eedbd3139151ab`

**Tech stack:** Python 3.12, `uv`, SQLModel/SQLAlchemy 2.0, Pydantic v2, Polars,
PostgreSQL/SQLite, pytest + Hypothesis.

---

## How this pack runs — waves of parallel agents

```
Wave 1  Correctness, security & hygiene     8 parallel tasks → 1z integration
Wave 2  Decouple the data layer             3 parallel tasks → 2z integration
Wave 3  Split the god-modules               2 parallel tasks → 3z integration
Wave 4  Core-path refactors                 5 parallel tasks → 4z integration
Wave 5  Tests, ops, scrapers & docs         4 parallel tasks → 5z integration

Wave 1 ─▶ Wave 2 ─▶ Wave 3 ─▶ Wave 4 ─▶ Wave 5
```

**Orchestration (how to drive it):** For each wave, in order:
1. Dispatch every `task-<wave><letter>.md` in that wave's directory to its own
   agent — they run **concurrently**.
2. Wait for all of them to merge to the wave branch.
3. Run the wave's `task-<wave>z-integration.md` (a single agent) — it does the
   cross-cutting cleanup, runs the full test suite, and verifies the wave.
4. Only then start the next wave.

A wave's tasks are collision-free because **each task owns a disjoint set of
files** and fixes *every* finding in those files (cross-cutting fixes — bare
excepts, `ic()` calls, `datetime.utcnow`, unused imports — are folded into
whichever task owns the file, never split into their own task).

## Collision protocol

1. A task edits **only the files in its "Files" list**. If two tasks would touch
   one file, they are not in the same wave — this is already enforced below.
2. Each task works on its own branch: `remediation/wave-<N>/task-<NX>-<slug>`.
3. `__init__.py` / registry / cross-file import rewiring is done by the wave's
   `z` integration task, never by a parallel task.
4. If a task finds it must edit a file another wave-peer owns: **stop and flag**.
5. Every task ends with `uv run ruff check --fix` on its own files and a green
   `uv run pytest` for any suite it touched.

## Full backlog — every finding, defer nothing

Severity key: **P1/P2/P3** = Code Review priority; **RF** = Refactoring issue;
**R#** = Developer-Assessment risk-register item.

<!-- Wave 1 -->
| ID(s) | Finding | Wave · Task |
|-------|---------|-------------|
| P1-OPS-001 | Restore missing `app/states/postgres_config.py` (pydantic-settings) | 1a |
| P1-SEC-001, RF-ARCH-001 | SQL injection — 4 f-string `text()` queries + L491 bare-string crash in `unified_state_loader.py` | 1b |
| P1-SEC-002 | `run_custom_query` executes arbitrary SQL — restrict to read-only SELECT | 1c |
| P1-OPS-001 (part) | Narrow the `db_manager` singleton guard so a missing module fails loud | 1c |
| RF-SMELL-001, P3-QUAL-005 | 7 inert `__post_init__` normalizers → Pydantic `model_validator` | 1d |
| RF-DEAD-002 | Duplicate `state_id` field on `UnifiedTransactionPerson` | 1d |
| P3-QUAL-002 | `datetime.utcnow()` → `datetime.now(timezone.utc)` (all unified models) | 1d |
| P1-SEC-003 | `op.py` — keep DB URL in `SecretStr`, `extra='forbid'` on credential settings | 1e |
| P1-ARC-001, RF-CPLX-002, R4 | Delete non-runnable `app/main.py` scratch code | 1f |
| P3-QUAL-003, RF-SMELL-006 | Delete `app/funcs/depreciated.py` | 1f |
| RF-DEAD-001 | Strip ~333 commented-out lines from `texas_filers.py` (+ `main.py`, covered by 1f); delete commented-out class body in `ok_expenditure.py` | 1g |
| P2-TEST-001 (part) | CI runs the wrong test dir — point `ci-tests.yml` at `tests/` | 1h |
| R6 | No dependency scanning — add `pip-audit`/Dependabot + SBOM | 1h |
| R7 | Dual lockfiles — delete `poetry.lock`, standardize on uv | 1h |
| R8 | `campaignfinance_dev.db` committed — remove + gitignore | 1h |
| RF-DEAD-003 | Unused/redundant imports — repo-wide `ruff check --fix` | 1z |
<!-- Wave 2 -->
| P2-ARC-002, RF-SMELL-005 | Global `db_manager` + import-time DDL → factory; inject session into `UnifiedSQLModelBuilder`; break circular import | 2a |
| RF-SMELL-002 (part) | Retire dead duplicate layer — delete `unified_models.py` + dead `unified_integration.py` example funcs | 2b |
| P2-OPS-002 | Centralize logging via `dictConfig`, env-driven PaperTrail, cache `Logger` | 2c |
| P3-QUAL-004 | Migrate legacy `from abcs/funcs/logger` imports → absolute `app.*`; delete `_path_setup.py` | 2z |
<!-- Wave 3 -->
| RF-SMELL-002 (part), R1 | Split `unified_sqlmodels.py` (1856 lines) → `models/`, `enums.py`, `builders.py`, `processor.py` — retires the god-module change-amplifier | 3a |
| RF-MAGIC-001 | `RecordType` enum / `RECORD_TYPE_CODES` + `PLACEHOLDER_NAMES` shared constants | 3a |
| RF-MAGIC-002 | `AMOUNT_BUCKETS` constant + reusable `MONEY_TYPE` column | 3a |
| RF-MAGIC-003 | Hardcoded `"TX"` default state code in the multi-state survivorship stage — extract to a `DEFAULT_STATE` sentinel or require explicit caller-supplied state | 3a |
| RF-DRY-003 | Texas validator address-extraction mixin + shared before-validators; remove 4× redundant `clear_blank_strings` re-registration on subclasses | 3b |
| RF-DRY-004 | Oklahoma validator DRY — `parse_candidate_name` and `parse_zipcode` duplicated across 2–3 OK validator classes; extract to shared OK mixin | 3b |
<!-- Wave 4 -->
| RF-DRY-002, RF-CPLX-001 | Detail-builder registry + extract `process_record` god function | 4a |
| RF-SMELL-003 | `_determine_transaction_type` if/elif chain → keyword map | 4a |
| P2-PERF-002, R11 | Streaming/batched record processing (`process_record_stream`, Polars scan) | 4a |
| RF-DRY-001 | Version-snapshot helper + `_to_json_safe` (fixes latent `json.dumps` bug) | 4b |
| RF-CPLX-003 (part) | Deep-nesting + N+1 in `unified_database.py` analysis loops | 4b |
| P2-PERF-001, RF-CPLX-003 (part) | N+1 / per-row sessions in builder + loader — one session per batch, pre-loaded dicts | 4c |
| P2-MNT-001, R5 | Narrow 20+ bare/broad `except` clauses (silent error handling); aggregate row failures into stats | 4c |
| RF-SMELL-004 | Value objects — `PersonName`, `AddressParts`, `Officer` | 4d |
| P2-ARC-001 | Base/Create/Read/Table model split for validators | 4e |
<!-- Wave 5 -->
| P2-TEST-001 (part), R10 | Characterization + unit tests for the unified core and legacy validators | 5a |
| R2 | Scraper hardening — markup-drift detection, fixture-based tests, failure alerting | 5b |
| R9 | Production orchestration — scheduler + production runtime entrypoint | 5c |
| R3 | Architecture / ERD diagram + per-module legacy docs | 5d |
| R12 | Data-classification + retention ADR | 5d |

Cross-cutting findings **P3-QUAL-001** (`ic()` → `Logger`) and the remaining
**P2-MNT-001** bare-except instances are not separate tasks — every file-owning
task in every wave fixes the `ic()` calls and bare `except` clauses **in the
files it touches** as part of its work. The `z` integration tasks verify none
remain.

Two additional cross-cutting items from the May 24 re-run are handled the same
way (fix in the file-owning task, `1z` verifies repo-wide):

- **`asyncio.run()` inside `__init__`** (Developer Assessment red flag) — calling
  `asyncio.run()` in `__init__` blocks the event loop and prevents use inside
  async contexts. Locate the offending `__init__` (`grep -rn "asyncio.run"
  app/`), replace with a sync helper that the async caller `await`s, or defer
  initialization to an explicit `async def setup()` classmethod. The wave-1 task
  that owns the file fixes it; `1z` confirms `grep -rn "asyncio\.run(" app/`
  returns nothing inside `__init__` bodies.

- **Legacy `Optional[X]` typing** — Python 3.12 prefers `X | None`. All files
  already pass through `ruff check --fix` as part of their wave tasks; the `1z`
  integration adds `ruff check --select UP007 --fix` as an explicit
  modernization sweep over `app/` to batch-convert `Optional[X]` → `X | None`
  and `Union[X, Y]` → `X | Y` in one go.

## Conventions

- TDD: failing test → see it fail → implement → see it pass → commit. One green
  step per commit; Conventional Commit messages.
- Tests under `tests/`. Run `uv run pytest`.
- Each task reads the relevant source report (URLs above) for the full
  recommended code for its findings — the reports contain before/after snippets.
- Do not edit `CLAUDE.md` or `.claude/` (environment-protected).
- A wave is "done" only when its `z` integration task reports the full suite green.
