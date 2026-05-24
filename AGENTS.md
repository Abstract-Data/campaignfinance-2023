## Learned User Preferences

- For multi-agent pipeline work (data-resolution, state-data-cli), dispatch parallel agents within each wave and run waves sequentially; do not assign one monolithic agent to execute all waves
- Launch an entire parallel wave as one multitask batch (one agent per task brief), then wait for that wave to complete and merge before starting the next wave
- Integration tasks (`*z`) are single serial agents that run only after all parallel tasks in that wave are merged
- Hand each worker the full `task-*.md` brief from `prompts/data-resolution-pipeline/`; do not summarize or substitute a shorter brief
- Use GitButler (`but` commands) for branch/commit workflow; consolidate worker output on one phase branch with one commit per task
- Prefer incremental fixes over redesigns; describe structural or provider changes and wait for explicit approval before implementing
- Do not reiterate or summarize subagent results to the user unless asked or multi-task synthesis is required
- After `/review`, split recommended fixes across parallel agents partitioned by file ownership (same wave pattern as pipeline tasks)

## Learned Workspace Facts

- Data-resolution pipeline is orchestrated in 11 waves (Wave 0–10) per `.cursor/plans/data_resolution_waves_d3596502.plan.md`; Phase 0 is a verification gate, not a greenfield rebuild
- Authoritative task briefs live under `prompts/data-resolution-pipeline/` (30 `task-*.md` files); parallel tasks create new files only and `*z` integration tasks own registries, `__init__.py`, and cross-task wiring
- Phase 0 Wave 0 gate failures commonly involve stubbed transaction loading in `scripts/loaders/production_loader.py`, incomplete DB bootstrap (e.g. `states`/`file_origins`), and missing report reconciliation per `task-0z-integration.md`
- Before Phase 1 integration, run `uv run cf prepare texas` then full load via `scripts/loaders/production_loader.py`
- State Data CLI prerequisite is done: `app/cli/` with `cf prepare texas` and related commands
- Phase 0 code paths live under `app/core/source_models/`, `scripts/loaders/`, and `tests/resolve/`; Phase 1 foundation lives under `app/resolve/` (`models/`, `standardize/`, `stages/`, `cli.py`, `run.py`)
- Wave 0 integration agents must not create or edit `app/resolve/`; stop after PASS/FAIL gate report and let the coordinator spawn Wave 1 parallel agents (1a, 1b, 1c)
- Phase 0 Wave 0 verification gate passed; task 1z wired Phase 1 stages 1→2→3→7 (`build_resolution_input` → blocking → fastpath → survivorship)
- `scripts/loaders/production_loader.py` applies `max_records` per file (not globally) so subset loads reach all record types; nullable pledge entity FKs allow PLDG rows without Wave 1 entity resolution
- Texas CSV→parquet conversion (`app/states/texas/texas_converter.py`) uses `infer_schema_length=0` (all-string columns) and skips `CFS-Codes`/`CFS-ReadMe` metadata `.txt` files
- Legacy codebase imports (e.g. `from abcs import …`) expect `app/` on `sys.path`; CLI entry points must bootstrap paths so `uv run cf` and `python -m app.cli` work without manual `PYTHONPATH`

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **campaignfinance** (4489 symbols, 10906 relationships, 246 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## When Debugging

1. `gitnexus_query({query: "<error or symptom>"})` — find execution flows related to the issue
2. `gitnexus_context({name: "<suspect function>"})` — see all callers, callees, and process participation
3. `READ gitnexus://repo/campaignfinance/process/{processName}` — trace the full execution flow step by step
4. For regressions: `gitnexus_detect_changes({scope: "compare", base_ref: "main"})` — see what your branch changed

## When Refactoring

- **Renaming**: MUST use `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` first. Review the preview — graph edits are safe, text_search edits need manual review. Then run with `dry_run: false`.
- **Extracting/Splitting**: MUST run `gitnexus_context({name: "target"})` to see all incoming/outgoing refs, then `gitnexus_impact({target: "target", direction: "upstream"})` to find all external callers before moving code.
- After any refactor: run `gitnexus_detect_changes({scope: "all"})` to verify only expected files changed.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Tools Quick Reference

| Tool | When to use | Command |
|------|-------------|---------|
| `query` | Find code by concept | `gitnexus_query({query: "auth validation"})` |
| `context` | 360-degree view of one symbol | `gitnexus_context({name: "validateUser"})` |
| `impact` | Blast radius before editing | `gitnexus_impact({target: "X", direction: "upstream"})` |
| `detect_changes` | Pre-commit scope check | `gitnexus_detect_changes({scope: "staged"})` |
| `rename` | Safe multi-file rename | `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` |
| `cypher` | Custom graph queries | `gitnexus_cypher({query: "MATCH ..."})` |

## Impact Risk Levels

| Depth | Meaning | Action |
|-------|---------|--------|
| d=1 | WILL BREAK — direct callers/importers | MUST update these |
| d=2 | LIKELY AFFECTED — indirect deps | Should test |
| d=3 | MAY NEED TESTING — transitive | Test if critical path |

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/campaignfinance/context` | Codebase overview, check index freshness |
| `gitnexus://repo/campaignfinance/clusters` | All functional areas |
| `gitnexus://repo/campaignfinance/processes` | All execution flows |
| `gitnexus://repo/campaignfinance/process/{name}` | Step-by-step execution trace |

## Self-Check Before Finishing

Before completing any code modification task, verify:
1. `gitnexus_impact` was run for all modified symbols
2. No HIGH/CRITICAL risk warnings were ignored
3. `gitnexus_detect_changes()` confirms changes match expected scope
4. All d=1 (WILL BREAK) dependents were updated

## Keeping the Index Fresh

After committing code changes, the GitNexus index becomes stale. Re-run analyze to update it:

```bash
npx gitnexus analyze
```

If the index previously included embeddings, preserve them by adding `--embeddings`:

```bash
npx gitnexus analyze --embeddings
```

To check whether embeddings exist, inspect `.gitnexus/meta.json` — the `stats.embeddings` field shows the count (0 means no embeddings). **Running analyze without `--embeddings` will delete any previously generated embeddings.**

> Claude Code users: A PostToolUse hook handles this automatically after `git commit` and `git merge`.

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
