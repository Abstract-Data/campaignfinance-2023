## Learned User Preferences

- For multi-agent pipeline work (data-resolution, state-data-cli), dispatch parallel agents within each wave and run waves sequentially; do not assign one monolithic agent to execute all waves
- Launch an entire parallel wave as one multitask batch (one agent per task brief), then wait for that wave to complete and merge before starting the next wave
- Integration tasks (`*z`) are single serial agents that run only after all parallel tasks in that wave are merged
- Hand each worker the full `task-*.md` brief from `prompts/data-resolution-pipeline/`; do not summarize or substitute a shorter brief
- Use GitButler (`but` commands) for branch/commit workflow; consolidate worker output on one phase branch with one commit per task
- Prefer incremental fixes over redesigns; describe structural or provider changes and wait for explicit approval before implementing
- Do not reiterate or summarize subagent results to the user unless asked or multi-task synthesis is required

## Learned Workspace Facts

- Data-resolution pipeline is orchestrated in 11 waves (Wave 0–10) per `.cursor/plans/data_resolution_waves_d3596502.plan.md`; Phase 0 is a verification gate, not a greenfield rebuild
- Authoritative task briefs live under `prompts/data-resolution-pipeline/` (30 `task-*.md` files); parallel tasks create new files only and `*z` integration tasks own registries, `__init__.py`, and cross-task wiring
- Phase 0 Wave 0 gate failures commonly involve stubbed transaction loading in `scripts/loaders/production_loader.py`, incomplete DB bootstrap (e.g. `states`/`file_origins`), and missing report reconciliation per `task-0z-integration.md`
- Before Phase 1 integration, run `uv run cf prepare texas` then full load via `scripts/loaders/production_loader.py`
- State Data CLI prerequisite is done: `app/cli/` with `cf prepare texas` and related commands
- Phase 0 code paths live under `app/core/source_models/`, `scripts/loaders/`, and `tests/resolve/`; Phases 1–4 target `app/resolve/` (not started as of plan authoring)
- Legacy codebase imports (e.g. `from abcs import …`) expect `app/` on `sys.path`; CLI entry points must bootstrap paths so `uv run cf` and `python -m app.cli` work without manual `PYTHONPATH`
