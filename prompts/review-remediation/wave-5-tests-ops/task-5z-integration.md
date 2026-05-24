# Task 5z — Wave 5 integration & final verification

> **Wave 5, serial — runs LAST, after 5a–5d merge. Final task of the pack.**
> Branch `remediation/wave-5/task-5z-integration`. Read the pack README.

## Context

This is the last task. It verifies the whole remediation effort — every finding
from all three review reports — is implemented, and that the codebase is green
and consistent.

## What to do

1. **Full backlog audit.** Walk the backlog table in the pack README and
   confirm **every row** is done — Priority 1/2/3 code-review items, all 18
   `RF-*` issues, all 12 `R#` risks. Mark off each ID against the code.
2. **Full suite + coverage gate.** `uv run pytest tests app/tests
   --cov=app --cov-report=term` — must be green and clear the CI coverage gate.
3. **Lint/quality gate.** `uv run ruff check .` clean; confirm no `ic()` calls,
   no bare `except Exception`, no f-string SQL, no `datetime.utcnow`, no
   commented-out code blocks remain anywhere in `app/`.
4. **Metrics check** (Refactoring Report targets): no module > ~600 lines, no
   function > ~50 lines, 0 inert `__post_init__`, 0 raw f-string SQL.
5. **End-to-end smoke.** Run the production orchestration entrypoint (task 5c)
   against a small sample to confirm download→convert→verify→load works on the
   remediated codebase.
6. Write a short `prompts/review-remediation/COMPLETION.md` summarizing what was
   done per wave and confirming the backlog is fully cleared.

## Steps

- [ ] **1** — Backlog audit; record any gap and fix it (do not defer — the pack
  mandate is defer nothing).
- [ ] **2** — Full suite + coverage green.
- [ ] **3** — `ruff check .` clean; run the residue greps above — all empty.
- [ ] **4** — Metrics check passes.
- [ ] **5** — End-to-end smoke passes.
- [ ] **6** — Write `COMPLETION.md`. Commit.

## Acceptance criteria

- [ ] Every backlog row across all 5 waves is verifiably implemented.
- [ ] `uv run pytest` green; coverage clears the gate; `ruff check` clean.
- [ ] The residue greps (`ic(`, `except Exception`, `text(f"`,
  `datetime.utcnow`, commented-out code) are all empty in `app/`.
- [ ] The end-to-end pipeline runs on the remediated codebase.
- [ ] `COMPLETION.md` documents the finished remediation.

## Collision protocol

Cut after 5a–5d merge. Final integration — expected to touch any straggler;
runs alone.
