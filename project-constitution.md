# Project Constitution

Immutable principles for campaignfinance. These are non-negotiable constraints —
they outrank convenience, velocity, and style. Changing them requires an
explicit, recorded decision (a new ADR), not a silent edit.

## Principles

1. **Data fidelity is paramount.** Campaign-finance data must never be silently
   altered, dropped, or merged. An unmapped column, an unexplained row-count
   change, or a dedup collision halts the pipeline — it is never "handled" by
   discarding data.

2. **Code is the source of truth; docs must match it.** When documentation and
   code disagree, the code wins and the docs are corrected.

3. **Security and correctness outrank everything else.** Parameterized queries
   only. Secrets resolve through the 1Password SDK. No change ships that trades
   correctness or safety for speed.

4. **Personal data is protected.** Donor and filer PII (names, addresses,
   phones, employers) is never logged, never committed, and never written to a
   non-gitignored file.

5. **State handling stays uniform.** Every state is processed through the same
   abstract base classes and the unified field library. No state gets bespoke,
   un-abstracted logic outside its state module.

6. **Decisions are recorded.** Significant tool, model, schema, or architecture
   choices are written as ADRs in `docs/adr/`. An undocumented decision is
   technical debt.

7. **Enforcement over intention.** Rules that matter are backed by hooks, CI, or
   tests — not by hoping an agent or contributor remembers them.

## Definition of Done

A task is done only when: tests pass, lint and format are clean, type hints are
present on new public functions, no secrets or PII are committed, documentation
affected by the change is updated, and — for risky operations — a rollback path
is documented. See `AGENTS.md` for the full checklist.
