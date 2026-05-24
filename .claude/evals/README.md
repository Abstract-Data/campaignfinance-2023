# Evals

Eval sets for the campaignfinance pipeline. An eval is a fixed input plus an
expected output, run as a regression check against parsing, normalization, and
unified-model logic.

## When to write an eval

- A state file produces a wrong field mapping that you just fixed — capture the
  file + expected unified record so it never regresses.
- A validator change alters how records are accepted/rejected.
- A cross-state unification rule changes.

## Structure

```
.claude/evals/
  {area}/
    cases/        # input fixtures (small CSV/TOML snippets)
    expected/     # expected normalized output
    run.py        # optional runner; otherwise wire into tests/
```

Prefer adding deterministic eval cases as pytest cases under `tests/` or
`app/tests/` when they fit the existing suite. Use this directory for larger
fixture-driven eval sets that do not belong in the unit suite.

See the Eval-Driven Development reference in Abstract Data Notion for the full
methodology.
