# NOTES

Bootstrap context for any AI tool or new contributor. Plain Markdown, no
tool-specific syntax. Full standards live in AGENTS.md.

## Project

Campaign finance data processing system. Aggregates, normalizes, and analyzes
US state campaign-finance data behind a unified cross-state data model.
Currently supports Texas and Oklahoma. Internal Abstract Data project, active
development.

## Tech stack

- Language: Python 3.12+
- Package manager: uv
- Data: Polars (primary), pandas (legacy interop)
- Models/validation: SQLModel + Pydantic v2
- Database: PostgreSQL (production), SQLite (development)
- Scraping: Selenium (state campaign-finance portals)
- Tests: pytest + Hypothesis
- Lint/format: Ruff
- Secrets: 1Password SDK (with .env fallback for local dev)

## Setup

```
uv sync                  # install dependencies
uv run pytest            # run the test suite
uv run python app/main.py
```

## Project structure

- app/abcs/   abstract base classes (state-agnostic processing)
- app/core/   unified cross-state models, field library, loaders
- app/ingest/ schema-driven file parsing (GenericFileReader)
- app/states/ Texas and Oklahoma implementations
- app/funcs/  shared utilities
- scripts/    loaders, debug, analysis
- tests/      integration tests; app/tests/ holds unit tests
- docs/       architecture, data dictionary, runbook, guardrails

## Testing

Unit tests in app/tests/, integration tests in tests/. Pytest + Hypothesis for
property-based testing. Run `uv run pytest`; all tests must pass before commit.

## Key decisions

The pipeline is download → ingest → per-state validation → unified model →
PostgreSQL load. State-agnostic processing uses abstract base classes; a unified
field library maps every state's columns to common fields. Full rationale is in
docs/adr/.

## Active constraints

- Do not modify downloaded data files in tmp/ — treat them as read-only source.
- Do not commit .env or credentials; secrets resolve through the 1Password SDK.
- Do not use string-interpolated SQL; use parameterized SQLModel queries.
- Do not log donor/filer PII; log record IDs and counts instead.
- New states inherit the ABC pattern and register field mappings.
