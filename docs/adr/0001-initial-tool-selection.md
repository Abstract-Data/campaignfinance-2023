# ADR 0001: Initial tool & stack selection

**Date:** 2026-05-23
**Status:** accepted

## Context

The campaignfinance project aggregates, normalizes, and analyzes US state
campaign-finance data (currently Texas and Oklahoma) behind a unified
cross-state data model. This ADR records the foundational stack decisions as
they stood when the repository was aligned to the Abstract Data project
template. It is written retroactively to give the project a concrete decision
baseline — earlier choices were not separately recorded.

## Decision

- **Language / runtime:** Python 3.12+, managed with **uv**. uv gives fast,
  reproducible installs and a single lockfile (`uv.lock`).
- **Data processing:** **Polars** as the primary DataFrame library for
  high-performance, lazy transforms; **pandas** retained only for legacy
  interop and analysis convenience.
- **Validation & models:** **Pydantic v2** and **SQLModel** — SQLModel unifies
  the ORM layer with Pydantic validation so state validators and the unified
  model share one type system.
- **Persistence:** **PostgreSQL** in production, **SQLite** for local
  development.
- **Ingestion:** schema-driven parsing via a `GenericFileReader`, with
  state-agnostic processing built on abstract base classes (`StateCategoryClass`
  and friends).
- **Acquisition:** **Selenium** for scraping state campaign-finance portals.
- **Secrets:** the **1Password SDK** (`onepassword-sdk`, wrapped in
  `app/op.py`), with `.env` fallback for local development.
- **Testing:** **pytest** plus **Hypothesis** for property-based testing of
  validators and the file reader.
- **Lint / format:** **Ruff**.
- **AI tooling:** Claude Code as the primary agent environment, with this
  repository aligned to the Abstract Data project template (enforcement hooks,
  subagents, Cursor rules, CI suite).

## Consequences

- A single Pydantic/SQLModel type system keeps state validators and the unified
  model consistent, at the cost of coupling ORM and validation concerns.
- Polars-first means new transforms must be written as Polars expressions;
  contributors familiar only with pandas have a short learning curve.
- The ABC-based ingestion design makes adding a new state a well-scoped task but
  requires every state to conform to the abstract interfaces.
- Selenium-based acquisition is sensitive to state-portal markup changes and is
  a known operational fragility (tracked in `docs/RUNBOOK.md`).
- Future significant changes — adding a state, swapping the DataFrame library,
  changing the persistence layer — must be recorded as new ADRs that supersede
  the relevant parts of this one.
