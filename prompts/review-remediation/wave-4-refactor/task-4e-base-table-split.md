# Task 4e — Base/Create/Read/Table model split for validators

> **Wave 4, parallel. Branch `remediation/wave-4/task-4e-base-table-split`.**
> Requires Wave 3 merged. Read the pack README and the Code Review Report
> (**P2-ARC-001**).

## Context

The Multi-Model pattern is absent. Texas validators (`TECFilerName`,
`TECFilerIdentity`, `TECTreasurer`, etc.) are declared `table=True` yet are also
the objects passed to `model_validate()` in the validation ABC. A `table=True`
model carries every internal column (PKs, `created_at`, FKs, relationships) and
SQLModel disables instance validation by default — so using table classes as
the parse boundary both skips validation and exposes internal fields. This is
the documented SQLModel anti-pattern.

## Files

- **Modify:** `app/states/texas/validators/*.py`,
  `app/states/oklahoma/validators/*.py`
- **Modify:** `app/abcs/abc_validation.py` (the `model_validate()` call surface)
- **Create:** `tests/test_validator_model_split.py`

## What to implement (P2-ARC-001)

Adopt the Base/Create/Table split for each state validator record type:

- A non-table `...Base` (or plain Pydantic `...Create`) model is the
  **validation/ingest surface** — it carries only the real input fields and has
  validation enabled.
- A separate `table=True` model is **persistence only**, inheriting the base and
  adding `id`/PKs/audit/FK/relationship fields.
- Conversion is `Table.model_validate(create_obj)`.
- Update `abc_validation.py` so `model_validate()` is called against the
  non-table `...Create`/`...Base` model, not the table class.

Work record-type by record-type so the suite stays green throughout. The Code
Review Report gives the `FilerBase`/`Filer` example to follow.

## Steps

- [ ] **1** — `tests/test_validator_model_split.py`: failing tests that the
  ingest surface for a representative record type (a) rejects an unknown field,
  (b) does **not** require persistence-only fields (`id`, `created_at`), and
  (c) `model_validate` actually runs validators.
- [ ] **2** — Run; expect fail. **3** — Split each validator into Base/Table;
  rewire `abc_validation.py`. Run the suite after each record type.
- [ ] **4** — Full suite green. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] Each state validator record type has a non-table validation model and a
  separate `table=True` persistence model.
- [ ] `abc_validation.py` validates against the non-table model.
- [ ] No `table=True` class is used directly as a parse/IO surface.
- [ ] The validator suite is green.

## Collision protocol

You own `app/states/*/validators/` and `abc_validation.py` for Wave 4. Wave 3
task 3b already added the validator mixin — build on it. Tasks 4a–4d own
`app/core/` files — disjoint.
