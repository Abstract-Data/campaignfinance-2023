# Task 3b — Validator mixins: Texas address + Oklahoma shared helpers

> **Wave 3, parallel. Branch `remediation/wave-3/task-3b-texas-validator-mixin`.**
> Requires Wave 2 merged. Read the pack README and the Refactoring Report
> (**RF-DRY-003**, **RF-DRY-004**).

## Context

### Texas (RF-DRY-003)

Every Texas validator class re-declares the same prefix-filtering address
extraction (`{k: v for k, v in values.items() if k.startswith('chairStreet')}`)
and the same `clear_blank_strings` / `format_dates` / `format_zipcodes`
`model_validator(mode='before')` chain. There are five `validate_addresses`
classmethods in `texas_filers.py` alone, and the cleanup chain repeats across
`texas_settings.py`, `texas_contributions.py`, `texas_filers.py`,
`texas_expenses.py`.

Additionally, `clear_blank_strings` is **re-registered on subclasses 4 times**
despite inheritance already carrying the validator — the duplicate registrations
are dead weight (and in Pydantic v2 can cause duplicate-validation surprises).

### Oklahoma (RF-DRY-004)

`parse_candidate_name` and `parse_zipcode` are duplicated across 2–3 Oklahoma
validator classes. These should live in a single shared OK helper module
(or the same cross-state `_mixins.py` if the logic is identical to TX).

## Files

- **Create:** `app/states/texas/validators/_mixins.py`
- **Modify:** `app/states/texas/validators/texas_filers.py`,
  `texas_settings.py`, `texas_contributions.py`, `texas_expenses.py`
- **Create:** `app/states/oklahoma/validators/_helpers.py`
- **Modify:** Oklahoma validator classes that duplicate `parse_candidate_name` /
  `parse_zipcode` (locate with `grep -rn "parse_candidate_name\|parse_zipcode"
  app/states/oklahoma/`)

## What to implement

### RF-DRY-003 — Texas mixin

- Create an `extract_address(values, prefix)` helper and an
  `AddressValidatedModel` mixin (in `_mixins.py`) that registers the common
  `before` validators (`clear_blank_strings`, `format_dates`, `format_zipcodes`)
  **once**. Texas validator classes inherit the mixin instead of re-declaring
  the chain.
- Replace the five `validate_addresses` classmethods and the repeated
  `model_validator(mode='before')` blocks with the mixin/helper. Also fold the
  near-duplicated `format_payee_name` / `check_name` logic into a shared helper.
- **Remove the 4× redundant `clear_blank_strings` re-registrations on
  subclasses** — if a subclass inherits `AddressValidatedModel`, it must not
  re-declare `clear_blank_strings` as its own validator.

### RF-DRY-004 — Oklahoma helpers

- Create `app/states/oklahoma/validators/_helpers.py` with a single
  `parse_candidate_name(raw: str) -> dict` and a single
  `parse_zipcode(raw: str) -> str` (or equivalent signatures matching current
  usage).
- Update every Oklahoma validator class that currently defines its own copy to
  import from `_helpers.py` instead.

Behaviour must be identical for both states — existing tests pass unchanged.

## Steps

- [ ] **1** — Create `_mixins.py` with `extract_address` + `AddressValidatedModel`.
- [ ] **2** — Rewire Texas validators to inherit the mixin; delete duplicated
  blocks; remove the 4× redundant `clear_blank_strings` subclass re-registrations.
- [ ] **3** — `grep -rn "parse_candidate_name\|parse_zipcode"
  app/states/oklahoma/` — list all duplicated sites.
- [ ] **4** — Create `_helpers.py`; update Oklahoma validator classes to import
  from it; delete the per-class copies.
- [ ] **5** — `uv run pytest` for the Texas and Oklahoma validator suites —
  must pass unchanged. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] One `extract_address` helper + one `AddressValidatedModel` mixin exist.
- [ ] The five Texas `validate_addresses` copies and the repeated `before`-validator
  chains are gone; classes inherit the mixin.
- [ ] No subclass re-registers `clear_blank_strings` if it inherits the mixin.
- [ ] `parse_candidate_name` and `parse_zipcode` each defined once in
  `app/states/oklahoma/validators/_helpers.py`.
- [ ] All Texas and Oklahoma validator tests pass with no behaviour change.

## Collision protocol

You own the four Texas validator files + the new Texas `_mixins.py` + all
Oklahoma validator files + the new OK `_helpers.py`. Task 3a owns `app/core/` —
disjoint. The Base/Create/Table split of these validators is **Wave 4 task 4e**
— do not do it here; only de-duplicate.
