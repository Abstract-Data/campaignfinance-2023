# Task 1e — Secret hygiene in `op.py`

> **Wave 1, parallel. Branch `remediation/wave-1/task-1e-secret-hygiene`.**
> Read the pack README and the Code Review Report (**P1-SEC-003**).

## Context

`app/op.py` resolves secrets from 1Password into `SecretStr` (good) but
`OnePasswordItem.database_url` (`op.py:110-122`) calls `.get_secret_value()` and
concatenates user/password/host into a plain `str` — the cleartext password then
exists as an interned string and leaks into any traceback/`repr`/log. Settings
models also use `extra='ignore'`, which silently drops typo'd credential vars.

## Files

- **Modify:** `app/op.py`
- **Create:** `tests/test_op_secrets.py`

## What to implement (P1-SEC-003)

- Build the DB URL with SQLAlchemy's `URL.create("postgresql", username=...,
  password=pwd.get_secret_value(), host=..., port=..., database=...)`. `str(url)`
  masks the password as `***` while the engine still connects — so the cleartext
  password is never an interned plain string in the URL object.
- Set `extra='forbid'` on `OnePasswordSettings` and any credential-bearing
  settings model so a mistyped env var fails loudly.
- While in this file: `op.py:38-39` catches `Error` and returns `None`, silently
  hiding 1Password failures — narrow it to the specific exception and log via
  the project `Logger` at `error` level (this file's share of P2-MNT-001).

## Steps

- [ ] **1** — `tests/test_op_secrets.py`: failing tests that `str()` of the
  built URL masks the password, that an unknown extra field raises (not
  silently dropped), and that a 1Password resolution failure is logged/raised
  rather than silently `None`.
- [ ] **2** — Run; expect fail. **3** — Implement. **4** — Run; pass.
  `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] The DB password never appears in `str()`/`repr()` of the URL object.
- [ ] Credential settings models use `extra='forbid'`.
- [ ] `op.py:38-39` no longer swallows failures silently.

## Collision protocol

You own `app/op.py`. No other wave-1 task touches it.
