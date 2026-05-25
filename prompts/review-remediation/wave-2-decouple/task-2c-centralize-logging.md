# Task 2c — Centralize logging

> **Wave 2, parallel. Branch `remediation/wave-2/task-2c-centralize-logging`.**
> Requires Wave 1 merged. Read the pack README and the Code Review Report
> (**P2-OPS-002**).

## Context

`app/logger.py` hardcodes the PaperTrail host/port as class constants (ignoring
the `PAPERTRAIL_*` env vars in `.env.example`). `Logger` is a `@dataclass` whose
`__post_init__` builds a `SysLogHandler` (a network socket), a
`TimedRotatingFileHandler`, and a `StreamHandler` **every time** a `Logger(...)`
is constructed — and modules build new loggers freely (e.g.
`abc_validation.py:31-34` rebuilds one on every `.logger` property access). So
handlers/sockets leak and log lines duplicate. `SysLogHandler` also has no
connection timeout — an unreachable host can stall startup.

## Files

- **Modify:** `app/logger.py`
- **Modify:** `app/abcs/abc_validation.py`
- **Create:** `tests/test_logging_config.py`

## What to implement (P2-OPS-002)

- Configure logging **once** at process start via `logging.config.dictConfig`.
  Read the PaperTrail host/port from settings/env (`PAPERTRAIL_HOST`,
  `PAPERTRAIL_PORT`) — do not hardcode them. Give the syslog handler a
  connection timeout (or make it optional/best-effort so an unreachable host
  cannot block).
- Have modules obtain loggers via `logging.getLogger(__name__)` (cached and
  idempotent) rather than constructing handler-building `Logger` objects.
  Provide a thin compatibility shim if many call sites use the old `Logger(...)`
  API, so this task does not have to touch every caller.
- Fix `abc_validation.py` so it does **not** rebuild a `Logger` on every
  `.logger` property read — cache it (module-level `getLogger`, or compute once).

## Steps

- [ ] **1** — `tests/test_logging_config.py`: failing tests that PaperTrail
  host/port come from env, and that obtaining the same logger twice does not
  add duplicate handlers.
- [ ] **2** — Run; expect fail. **3** — Implement `dictConfig` setup + the
  `getLogger` shift; fix `abc_validation.py`. **4** — Run; pass.
  `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] PaperTrail host/port are read from env, not hardcoded.
- [ ] Handlers are not duplicated when loggers are re-obtained (proven by test).
- [ ] An unreachable PaperTrail host cannot stall the process.
- [ ] `abc_validation.py` no longer rebuilds a logger per property access.

## Collision protocol

You own `app/logger.py` and `app/abcs/abc_validation.py`. No other wave-2 task
touches them.
