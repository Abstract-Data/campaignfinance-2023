# Task 4z — Wave 4 integration

> **Wave 4, serial — runs LAST, after 4a–4e merge.**
> Branch `remediation/wave-4/task-4z-integration`. Read the pack README.

## Context

Wave 4 reworked the core ingest path — detail-builder registry, version helper,
N+1 elimination, value objects, and the validator Base/Table split. This task
wires the pieces together and verifies the wave.

## What to do

1. **Adopt value objects.** Confirm `builders.py` (4c) uses the `PersonName` /
   `AddressParts` / `Officer` types from `value_objects.py` (4d); wire any
   remaining ad-hoc `*_data` dict the processor still passes.
2. **Verify the streaming path.** Confirm `production_loader.py` uses
   `process_record_stream` (4a) and Polars batched/lazy reads so peak memory is
   bounded by batch size, not file size — fix the loader's `pl.read_*` call to
   `pl.scan_*`/`iter_slices` if 4a left that to integration.
3. **Cross-cutting residue check.** `grep -rn "except Exception" app/core/`
   and `grep -rn "ic(" app/core/` — confirm Wave 4 cleared the core-path
   instances; fix any straggler. In particular, confirm the former
   `unified_sqlmodels.py:1446` lookup-helper bare-except (relocated into
   `builders.py` or `processor.py` by the Wave 3 split) was narrowed by task
   4a or 4c — it must not fall between them.
4. **Run the full suite** including the new per-builder, versioning,
   loader-performance, value-object, and validator-split tests.

## Steps

- [ ] **1** — Wire value-object adoption and the streaming loader read.
- [ ] **2** — Residue grep; fix stragglers.
- [ ] **3** — `uv run pytest tests app/tests` — full suite green. Commit.
- [ ] **4** — Confirm every Wave 4 backlog row is satisfied (RF-DRY-001/002,
  RF-CPLX-001/003, RF-SMELL-003/004, P2-PERF-001/002, P2-MNT-001, P2-ARC-001, R11).

## Acceptance criteria

- [ ] Full suite green; the core path uses streaming, the registry, value
  objects, and one-session-per-batch.
- [ ] No bare `except Exception` or `ic()` remains on the core ingest path.
- [ ] Every Wave 4 backlog item is verifiably done.

## Collision protocol

Cut after 4a–4e merge. Expected to touch integration seams across the wave's
files — runs alone.
