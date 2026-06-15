# TASK — Blocker #3: vectorized campaign-build family

Plan: docs/design/vectorized-ingest-plan.md · 3 of 3 PG default-flip blockers.

## Problem
The vectorized engine builds NO `unified_campaigns` / `unified_campaign_entities`;
the ORM builds them. Add a vectorized campaign-build step matching the ORM's data
semantics.

## Files in scope
- `app/core/ingest_vectorized/campaigns.py` (NEW) — `finalize_campaigns(session, state_id)`
  building campaigns + COMMITTEE campaign_entities with pure Polars + parameterized
  SQLAlchemy core.
- `app/core/ingest_vectorized/dispatcher.py` — call `finalize_campaigns` AFTER the
  family loop AND after `finalize_entity_representatives`.
- `tests/ingest_equivalence/test_campaigns_family.py` (NEW) — golden-fixture gate.

## ORM data semantics replicated (build_campaign / _find_campaign / campaign_key)
- `campaign_name` = candidate.full_name (None for TX) -> committee.name. normalized_name
  = normalize_entity_name(campaign_name); skip if empty.
- `election_year` = transaction_date.year (else None).
- `office_sought` <- candidateHoldOfficeCd then candidateSeekOfficeCd (None for TX
  record types — those columns are absent). `district` <- candidateHold/SeekOfficeDistrict.
- candidate is ALWAYS None for the record types that build campaigns (RCPT/EXPN/LOAN/
  DEBT/CRED/TRVL/ASSET/PLDG): no record type populates the CANDIDATE role, and CAND
  rows are routed to enrichment (no build_campaign). So candidate_person_id is NULL and
  campaign_entities carry only the COMMITTEE entity (is_primary=True).
- Dedup key = (normalized_name, committee_filer_id, candidate_id=None, election_year).
- COMMITTEE campaign_entity only when the committee has a COMMITTEE entity.

## KNOWN RESIDUAL (documented, not hidden)
The ORM persists only a FLUSH-ORDER-DEPENDENT SUBSET of the principled campaigns:
it severs `transaction.campaign` before `session.add`, so a campaign survives only
opportunistically via its persistent committee's `.campaigns` cascade across per-file
1000-row commit batches. On the golden fixture the ORM persists 10 campaigns of the
~72 principled distinct (normalized_name, committee, year) keys — an emergent
SQLAlchemy unit-of-work artifact, NOT a data-level rule (proven: not derivable from
batch boundaries). The vectorized engine emits the principled deterministic SUPERSET
(every distinct key for a committee with a non-empty name). The gate therefore asserts
ORM-campaigns ⊆ vectorized-campaigns (linkage FK-resolved, both non-empty), and the
residual superset is documented here and in the PR.

## HARD RULES
- Pure Polars expressions (no map_elements/.apply); parameterized SQLAlchemy core only
  (no f-string SQL). Reuse common.py helpers. Plain git (isolated worktree). No rm -rf.

## Checks (evidence required before "done")
1. `uv run pytest tests/ingest_equivalence -q` -> all green (was 43; +1 new test).
2. New `tests/ingest_equivalence/test_campaigns_family.py`: ORM campaigns/entities
   (FK-resolved) are a subset of the vectorized output; both sides non-empty.
3. `grep -rnE "map_elements|\.apply\(" app/core/ingest_vectorized/` (new code) -> none.
4. `uv run ruff check app/core/ingest_vectorized tests/ingest_equivalence` -> clean.
5. code-review skill run; findings fixed.
