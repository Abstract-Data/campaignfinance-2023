# ADR 0002: Data classification and retention

**Date:** 2026-05-25
**Status:** accepted

## Context

The campaignfinance pipeline ingests **public-record** campaign-finance filings from
state ethics commissions. Those filings nonetheless contain personally identifiable
information (PII): donor and filer names, postal addresses, employers, occupations,
and phone numbers. There is no documented classification or retention policy for
this data in the repository.

## Decision

- **Classification:** Treat all ingested state filing data as **public record /
  restricted operational data** — public by source, but subject to internal access
  controls and logging guardrails (see `docs/GUARDRAILS.md` PII-logging rules).
- **Retention:** Retain loaded data indefinitely as the analytical source of truth
  unless a state agency formally retracts or amends a filing; do not implement
  automated deletion of donor/filer PII from the unified schema.
- **Minimization in logs:** Never log raw PII fields at INFO or above; use record
  IDs, file origins, and aggregate counts in operational logs.
- **Downstream use:** Entity-resolution outputs may link records across filings;
  human review queues must treat matched clusters as sensitive.

## Consequences

- No GDPR-style erasure workflow is implemented; legal review is required before
  any "right to be forgotten" feature.
- Backup and production access policies must treat PostgreSQL as sensitive.
- Future ADRs are required if retention windows or anonymization are introduced.
