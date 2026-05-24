# Golden-Set Fixture Guide

This directory holds hand-labeled pair fixtures for the probabilistic-matching
regression harness (`tests/resolve/test_match_quality.py`). Each CSV contains
pairs of standardized entity records with a ground-truth `label` column
(`match` / `no_match`). The harness computes precision and recall against these
labels and fails CI if precision drops below `PRECISION_FLOOR` (currently 0.95).

## Files

| File | Entity type | Pairs | Columns |
|------|-------------|-------|---------|
| `person_pairs.csv` | Individual persons | 52 | person name fields + address (_a / _b suffix) |
| `organization_pairs.csv` | Businesses / orgs | 50 | normalized org name + address (_a / _b suffix) |
| `committee_pairs.csv` | Political committees | 50 | filer_id + normalized org name + address (_a / _b suffix) |

## Column Reference

### person_pairs.csv

| Column | Source field | Notes |
|--------|-------------|-------|
| `pair_id` | — | Unique pair ID (e.g., `p001`) |
| `label` | — | `match` or `no_match` (ground truth) |
| `notes` | — | Category tag (see below) |
| `first_name_a/b` | `ResolutionInput.first_name` | Standardized first name |
| `middle_name_a/b` | `ResolutionInput.middle_name` | May be empty |
| `last_name_a/b` | `ResolutionInput.last_name` | Standardized last name |
| `suffix_a/b` | `ResolutionInput.suffix` | Jr, Sr, III, etc. |
| `line_1_a/b` | `ResolutionInput.line_1` | Street address |
| `city_a/b` | `ResolutionInput.city` | City |
| `state_a/b` | `ResolutionInput.state` | Two-letter state code |
| `zip5_a/b` | `ResolutionInput.zip5` | Five-digit ZIP |
| `raw_name_a/b` | `ResolutionInput.raw_name` | Original raw name string |

### organization_pairs.csv

| Column | Source field | Notes |
|--------|-------------|-------|
| `pair_id` | — | Unique pair ID (e.g., `o001`) |
| `label` | — | `match` or `no_match` |
| `notes` | — | Category tag |
| `raw_name_a/b` | `ResolutionInput.raw_name` | Original name before normalization |
| `normalized_org_a/b` | `ResolutionInput.normalized_org` | Output of `normalize_org_name()` |
| `line_1_a/b` | `ResolutionInput.line_1` | Street address |
| `city_a/b` | `ResolutionInput.city` | City |
| `state_a/b` | `ResolutionInput.state` | State |
| `zip5_a/b` | `ResolutionInput.zip5` | ZIP |

### committee_pairs.csv

Same as `organization_pairs.csv`, plus:

| Column | Source field | Notes |
|--------|-------------|-------|
| `filer_id_a/b` | `ResolutionInput.source_id` | Texas Ethics Commission filer ID |

## Category Tags (`notes` column)

| Tag | Description |
|-----|-------------|
| `easy_match_same_address` | Same name, same address — trivially identical |
| `easy_match_diff_address` | Same name, different filing address (one may be PO Box) |
| `easy_match_same_filer_id` | Same committee filer ID |
| `same_name_diff_filer_id` | Same committee re-registered under new ID |
| `name_variant_nickname` | Nickname vs full name (Jim/James, Liz/Elizabeth) |
| `suffix_variant_corp_inc` | Legal-suffix variation handled by `normalize_org_name` |
| `abbreviation_hard` | Abbreviated name vs full name (SW vs Southwest) |
| `typo_last_name` / `typo_in_name` | Minor typo in one record |
| `middle_name_variant` | Initial vs full middle name |
| `spelling_variant` | Alternate spelling (Ann/Anne) |
| `name_variant_cultural` | Cross-language equivalents (Jose/Joseph) |
| `shared_address_diff_person` | Same address, genuinely different people |
| `diff_person_same_last_name` | Same last name, different first name |
| `similar_name_diff_org` | Similar-sounding orgs that are distinct |
| `hard_*` (committee) | Hard committee pairs requiring probabilistic matching |

## Hard Cases (Must Remain in Set)

The following category types must always be represented to stress-test the
probabilistic scorer:

1. **Name variants / nicknames** — Jim vs James, Bob vs Robert (persons)
2. **Typos** — single-character edit distance in name or org name
3. **Shared address, different people** — same address, different first names
4. **Legal-suffix variants** — Corp vs Inc vs LLC (handled by normalization; easy)
5. **Abbreviations** — Tex vs Texas, SW vs Southwest, Assoc vs Association
6. **Committee re-registration** — same committee, new filer ID

## How to Add Pairs

1. **Choose the right CSV** based on entity type.
2. **Assign the next sequential `pair_id`** (e.g., `p053`, `o051`, `c051`).
3. **Assign a `label`** — `match` if a human expert confirms these records
   represent the same real-world entity; `no_match` otherwise.
4. **Fill both record columns** (`_a` and `_b` suffix) using pre-standardized
   field values (output of `stage1` / `ResolutionInput` format).
5. **Pick or create a `notes` tag** from the table above; add new tags to the
   table if the case is novel.
6. **Run the harness** — `uv run pytest tests/resolve/test_match_quality.py -v`
   — to confirm precision still meets the floor.
7. **Commit** the updated CSV; CI will enforce the floor on every subsequent run.

## Precision Floor

`PRECISION_FLOOR = 0.95` is defined in `test_match_quality.py`. Raise it when
recall-focused improvements land and the scorer becomes more selective. The
recall metric is reported but not gated; it is expected to rise across Phase 2
and Phase 3 waves.

## Stale Data Warning

The golden set is drawn from synthetic Texas-style records. When real labeled
pairs from production data become available, add them and deprecate their
synthetic equivalents (keep the pair ID, update fields and source to `real_data`
in the `notes` column).
