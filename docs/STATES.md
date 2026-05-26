# States Reference
# Campaign Finance — State-Specific Configurations & Quirks
# Last Updated: 2026-05-25

This document is the agent reference for state-specific data formats, portal behavior,
known quirks, and scraper patterns. Consult this before:
- Writing or modifying a state scraper or downloader
- Adding field mappings for a new state
- Debugging a state-specific ingestion failure
- Running DriftDetector baseline updates

See also: `docs/RUNBOOK.md` for operational debug commands,
`docs/DATA_DICTIONARY.md` for field definitions.

---

## Implemented States

| State | Module | Status | Data Format | Portal Type |
|-------|--------|--------|-------------|-------------|
| Texas | `app/states/texas/` | ✅ Production | CSV → Parquet | Bulk CSV download (TEC) |
| Oklahoma | `app/states/oklahoma/` | ✅ Production | CSV | Direct download |
| Ohio | `app/states/ohio/` | 🔄 In progress | TBD | TBD |

---

## Texas

**Regulator:** Texas Ethics Commission (TEC)  
**Portal:** https://www.ethics.state.tx.us/  
**Module:** `app/states/texas/`  
**Data categories:** Contributions (`contributions_*`), Expenditures (`expendi_*`),
Loans, Travel

### File Format

- Source files are **ZIP archives** containing delimited CSV files
- Must be converted to Parquet before ingestion: `app/states/texas/texas_converter.py`
- `infer_schema_length=0` is used — **all columns read as strings** (never let Polars
  infer types on Texas data; numeric columns contain state-specific codes mixed with values)
- **Skip metadata files**: `CFS-Codes*.txt` and `CFS-ReadMe*.txt` are TEC documentation
  files, not data — `_is_metadata_file()` in the converter filters these out

### Known Quirks

- **Encoding**: Latin-1 encoded, not UTF-8. The converter handles this; do not assume
  UTF-8 when reading raw CSV.
- **Missing `address_line1`/`address_line2`**: Texas contributions sometimes split the
  address across non-standard column names. Verify mappings in `StateFieldMapping` for
  Texas before adding new address fields.
- **`TexasCategory` enum**: Defines the contribution/expenditure/loan categories.
  Import from `app/states/texas` — do not hardcode category strings.
- **Scraper fragility**: Texas uses a Selenium scraper for filer search. The
  `DriftDetector` must be run against `tests/fixtures/scrapers/texas_filer_search.html`
  to catch portal layout changes. If the scraper errors, check for portal updates first.
- **Legacy `sys.path` bootstrap**: Older scripts under `scripts/` add `app/` to
  `sys.path` manually. CLI entry points via `uv run cf` handle this automatically — do
  not replicate the manual path hack in new code.

### CLI Commands

```bash
# Prepare data (download + convert)
uv run cf prepare texas

# Load into DB
uv run python scripts/loaders/production_loader.py high_performance texas_2024

# Check field mappings
uv run python -c "
from app.core.unified_field_library import UnifiedFieldLibrary
lib = UnifiedFieldLibrary()
print(lib.get_state_mappings('texas'))
"
```

### Validation Models

Texas contributions use a four-level model hierarchy in
`app/states/texas/validators/texas_contributions.py`. Models use `extra='forbid'` — any
unexpected field in the source data raises a validation error rather than silently
dropping it.

---

## Oklahoma

**Regulator:** Oklahoma Ethics Commission  
**Portal:** https://guardian.ok.gov/  
**Module:** `app/states/oklahoma/`  
**Data categories:** Contributions, Expenditures

### File Format

- Direct CSV download — no ZIP extraction required
- Standard UTF-8 encoding

### Known Quirks

- **`OklahomaContribution` model split (Wave 2b):** The contribution model uses a
  four-level discriminated union to handle individual vs. entity contributors with
  different required fields. Do not collapse back to a flat model — the split exists to
  enforce `extra='forbid'` at each level.
- **`oklahoma/funcs/` pattern**: Oklahoma uses `partial(funcs.xxx)` for validator
  composition. Do not replace with raw `functools.partial` calls from the standard
  library — the project-specific `funcs` module handles state context injection.
- **No Selenium**: Oklahoma uses direct HTTP download, no browser automation. There is
  no `DriftDetector` baseline for Oklahoma; scraper failures indicate a URL or format
  change at the portal level.

### CLI Commands

```bash
# Load Oklahoma data
uv run python scripts/loaders/production_loader.py testing oklahoma_2020
uv run python scripts/loaders/production_loader.py high_performance oklahoma_2021
```

---

## Ohio

**Status:** In progress  
**Module:** `app/states/ohio/`

Ohio is not yet in production. Consult the implementation plan in `prompts/` for the
current task status. Do not write Ohio-specific validators until the portal format is
confirmed and a field mapping has been approved.

---

## Adding a New State

1. Create `app/states/{state}/` with `__init__.py`
2. Add `StateFieldMapping` entries to `UnifiedFieldLibrary` for the state
3. Write validators under `app/states/{state}/validators/` using `extra='forbid'`
4. Add a Selenium `DriftDetector` if the portal requires browser automation; save an
   HTML fixture under `tests/fixtures/scrapers/{state}_*.html`
5. Add the state to `docs/STATES.md` (this file)
6. Run `uv run pytest tests/ -q` — all existing tests must still pass

See `CONTRIBUTING.md` for the full checklist.

---

## DriftDetector — Baseline Management

`app/scrapers/drift_detector.py` — structural fingerprint comparison for Selenium scrapers.

**When to update a baseline:**
- The portal intentionally redesigned its layout (confirmed with TEC/regulator)
- A scraper update was deployed that handles the new layout

**How to update:**
```bash
# 1. Run the scraper manually and save the current HTML
uv run python -c "
from app.scrapers.texas_scraper import fetch_page_html
html = fetch_page_html('filer_search')
open('tests/fixtures/scrapers/texas_filer_search.html', 'w').write(html)
"

# 2. Run drift detection against the new baseline
uv run pytest tests/ -q -k "drift"
```

**Never update the baseline without human review** — a portal change may indicate data
format changes that require validator updates before the next ingestion run.
