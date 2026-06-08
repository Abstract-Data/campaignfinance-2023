# Data Note: TEC Contributor Address Limitation

**Date:** 2026-06-07
**Status:** Accepted

## Summary

TEC (Texas Ethics Commission) public extract files contain **no contributor
street lines**.  Contribution CSV headers carry only:

```
city, state, county, country, zip, region
```

There is no `contributorStreet1` or `contributorStreet2` column in TEC RCPT
files.  Likewise, TRVL (travel) records omit traveller street addresses.

## Consequence for the resolve pipeline

`address.line_1` comparisons in the entity resolution scoring function only
fire for entities sourced from **filer**, **treasurer**, **lender**, or
**candidate** records — record types whose CSV schemas do carry full address
fields.  Contributor and traveller entities will have `line_1 = NULL` in
`resolution_input`, so the address blocking key is absent and the address
similarity feature scores as 0 (no match signal either way).

This is a **source-data limitation, not a bug**.  The pipeline handles it
correctly: `address.line_1` is nullable throughout the unified and resolution
schemas, and the scorer is designed to degrade gracefully when address signals
are absent.

## Verification

Inspect the raw TEC RCPT CSV column headers to confirm no street column exists:

```
recordType, formTypeCd, schedFormTypeCd, reportInfoIdent, receivedDt,
infoOnlyFlag, filerIdent, filerTypeCd, filerName, contributorNameOrganization,
contributorNameLast, contributorNameSuffixCd, contributorNameFirst,
contributorNamePrefixCd, contributorNameShort, contributorStreetCity,
contributorStreetStateCd, contributorStreetCountyCd, contributorStreetCountryCd,
contributorStreetPostalCode, contributorStreetRegion, ...
```

`contributorStreetCity` is present; `contributorStreet1`/`contributorStreet2`
are absent.
