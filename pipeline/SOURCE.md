# Data Sources

## Crime — UK Police street-level data
- **Source:** data.police.uk custom download
- **URL:** https://data.police.uk/data/archive/
- **Forces selected:** Metropolitan Police Service; City of London Police
- **Date range:** [2016-01] to [2026-04]  (10-year window)
- **Data type:** Street-level crime CSVs (`*-street.csv`)
- **Downloaded on:** [2026-06-18]
- **Licence:** Open Government Licence v3.0 (OGL-UK-3.0)
- **Attribution:** Contains public sector information licensed under the
  Open Government Licence v3.0. Data sourced from data.police.uk.

### Reproduction
See `pipeline/00_download.R` for the documented acquisition steps.

### Known caveats
- Counts are anonymised and snapshotted at publication; figures may differ
  from official statistics.
- British Transport Police records excluded (rail-network crime not captured).
- City of London has a very small resident population — per-capita rates
  will be extreme; handle explicitly downstream.