# Pipeline Overview

This pipeline is organized around reproducible R scripts that consume raw data from `data/raw/`, produce cleaned outputs in `data/processed/`, and document sources in `pipeline/SOURCES.md`.

## Expected raw layout
- `data/raw/crime/` — UK Police street-level crime CSVs (`*-street.csv`)
- `data/raw/LSAO_lookup/` — 2011/2021 LSOA-to-borough lookup CSV
- `data/raw/personal_well_being/` — income, wellbeing, and IMD source files

## Canonical scripts
1. `pipeline/00_download.R` — validates crime raw data month coverage.
2. `pipeline/00_LSAOlookup.R` — builds the borough lookup from raw LSOA correspondence.
3. `pipeline/00_crime_rowcounts.R` — computes per-year rowcounts for crime files.
4. `pipeline/dimension/01_crime_by_LSOA.R` — joins crime to boroughs and aggregates counts.
5. `pipeline/dimension/01_LSOA_by_population.R` — computes borough population joins and derived rates.

## How to run
Run the scripts from the repository root with `Rscript`:

```bash
Rscript pipeline/00_download.R
Rscript pipeline/00_LSAOlookup.R
Rscript pipeline/00_crime_rowcounts.R
Rscript pipeline/dimension/01_crime_by_LSOA.R
Rscript pipeline/dimension/01_LSOA_by_population.R
```

## Outputs
- `data/processed/lsoa_lookup.csv`
- `data/processed/crime_counts_by_year.csv`
- `data/processed/crime_by_borough.csv`
- `data/processed/crime_counts_by_crime_type.csv`
- `data/processed/crime_counts_by_crime_subcategory_type.csv`
- `data/processed/London_average_income.csv`
- `data/processed/IDMP_2015_n_2019.csv`
- `data/processed/london_population.csv`
- `data/processed/crime_type_by_year_and_population.csv`

## Notes
- `data/raw/` is ignored by git; `data/processed/` is intended for tracked derived outputs.
- Large intermediate outputs such as `data/processed/crime.csv` and `data/processed/crime_by_borough.csv` may be generated locally and are excluded from git tracking.
