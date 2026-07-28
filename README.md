# London Crime & Social Determinants Dashboard

Borough-level analysis of how recorded crime in London **associates** with social
determinants - income, deprivation, well-being, and life expectancy — built as a
reproducible R data pipeline feeding a Next.js web interface.

> **Scope note.** This is an observational, ecological analysis of 33 aggregated
> borough units. It identifies associations between crime rates and socioeconomic
> conditions; it does not, and cannot, establish causal determinants. Analytical
> choices below (feature exclusions, window limits) follow from taking that
> constraint seriously.

## What this project demonstrates

- **Reproducible pipeline engineering:** every derived output is regenerated from
  documented raw sources by ordered R scripts; raw data is never committed, but the
  recipe to rebuild it is.
- **Provenance discipline:** all sources, licences (OGL v3.0), download dates, and
  handling decisions are recorded in [`pipeline/SOURCES.md`](pipeline/SOURCES.md).
- **Honest measurement decisions:** documented exclusions and window limits rather
  than silently convenient data (see *Analytical decisions*).
- **Full-stack delivery:** R data layer → JSON contract → Next.js API routes →
  interactive choropleth frontend (in progress).

## Data

| Source | Grain | Coverage used |
|---|---|---|
| UK Police street-level crime (Met + City of London) | LSOA → borough | 2011-01 – 2026-04 |
| HMRC personal income (median) | borough, annual | 2011 – 2023 |
| IMD deprivation domain scores (MHCLG) | borough summary | 2015, 2019 snapshots |
| ONS4 personal well-being | borough, annual | 2011/12 onward |
| Life expectancy (M/F) | borough, rolling annual | 2011 onward |
| ONS mid-year population (rate denominator) | borough, annual | 2011 – 2024 |

Crime records are assigned to boroughs via the ONS LSOA(2011)→LSOA(2021)→LAD(2022)
lookup, harmonising both LSOA code vintages across the 15-year window.

## Analytical decisions (and why)

- **Analysis window 2011–2023; crime trend to 2024.** Cross-metric analysis is
  bounded by the shortest denominator (population to mid-2024, income to 2023).
  2025+ appears only as flagged partial counts; 2026 (four months) is never shown
  as a full year.
- **IMD Crime domain excluded from analysis.** It is constructed from recorded
  crime, so using it to explain crime rates is circular. It is retained solely as
  an external validation check on our police-derived rates.
- **Median income over mean.** Borough income is right-skewed; medians resist
  distortion by high earners.
- **Documented exclusions in the LSOA join.** Records with blank LSOA codes
  (ungeocoded by police) and boundary-spillover records mapping outside the 33
  boroughs (GSS `E09*`) are excluded, with counts logged in
  `pipeline/logs/lsoa_lookup.log`.
- **Per-capita caveat.** City of London's tiny resident population makes its
  per-capita rates extreme; the interface allows excluding it from scales.

## Repository layout

```
pipeline/               R pipeline: acquisition checks, LSOA lookup, tidy scripts,
                        unification, QA logs, SOURCES.md
pipeline/experimental/  exploratory scripts (not part of the canonical run)
data/raw/               raw source files (gitignored; rebuild per SOURCES.md)
data/processed/         derived outputs: lookup, boroughs.json, coverage.json
web/                    Next.js frontend and API routes
projects-plan.md        roadmap: issues, branches, acceptance criteria
```

## Reproducing the pipeline

1. Populate `data/raw/` as described in `pipeline/SOURCES.md`.
2. From the repository root, run in order:
   ```bash
   Rscript pipeline/00_download.R        # verifies raw monthly coverage
   Rscript pipeline/00_LSAOlookup.R      # LSOA→borough lookup + coverage log
   Rscript pipeline/00_crime_rowcounts.R # per-year row-count sanity log
   # tidy + unification scripts (10_–20_) as they land — see projects-plan.md
   ```
3. Frontend: `cd web && npm install && npm run dev`

## Status

- **Done:** monorepo + CI (lint/build on PRs, protected branches); crime raw-file
  coverage validation; LSOA→borough lookup with logged exclusions and 33-borough
  verification; per-year row-count QA.
- **In progress:** per-source tidy scripts to a common long schema; metric coverage
  matrix; SOURCES.md consolidation.
- **Next:** unified `boroughs.json` + `coverage.json` export, borough GeoJSON, API
  routes, choropleth with coverage-aware controls, narrative write-up.

## Known limitations

Ecological analysis over 33 units — associations only, no causal identification.
Police geocoding gaps (blank LSOAs) are excluded, not imputed. IMD exists at two
snapshots, so deprivation supports cross-sectional comparison, not trends. British
Transport Police records (rail-network crime) are out of scope.

## References

- Sources & licences: `pipeline/SOURCES.md`
- Roadmap: `projects-plan.md`

Contains public sector information licensed under the Open Government Licence v3.0.