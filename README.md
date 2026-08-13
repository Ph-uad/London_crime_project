# London Crime & Social Determinants Dashboard

Borough-level analysis of how recorded crime in London **associates** with
social determinants — income, deprivation, well-being and life expectancy —
built as a reproducible R data pipeline feeding a Next.js web interface.

> **Scope note.** This is an observational, ecological analysis of 33 aggregated
> borough units. It identifies associations between crime rates and
> socioeconomic conditions; it does not, and cannot, establish causal
> determinants. The analytical choices below follow from taking that constraint
> seriously.

## What this project demonstrates

- **Reproducible pipeline engineering:** every derived output is regenerated
  from documented raw sources by ordered R scripts. Raw data is never
  committed, but the recipe to rebuild it is, and a synthetic-fixture smoke
  test proves the scripts still run.
- **Provenance discipline:** sources, licences, download dates and every
  handling decision are recorded in [`pipeline/SOURCES.md`](pipeline/SOURCES.md),
  including which licences are *verified* and which are still *assumed*.
- **Checks that can fail:** each stage asserts its own output — exclusion
  buckets sum to the raw total, 33 boroughs with no NA key, every crime type
  mapped, no rate published for a partial year — and exits non-zero when an
  assertion breaks.
- **Honest measurement decisions:** documented exclusions and window limits
  rather than silently convenient data (see *Analytical decisions*).
- **Full-stack delivery:** R data layer → JSON contract → Next.js API routes →
  interactive choropleth frontend (not yet started).

## Data

| Source | Grain | Coverage used | State |
|---|---|---|---|
| UK Police street-level crime (Met + City of London) | LSOA → borough | 2011-01 – 2026-04 | **incomplete — see below** |
| HMRC personal income (median) | borough, annual | 1999 – 2023 | in pipeline |
| IMD deprivation domain scores (MHCLG) | borough summary | 2015, 2019 snapshots | in pipeline |
| ONS mid-year population (rate denominator) | borough, annual | 2011 – 2024 | in pipeline |
| ONS4 personal well-being | borough, annual | 2011/12 onward | not yet acquired |
| Life expectancy (M/F) | borough, rolling annual | 2011 onward | not yet acquired |

Crime records are assigned to boroughs via the ONS LSOA(2011)→LSOA(2021)→
LAD(2022) lookup, harmonising both code vintages across the window.

> **Crime coverage gap.** 33 Metropolitan monthly files are missing across
> 2012–2015. Metropolitan volume reads at 12–19% of normal in those years while
> City of London — complete throughout — is flat, so this is file availability,
> not a fall in crime. `pipeline/00_download.R` fails on it by design. Borough
> rates for 2012–2015 must not be published until the files are re-downloaded.

## Analytical decisions (and why)

- **Analysis window 2011–2023; crime trend to 2024.** Cross-metric analysis is
  bounded by the shortest denominator (population to mid-2024, income to
  2023-24). Years that are not twelve months of data get a `coverage_flag` and
  no published rate — a four-month 2026 is never shown as a full year.
- **IMD Crime domain excluded from analysis.** It is constructed from recorded
  crime, so using it to explain crime rates is circular. It is retained solely
  as an external validation check, in `imd_crime_validation.csv`.
- **IMD ranks excluded; scores retained.** 2015 and 2019 ranks are not
  methodologically comparable, so a rank change is not a deprivation change.
- **Median income over mean.** Borough income is right-skewed; medians resist
  distortion by high earners. Mean and taxpayer counts are kept as
  supplementary rows.
- **Missing survey years are recorded, never interpolated.** Income has no
  2008-09 survey year; the gap is logged and left empty.
- **Documented exclusions in the LSOA join.** Blank LSOA codes (ungeocoded by
  the police), boundary-spillover records outside the 33 boroughs, and codes
  matching no ONS LSOA are each counted separately in
  `pipeline/logs/exclusions.log`. Coverage is measured against records that
  carry a code, and the 99.5% threshold is enforced.
- **Both crime-type vocabularies mapped into one series.** police.uk changed
  its categories in April 2013; both are mapped, and every row carries a
  `vocabulary` column marking which side of the change its label belongs to.
  The mapping makes the series continuous, not comparable —
  see `SOURCES.md`.
- **Per-capita caveat.** City of London's small resident population makes its
  per-capita rates extreme; the interface will allow excluding it from scales.

## Repository layout

```
pipeline/               canonical R pipeline (data.table), SOURCES.md, logs
pipeline/tests/         synthetic-fixture smoke test, run in CI
pipeline/experimental/  retired exploratory scripts — not part of the run
data/raw/               raw source files (gitignored; rebuild per SOURCES.md)
data/processed/         derived outputs
web/                    Next.js frontend and API routes
projects-plan.md        roadmap: issues, branches, acceptance criteria
```

## Reproducing the pipeline

1. Populate `data/raw/` as described in [`pipeline/SOURCES.md`](pipeline/SOURCES.md).
2. Install R with `data.table` and `jsonlite`.
3. From the repository root, run the scripts in the order listed in
   [`pipeline/README.md`](pipeline/README.md). Any non-zero exit means the run
   failed; do not use the outputs.
4. Frontend: `npm ci` at the root, then `npm run dev --workspace web`.

To check the pipeline without raw data: `Rscript pipeline/tests/smoke.R`.

## Status

**Done**

- Monorepo, CI (web lint/build + R parse and smoke test on PRs).
- LSOA→borough lookup across both code vintages, with a one-borough-per-code
  assertion and 33-borough verification.
- Crime→borough join with a single reconciling exclusion ledger, complete
  crime-type mapping, and borough-year and category aggregates.
- Population parsing and crime rates per 1,000 with explicit coverage flags.
- Income and IMD tidied to the common long schema; IMD Crime domain split out.
- QA that reconciles independent artefacts and can fail.

**Blocked**

- Crime rates for 2012–2015 — 33 Metropolitan monthly files must be
  re-downloaded first.

**Next**

- Acquire ONS4 well-being and borough life expectancy (issue 1.4), then tidy
  them (1.7, 1.8).
- Unified `boroughs.json` + `coverage.json` export (1.9).
- Borough GeoJSON (1.6), API routes (2.1, 2.2), choropleth with coverage-aware
  controls (Epic 3), narrative write-up (4.1).

## Known limitations

Ecological analysis over 33 units — associations only, no causal
identification. Police geocoding gaps (blank LSOAs) are excluded, not imputed.
IMD exists at two snapshots, so deprivation supports cross-sectional comparison
rather than trends, and its domains sit on three different scales. Crime
categories are not comparable across the April 2013 taxonomy change even after
mapping. Population is a mid-year estimate against calendar-year crime, a
constant six-month offset. British Transport Police records (rail-network
crime) are out of scope.

## References

- Sources, licences and decisions: [`pipeline/SOURCES.md`](pipeline/SOURCES.md)
- Pipeline usage: [`pipeline/README.md`](pipeline/README.md)
- Roadmap: [`projects-plan.md`](projects-plan.md)
- Review that prompted the current pipeline: [`codebase-review-issues.md`](codebase-review-issues.md)

Contains public sector information licensed under the Open Government Licence v3.0.
