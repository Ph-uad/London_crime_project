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
  responsive accessible shell → interactive choropleth, coverage-aware controls and a
  crime-vs-determinant scatterplot. All done, with 139 unit tests and 81 browser checks.

## Data

| Source | Grain | Coverage used | State |
|---|---|---|---|
| UK Police street-level crime (Met + City of London) | LSOA → borough | 2011-01 – 2026-04 | in pipeline — 368/368 months |
| HMRC personal income (median) | borough, annual | 1999 – 2023 | in pipeline |
| IMD deprivation domain scores (MHCLG) | borough summary | 2015, 2019 snapshots | in pipeline |
| ONS mid-year population (rate denominator) | borough, annual | 2011 – 2024 | in pipeline |
| ONS LAD boundaries (BGC, 20m generalised) | borough polygons | December 2022 | in pipeline |
| ONS4 personal well-being | borough, annual | 2011/12 – 2022/23 | in pipeline — **32 boroughs** |
| ONS life expectancy (M/F, birth & 65) | borough, 3-yr rolling | 2001-03 – 2022-24 | in pipeline — **32 boroughs** |

Crime records are assigned to boroughs via the ONS LSOA(2011)→LSOA(2021)→
LAD(2022) lookup, harmonising both code vintages across the window.

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
- **Per-capita caveat, and a coverage one.** City of London's small resident
  population (~8,000) makes its per-capita crime rates extreme, and it is why
  ONS publishes **no** well-being estimate for it (every cell marked `[u]`) and
  **no** life expectancy at all. Those two metrics cover 32 boroughs, declared
  per metric in `coverage.json` rather than left as a silent 32-vs-33 mismatch.
- **Year conventions differ by metric, on purpose.** Financial years (income,
  well-being) map to their start year; rolling three-year periods (life
  expectancy) map to their end year. Each metric publishes its `year_rule`, so
  a cross-metric pairing can be stated rather than assumed.

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
2. Install R with `data.table`, `jsonlite`, `readxl` and `sf`.
3. From the repository root, run the scripts in the order listed in
   [`pipeline/README.md`](pipeline/README.md). Any non-zero exit means the run
   failed; do not use the outputs.
4. Frontend, from the repository root: `npm ci`, then `npm run dev`.
   `npm run check` runs lint, type-check, the route tests and the build — the same
   sequence as CI. Run `npm ci` again after pulling changes that touch
   `package-lock.json`, or `npm run test` will fail with `vitest: not found`.

To check the pipeline without raw data: `Rscript pipeline/tests/smoke.R`.

## Status

**Done**

- Monorepo, CI (web lint/build, browser and accessibility checks, R parse and smoke test on PRs).
- LSOA→borough lookup across both code vintages, with a one-borough-per-code
  assertion and 33-borough verification.
- Crime→borough join with a single reconciling exclusion ledger, complete
  crime-type mapping, and borough-year and category aggregates.
- Population parsing and crime rates per 1,000 with explicit coverage flags.
- Income, IMD, well-being and life expectancy tidied to the common long schema;
  IMD Crime domain split out to a validation-only file.
- Unified `boroughs.json` (450 KB) and `coverage.json` — the contract that
  carries years, cadence, direction, scale and borough coverage per metric.
- Borough boundaries: 33 polygons, EPSG:4326, no self-intersections, GSS codes
  asserted against `boroughs.json`, no topology-breaking simplification.
- QA that reconciles independent artefacts and can fail (31 checks).
- Data API: `/api/metrics`, `/api/meta` and `/api/geo`, with 28 route tests that reject a
  typo'd parameter rather than silently returning everything.
- Responsive, accessible app shell with a validated design-token palette — verified by 28
  browser checks that run axe at 375, 768 and 1280 px on every route.
- Interactive dashboard: borough choropleth, metric switcher and borough exclusions, a
  per-metric year control, a borough detail panel, a crime-vs-determinant scatterplot and a
  summary strip — all reading direction, scale, cadence and coverage from the matrix rather
  than assuming them. 53 further browser checks, including a deuteranopia simulation of the
  colour ramp. **No mapping or charting library**: the map is inline SVG over hand-written
  Web Mercator, which is what makes it readable by a screen reader and assertable by a test.

**Next**

- Narrative write-up (4.1), Vercel deployment (0.4), CI workflow installation (0.3).
- **Issue 1.11**, a data-quality defect the choropleth exposed: two of the six IMD domains
  are published at a precision that leaves them with one and two distinct values across all
  33 boroughs. See the roadmap.

## Known limitations

Ecological analysis over 33 units — associations only, no causal
identification. Police geocoding gaps (blank LSOAs) are excluded, not imputed.
IMD exists at two snapshots, so deprivation supports cross-sectional comparison
rather than trends, and its domains sit on three different scales — and two of them, income
and employment, are published at a precision that leaves them with almost no borough-level
variance at all (issue 1.11). Well-being
ends at 2022-23, one year short of the analysis window, and is absent for City
of London; life expectancy is absent for City of London too. Crime
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
