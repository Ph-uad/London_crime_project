# London Crime × Social Determinants — GitHub Projects Plan

**Architecture (locked):** Next.js (App Router, TypeScript) serving both the frontend and
the data API via API routes · R data pipeline (offline, outputs versioned JSON/GeoJSON) ·
Deployed on Vercel free tier.
**Total estimate:** ~55–70 hrs across 5 epics.
**Dependency rule:** no issue depends on a later-numbered issue. `blocked-by:*` labels
encode hard dependencies.

> **This file is the single live roadmap.** The amendments previously held in
> `plan-revision-issues.md` were merged into it on 2026-08-19; that file is archived at
> [`documentation/2026-08-19-plan-revision-issues.md`](documentation/2026-08-19-plan-revision-issues.md)
> and no longer governs. Where the delivered work diverged from an issue's written
> criteria, the criteria are left **verbatim** and a **Delivered as** note records what was
> built and why. Full rationale:
> [`documentation/2026-08-19-pipeline-rebuild-record.md`](documentation/2026-08-19-pipeline-rebuild-record.md).

**Status values:** `Done` · `Done (pending run)` · `In progress` · `Planned` · `Superseded`

---

## Numbering — how the merge resolved the overlap

The revision reused numbers that the original plan had already assigned, so three issues
collide. Resolved as follows, and every `blocked-by` label below has been rewritten to match.

| Original | Fate | Now |
|---|---|---|
| 1.4 Ingest income, life expectancy, IMD, well-being | Split by source | **Superseded by 1.4–1.8** |
| 1.5 Build unified dataset and export JSON | Absorbed, plus the coverage matrix | **Superseded by 1.9** |
| 1.6 Borough boundaries GeoJSON | Unchanged in substance, renumbered | **1.10** |

The renumbering of boundaries to **1.10** is taken, not left open as the revision did: the
delivered script, `SOURCES.md` and both READMEs already cite it as 1.10.

**Two `blocked-by` labels in the revision were wrong and are corrected here.** The revision
gave new 1.5 (tidy income) and new 1.6 (tidy IMD) `blocked-by:1.1` — crime acquisition.
Neither touches crime data. Both need the borough lookup for GSS resolution, so both are
`blocked-by:1.2`. The delivered scripts confirm it: `10_tidy_income.R` and `11_tidy_imd.R`
read `lsoa_lookup.csv` and nothing from `data/raw/crime/`.

---

## Epic 0 — Repository & Infrastructure Setup

### Issue 0.1 — Initialise monorepo structure
**Status: Done**
**Branch:** `chore/setup/monorepo-init` · **Labels:** `setup` `no-dependencies` · **Estimate:** 1–2 hrs

**Description:**
Scaffold the repository with `web/` (Next.js app) and `pipeline/` (R scripts) directories.
Initialise git, add `.gitignore` files for Node and R, and create the root `README.md` with
project overview, architecture summary, and local setup instructions.

**Acceptance criteria:**
- [x] Root `README.md` exists with architecture summary and diagram placeholder
- [x] `web/` and `pipeline/` directories created
- [x] `.gitignore` covers `node_modules`, `.next`, `.env*`, `.Rhistory`, `.RData`, `*.Rproj.user`
- [x] `data/` directory with `raw/` (gitignored) and `processed/` (committed) subfolders
- [x] Initial commit pushed to `main`

**Delivered as:** `.gitignore` was rewritten 2026-08-12 — duplicate rules removed,
`derby.log` caught at any depth, and the retired Spark intermediates named explicitly.

---

### Issue 0.2 — Scaffold Next.js application
**Status: Done**
**Branch:** `chore/setup/nextjs-scaffold` · **Labels:** `setup` `frontend` `blocked-by:0.1` · **Estimate:** 1–2 hrs

**Description:**
Initialise the Next.js app in `web/` with TypeScript, ESLint, Tailwind CSS, and the App
Router. Verify dev server runs and a placeholder home page renders.

**Acceptance criteria:**
- [x] `npx create-next-app` completed with TypeScript + Tailwind + App Router
- [x] `npm run dev` serves a placeholder page locally
- [x] `npm run build` passes with zero errors
- [ ] Prettier config committed — **outstanding**

---

### Issue 0.3 — CI workflow for lint and build
**Status: In progress**
**Branch:** `chore/setup/ci-lint-build` · **Labels:** `setup` `ci` `blocked-by:0.2` · **Estimate:** 1–2 hrs

**Description:**
Add a GitHub Actions workflow that runs ESLint and `next build` on every pull request to
`main`. Failing checks block merge via branch protection.

**Acceptance criteria:**
- [ ] `.github/workflows/ci.yml` runs lint + build on PRs — file written, **not installed**
- [ ] Branch protection on `main` requires the CI check
- [ ] CI passes on a test PR

**Delivered as:** a replacement `ci.yml` exists but **is not installed** — GitHub workflow
files cannot be written by the remote tooling used, so it must be pasted in manually. It
adds a second job the original scope did not have: R install, a parse check over every
script, and `pipeline/tests/smoke.R`. That job is the control that prevents the Epic 1
failure recurring — three scripts that could not execute were committed precisely because
nothing ran them. It also fixes the trigger, which reached only PRs targeting `v2`.

**Amended acceptance criteria:**
- [ ] Workflow triggers on PRs and pushes to `main`
- [ ] A `pipeline` job parse-checks every `.R` file and runs the smoke test
- [ ] `npm ci` runs at the repository root, with `--workspace web` for lint and build
- [ ] The web job also runs `typecheck` and the 28 route tests

---

### Issue 0.4 — Vercel deployment pipeline
**Status: Planned**
**Branch:** `chore/setup/vercel-deploy` · **Labels:** `setup` `deploy` `blocked-by:0.2` · **Estimate:** 1 hr

**Description:**
Connect the repo to Vercel with `web/` as the project root. Production deploys from `main`;
preview deploys on every PR.

**Acceptance criteria:**
- [ ] Production URL serves the placeholder app
- [ ] PR preview deployments active
- [ ] Deployment URL added to `README.md`

---

## Epic 1 — Data Pipeline (R)

> **Epic 1 is complete.** Issues 1.1–1.10 all meet their acceptance criteria. The pipeline
> was rewritten on `data.table` in August 2026; the original `sparklyr` scripts are retired
> to `pipeline/experimental/` and are not part of the canonical run.

### Issue 1.1 — Acquire raw crime data (Met + City of London)
**Status: Done**
**Branch:** `feat/data/crime-acquisition` · **Labels:** `data` `no-dependencies` · **Estimate:** 2–3 hrs

**Description:**
Download the 10-year custom archive from data.police.uk filtered to Metropolitan Police
Service and City of London Police. Document the exact date range, download date, and licence
(OGL v3) in `pipeline/SOURCES.md`. Store raw CSVs in `data/raw/` (gitignored, with a
re-download script).

**Acceptance criteria:**
- [x] All monthly CSVs for the 10-year window present locally — **368/368** (184 months × 2 forces)
- [x] `pipeline/SOURCES.md` records URL, date range, licence, download date
- [x] `pipeline/00_download.R` (or documented manual steps) reproduces the acquisition
- [x] Row-count sanity check logged (per-year totals)

**Delivered as:** the window is 2011-01 – 2026-04 (15 years, not 10). `00_download.R`
**verifies** rather than downloads — the archive is a manual bulk export — and it checks
coverage **per force**, failing the run on any gap. This matters: 33 Metropolitan monthly
files were missing across 2012–2015 and a pooled check passed the entire time, because City
of London was complete throughout. Re-downloaded 2026-08-13; 2.77 M records recovered.

---

### Issue 1.2 — LSOA→borough lookup and code harmonisation
**Status: Done**
**Branch:** `feat/data/lsoa-borough-lookup` · **Labels:** `data` `blocked-by:1.1` · **Estimate:** 3–4 hrs

**Description:**
Download the ONS Open Geography Portal LSOA-to-local-authority lookup. Harmonise 2011 and
2021 LSOA codes across the 10-year window using the ONS correspondence table. Produce a
single clean lookup keyed on LSOA code → borough name/GSS code.

**Acceptance criteria:**
- [x] Lookup covers ≥99.5% of LSOA codes appearing in the crime data — **99.763%**
- [x] Unmatched codes logged with counts and a documented handling decision
- [x] 2011↔2021 boundary changes harmonised and footnoted in `SOURCES.md`
- [x] Output saved as `data/processed/lsoa_lookup.csv`

**Delivered as:** the criterion was ambiguous and previously resolved to 98.73%, 99.64% or
92.5% depending on which script you believed. The denominator is now pinned in `SOURCES.md`
— records carrying a **non-blank** LSOA code — and enforced in code. The lookup no longer
reads the crime files at all; record-level exclusion accounting moved to 1.3, so one script
owns it. A one-borough-per-code assertion was added: four such codes exist elsewhere in
England and Wales and joining on one would duplicate every attached crime record.

**Note:** `blocked-by:1.1` is retained but is now only a *verification* dependency — the
lookup is built from the ONS file alone.

---

### Issue 1.3 — Aggregate crime to borough-year rates
**Status: Done**
**Branch:** `feat/data/crime-aggregation` · **Labels:** `data` `blocked-by:1.2` · **Estimate:** 3–4 hrs

**Description:**
Join crime records to the lookup, aggregate to borough × year (and borough × year ×
crime-category for the category breakdown), and convert counts to rates per 1,000 residents
using ONS mid-year population estimates. Flag the City of London / Westminster
daytime-population caveat in the output metadata.

**Acceptance criteria:**
- [x] Output table: borough × year × category with `count` and `rate_per_1000`
- [x] ONS mid-year estimates sourced per year (not a single snapshot)
- [x] Totals reconcile with raw row counts (±0.5% after unmatched-LSOA exclusions) — **exact, 0 residual**
- [x] City of London anomaly documented in metadata field

**Delivered as:** split across `01_crime_by_borough.R` and `02_population_and_rates.R`.
Three additions the criteria did not anticipate:

- **A single exclusion ledger** (`logs/exclusions.log`), asserted to sum to the raw total.
  Previously four scripts each computed exclusions and reported four different figures.
- **No `NA` borough key.** A `left_join` had been leaving 36,574 records under an unnamed
  34th "borough" in one output, silently dropped by the next.
- **Complete crime-type mapping, asserted.** `Violent crime` and `Public disorder and
  weapons` had no branch and fell into a silent `"No-category"` bucket — Violence read
  **0** for 2011–2012. Both vocabularies are now mapped into one continuous series
  (owner's decision); every row carries a `vocabulary` column marking which side of the
  April 2013 change its label belongs to. The mapping makes the series continuous, not
  comparable — see `SOURCES.md`.

Rates are published only for complete twelve-month years; `coverage_flag` carries
`complete`, `partial_year` or `no_denominator`.

---

### ~~Issue 1.4 — Ingest income, life expectancy, IMD, well-being~~
**Status: Superseded** by issues 1.4–1.8. The single issue conflated four sources with
different grains, cadences and coverage, and named a well-being source that stops at 2013.

---

### Issue 1.4 — Acquire annual well-being and life-expectancy series (data swap)
**Status: Done**
**Branch:** `feat/data/annual-wellbeing-lifeexp` · **Labels:** `data` `no-dependencies` · **Estimate:** 1–2 hrs

**Description:**
Replace the ward well-being bundle (2009–2013 only) as the source for well-being and life
expectancy. Download the ONS borough-level annual personal well-being (ONS4) series
(2011/12 onwards) and the borough life-expectancy rolling annual series from the London
Datastore. Verify and record each dataset's licence in `SOURCES.md` (resolving the current
"licence: not documented" entry). The ward file is retained only for optional secondary
features (unemployment, child poverty, GCSE, PTAL, greenspace), clearly marked as 2011–2013
coverage.

**Acceptance criteria:**
- [x] ONS4 borough annual well-being file in `data/raw/` with its own `SOURCES.md` entry
- [x] Borough life-expectancy annual series in `data/raw/` with its own `SOURCES.md` entry
- [x] Licence verified (not assumed) and recorded for both
- [x] Well-being and life expectancy no longer share a single source URL in `SOURCES.md`

**Delivered as: sourced from ONS directly, not the London Datastore.** Both Datastore
copies were checked before download and rejected:

| Datastore dataset | Problem |
|---|---|
| `personal-well-being-borough-2r87d` | Apr 2011 – Mar 2019 only; last updated 2019 |
| `life-expectancy-...-borough-23gm7` | 2000-2002 to **2008-2010**, and **OGL v2** |

The life-expectancy copy is *older than the ward file it was meant to replace*. Both are GLA
re-publications of ONS data, so `00_download_metrics.R` goes to ONS: well-being 2011-12 to
2022-23, life expectancy 2001-2003 to 2022-2024, OGL v3.0 on both, licences read on the
dataset pages. Downloads are recorded in `logs/acquisition.log` with size, MD5 and UTC
timestamp.

**Technologies:** ONS · **Alternatives:** keep ward file only (3-year overlap; materially weaker analysis)

---

### Issue 1.5 — Tidy income to long format
**Status: Done**
**Branch:** `feat/data/tidy-income` · **Labels:** `data` `blocked-by:1.2` · **Estimate:** 2–3 hrs

**Description:**
`pipeline/10_tidy_income.R`: melt the HMRC wide file (`Number_of_Individuals_YYYY`,
`Mean_YYYY`, `Median_YYYY`) to the common long schema. Keep **median** as the analysis
metric (income is right-skewed; means are dragged by high earners); retain mean and taxpayer
counts as supplementary rows. Drop artefact columns (`NA_2023`, blank headers). Note the
missing 2008 survey year rather than interpolating it.

**Acceptance criteria:**
- [x] Output matches schema `borough_gss, borough_name, year, metric, value, source, geography_native, notes`
- [x] Median income present for every borough-year available in source, ≥ 2011
- [x] Artefact columns dropped and logged; 2008 gap documented, not filled
- [x] All borough names resolved to GSS codes with zero mismatches

**Delivered as:** the source labels **financial** years (`1999-00` … `2023-24`), each
assigned to its **start** year — metric year 2011 means tax year 2011/12, recorded in
`SOURCES.md`. There are **two** unlabelled trailing artefact columns, not one; both dropped
and logged. Boroughs are matched on GSS code, never on name. `blocked-by` corrected from
1.1 to 1.2.

**Technologies:** R (data.table `melt`, regex year extraction) · **Alternatives:** tidyr `pivot_longer`

---

### Issue 1.6 — Tidy IMD scores to long format
**Status: Done**
**Branch:** `feat/data/tidy-imd` · **Labels:** `data` `blocked-by:1.2` · **Estimate:** 2–3 hrs

**Description:**
`pipeline/11_tidy_imd.R`: extract only `*_Average_score_2015/2019` columns for the Income,
Employment, Education, Health, Barriers-to-Housing and Living-Environment domains.
**Exclude the IMD Crime domain from analysis outputs** (circular with the outcome variable)
— emit it to a separate validation-only file. Exclude all `Rank_*` and
`Proportion_of_LSOAs_*` columns from analysis; optionally retain ranks in a display-copy file.

**Acceptance criteria:**
- [x] Six domain scores × 2 snapshot years × 33 boroughs in the common schema
- [x] IMD Crime domain absent from the analysis output; present only in `imd_crime_validation.csv`
- [x] Exclusion rationale (circularity) recorded in `SOURCES.md` decisions section
- [x] 2015 vs 2019 methodology non-comparability of ranks noted

**Delivered as:** one thing the criteria did not anticipate — **the six domains are not on a
common scale, and two are legitimately negative.** Income and Employment are proportions
(0–0.3), Education / Barriers / Living Environment are scores (3.5–55), Health and Crime are
standardised (−1.7 to 1.0). A blanket "score ≥ 0" check is wrong and fails on half of London.
Each row carries its `scale_type`, and validation uses per-domain envelopes. This constrains
the frontend: one colour ramp across IMD domains is not meaningful (see 3.2, 3.3).

**Technologies:** R (data.table) · **Alternatives:** none material

---

### Issue 1.7 — Tidy well-being to long format
**Status: Done**
**Branch:** `feat/data/tidy-wellbeing` · **Labels:** `data` `blocked-by:1.4` · **Estimate:** 2–3 hrs

**Description:**
`pipeline/12_tidy_wellbeing.R`: tidy the ONS4 borough annual series (life satisfaction,
worthwhile, happiness, anxiety) to the common schema. If any secondary ward-file features
are retained, aggregate ward→borough with a **population-weighted** mean and recover the
true labels of the export-mangled `Subjective_well_being_..._1/_2/_3` columns from the source
workbook before use.

**Acceptance criteria:**
- [x] Four ONS4 measures per borough-year from 2011/12 onward in the common schema
- [x] Anxiety direction documented (higher = worse, unlike the other three)
- [x] Any ward-derived feature is population-weighted, never a plain mean
- [x] No column with a recovered/ambiguous label enters output without its verified name

**Delivered as:** no ward data is used at all, so the weighting and label-recovery traps do
not arise. Two findings the issue did not anticipate:

- **City of London has no well-being data.** All 48 of its cells (4 measures × 12 years) are
  marked `[u]` — sample too small to publish. The metric covers **32 boroughs**, declared as
  a permitted absence; the script fails if any *other* borough goes missing.
- **The series ends at 2022-23**, one year short of the 2011–2023 analysis window. ONS has
  published no later local-authority edition. Absent in `coverage.json`, not carried forward.

Only the `average-mean` estimate is used; the other four estimate types are proportions in
rating bands, not the borough's average score. Financial years map to their start year,
matching 1.5.

**Technologies:** R (data.table), ONS4 · **Alternatives:** drop ward secondaries entirely (taken)

---

### Issue 1.8 — Tidy life expectancy to long format
**Status: Done**
**Branch:** `feat/data/tidy-lifeexp` · **Labels:** `data` `blocked-by:1.4` · **Estimate:** 1–2 hrs

**Description:**
`pipeline/13_tidy_life_expectancy.R`: tidy the borough life-expectancy series (male and
female as separate metrics) to the common schema. Rolling multi-year periods (e.g. 2018–2020)
are assigned to their **end year** with the full period preserved in `notes`.

**Acceptance criteria:**
- [x] Male and female series per borough-year in the common schema
- [x] Period→year assignment rule applied consistently and documented
- [x] Coverage span reported in the run log

**Delivered as:** **four** metrics, not two — at birth *and* at age 65, male and female. The
at-65 figures are in the same ONS table, cost nothing, and are the more informative measure
for later-life inequality. An assertion checks at-65 sits below at-birth for every
borough-year, which is what would catch an age-group mix-up; the numbers look plausible
otherwise.

**City of London is absent from the source entirely** — ONS does not publish life expectancy
for ~8,000 residents. 32 boroughs, declared the same way as well-being.

**The end-year rule differs from 1.5 and 1.7**, which use the start year of a financial year.
This is deliberate — a three-year rolling window and a twelve-month accounting year are
different objects — and each metric publishes its `year_rule` so a cross-metric pairing can
be stated rather than assumed (see 3.6).

**Technologies:** R (data.table, readxl) · **Alternatives:** midpoint-year assignment (rejected; end year is what a reader assumes when a dashboard says "2024")

---

### ~~Issue 1.5 (original) — Build unified dataset and export JSON~~
**Status: Superseded** by issue 1.9, which adds the coverage matrix and the window rules.

---

### Issue 1.9 — Unify metrics, crime, and coverage matrix
**Status: Done (pending run)**
**Branch:** `feat/data/unify-coverage` · **Labels:** `data` `blocked-by:1.3` `blocked-by:1.5` `blocked-by:1.6` `blocked-by:1.7` `blocked-by:1.8` · **Estimate:** 3–4 hrs

**Description:**
`pipeline/20_unify_metrics.R`: bind all tidied sources plus borough crime rates into one long
table; export `data/processed/boroughs.json`. Enforce the analysis-window decision:
cross-metric analysis 2011–2023; crime-rate trend to 2024 (population denominator limit);
2025+ counts flagged `partial`; 2026 (4 months) never presented as a full year. Also emit
`data/processed/coverage.json`: for every metric, the years with data. Validation asserts 33
boroughs, value ranges, and schema conformance.

**Acceptance criteria:**
- [x] `boroughs.json` in the common schema; validation script passes
- [x] `coverage.json` lists available years per metric and drives no hardcoded year lists downstream
- [x] Window rules (2011–2023 analysis / 2024 trend / partial flags) implemented and documented in `SOURCES.md`
- [x] Combined export < 1 MB — **450 KB**

**Delivered as: `coverage.json` carries considerably more than "the years with data",**
because the data imposes constraints the issue did not foresee. Per metric it declares:

| Field | Why |
|---|---|
| `years`, `partial_years` | 3.4 — slider range, and years excluded from year-on-year comparison |
| `cadence` | `snapshot` (IMD) renders as discrete points, `annual` as a slider |
| `direction` | 3.7 — falling crime is good, falling life expectancy is not |
| `scale`, `unit` | IMD spans proportion, score and standardised; one ramp across them is wrong |
| `year_rule` | 3.6 — `calendar`, `financial_start`, `rolling_end` or `snapshot` |
| `boroughs_missing` | City of London, per metric — no silent 32-vs-33 mismatch |

Every metric must have a registry entry or the run fails; a default `higher_is_better` is
wrong for eleven of nineteen metrics. `boroughs.json` uses a normalised shape (boroughs and
metric metadata factored out of the observation rows) to stay inside the 1 MB budget —
logically the same borough × year × metric × value contract. Array fields keep their shape
at length 1, so `partial_years` is always `[2026]`, never `2026`.

**Verification:** built and validated against real income, IMD, well-being and life
expectancy, with a synthetic crime series. **Awaiting regeneration with the real crime data.**

**Technologies:** R (data.table, jsonlite) · **Alternatives:** per-metric JSON files (more requests, simpler diffs)

---

### Issue 1.10 — Borough boundaries GeoJSON
*(was 1.6 in the original plan)*
**Status: Done (pending run)**
**Branch:** `feat/data/boundaries-geojson` · **Labels:** `data` `no-dependencies` · **Estimate:** 2 hrs

**Description:**
Download the London Datastore statistical GIS borough boundaries, convert to GeoJSON,
simplify with `rmapshaper` to <500 KB, and verify GSS codes match the unified dataset.

**Acceptance criteria:**
- [x] `data/processed/london.geojson` < 500 KB
- [x] All 33 features carry GSS codes matching `boroughs.json`
- [x] Geometry validates (no self-intersections) and renders in geojson.io
- [x] Simplification tolerance documented

**Delivered as: no simplification is applied, and `rmapshaper` is not used.** Simplification
is only safe if it preserves topology. `sf::st_simplify` moves each borough's outline
independently, so neighbours that shared an edge stop sharing it and the choropleth grows
hairline slivers that no downstream step can repair. `rmapshaper` does preserve topology but
requires V8.

The budget is met instead by taking a generalisation level **ONS produced across the whole
UK coverage** — adjacent districts still share edges exactly — keeping only the 33 London
features, and rounding coordinates to 6 decimal places (~0.1 m, far finer than a 20 m
generalisation). If that ever misses the budget the script stops and directs you to ONS's
coarser BUC product rather than degrading geometry itself. Source is ONS via data.gov.uk
(OGL v3.0 verified), not the London Datastore.

Two things worth carrying forward: output is EPSG:4326 / RFC 7946, and **geometry is
validated in the source projection**, before reprojection — once coordinates are lon/lat, sf
validates with spherical `s2`, which rejects a merely duplicated vertex and would report
every ONS borough invalid.

**Verification:** all five guards tested against synthetic fixtures. **Awaiting first run
against the real ONS boundary file.**

**Technologies:** R (sf) · **Alternatives:** rmapshaper / mapshaper CLI / QGIS (all rejected — see above)

---

### Issue 1.11 — IMD income and employment scores carry no borough-level variance
**Status: Open — raised by 3.2**
**Branch:** `fix/pipeline/imd-score-precision` · **Labels:** `pipeline` `data-quality` `bug` · **Estimate:** 1–2 hrs

**Description:**
Building the choropleth surfaced this and nothing before it would have. Two of the six IMD domains
are published in `metrics_imd.csv` at one decimal place as *proportions*, which at borough level
leaves them with essentially no information:

| Metric | Scale | Distinct values across 33 boroughs (2019) |
|---|---|---|
| `imd_employment_score` | proportion | **1** — every borough is 0.1 |
| `imd_income_score` | proportion | **2** — 0.1 and 0.2 |
| `imd_education_skills_and_training_score` | score | 29 |
| `imd_health_deprivation_and_disability_score` | standardised | 17 |
| `imd_living_environment_score` | score | 31 |
| `imd_barriers_to_housing_and_services_score` | score | 31 |

The four `score` and `standardised` domains are fine. The two `proportion` domains are not: MHCLG
publishes the income and employment domain averages as proportions in the 0–1 range, so one decimal
place is a resolution of ten percentage points. Every London borough lands in the same bucket.

`pipeline/11_tidy_imd.R` contains no rounding — it reads the `"<Domain> - Average score"` columns
verbatim — so this is either the source file's own precision or a precision loss at acquisition.
**It could not be confirmed during 3.2 because `data/raw/` has been cleared**, which is itself worth
noting: the recipe in `SOURCES.md` is the only route back to the answer.

**Acceptance criteria:**
- [ ] Re-acquire the MHCLG local authority district summaries per `SOURCES.md` and record the
      precision the source actually publishes for the income and employment domain averages
- [ ] If the source carries more precision, fix the read and re-run; if it does not, record it in
      `SOURCES.md` as a known limitation of the borough-level summary file and consider deriving
      the borough average from the LSOA-level file instead
- [ ] Either way, `20_unify_metrics.R` gains a **distinct-value assertion**: a metric with fewer
      than three distinct values across 33 boroughs fails the run, or is explicitly declared in
      `coverage.json` as having no borough-level variance
- [ ] `pipeline/QA/01_QA.R` gains the same check, so this cannot recur silently

**What the frontend does in the meantime.** It handles it honestly rather than hiding it: the map
draws one flat mid-tone with a caption saying every borough has the same value and that this is the
source's precision, not a rendering fault; the summary cards refuse to name a "highest" and a
"lowest" that are the same borough; and the scatterplot declines to fit a line, saying the variable
has no variation at this precision. See `web/e2e/dashboard.spec.ts`.

**This is the argument for building the visualisation.** Nineteen metrics passed 31 QA checks and
139 unit tests without anyone noticing that two of them are constants. Drawing them on a map took
about a second.

**Technologies:** R (data.table) · **Alternatives:** derive borough averages from the LSOA-level IMD file

---

## Epic 2 — API Layer (Next.js Routes)

> **Epic 2 is complete.** Both routes are delivered and covered by tests in CI. The
> remaining item is the Vercel latency measurement in 2.2, which needs issue 0.4.

### Issue 2.1 — Metrics API route
**Status: Done**
**Branch:** `feat/api/metrics-route` · **Labels:** `backend` `blocked-by:0.2` `blocked-by:1.9` · **Estimate:** 2–3 hrs (+1 hr for coverage metadata)

**Description:**
Implement `GET /api/metrics` serving the unified dataset with query-parameter filtering:
`?metric=`, `?year=`, `?borough=`. Typed response schema shared with the frontend.

**Acceptance criteria:**
- [x] Route returns full dataset and filtered subsets correctly
- [x] Invalid params return 400 with a clear message
- [x] Shared TypeScript types in `web/lib/types.ts`
- [x] Cache headers set (`s-maxage`, immutable data)
- [x] `GET /api/meta` (or `/api/metrics?meta=true`) returns the coverage matrix
- [x] Responses include `partial` flags so clients cannot mistake 4-month 2026 for a year

**Delivered as:** `/api/meta` as its own route rather than `?meta=true`, so it caches
separately from a filtered query. `web/lib/types.ts` mirrors `coverage.json` **in full** —
`direction`, `scale`, `cadence`, `year_rule` and `boroughs_missing` included — so Epic 3
cannot reach for a field the API declined to pass through.

Three things beyond the written criteria:

- **Every `/api/metrics` response carries the coverage entry for the metrics it returned.**
  The partial-year flags travel with the data rather than needing a second call a caller
  might skip.
- **An unknown query parameter is a 400, not an ignored one.** `?metrics=` (plural) would
  otherwise return the entire dataset and look like it worked — the same class of failure
  as a silent fallback in the pipeline.
- **28 route tests** (`web/tests/api.test.ts`, vitest), wired into the CI web job alongside
  a type-check. They import the handlers and call them with `Request` objects — no server,
  no network — and read the real `data/processed` exports, so they double as a contract
  check on pipeline output. Verified to fail when the unknown-parameter guard is removed
  and when filters are OR-ed instead of AND-ed.

**Data access:** `web/lib/data.ts` is the only module that knows where the exports live.
`boroughs.json` and `coverage.json` are imported through a `@data/*` tsconfig alias and
bundled at build time — which is what justifies the year-long `s-maxage`.
**Consequence:** the web build now has a hard dependency on those two files being present
and committed. A missing export fails the build with `Module not found`, deliberately.

**Technologies:** Next.js API routes, TypeScript · **Alternatives:** standalone FastAPI/Express service (unjustified at this data size)

---

### Issue 2.2 — Boundaries API route
**Status: Done**
**Branch:** `feat/api/geo-route` · **Labels:** `backend` `blocked-by:0.2` `blocked-by:1.10` · **Estimate:** 1–2 hrs

**Description:**
Serve `GET /api/geo` returning the simplified borough GeoJSON with long-lived cache headers.

**Acceptance criteria:**
- [x] Route returns valid GeoJSON, content-type `application/geo+json`
- [x] Response cached at the edge
- [ ] Load time < 300 ms on Vercel preview — **cannot be measured until 0.4 is done**

*`blocked-by` updated from 1.6 to 1.10 per the renumbering above.*

**Delivered as:** `london.geojson` cannot use the `@data/*` alias — TypeScript and the
bundler treat only `.json` as a JSON module — so it is read from disk, with
`next.config.ts` tracing it into the deployment bundle. Renaming the pipeline output would
have been simpler but `london.geojson` is what 1.10's criteria name.

A missing file returns **503** naming the script to run, rather than a generic 500. Tests
assert 33 features, GSS codes matching the metrics data, and that coordinates are WGS84
degrees — British National Grid eastings here would render the map in the North Sea.

**Technologies:** Next.js API routes · **Alternatives:** static import into the client bundle (simpler, loses the API showcase)

---

## Epic 3 — Frontend

> **Epic 3 is complete.** 3.1 through 3.8 are delivered, with **139 unit tests** and **82
> browser checks** — axe-core over every route and four dashboard states at 375 / 768 /
> 1280 px — running in CI against the real production build.
>
> **Three data constraints ran through this epic.** They come from `coverage.json`, they were
> not optional, and each is now enforced by a test that has been shown to fail:
> 1. **Two metrics cover 32 boroughs, not 33** — City of London has no well-being and no
>    life expectancy. Those boroughs render in a hatched no-data style, are named in the
>    legend, say "no data" in the table, and are dropped from the correlation with the count
>    shown. Their ranks are n/32.
> 2. **Direction is per metric.** The map reads *darker = worse* for every metric, so the
>    ramp reverses for `higher_is_better`; extremes are labelled by meaning rather than by
>    value; trend arrows carry a word as well as a colour; `neutral` metrics get no
>    better/worse claim at all.
> 3. **IMD domains sit on three different scales** — proportion, score, standardised — and
>    two go negative. Standardised metrics get a diverging ramp centred on zero and no
>    quantile classing; everything else gets quantile classes on the sequential ramp.
>
> **Two things the epic found that nothing upstream had.** Both are recorded rather than
> quietly patched:
> - **Issue 1.11**, a data-quality defect: two of the six IMD domains have one and two
>   distinct values across all 33 boroughs. They passed 31 QA checks; drawing them on a map
>   took a second.
> - A **516 KB client-bundle regression** inherited from 3.1, in which every visitor
>   downloaded all 6,001 observations to render the site header. Fixed in 3.8.
>
> **No runtime dependency was added for the whole epic.** No MapLibre, no d3, no chart
> library — see 3.2 for the argument and its cost.

### Issue 3.1 — Responsive layout shell
**Status: Done**
**Branch:** `feat/frontend/layout-shell` · **Labels:** `frontend` `blocked-by:0.2` · **Estimate:** 2–3 hrs

**Description:**
Build the app shell: header, navigation, main content grid, and footer with data
attributions. Mobile-first with a defined breakpoint where the map/controls stack vertically
(requirement: multi-device access).

**Acceptance criteria:**
- [x] Renders correctly at 375 px, 768 px, 1280 px widths
- [x] Controls stack below the map on mobile
- [x] Data source attributions in footer (OGL licence compliance)
- [x] Lighthouse accessibility score ≥ 90 on the shell

**Delivered as:** three routes (`/`, `/insights`, `/methodology`); the scaffold's `/one`
placeholder is removed. The dashboard is one grid — single column to 1024px, where the map
takes two thirds and the controls move alongside. DOM order never diverges from visual
order, so reading and tab sequence match at every width.

**Verification, not assertion.** The last two criteria were measurable, so they are
measured: `web/e2e/shell.spec.ts` renders the real production build at all three widths and
runs **axe-core** over every route at each, against the same `wcag2a/2aa/21a/21aa` rule set
Lighthouse scores its accessibility category on. 28 checks, in CI. They also assert no
horizontal overflow, reachable navigation, and 44px minimum touch targets. Screenshots at
each width are in `documentation/screenshots/`.

Two real faults were caught this way and fixed:

- **Contrast.** `--text-muted` is 3.50:1 on the light surface — the palette's chart-chrome
  colour, under the 4.5:1 AA floor. It had been used for captions and footer prose; 17
  nodes failed. Prose now uses `--text-secondary` (7.73:1), and the token carries a comment
  saying it is for axis labels only.
- **A scrollable region with no keyboard access.** The methodology coverage table scrolls
  horizontally at 375px and could not be scrolled by keyboard.

**Beyond the criteria:**

- **Design tokens for the whole epic.** `app/globals.css` defines surfaces, ink,
  categorical, sequential, diverging, status and no-data as CSS custom properties. The
  categorical slots were run through a palette validator rather than eyeballed — all-pairs,
  both modes, worst CVD ΔE 9.2 light / 9.4 dark against a ≥8 target — and capped at three,
  because a fourth slot puts yellow beside orange and fails the floor for scatter and
  choropleth. 3.2 onwards read these roles and add no raw hex.
- **Dark mode is selected, not flipped.** Its own steps for the dark surface, with a stored
  choice beating the OS setting in both directions, applied before first paint.
- **Live figures.** The shell reads the coverage matrix, so 33 boroughs, 19 metrics and the
  window are real. Footer attributions are derived from each metric's `source` rather than
  typed, and the partial-coverage note is generated — it cannot drift from the data.

**One architectural fix this surfaced:** `lib/data.ts` had a `node:fs` import for the
GeoJSON reader, and the header is a client component that reaches it through `lib/site.ts`.
Type-check and lint both accepted that; only `next build` rejected it. Filesystem access
moved to `lib/geo.ts` behind the `server-only` marker, so the same mistake now fails with
its cause named.

---

### Issue 3.2 — Choropleth map component
**Status: Done**
**Branch:** `feat/frontend/choropleth-map` · **Labels:** `frontend` `blocked-by:2.1` `blocked-by:2.2` `blocked-by:3.1` · **Estimate:** 5–6 hrs

**Description:**
Render the borough choropleth, joining boundary geometry to metric values. Colour scale with a
legend component.

**Acceptance criteria:**
- [x] All 33 boroughs render and colour by the selected metric
- [x] Legend reflects the active scale and units
- [x] Touch pan/zoom works on mobile
- [x] No-data boroughs styled distinctly, not omitted
- [x] Boroughs listed in the metric's `boroughs_missing` render in the no-data style — City of London on well-being and life expectancy is the live case, not hypothetical
- [x] Colour scale direction follows the metric's `direction`; a `higher_is_worse` metric is not coloured like a `higher_is_better` one
- [x] Legend units come from the metric's `unit`, and diverging metrics (`standardised`) use a scale centred on zero

**Delivered as** an inline SVG map, **not MapLibre GL**. The decision and its cost are recorded
here because the plan named MapLibre as the likely technology:

- The map is 33 static polygons with no basemap. MapLibre's value is tiles, labels and a style
  pipeline. Using it here means either a third-party tile endpoint — a network dependency, an API
  key and an attribution obligation this project does not otherwise carry — or a style with no
  basemap, which is ~900 KB of WebGL to fill polygons.
- It renders into a canvas, which is one opaque node to a screen reader and to axe, and which
  Playwright cannot assert on without pixel diffing. Issue 3.8 asks for a *measured* accessibility
  pass, so an SVG the tests can read is worth more than a GPU this does not need.
- **The cost, stated:** no basemap context and no street detail. Pan and zoom operate on the SVG
  viewBox instead, so the touch criterion is met, but a reader cannot zoom in to see roads. For a
  borough-level choropleth that is the right trade; for a point map of individual crimes it would
  not be. **No new runtime dependency was added for the whole epic** — projection, scales, ticks
  and the correlation are about 400 lines of tested code against ~250 KB of library.

**Projection.** Exact Web Mercator, hand-written and checked against the closed form at known
latitudes (`tests/projection.test.ts`), computed **on the server**: the browser receives 33 path
strings in viewBox units rather than 171 KB of longitude/latitude pairs it would have to project on
every render. The join to metric values is on GSS code, and the test that proves it reorders the
borough list — the pipeline currently writes both files in the same order, so a positional join
would pass by luck today and silently draw every borough with its neighbour's outline the moment
either file is re-sorted.

**Colour.** Three rules, all read from `coverage.json`, none guessable from the values:

- **Darker = worse, for every metric.** `higher_is_worse` runs light→dark with value;
  `higher_is_better` runs dark→light. So crime and median income can be compared without
  relearning the ramp. `neutral` metrics get no better/worse claim at all.
- **Quantile classes, not equal intervals.** City of London's crime rate is 671 per 1,000 against a
  median of 113; seven equal intervals put 32 boroughs in the lowest class. The cost — quantiles
  flatten magnitude — is paid back by printing the real break values in the legend and by 3.3's
  exclusion control.
- **Diverging metrics are centred on zero and do NOT use quantiles.** The IMD health domain runs
  −1.4 to +0.4, whose observed midpoint is −0.5; putting the neutral colour there would destroy the
  only thing a diverging ramp is for.

**No-data is a hatch as well as a colour** (`<pattern>` + `--no-data`), because a mid grey between a
pale ramp step and a dark one is exactly where colour alone fails.

**Accessibility.** The SVG is `role="img"` with a description naming the metric, the year and how
many boroughs have no data. It is not the keyboard path and does not pretend to be — 33 focusable
polygons in geographic order is a tab sequence nobody can hold in their head. A **borough table**
beneath it carries the same values, the same selection and the same no-data states, one tab stop
per row, and doubles as the exact-value view for everyone.

---

### Issue 3.3 — Metric switcher and feature toggles
**Status: Done**
**Branch:** `feat/frontend/metric-controls` · **Labels:** `frontend` `blocked-by:3.2` · **Estimate:** 3–4 hrs

**Acceptance criteria:**
- [x] Metric switch re-renders map + legend without reload
- [x] Borough exclusion recomputes colour scale and charts
- [x] State held in URL query params (shareable views)
- [x] Keyboard accessible
- [x] The metric list is built from `coverage.json`, with labels from `label` — no hardcoded metric names
- [x] Switching between IMD domains rebuilds the scale rather than reusing it, since the domains do not share units

**Delivered as** a native `<select>` with `<optgroup>`, grouped by metric family derived from the
data. Nothing about the 19 metrics is typed into the component: a metric added to the pipeline
appears without a code change, and one removed cannot linger as a dead option. A native select is
keyboard-correct and screen-reader-correct with no ARIA of ours, and on a phone it opens the
platform picker.

**Exclusions** drop a borough from the colour classes, the correlation and the summary figures, and
draw it faded on the map rather than removing it — a hole in the map reads as missing data, which is
a different claim from "the reader took this one out". The per-borough checkboxes live in the
borough table, where the value they affect is already visible; the control panel carries only the
one exclusion the data argues for (City of London, ~8,000 residents).

**URL state** is mirrored with `window.history.replaceState`, not the router. **Trade-off:** the
back button does not step through metric changes. A router navigation would re-run the route on
every drag of the year slider for state that is entirely client-side; for a dashboard whose whole
state is one shareable URL this is the right way round. The **initial** state is parsed on the
server from the request's search params, so a shared link renders correctly in the first HTML
instead of flashing the default view.

**A stale link explains itself.** Unlike the API — which rejects unknown parameters, because a
silently dropped `?metrics=` returns everything and looks like it worked — a page URL ignores
unknown parameters (it collects `utm_source` from anything that links to it) but reports every
value it could not honour in a visible notice. `?metric=not_a_metric&year=1999` renders a working
dashboard and says what it did with each.

---

### Issue 3.4 — Year slider with crime trend
**Status: Done**
**Branch:** `feat/frontend/year-slider` · **Labels:** `frontend` `blocked-by:3.2` · **Estimate:** 2–3 hrs

**Acceptance criteria:**
- [x] Slider covers the full window; map updates ≤ 150 ms after release
- [x] Disabled/static for snapshot-only metrics (IMD), with explanatory hint
- [x] Usable by touch on mobile
- [x] Slider range and enabled years come from each metric's coverage — no hardcoded ranges
- [x] Snapshot metrics (IMD) render as discrete selectable points, not a continuous slider
- [x] Partial years visually marked and excluded from year-on-year comparisons
- [x] Metrics whose series ends early show the end of their own range, not the global one

**Delivered as** two controls chosen by `cadence`. Annual metrics get a slider **indexed by position
in the metric's own year list**, not by year number, so a gap in a series cannot be scrubbed into —
dragging a handle through a year the source never collected would be interpolation by interface.
Snapshot metrics get radio buttons: IMD is two snapshots four years apart, and a slider across them
invites a reader to look for 2017 and read the absence as a dip.

Ranges come from the selected metric, never the global window — crime runs to 2024 and well-being
stops at 2022, so a shared slider leaves two years of empty map with no explanation. Switching to a
shorter metric snaps the year and **says so** in the notice region.

The render is immediate and only the **URL write** is debounced (150 ms), which is what "debounced
updates" and "≤150 ms after release" mean in practice. The slider is 44 px tall, because the native
track is about 4 px.

---

### Issue 3.5 — Borough tooltip and detail panel
**Status: Done**
**Branch:** `feat/frontend/borough-detail` · **Labels:** `frontend` `blocked-by:3.2` · **Estimate:** 3–4 hrs

**Acceptance criteria:**
- [x] Hover shows tooltip on desktop; tap opens panel on touch devices
- [x] All metrics with units and borough rank (n/33)
- [x] Panel dismissible and keyboard navigable
- [x] Rank denominator reflects the metric's actual borough coverage — n/32 where City of London is absent, not n/33
- [x] A metric with no value for that borough-year says why (suppressed, no denominator, outside its series), rather than showing a blank

**Delivered as** a panel rather than a modal — a modal needs a focus trap and an inert background
and buys nothing here, since the map underneath stays useful and at 375 px the panel is simply the
next thing down the page. Focus moves to the heading on open; Escape and a Close button dismiss it.

**Ranks use each metric's own coverage.** Well-being and life expectancy are n/32. Printing n/33
would assert a position for City of London that the source refuses to estimate. Ranks are
competition-ranked, so ties share a rank and the panel says how many are tied — which is how
`imd_employment_score` reads as "1st of 33 (tied with 32)" rather than as a meaningful first place.

**An empty cell says which kind of empty it is.** "Not published for City of London — the resident
population is too small", "IMD exists only for 2015 and 2019, not 2023", and "no value published for
Camden in 2019" are three different facts about three different things, and a blank collapses them
into one unhelpful one.

**A metric whose series does not reach the selected year shows its nearest published year, labelled
as such.** Showing nothing for every IMD domain because the reader is on 2023 would be correct and
useless; showing the 2019 value as though it were 2023 would be worse.

The hover tooltip is mouse-only and `aria-hidden`: everything in it is in the table and the panel,
both reachable, and a tooltip that is also a live region announces on every pixel of movement.

---

### Issue 3.6 — Crime-vs-metric scatterplot
**Status: Done**
**Branch:** `feat/frontend/scatterplot` · **Labels:** `frontend` `blocked-by:3.3` · **Estimate:** 4–5 hrs

**Acceptance criteria:**
- [x] Points update with metric/year selection and exclusions
- [x] Pearson r displayed with a "correlation ≠ causation" footnote
- [x] Hovering a point highlights the borough on the map (linked brushing)
- [x] Renders legibly at mobile widths
- [x] Mismatched series pair on nearest-available-year; the pairing is printed on the chart
- [x] No silent interpolation anywhere
- [x] Axis/legend language is associative, never causal
- [x] Where the two series use different `year_rule` values, the printed pairing says so
- [x] Boroughs missing from either series are dropped from the fit and the dropped count is shown

**Delivered as** hand-drawn SVG with an OLS fit and Pearson r computed in `lib/stats.ts`. Most of
the work is in what it refuses to do: it never interpolates (each series is taken at its own nearest
published year and **both** years are printed); it names the year rules when they differ, because
"2019 × 2019" hides that one covers 2017–2019 and the other April 2019 to March 2020; and it counts
what it dropped rather than absorbing it into a quieter n.

**Legibility at 375 px** is handled by measuring the container with a `ResizeObserver` and drawing in
real pixels, so a 12 px axis label is 12 px at every width. A fixed viewBox scaled by CSS shrinks the
type with the chart, which is how a scatter becomes unreadable on a phone while looking fine on the
machine it was built on.

**Two faults this component surfaced:**

- **A fit with no data behind it.** The zero-variance guard was written as `sxx === 0`, which is not
  the test that holds: 33 identical values of 0.1 have a mean of 0.10000000000000002, so the sum of
  squared deviations is ~5.8e-34 rather than 0 — enough to return a slope and an r made entirely of
  rounding error. `imd_employment_score` is exactly this case. The guard is now a
  magnitude-relative tolerance, and the chart says "no variation between boroughs at this
  precision" instead of drawing a line.
- **A fitted line drawn outside the plot.** The line ran to the padded domain edges, which for a
  steep fit is off the chart and over the axis labels — implying values the chart is not showing.
  It is clipped to the plotting rectangle.

---

### Issue 3.7 — KPI summary panel
**Status: Done**
**Branch:** `feat/frontend/kpi-panel` · **Labels:** `frontend` `blocked-by:3.3` · **Estimate:** 2–3 hrs

**Acceptance criteria:**
- [x] Four KPI cards update with metric/year selection
- [x] Trend arrows with correct direction semantics (falling crime = positive)
- [x] Cards wrap cleanly on mobile
- [x] Arrow semantics are read from the metric's `direction`, not hardcoded
- [x] "Highest" and "lowest" are labelled by meaning rather than by raw value
- [x] Long-run change endpoints skip partial years

**Delivered as** four cards whose wording is derived from `direction`: the extremes read "Highest —
the worse end" and "Lowest — the worse end", so the same card is always the bad one and a reader is
never invited to congratulate the borough with the most burglaries. `neutral` metrics get plain
"Highest"/"Lowest" and no judgement. **The arrow is never the only signal** — "improving" /
"worsening" is spelled out, so the meaning survives greyscale, colour blindness and a screen reader.

**Two refusals:**

- **No long-run change across IMD snapshots.** The pipeline already drops the 2015 and 2019 ranks as
  non-comparable; presenting a change between their scores as a trend puts a number on something the
  source does not support. The card says "Two snapshots four years apart are not a trend."
- **No "highest" and "lowest" that are the same borough.** Where a metric has no variation the cards
  collapse to one reading "No variation between boroughs", because naming Barking and Dagenham as
  both the highest and the lowest is technically true and reads as a bug.

The London figure is labelled a **borough mean, unweighted** — a population-weighted mean is a
different quantity, dominated by the large outer boroughs, and the unit of analysis throughout this
project is the borough.

---

### Issue 3.8 — Cross-device and accessibility pass
**Status: Done**
**Branch:** `fix/frontend/responsive-a11y` · **Labels:** `frontend` `qa` `blocked-by:3.4` `blocked-by:3.5` `blocked-by:3.6` `blocked-by:3.7` · **Estimate:** 3–4 hrs

**Acceptance criteria:**
- [x] All interactions usable at 375 px width with touch only
- [x] Lighthouse accessibility ≥ 95 on the main page
- [x] Choropleth palette passes a deuteranopia simulation
- [x] No horizontal scroll at any breakpoint
- [x] The no-data style is distinguishable from the palette's extremes without relying on colour alone

**Delivered as measurement, not assertion.** `web/e2e/dashboard.spec.ts` is **53 checks** against the
real production build, joining 3.1's 29 for **82 browser checks** in CI, alongside **139 unit
tests**. Every criterion above is a check:

- **axe-core** over four dashboard states — default, a 32-borough metric, a diverging metric, and one
  with a detail panel and an exclusion open — at 375 / 768 / 1280 px. Zero violations against the
  same `wcag2a/2aa/21a/21aa` rule set Lighthouse scores its accessibility category on.
- **Touch targets** measured on the *effective* target: for a checkbox wrapped in a label, WCAG 2.5.5
  measures the label, so the test resolves to it — but a checkbox with no wrapping label is still
  reported as a 16 px target rather than skipped.
- **Deuteranopia** simulated with the Machado et al. (2009) matrix at severity 1.0, applied in
  *linear* RGB (the usual shortcut of applying it to gamma-encoded sRGB exaggerates separation in
  the shadows, which is the region a sequential ramp depends on). The property tested is the one
  that makes a sequential ramp CVD-safe: lightness ordering must survive the simulation, so a
  deuteranope can still say which of two swatches is higher. The diverging poles are checked for
  ΔE separation — blue against red rather than red against green is why this passes.
- **No-data without colour**: the `<pattern>` exists, exactly one borough uses it on a 32-borough
  metric, and the word "no data" appears in both the legend and the table.

**Two guards were verified by deliberately breaking the code** and confirming they fail:

1. Making `darkIsHigh()` ignore `direction`. The unit test caught it — but **the browser test did
   not**, because it only checked the legend's caption, which is generated by a different code path
   from the fills. A build that ignored `direction` entirely still printed "Darker means lower" over
   a ramp running the other way. The test now reads the painted colours and asserts the lightness
   ordering flips between a `higher_is_worse` and a `higher_is_better` metric.
2. Making `rankOf()` use 33 as the denominator regardless of coverage. Caught by both.

**A CI-only failure, and what it exposed.** The first CI run failed on 3.1's theme test at
the assertion *after* a page reload, having passed the assertion three lines earlier. It could
not be reproduced locally — 100+ runs, including under CI conditions with four workers — so it
was diagnosed from the failure's shape instead. Exactly one code path produces it: the toggle
stamped `data-theme` on the document and *then* wrote to `localStorage` inside a `catch` that
swallowed everything. A refused write therefore left the click looking successful, the
attribute assertion passing, and the choice gone on the next load — reported as a missing
attribute with nothing in the log mentioning storage.

Confirmed by making `localStorage.setItem` throw and observing the identical signature. Three
changes: the toggle records a failed write as `data-theme-persisted="false"` rather than
discarding it; the test asserts persistence at the point it happens, with a message that names
it; and a new test pins the degradation contract — a browser that refuses storage still gets a
working toggle for the session and a visible reason the next load will not remember it. Two
supporting fixes went in with it: `suppressHydrationWarning` on `<html>`, which this pattern
requires and which was missing since 3.1, and `trace: "on-first-retry"` plus trace upload in
CI, so the next failure that only happens on a runner is diagnosed from evidence rather than
from its shape.

**One per-request performance defect fixed.** Reading `searchParams` makes the dashboard route
dynamic, so its body runs on every request — and it was calling `buildSeries` over all 6,001
observations each time, for a result that cannot change while the process lives. Memoised.

**One measured regression fixed, inherited from 3.1.** `components/site-header.tsx` is a client
component; it imported `lib/site.ts`, which imported `lib/data.ts`, which imports the 516 KB
observation export. Turbopack could not drop it, so **every visitor downloaded all 6,001
observations in order to render the word "Dashboard" in the header** — confirmed by grepping the
built chunk for `{"borough_gss":…}` and finding 6,001 of them. The coverage matrix now lives in
`lib/coverage.ts` (9 KB, client-safe) and the bulk export sits behind a `server-only` marker in
`lib/data.ts`. Client JavaScript fell from 1.2 MB to 716 KB.

---

## Epic 4 — Narrative & Release

### Issue 4.1 — Insights and methodology content
**Status: Planned**
**Branch:** `feat/docs/insights-content` · **Labels:** `docs` `blocked-by:3.7` · **Estimate:** 3–4 hrs

**Description:**
Write the landing narrative: 3–4 headline findings with numbers, the original big-data
pipeline architecture diagram (Spark/Hive/Hadoop context), and a limitations section.

**Acceptance criteria:**
- [ ] Headline insight appears above the fold, before any tooling mention
- [ ] Architecture diagram embedded (original pipeline + this rebuild)
- [ ] Limitations section covers the named caveats
- [ ] All claims traceable to the dataset
- [ ] All copy states associations; a limitations note covers ecological inference (33 aggregated units) and the IMD-crime-domain exclusion
- [ ] The 15-year window and the 2011–2023 analysis window are stated explicitly

**Note:** the limitations list has grown since the amendment was written. It should now also
cover: the April 2013 crime taxonomy change (categories continuous but not comparable); City
of London absent from two metrics; well-being ending a year short of the window; and the
mid-year-population against calendar-year-crime offset. All are recorded in `SOURCES.md`.
The retired `pipeline/experimental/` Spark scripts are the source material for the
architecture diagram.

**Technologies:** MDX or static page content, draw.io/Excalidraw · **Alternatives:** separate blog post linking to the app

---

### Issue 4.2 — Performance audit and release
**Status: Planned**
**Branch:** `chore/release/perf-v1` · **Labels:** `deploy` `qa` `blocked-by:3.8` `blocked-by:4.1` · **Estimate:** 2–3 hrs

**Description:**
Lighthouse performance pass (code-split the map bundle, lazy-load below-fold charts), final
README with screenshots and live URL, tag `v1.0.0`, and close out the project board.

**Acceptance criteria:**
- [ ] Lighthouse performance ≥ 85 mobile, ≥ 95 desktop
- [ ] README includes screenshots, live URL, local setup, data licences
- [ ] `v1.0.0` tag and GitHub Release created
- [ ] All board issues closed or explicitly moved to a v2 milestone

**Technologies:** Lighthouse, GitHub Releases · **Alternatives:** Vercel Analytics for post-launch monitoring

---

## Housekeeping not tracked as issues

| Item | Action |
|---|---|
| `_to_delete/` at the repository root | Delete — holds retired scripts and superseded outputs |
| `crime.csv`, `crime_by_borough.csv` (7.6 GB) in `data/processed/` | Delete — retired Spark intermediates; QA warns while they exist |
| Empty `pipeline/dimension/` directory | Delete |
| Income and IMD licences recorded as *assumed* | Verify on the dataset pages and update `SOURCES.md` |
| Regenerate `boroughs.json`, `coverage.json`, `london.geojson` on real data | One pipeline run — see `pipeline/README.md` |
