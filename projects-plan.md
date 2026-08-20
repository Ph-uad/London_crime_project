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

> **Three data constraints run through this epic.** They come from `coverage.json` and are
> not optional:
> 1. **Two metrics cover 32 boroughs, not 33** — City of London has no well-being and no
>    life expectancy. Missing boroughs must render as an explicit no-data state, never be
>    omitted or coloured as zero.
> 2. **Direction is per metric.** Anxiety and crime are `higher_is_worse`; most others are
>    `higher_is_better`; taxpayer count is `neutral`. Read it from `coverage.json`.
> 3. **IMD domains sit on three different scales** — proportion, score, standardised — and
>    two go negative. One shared colour ramp across them is meaningless.

### Issue 3.1 — Responsive layout shell
**Status: In progress**
**Branch:** `feat/frontend/layout-shell` · **Labels:** `frontend` `blocked-by:0.2` · **Estimate:** 2–3 hrs

**Description:**
Build the app shell: header, navigation, main content grid, and footer with data
attributions. Mobile-first with a defined breakpoint where the map/controls stack vertically
(requirement: multi-device access).

**Acceptance criteria:**
- [ ] Renders correctly at 375 px, 768 px, 1280 px widths
- [ ] Controls stack below the map on mobile
- [ ] Data source attributions in footer (OGL licence compliance)
- [ ] Lighthouse accessibility score ≥ 90 on the shell

**Note:** currently a bare scaffold — `layout.tsx` has no header, navigation or footer.
Attribution must name ONS, MHCLG, HMRC and data.police.uk, and carry the Ordnance Survey +
ONS rights statement that comes with the boundary data.

---

### Issue 3.2 — Choropleth map component
**Status: Planned**
**Branch:** `feat/frontend/choropleth-map` · **Labels:** `frontend` `blocked-by:2.1` `blocked-by:2.2` `blocked-by:3.1` · **Estimate:** 5–6 hrs

**Description:**
Render the borough choropleth with MapLibre GL via react-map-gl, joining `/api/geo` geometry
to `/api/metrics` values. Colour scale via d3-scale with a legend component.

**Acceptance criteria:**
- [ ] All 33 boroughs render and colour by the selected metric
- [ ] Legend reflects the active scale and units
- [ ] Touch pan/zoom works on mobile
- [ ] No-data boroughs styled distinctly, not omitted
- [ ] Boroughs listed in the metric's `boroughs_missing` render in the no-data style — City of London on well-being and life expectancy is the live case, not hypothetical
- [ ] Colour scale direction follows the metric's `direction`; a `higher_is_worse` metric is not coloured like a `higher_is_better` one
- [ ] Legend units come from the metric's `unit`, and diverging metrics (`standardised`) use a scale centred on zero

**Technologies:** MapLibre GL JS, react-map-gl, d3-scale · **Alternatives:** Leaflet + react-leaflet, deck.gl

---

### Issue 3.3 — Metric switcher and feature toggles
**Status: Planned**
**Branch:** `feat/frontend/metric-controls` · **Labels:** `frontend` `blocked-by:3.2` · **Estimate:** 3–4 hrs

**Description:**
Control panel to switch the mapped metric and toggle features on/off — hide/show metrics in
the comparison views and exclude boroughs (e.g. remove the City of London outlier) from
scales and charts (requirement: adjustable parameters).

**Acceptance criteria:**
- [ ] Metric switch re-renders map + legend without reload
- [ ] Borough exclusion recomputes colour scale and charts
- [ ] State held in URL query params (shareable views)
- [ ] Keyboard accessible
- [ ] The metric list is built from `coverage.json`, with labels from `label` — no hardcoded metric names
- [ ] Switching between IMD domains rebuilds the scale rather than reusing it, since the domains do not share units

**Technologies:** React state, Next.js searchParams · **Alternatives:** Zustand/Redux (unnecessary at this scale)

---

### Issue 3.4 — Year slider with crime trend
**Status: Planned**
**Branch:** `feat/frontend/year-slider` · **Labels:** `frontend` `blocked-by:3.2` · **Estimate:** 2–3 hrs

**Description:**
Year slider scrubbing the crime layer across the window, with debounced updates and the
active year displayed prominently.

**Acceptance criteria:**
- [ ] Slider covers the full window; map updates ≤ 150 ms after release
- [ ] Disabled/static for snapshot-only metrics (IMD), with explanatory hint
- [ ] Usable by touch on mobile
- [ ] Slider range and enabled years come from `/api/meta` per selected metric — no hardcoded ranges
- [ ] Snapshot metrics (IMD) render as discrete selectable points, not a continuous slider
- [ ] Partial years visually marked and excluded from year-on-year comparisons
- [ ] Metrics whose series ends early show the end of their own range, not the global one — well-being stops at 2022 while crime runs to 2024

**Technologies:** React, Radix UI slider (or native input) · **Alternatives:** animated auto-play timeline (defer to v2)

---

### Issue 3.5 — Borough tooltip and detail panel
**Status: Planned**
**Branch:** `feat/frontend/borough-detail` · **Labels:** `frontend` `blocked-by:3.2` · **Estimate:** 3–4 hrs

**Description:**
Hover tooltip (desktop) and tap-to-open detail panel (mobile) showing all metrics for the
selected borough and year, with borough rank per metric for at-a-glance comparison.

**Acceptance criteria:**
- [ ] Hover shows tooltip on desktop; tap opens panel on touch devices
- [ ] All metrics with units and borough rank (n/33)
- [ ] Panel dismissible and keyboard navigable
- [ ] Rank denominator reflects the metric's actual borough coverage — n/32 where City of London is absent, not n/33
- [ ] A metric with no value for that borough-year says why (suppressed, no denominator, outside its series), rather than showing a blank

**Technologies:** React, MapLibre events · **Alternatives:** permanent sidebar (weaker on mobile)

---

### Issue 3.6 — Crime-vs-metric scatterplot
**Status: Planned**
**Branch:** `feat/frontend/scatterplot` · **Labels:** `frontend` `blocked-by:3.3` · **Estimate:** 4–5 hrs

**Description:**
Scatterplot of crime rate against the selected social-determinant metric, one point per
borough, with hover labels and a fitted trend line + correlation coefficient. This is the
component that makes the project's core association visible. Respects borough exclusions
from 3.3.

**Acceptance criteria:**
- [ ] Points update with metric/year selection and exclusions
- [ ] Pearson r displayed with a "correlation ≠ causation" footnote
- [ ] Hovering a point highlights the borough on the map (linked brushing)
- [ ] Renders legibly at mobile widths
- [ ] Mismatched series pair on nearest-available-year; the pairing is printed on the chart (e.g. "crime 2019 × IMD 2019")
- [ ] No silent interpolation anywhere
- [ ] Axis/legend language is associative ("crime rate vs median income"), never causal
- [ ] Where the two series use different `year_rule` values, the printed pairing says so — pairing a calendar year against a rolling period ending that year is not the same as pairing two calendar years
- [ ] Boroughs missing from either series are dropped from the fit and the dropped count is shown, not silently excluded

**Technologies:** visx or D3, React · **Alternatives:** Recharts (faster, less control), Observable Plot

---

### Issue 3.7 — KPI summary panel
**Status: Planned**
**Branch:** `feat/frontend/kpi-panel` · **Labels:** `frontend` `blocked-by:3.3` · **Estimate:** 2–3 hrs

**Description:**
At-a-glance KPI strip: highest/lowest borough for the active metric, London-wide average,
and long-run change for crime (requirement: signal insight at a glance).

**Acceptance criteria:**
- [ ] Four KPI cards update with metric/year selection
- [ ] Trend arrows with correct direction semantics (falling crime = positive)
- [ ] Cards wrap cleanly on mobile
- [ ] Arrow semantics are read from the metric's `direction`, not hardcoded — a falling anxiety score is good, a falling life expectancy is not
- [ ] "Highest" and "lowest" are labelled by meaning (most/least deprived, best/worst) rather than by raw value, so a `higher_is_worse` metric does not read as a league table won by the worst borough
- [ ] Long-run change endpoints skip partial years

**Technologies:** React, Tailwind CSS · **Alternatives:** sparklines per card (defer to v2)

---

### Issue 3.8 — Cross-device and accessibility pass
**Status: Planned**
**Branch:** `fix/frontend/responsive-a11y` · **Labels:** `frontend` `qa` `blocked-by:3.4` `blocked-by:3.5` `blocked-by:3.6` `blocked-by:3.7` · **Estimate:** 3–4 hrs

**Description:**
Systematic pass on real or emulated devices (small phone, tablet, desktop): touch targets
≥ 44 px, contrast ratios, focus order, screen-reader labels on controls, and a
colour-blind-safe palette check for the choropleth.

**Acceptance criteria:**
- [ ] All interactions usable at 375 px width with touch only
- [ ] Lighthouse accessibility ≥ 95 on the main page
- [ ] Choropleth palette passes a deuteranopia simulation
- [ ] No horizontal scroll at any breakpoint
- [ ] The no-data style is distinguishable from the palette's extremes without relying on colour alone

**Technologies:** Lighthouse, Chrome DevTools device emulation · **Alternatives:** BrowserStack for real-device coverage

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
