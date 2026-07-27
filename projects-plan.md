# London Quality of Life × Crime — Web Rebuild: GitHub Projects Plan

> Note: this file is a roadmap; the current repo state is documented in README.md and pipeline/README.md.


**Architecture (locked):** Next.js (App Router, TypeScript) serving both the frontend and the data API via API routes · R data pipeline (offline, outputs versioned JSON/GeoJSON) · Deployed on Vercel free tier.
**Total estimate:** ~55–70 hrs across 5 epics.
**Dependency rule:** no issue depends on a later-numbered issue. `blocked-by:*` labels encode hard dependencies.

---

## Epic 0 — Repository & Infrastructure Setup

### Issue 0.1 — Initialise monorepo structure
**Status: Done**
**Branch:** `chore/setup/monorepo-init`
**Labels:** `setup` `no-dependencies`
**Estimate:** 1–2 hrs

**Description:**
Scaffold the repository with `web/` (Next.js app) and `pipeline/` (R scripts) directories. Initialise git, add `.gitignore` files for Node and R, and create the root `README.md` with project overview, architecture summary, and local setup instructions.

**Acceptance criteria:**
- [ ] Root `README.md` exists with architecture summary and diagram placeholder
- [ ] `web/` and `pipeline/` directories created
- [ ] `.gitignore` covers `node_modules`, `.next`, `.env*`, `.Rhistory`, `.RData`, `*.Rproj.user`
- [ ] `data/` directory with `raw/` (gitignored) and `processed/` (committed) subfolders
- [ ] Initial commit pushed to `main`

**Technologies:** Git, GitHub
**Alternatives:** GitLab, Bitbucket

---

### Issue 0.2 — Scaffold Next.js application
**Status: Done**
**Branch:** `chore/setup/nextjs-scaffold`
**Labels:** `setup` `frontend` `blocked-by:0.1`
**Estimate:** 1–2 hrs

**Description:**
Initialise the Next.js app in `web/` with TypeScript, ESLint, Tailwind CSS, and the App Router. Verify dev server runs and a placeholder home page renders.

**Acceptance criteria:**
- [ ] `npx create-next-app` completed with TypeScript + Tailwind + App Router
- [ ] `npm run dev` serves a placeholder page locally
- [ ] `npm run build` passes with zero errors
- [ ] Prettier config committed

**Technologies:** Next.js, TypeScript, Tailwind CSS
**Alternatives:** Remix, SvelteKit, plain Vite + React

---

### Issue 0.3 — CI workflow for lint and build
**Status: Planned**
**Branch:** `chore/setup/ci-lint-build`
**Labels:** `setup` `ci` `blocked-by:0.2`
**Estimate:** 1–2 hrs

**Description:**
Add a GitHub Actions workflow that runs ESLint and `next build` on every pull request to `main`. Failing checks block merge via branch protection.

**Acceptance criteria:**
- [ ] `.github/workflows/ci.yml` runs lint + build on PRs
- [ ] Branch protection on `main` requires the CI check
- [ ] CI passes on a test PR

**Technologies:** GitHub Actions
**Alternatives:** CircleCI, Vercel-only checks

---

### Issue 0.4 — Vercel deployment pipeline
**Status: Planned**
**Branch:** `chore/setup/vercel-deploy`
**Labels:** `setup` `deploy` `blocked-by:0.2`
**Estimate:** 1 hr

**Description:**
Connect the repo to Vercel with `web/` as the project root. Production deploys from `main`; preview deploys on every PR.

**Acceptance criteria:**
- [ ] Production URL serves the placeholder app
- [ ] PR preview deployments active
- [ ] Deployment URL added to `README.md`

**Technologies:** Vercel
**Alternatives:** Netlify, Cloudflare Pages, Railway

---

## Epic 1 — Data Pipeline (R)

### Issue 1.1 — Acquire raw crime data (Met + City of London)
**Status: In progress**
**Branch:** `feat/data/crime-acquisition`
**Labels:** `data` `no-dependencies`
**Estimate:** 2–3 hrs

**Description:**
Download the 10-year custom archive from data.police.uk filtered to Metropolitan Police Service and City of London Police. Document the exact date range, download date, and licence (OGL v3) in `pipeline/SOURCES.md`. Store raw CSVs in `data/raw/` (gitignored, with a re-download script).

**Acceptance criteria:**
- [ ] All monthly CSVs for the 10-year window present locally
- [ ] `pipeline/SOURCES.md` records URL, date range, licence, download date
- [ ] `pipeline/00_download.R` (or documented manual steps) reproduces the acquisition
- [ ] Row-count sanity check logged (per-year totals)

**Technologies:** R, data.police.uk archive
**Alternatives:** police.uk API (rate-limited, impractical for 10 years)

---

### Issue 1.2 — LSOA→borough lookup and code harmonisation
**Status: Done**
**Branch:** `feat/data/lsoa-borough-lookup`
**Labels:** `data` `blocked-by:1.1`
**Estimate:** 3–4 hrs

**Description:**
Download the ONS Open Geography Portal LSOA-to-local-authority lookup. Harmonise 2011 and 2021 LSOA codes across the 10-year window using the ONS correspondence table. Produce a single clean lookup keyed on LSOA code → borough name/GSS code.

**Acceptance criteria:**
- [ ] Lookup covers ≥99.5% of LSOA codes appearing in the crime data
- [ ] Unmatched codes logged with counts and a documented handling decision
- [ ] 2011↔2021 boundary changes harmonised and footnoted in `SOURCES.md`
- [ ] Output saved as `data/processed/lsoa_lookup.csv`

**Technologies:** R (dplyr, readr), ONS Open Geography Portal
**Alternatives:** point-in-polygon spatial join with `sf` (slower, more robust)

---

### Issue 1.3 — Aggregate crime to borough-year rates
**Status: In progress**
**Branch:** `feat/data/crime-aggregation`
**Labels:** `data` `blocked-by:1.2`
**Estimate:** 3–4 hrs

**Description:**
Join crime records to the lookup, aggregate to borough × year (and borough × year × crime-category for the category breakdown), and convert counts to rates per 1,000 residents using ONS mid-year population estimates. Flag the City of London / Westminster daytime-population caveat in the output metadata.

**Acceptance criteria:**
- [ ] Output table: borough × year × category with `count` and `rate_per_1000`
- [ ] ONS mid-year estimates sourced per year (not a single snapshot)
- [ ] Totals reconcile with raw row counts (±0.5% after unmatched-LSOA exclusions)
- [ ] City of London anomaly documented in metadata field

**Technologies:** R (dplyr), ONS population estimates
**Alternatives:** DuckDB for the heavy join if memory becomes a problem

---

### Issue 1.4 — Ingest income, life expectancy, IMD, well-being
**Status: In progress**
**Branch:** `feat/data/qol-dimensions`
**Labels:** `data` `no-dependencies`
**Estimate:** 3–4 hrs

**Description:**
Download and tidy the four quality-of-life datasets from the GLA London Datastore / ONS: mean income by borough (per financial year), male/female life expectancy, IMD 2015 & 2019 borough summaries, and ONS4 well-being scores. Standardise borough names to GSS codes.

**Acceptance criteria:**
- [ ] Each dataset tidied to borough × year (or borough × snapshot for IMD)
- [ ] All borough identifiers mapped to GSS codes — zero name-mismatch joins
- [ ] Sources and licences appended to `SOURCES.md`
- [ ] Missing values explicitly recorded, not silently dropped

**Technologies:** R (dplyr, readr), GLA London Datastore, ONS
**Alternatives:** NOMIS API for programmatic pulls

---

### Issue 1.5 — Build unified dataset and export JSON
**Status: Planned**
**Branch:** `feat/data/unified-export`
**Labels:** `data` `blocked-by:1.3` `blocked-by:1.4`
**Estimate:** 2–3 hrs

**Description:**
Join crime rates with the four QoL dimensions into one tidy long-format dataset. Add validation checks (33 boroughs present, year coverage, value ranges). Export `data/processed/boroughs.json` shaped for direct API consumption.

**Acceptance criteria:**
- [ ] Single JSON: borough, GSS code, year, metric, value
- [ ] Validation script asserts borough count, year span, no negative rates
- [ ] File size < 1 MB
- [ ] Committed to repo with generation timestamp in metadata

**Technologies:** R (dplyr, jsonlite)
**Alternatives:** Parquet + server-side conversion (overkill at this size)

---

### Issue 1.6 — Borough boundaries GeoJSON
**Status: Planned**
**Branch:** `feat/data/boundaries-geojson`
**Labels:** `data` `no-dependencies`
**Estimate:** 2 hrs

**Description:**
Download the London Datastore statistical GIS borough boundaries, convert to GeoJSON, simplify with `rmapshaper` to <500 KB, and verify GSS codes match the unified dataset.

**Acceptance criteria:**
- [ ] `data/processed/london.geojson` < 500 KB
- [ ] All 33 features carry GSS codes matching `boroughs.json`
- [ ] Geometry validates (no self-intersections) and renders in geojson.io
- [ ] Simplification tolerance documented

**Technologies:** R (sf, rmapshaper)
**Alternatives:** mapshaper CLI, QGIS

---

## Epic 2 — API Layer (Next.js Routes)

### Issue 2.1 — Metrics API route
**Status: Planned**
**Branch:** `feat/api/metrics-route`
**Labels:** `backend` `blocked-by:0.2` `blocked-by:1.5`
**Estimate:** 2–3 hrs

**Description:**
Implement `GET /api/metrics` serving the unified dataset with query-parameter filtering: `?metric=`, `?year=`, `?borough=`. Typed response schema shared with the frontend.

**Acceptance criteria:**
- [ ] Route returns full dataset and filtered subsets correctly
- [ ] Invalid params return 400 with a clear message
- [ ] Shared TypeScript types in `web/lib/types.ts`
- [ ] Cache headers set (`s-maxage`, immutable data)

**Technologies:** Next.js API routes, TypeScript
**Alternatives:** standalone FastAPI/Express service (separate hosting, unjustified at this data size)

---

### Issue 2.2 — Boundaries API route
**Status: Planned**
**Branch:** `feat/api/geo-route`
**Labels:** `backend` `blocked-by:0.2` `blocked-by:1.6`
**Estimate:** 1–2 hrs

**Description:**
Serve `GET /api/geo` returning the simplified borough GeoJSON with long-lived cache headers.

**Acceptance criteria:**
- [ ] Route returns valid GeoJSON, content-type `application/geo+json`
- [ ] Response cached at the edge
- [ ] Load time < 300 ms on Vercel preview

**Technologies:** Next.js API routes
**Alternatives:** static import into the client bundle (simpler, loses the API showcase)

---

## Epic 3 — Frontend

### Issue 3.1 — Responsive layout shell
**Status: In progress**
**Branch:** `feat/frontend/layout-shell`
**Labels:** `frontend` `blocked-by:0.2`
**Estimate:** 2–3 hrs

**Description:**
Build the app shell: header, navigation, main content grid, and footer with data attributions. Mobile-first with a defined breakpoint where the map/controls stack vertically (requirement: multi-device access).

**Acceptance criteria:**
- [ ] Renders correctly at 375 px, 768 px, 1280 px widths
- [ ] Controls stack below the map on mobile
- [ ] Data source attributions in footer (OGL licence compliance)
- [ ] Lighthouse accessibility score ≥ 90 on the shell

**Technologies:** Next.js, Tailwind CSS
**Alternatives:** CSS Modules, styled-components

---

### Issue 3.2 — Choropleth map component
**Status: Planned**
**Branch:** `feat/frontend/choropleth-map`
**Labels:** `frontend` `blocked-by:2.1` `blocked-by:2.2` `blocked-by:3.1`
**Estimate:** 5–6 hrs

**Description:**
Render the borough choropleth with MapLibre GL via react-map-gl, joining `/api/geo` geometry to `/api/metrics` values. Colour scale via d3-scale with a legend component.

**Acceptance criteria:**
- [ ] All 33 boroughs render and colour by the selected metric
- [ ] Legend reflects the active scale and units
- [ ] Touch pan/zoom works on mobile
- [ ] No-data boroughs styled distinctly, not omitted

**Technologies:** MapLibre GL JS, react-map-gl, d3-scale
**Alternatives:** Leaflet + react-leaflet, deck.gl

---

### Issue 3.3 — Metric switcher and feature toggles
**Status: Planned**
**Branch:** `feat/frontend/metric-controls`
**Labels:** `frontend` `blocked-by:3.2`
**Estimate:** 3–4 hrs

**Description:**
Control panel to switch the mapped metric and toggle features on/off — hide/show metrics in the comparison views and exclude boroughs (e.g. remove the City of London outlier) from scales and charts (requirement: adjustable parameters).

**Acceptance criteria:**
- [ ] Metric switch re-renders map + legend without reload
- [ ] Borough exclusion recomputes colour scale and charts
- [ ] State held in URL query params (shareable views)
- [ ] Keyboard accessible

**Technologies:** React state, Next.js searchParams
**Alternatives:** Zustand/Redux (unnecessary at this scale)

---

### Issue 3.4 — Year slider with crime trend
**Status: Planned**
**Branch:** `feat/frontend/year-slider`
**Labels:** `frontend` `blocked-by:3.2`
**Estimate:** 2–3 hrs

**Description:**
Year slider scrubbing the crime layer across the 10-year window, with debounced updates and the active year displayed prominently.

**Acceptance criteria:**
- [ ] Slider covers the full window; map updates ≤ 150 ms after release
- [ ] Disabled/static for snapshot-only metrics (IMD), with explanatory hint
- [ ] Usable by touch on mobile

**Technologies:** React, Radix UI slider (or native input)
**Alternatives:** animated auto-play timeline (defer to v2)

---

### Issue 3.5 — Borough tooltip and detail panel
**Status: Planned**
**Branch:** `feat/frontend/borough-detail`
**Labels:** `frontend` `blocked-by:3.2`
**Estimate:** 3–4 hrs

**Description:**
Hover tooltip (desktop) and tap-to-open detail panel (mobile) showing all six metrics for the selected borough and year, with borough rank per metric for at-a-glance comparison.

**Acceptance criteria:**
- [ ] Hover shows tooltip on desktop; tap opens panel on touch devices
- [ ] All metrics with units and borough rank (n/33)
- [ ] Panel dismissible and keyboard navigable

**Technologies:** React, MapLibre events
**Alternatives:** permanent sidebar (weaker on mobile)

---

### Issue 3.6 — Crime-vs-metric scatterplot
**Status: Planned**
**Branch:** `feat/frontend/scatterplot`
**Labels:** `frontend` `blocked-by:3.3`
**Estimate:** 4–5 hrs

**Description:**
Scatterplot of crime rate against the selected QoL metric, one point per borough, with hover labels and a fitted trend line + correlation coefficient. This is the component that makes the project's core correlation claim visible. Respects borough exclusions from 3.3.

**Acceptance criteria:**
- [ ] Points update with metric/year selection and exclusions
- [ ] Pearson r displayed with a "correlation ≠ causation" footnote
- [ ] Hovering a point highlights the borough on the map (linked brushing)
- [ ] Renders legibly at mobile widths

**Technologies:** visx or D3, React
**Alternatives:** Recharts (faster, less control), Observable Plot

---

### Issue 3.7 — KPI summary panel
**Status: Planned**
**Branch:** `feat/frontend/kpi-panel`
**Labels:** `frontend` `blocked-by:3.3`
**Estimate:** 2–3 hrs

**Description:**
At-a-glance KPI strip: highest/lowest borough for the active metric, London-wide average, and 10-year change for crime (requirement: signal insight at a glance).

**Acceptance criteria:**
- [ ] Four KPI cards update with metric/year selection
- [ ] Trend arrows with correct direction semantics (falling crime = positive)
- [ ] Cards wrap cleanly on mobile

**Technologies:** React, Tailwind CSS
**Alternatives:** sparklines per card (defer to v2)

---

### Issue 3.8 — Cross-device and accessibility pass
**Status: Planned**
**Branch:** `fix/frontend/responsive-a11y`
**Labels:** `frontend` `qa` `blocked-by:3.4` `blocked-by:3.5` `blocked-by:3.6` `blocked-by:3.7`
**Estimate:** 3–4 hrs

**Description:**
Systematic pass on real or emulated devices (small phone, tablet, desktop): touch targets ≥ 44 px, contrast ratios, focus order, screen-reader labels on controls, and a colour-blind-safe palette check for the choropleth.

**Acceptance criteria:**
- [ ] All interactions usable at 375 px width with touch only
- [ ] Lighthouse accessibility ≥ 95 on the main page
- [ ] Choropleth palette passes a deuteranopia simulation
- [ ] No horizontal scroll at any breakpoint

**Technologies:** Lighthouse, Chrome DevTools device emulation
**Alternatives:** BrowserStack for real-device coverage

---

## Epic 4 — Narrative & Release

### Issue 4.1 — Insights and methodology content
**Status: Planned**
**Branch:** `feat/docs/insights-content`
**Labels:** `docs` `blocked-by:3.7`
**Estimate:** 3–4 hrs

**Description:**
Write the landing narrative: 3–4 headline findings with numbers, the original big-data pipeline architecture diagram (Spark/Hive/Hadoop context), and a limitations section (LSOA harmonisation, City of London per-capita anomaly, IMD two-snapshot constraint, correlation vs causation).

**Acceptance criteria:**
- [ ] Headline insight appears above the fold, before any tooling mention
- [ ] Architecture diagram embedded (original pipeline + this rebuild)
- [ ] Limitations section covers the four named caveats
- [ ] All claims traceable to the dataset

**Technologies:** MDX or static page content, draw.io/Excalidraw
**Alternatives:** separate blog post linking to the app

---

### Issue 4.2 — Performance audit and release
**Status: Planned**
**Branch:** `chore/release/perf-v1`
**Labels:** `deploy` `qa` `blocked-by:3.8` `blocked-by:4.1`
**Estimate:** 2–3 hrs

**Description:**
Lighthouse performance pass (code-split the map bundle, lazy-load below-fold charts), final README with screenshots and live URL, tag `v1.0.0`, and close out the project board.

**Acceptance criteria:**
- [ ] Lighthouse performance ≥ 85 mobile, ≥ 95 desktop
- [ ] README includes screenshots, live URL, local setup, data licences
- [ ] `v1.0.0` tag and GitHub Release created
- [ ] All board issues closed or explicitly moved to a v2 milestone

**Technologies:** Lighthouse, GitHub Releases
**Alternatives:** Vercel Analytics for post-launch monitoring
