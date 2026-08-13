# Plan Revision — New & Amended Issues

**Context:** The metric sources have mismatched time coverage and geographies, and the
objective is reframed from "quality of life" to "social determinants associated with
crime" (association, not causation). This revision: replaces issue 1.4 with per-source
tidy issues, adds a data-swap acquisition issue, adds a coverage-matrix artefact,
amends the API and frontend issues to consume it, and records the analysis-window
decision.

**Issue bookkeeping:** Old 1.4 is superseded by 1.4–1.8 below. Old 1.5 (unified export)
is superseded by 1.9. Old 1.6 (boundaries) is unchanged — renumber to 1.10 or keep as-is
on the board. Issues 2.1, 3.4, 3.6 are amended in place (new criteria appended).

---

### Issue 1.4 — Acquire annual well-being and life-expectancy series (data swap)
**Branch:** `feat/data/annual-wellbeing-lifeexp`
**Labels:** `data` `no-dependencies`
**Estimate:** 1–2 hrs

**Description:**
Replace the ward well-being bundle (2009–2013 only) as the source for well-being and
life expectancy. Download the ONS borough-level annual personal well-being (ONS4)
series (2011/12 onwards) and the borough life-expectancy rolling annual series from
the London Datastore. Verify and record each dataset's licence in `SOURCES.md`
(resolving the current "licence: not documented" entry). The ward file is retained
only for optional secondary features (unemployment, child poverty, GCSE, PTAL,
greenspace), clearly marked as 2011–2013 coverage.

**Acceptance criteria:**
- [ ] ONS4 borough annual well-being file in `data/raw/` with its own `SOURCES.md` entry
- [ ] Borough life-expectancy annual series in `data/raw/` with its own `SOURCES.md` entry
- [ ] Licence verified (not assumed) and recorded for both
- [ ] Well-being and life expectancy no longer share a single source URL in `SOURCES.md`

**Technologies:** London Datastore, ONS
**Alternatives:** keep ward file only (3-year overlap; materially weaker analysis)

---

### Issue 1.5 — Tidy income to long format
**Branch:** `feat/data/tidy-income`
**Labels:** `data` `blocked-by:1.1`
**Estimate:** 2–3 hrs

**Description:**
`pipeline/10_tidy_income.R`: melt the HMRC wide file (`Number_of_Individuals_YYYY`,
`Mean_YYYY`, `Median_YYYY`) to the common long schema. Keep **median** as the analysis
metric (income is right-skewed; means are dragged by high earners); retain mean and
taxpayer counts as supplementary rows. Drop artefact columns (`NA_2023`, blank headers).
Note the missing 2008 survey year rather than interpolating it.

**Acceptance criteria:**
- [ ] Output matches schema `borough_gss, borough_name, year, metric, value, source, geography_native, notes`
- [ ] Median income present for every borough-year available in source, ≥ 2011
- [ ] Artefact columns dropped and logged; 2008 gap documented, not filled
- [ ] All borough names resolved to GSS codes with zero mismatches

**Technologies:** R (data.table `melt`, regex year extraction)
**Alternatives:** tidyr `pivot_longer`

---

### Issue 1.6 — Tidy IMD scores to long format
**Branch:** `feat/data/tidy-imd`
**Labels:** `data` `blocked-by:1.1`
**Estimate:** 2–3 hrs

**Description:**
`pipeline/11_tidy_imd.R`: extract only `*_Average_score_2015/2019` columns for the
Income, Employment, Education, Health, Barriers-to-Housing and Living-Environment
domains. **Exclude the IMD Crime domain from analysis outputs** (circular with the
outcome variable) — emit it to a separate validation-only file. Exclude all `Rank_*`
and `Proportion_of_LSOAs_*` columns from analysis; optionally retain ranks in a
display-copy file.

**Acceptance criteria:**
- [ ] Six domain scores × 2 snapshot years × 33 boroughs in the common schema
- [ ] IMD Crime domain absent from the analysis output; present only in `imd_crime_validation.csv`
- [ ] Exclusion rationale (circularity) recorded in `SOURCES.md` decisions section
- [ ] 2015 vs 2019 methodology non-comparability of ranks noted

**Technologies:** R (data.table)
**Alternatives:** none material

---

### Issue 1.7 — Tidy well-being to long format
**Branch:** `feat/data/tidy-wellbeing`
**Labels:** `data` `blocked-by:1.4`
**Estimate:** 2–3 hrs

**Description:**
`pipeline/12_tidy_wellbeing.R`: tidy the ONS4 borough annual series (life satisfaction,
worthwhile, happiness, anxiety) to the common schema. If any secondary ward-file
features are retained, aggregate ward→borough with a **population-weighted** mean and
recover the true labels of the export-mangled `Subjective_well_being_..._1/_2/_3`
columns from the source workbook before use.

**Acceptance criteria:**
- [ ] Four ONS4 measures per borough-year from 2011/12 onward in the common schema
- [ ] Anxiety direction documented (higher = worse, unlike the other three)
- [ ] Any ward-derived feature is population-weighted, never a plain mean
- [ ] No column with a recovered/ambiguous label enters output without its verified name

**Technologies:** R (data.table), ONS4
**Alternatives:** drop ward secondaries entirely (simpler; less feature breadth)

---

### Issue 1.8 — Tidy life expectancy to long format
**Branch:** `feat/data/tidy-lifeexp`
**Labels:** `data` `blocked-by:1.4`
**Estimate:** 1–2 hrs

**Description:**
`pipeline/13_tidy_life_expectancy.R`: tidy the borough life-expectancy series (male
and female as separate metrics) to the common schema. Rolling multi-year periods
(e.g. 2018–2020) are assigned to their **end year** with the full period preserved
in `notes`.

**Acceptance criteria:**
- [ ] Male and female series per borough-year in the common schema
- [ ] Period→year assignment rule applied consistently and documented
- [ ] Coverage span reported in the run log

**Technologies:** R (data.table)
**Alternatives:** midpoint-year assignment (document whichever is chosen)

---

### Issue 1.9 — Unify metrics, crime, and coverage matrix
**Branch:** `feat/data/unify-coverage`
**Labels:** `data` `blocked-by:1.3` `blocked-by:1.5` `blocked-by:1.6` `blocked-by:1.7` `blocked-by:1.8`
**Estimate:** 3–4 hrs

**Description:**
`pipeline/20_unify_metrics.R`: bind all tidied sources plus borough crime rates into
one long table; export `data/processed/boroughs.json`. Enforce the analysis-window
decision: cross-metric analysis 2011–2023; crime-rate trend to 2024 (population
denominator limit); 2025+ counts flagged `partial`; 2026 (4 months) never presented
as a full year. Also emit `data/processed/coverage.json`: for every metric, the years
with data. Validation asserts 33 boroughs, value ranges, and schema conformance.

**Acceptance criteria:**
- [ ] `boroughs.json` in the common schema; validation script passes
- [ ] `coverage.json` lists available years per metric and drives no hardcoded year lists downstream
- [ ] Window rules (2011–2023 analysis / 2024 trend / partial flags) implemented and documented in `SOURCES.md`
- [ ] Combined export < 1 MB

**Technologies:** R (data.table, jsonlite)
**Alternatives:** per-metric JSON files (more requests, simpler diffs)

---

### Issue 2.1 (amendment) — Metrics API serves coverage metadata
**Branch:** `feat/api/meta-coverage`
**Labels:** `backend` `blocked-by:1.9`
**Estimate:** +1 hr on original

**Additional acceptance criteria:**
- [ ] `GET /api/meta` (or `/api/metrics?meta=true`) returns the coverage matrix
- [ ] Responses include `partial` flags so clients cannot mistake 4-month 2026 for a year

---

### Issue 3.4 (amendment) — Year slider driven by coverage matrix
**Additional acceptance criteria:**
- [ ] Slider range and enabled years come from `/api/meta` per selected metric — no hardcoded ranges
- [ ] Snapshot metrics (IMD) render as discrete selectable points, not a continuous slider
- [ ] Partial years visually marked and excluded from year-on-year comparisons

---

### Issue 3.6 (amendment) — Scatterplot year-pairing rule
**Additional acceptance criteria:**
- [ ] Mismatched series pair on nearest-available-year; the pairing is printed on the chart (e.g. "crime 2019 × IMD 2019")
- [ ] No silent interpolation anywhere
- [ ] Axis/legend language is associative ("crime rate vs median income"), never causal

---

### Issue 4.1 (amendment) — Narrative reflects associative framing
**Additional acceptance criteria:**
- [ ] All copy states associations; a limitations note covers ecological inference (33 aggregated units) and the IMD-crime-domain exclusion
- [ ] The 15-year window and the 2011–2023 analysis window are stated explicitly
