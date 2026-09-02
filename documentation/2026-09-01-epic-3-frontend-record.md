# Epic 3 — the frontend, 3.2 to 3.8

**Date:** 2026-09-01
**Covers:** issues 3.2 (choropleth), 3.3 (metric controls), 3.4 (year control),
3.5 (borough detail), 3.6 (scatterplot), 3.7 (KPI panel), 3.8 (cross-device and
accessibility pass).
**Status at the end:** Epic 3 complete. 139 unit tests, 82 browser checks, all
passing against a real production build.

**Amended 2026-09-01,** after the first CI run failed on a test that passed
locally. The finding is in §4; the body above it is unchanged.

This is a record, not a living document. Current state is in
[`../projects-plan.md`](../projects-plan.md) and [`../README.md`](../README.md).

---

## 1. What was built

One route, `/`, rendering a dashboard whose entire state is in the query string:

```
/?metric=crime_rate_per_1000&year=2023&compare=income_median&exclude=E09000001&borough=E09000007
```

| Component | Issue | File |
|---|---|---|
| Choropleth with pan/zoom, hatched no-data, legend | 3.2 | `components/dashboard/choropleth.tsx`, `legend.tsx` |
| Borough table — the keyboard and screen-reader path | 3.2, 3.5 | `borough-table.tsx` |
| Metric switcher, borough exclusions | 3.3 | `metric-controls.tsx` |
| Year slider / snapshot radios, per metric | 3.4 | `year-control.tsx` |
| Borough detail, all 19 metrics with ranks | 3.5 | `borough-detail.tsx` |
| Crime-vs-determinant scatter, OLS + Pearson r | 3.6 | `scatterplot.tsx` |
| Summary strip | 3.7 | `kpi-panel.tsx` |

The pure layer under them is separately testable and has no React in it:
`lib/projection.ts` (Web Mercator, fitting, path building), `lib/series.ts` (the
compact index and year pairing), `lib/scales.ts` (colour classing, axis ticks),
`lib/stats.ts` (rank, OLS, Pearson r, long-run change), `lib/format.ts`,
`lib/url-state.ts`.

---

## 2. The three decisions worth defending

### 2.1 No mapping or charting library

The plan named MapLibre GL + react-map-gl for 3.2 and visx or D3 for 3.6. Neither
was used. The reasoning:

- The map is **33 static polygons with no basemap**. MapLibre's value is tiles,
  labels and a style pipeline. Using it means either a third-party tile endpoint —
  a network dependency, an API key and an attribution obligation this project does
  not otherwise carry — or a style with no basemap, which is ~900 KB of WebGL to
  fill polygons.
- MapLibre renders into a **canvas**, which is one opaque node to a screen reader
  and to axe, and which Playwright cannot assert on without pixel diffing. Issue
  3.8 asks for a *measured* accessibility pass. An SVG the tests can read is worth
  more than a GPU this does not need.
- The chart is 33 points, one line and two axes. A chart library's value is in the
  parts not needed here.

**What this cost.** No basemap and no street detail; pan and zoom operate on the
SVG viewBox, so a reader cannot zoom in to see roads. For a borough choropleth
that is right; for a point map of individual crimes it would not be. It also cost
about 400 lines of code that had to be written and tested — against roughly 250 KB
of library that would have arrived tested. The trade was taken because the
accessibility and testability arguments are specific to this project's stated
goals, not because dependencies are bad.

**What it bought,** beyond the bundle: every acceptance criterion in 3.2–3.8 is
assertable from the DOM, which is why the deuteranopia check and the ramp-direction
check exist at all.

### 2.2 Darker means worse, for every metric

`direction` is in the coverage matrix precisely because it cannot be inferred from
values. The map uses it to keep one reading convention: a `higher_is_worse` metric
runs light→dark with value and a `higher_is_better` metric runs dark→light, so
crime and median income can be compared without reversing the ramp mentally
between them. `neutral` metrics (taxpayer count) make no better/worse claim and
say so in the legend.

The same rule propagates: extremes are labelled "the worse end" rather than
"highest"; trend arrows read improvement from `direction`, not from the sign of
the delta; ranks put the worst borough at 1 for every directional metric.

### 2.3 Quantile classes, except where they would lie

Sequential metrics use quantile breaks. City of London's crime rate is 671 per
1,000 against a median of 113; seven equal intervals put 32 boroughs in the lowest
class and produce a map of one dot. The cost — quantiles flatten magnitude — is
paid back by printing the real break values in the legend and by 3.3's exclusion
control, which exists so a reader can drop the outlier and re-class the rest.

Diverging metrics do **not** use quantiles. The IMD health domain runs −1.4 to
+0.4, whose observed midpoint is −0.5. A quantile break would put the neutral
colour there, destroying the one thing a diverging ramp is for. Standardised
metrics get an equal-interval scale symmetric about zero.

---

## 3. What the epic found

Two things, neither of which anything upstream had caught. Both are recorded in
the roadmap rather than quietly patched.

### 3.1 Two IMD domains carry no borough-level variance — plan issue 1.11

| Metric | Scale | Distinct values across 33 boroughs (2019) |
|---|---|---|
| `imd_employment_score` | proportion | **1** — every borough is 0.1 |
| `imd_income_score` | proportion | **2** — 0.1 and 0.2 |
| `imd_education_skills_and_training_score` | score | 29 |
| `imd_health_deprivation_and_disability_score` | standardised | 17 |
| `imd_living_environment_score` | score | 31 |
| `imd_barriers_to_housing_and_services_score` | score | 31 |

MHCLG publishes the income and employment domain averages as proportions in the
0–1 range. At one decimal place that is a resolution of ten percentage points, and
every London borough lands in the same bucket. The four `score` and `standardised`
domains are unaffected.

`pipeline/11_tidy_imd.R` contains no rounding — it reads the
`"<Domain> - Average score"` columns verbatim — so this is either the source file's
own precision or a precision loss at acquisition. **It could not be resolved during
this work because `data/raw/` has been cleared**, which is itself a finding: the
recipe in `SOURCES.md` is now the only route back to the answer.

These nineteen metrics passed 31 QA checks and 139 unit tests without anyone
noticing that two of them are constants. Drawing them on a map took a second.
That is the argument for building the visualisation, and it is why issue 1.11 asks
for a distinct-value assertion in `20_unify_metrics.R` and in QA rather than only
a re-acquisition.

**What the frontend does in the meantime,** rather than hiding it: the map draws
one flat mid-tone with a caption saying every borough has the same value and that
this is the source's precision, not a rendering fault; the summary cards refuse to
name a "highest" and a "lowest" that are the same borough; the scatterplot
declines to fit a line.

### 3.2 A 516 KB client-bundle regression inherited from 3.1

`components/site-header.tsx` is a client component. It imported `lib/site.ts` for
the nav labels; `lib/site.ts` imported `lib/data.ts` for the borough count; and
`lib/data.ts` imports the 516 KB observation export. Turbopack could not drop the
JSON, because the module derives `observations` and `allYears` from it at module
scope.

**Every visitor downloaded all 6,001 observations in order to render the word
"Dashboard" in the header.** Confirmed by grepping the built chunk for
`{"borough_gss":…}` and finding exactly 6,001.

Fixed by splitting: `lib/coverage.ts` holds the 9 KB matrix and is client-safe;
`lib/data.ts` holds the bulk export behind a `server-only` marker. Client
JavaScript fell from 1.2 MB to 716 KB. The marker is the enforcement — a repeat is
now a build failure naming the cause rather than a silent half megabyte.

This is the same class of fault as the `node:fs` import that 3.1 found, and the
same fix. Type-check and lint accept both.

---

## 4. Faults found in the work itself

Recorded because each was caught by a check that could have been written to pass.

**A fit built out of rounding error.** The zero-variance guard in `fitLine` was
`sxx === 0`, which is not the test that holds. Thirty-three identical values of 0.1
have a mean of 0.10000000000000002, so each deviation is about −1.4e-17 and the sum
of squared deviations is ~5.8e-34 — non-zero, and enough to return a slope and a
correlation coefficient made entirely of floating-point residue.
`imd_employment_score` is exactly this case, so the scatterplot would have shown a
fitted line and an *r* with no data behind either. The guard is now a
magnitude-relative tolerance; real data clears it by twenty-odd orders of magnitude.

**A weak test that passed on a broken build.** `darkIsHigh()` was deliberately
changed to ignore `direction`. The unit test caught it. The browser test did
**not** — it was reading the legend's caption, which is generated by a different
code path from the fills, so a build that ignored `direction` entirely still
printed "Darker means lower" over a ramp running the other way. The browser test
now reads the painted colours and asserts the lightness ordering flips between a
`higher_is_worse` and a `higher_is_better` metric.

This is the more useful of the two findings. A test that asserts on the caption
next to a thing, rather than the thing, is the standard way an acceptance criterion
gets marked done without being met.

**A long-run change across two IMD snapshots.** `longRunChange` computed
2015 → 2019 for the deprivation domains. The pipeline already drops the 2015 and
2019 *ranks* as non-comparable; presenting a change between their scores as a trend
put a number on something the source does not support. It now refuses for any
`snapshot` metric.

**A fitted line drawn outside the plot**, over the axis labels, implying values the
chart was not showing. Clipped.

**A swallowed storage failure, found by CI and not by any local run.** The first CI run failed
on the theme test at the assertion after a reload, having passed the equivalent assertion three
lines earlier. It did not reproduce locally in 100+ attempts, including under CI conditions with
four workers, so it was diagnosed from the failure's *shape*: only one code path can pass line
114 and fail line 117. `ThemeToggle` set `document.documentElement.dataset.theme` and then wrote
to `localStorage` inside a `catch {}` that discarded any error. A refused write left the
attribute set — so the click looked successful — and the choice absent on the next load, three
assertions away, with nothing connecting the two.

Reproduced deliberately by making `setItem` throw, which produced the identical signature. The
write failure is now recorded as `data-theme-persisted="false"` on the document, the test
asserts persistence where it happens, and the degradation contract has its own test.

The general lesson is the same one as the ramp-caption test: **a `catch` that discards the error
and a test that asserts on a proxy both convert a specific failure into a vague one.** Neither
is visible while things work.

**A missing `suppressHydrationWarning`** on `<html>`, present since 3.1. The inline theme script
deliberately puts an attribute on `<html>` that the server did not render; without the
attribute, React 19 can treat that as a mismatch and re-render on the client, dropping it.

**`buildSeries` running on every request.** The dashboard route is dynamic because it reads
`searchParams`, so its body executes per request — including a walk over all 6,001 observations
building a structure that cannot change while the process lives. Memoised.

**A one-slot geometry cache** that any caller alternating between two borough lists
would thrash, paying for a 6,587-vertex projection on every call while appearing to
work. Now keyed.

**Two live regions announcing the same value.** The year readout was an `<output>`,
which carries an implicit `role="status"`, on top of the slider's own
`aria-valuetext`. Found because a test could not disambiguate the two, which is a
reasonable proxy for a screen reader having the same problem.

---

## 5. How it is verified

| Suite | Covers | Count |
|---|---|---|
| `tests/api.test.ts` | the three routes, called directly | 28 |
| `tests/projection.test.ts` | Web Mercator against the closed form | 17 |
| `tests/geo.test.ts` | the real GeoJSON; join by GSS code; geography sanity | 11 |
| `tests/series.test.ts` | compact index, nearest-year pairing, absence reasons | 19 |
| `tests/scales.test.ts` | direction, quantile vs diverging, degenerate domains | 19 |
| `tests/stats.test.ts` | rank denominators, OLS and *r*, partial years | 26 |
| `tests/url-state.test.ts` | parse, fall back, round-trip every metric | 19 |
| `e2e/shell.spec.ts` | shell + axe at three widths on every route; theme persistence | 29 |
| `e2e/dashboard.spec.ts` | every 3.2–3.8 criterion, axe, CVD simulation | 53 |

Points worth naming:

- **The projection is checked against the closed form**, with expected values
  computed independently rather than by the code under test.
- **The GSS-code join test reorders the borough list.** The pipeline currently
  writes `london.geojson` and `boroughs.json` in the same order, so a positional
  join passes by luck today and would silently draw every borough with a
  neighbour's outline the moment either file is re-sorted.
- **Pearson r is checked against Anscombe's first quartet** (r = 0.816), a value
  published in every statistics text, so the test checks the formula rather than
  itself.
- **Deuteranopia** is simulated with the Machado et al. (2009) matrix at severity
  1.0, applied in *linear* RGB. The usual shortcut of applying it to gamma-encoded
  sRGB exaggerates separation in the shadows, which is the region a sequential ramp
  depends on. The property tested is the one that makes a sequential ramp CVD-safe:
  lightness ordering must survive the simulation.
- **Touch targets are measured on the effective target.** WCAG 2.5.5 measures the
  label for a checkbox wrapped in one, so the test resolves to it — but a checkbox
  with *no* wrapping label is still reported as a 16 px target rather than skipped.

Screenshots at 375 / 768 / 1280 px in both themes, plus the diverging, 32-borough,
excluded-outlier, detail-panel and no-variance states, are in
[`screenshots/`](screenshots/).

---

## 6. Known limitations of what was built

- **No basemap.** Consequence of 2.1 above. Pan and zoom work; there is nothing
  underneath to orient against beyond borough outlines and the Thames.
- **The back button does not step through dashboard state.** State is mirrored with
  `history.replaceState` to avoid re-running the route on every slider tick. The URL
  is always shareable and always correct; it is just not a history stack.
- **The scatterplot has no per-point accessible equivalent.** The map has the
  borough table; the scatter has its `aria-label` summary (n, *r*, the pairing) and
  the exact values in the table and detail panel, but there is no equivalent of
  "which point is where". Its unique content is the *relationship*, which the *r*
  and the fit describe in words.
- **The KPI "borough mean" is unweighted.** Deliberate and labelled — a
  population-weighted London mean is a different quantity — but it is not the
  London-wide rate a reader might assume.
- **Ranks are competition-ranked and can be near-meaningless on coarse data.**
  `imd_employment_score` reads "1st of 33 (tied with 32)". Honest, but the tie count
  is doing all the work.
- **The dashboard route is dynamic**, because it reads `searchParams`. The data is
  still bundled at build time, so there is no per-request I/O beyond the parse, but
  it is not statically prerendered and Vercel will bill it accordingly (issue 0.4).
