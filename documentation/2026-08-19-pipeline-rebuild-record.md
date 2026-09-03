# Record : Pipeline Review, Rebuild and Epic 1 Completion

**Record created:** 2026-08-19
**Work period:** 2026-08-12 to 2026-08-19
**Scope:** review of the existing codebase against `plan-revision-issues.md`, remediation of
the findings, and completion of Epic 1 (issues 1.4, 1.7, 1.8, 1.9, 1.10).
**Related records:** [`../codebase-review-issues.md`](../codebase-review-issues.md) :
the full findings register, including a Corrections block.

---

## 1. Summary

The pipeline was reviewed against the plan, three data-integrity faults were found in
committed outputs, the R pipeline was rewritten, and Epic 1 was carried to completion.
Epic 1 issues 1.1–1.10 now all meet their acceptance criteria. Nothing blocks Epic 2.

The review's headline finding was that **the committed processed outputs were wrong and
the committed scripts could not regenerate them**. That is no longer true of either.

---

## 2. Tasks handled

| # | Task | Date | Outcome |
|---|---|---|---|
| 1 | Codebase review against `plan-revision-issues.md` | 12 Aug | 12 issues, severity-ranked, each verified against the data rather than inferred from code |
| 2 | Remediation of P0/P1 findings | 12 Aug | Pipeline rewritten on `data.table`; Spark scripts retired to `pipeline/experimental/` |
| 3 | Re-download of 33 missing Metropolitan months | 13 Aug *(user)* | 368/368 monthly files present; 2.77 M records recovered |
| 4 | Issue 1.4 : acquire well-being and life expectancy | 16 Aug | Sources located, licences verified, `00_download_metrics.R` written and tested |
| 5 | Issues 1.7 / 1.8 : tidy both series | 16 Aug | `12_tidy_wellbeing.R`, `13_tidy_life_expectancy.R` |
| 6 | Issue 1.9 : unify and build the coverage matrix | 16 Aug | `20_unify_metrics.R` → `boroughs.json`, `coverage.json` |
| 7 | Issue 1.10 : borough boundaries | 19 Aug | `03_borough_boundaries.R` → `london.geojson` |

---

## 3. Faults found and fixed

Each was verified against the actual files before being reported, and each is now covered
by an assertion that fails the run.

| Fault | Evidence at the time | Fix |
|---|---|---|
| **33 Metropolitan monthly files missing (2012–2015)** | Met rows read 389k / 157k / 234k / 476k against ~1.1 M normal, while City of London : complete throughout : was flat. Camden's committed rate/1,000 read 229 → 72 → 27 → 41 → 85 | Files re-downloaded. `00_download.R` now checks **per force** and exits non-zero. A pooled check passed the whole time because one force was always complete |
| **Pre-2013 crime taxonomy unmapped** | `Violent crime` and `Public disorder and weapons` had no branch in `categorize_crime()`; 240,929 records fell into a silent `"No-category"` bucket and Violence read **0** for 2011–2012 | Both vocabularies mapped; an unmapped crime type now fails the run. No fallback bucket exists |
| **Four scripts, four different exclusion totals** | Coverage resolved to 98.73%, 99.64% or 92.5% depending on which script you believed | One exclusion ledger, asserted to sum to the raw total. Denominator defined in `SOURCES.md` |
| **QA assertion could not fail** | `difference <- aggregated_total - matched_rows` is a quantity minus itself; `dataQuality.log` recorded `Difference 0` on every run | QA now reconciles artefacts produced by *different* scripts. Verified against three real failure modes |
| **Silent 34th "borough"** | `left_join` left 36,574 records under an `NA` key in one output, silently dropped by the next | `NA` borough keys are rejected; unmatched records go to the ledger |
| **Raw data inside the pipeline's write path** | `data/raw/crime/` empty; the only copy of ~5.3 GB sat in `data/processed/crime/`, which `01_crime_by_LSOA.R` cleared with `mode = "overwrite"` | Data moved by the user. `_common.R` refuses to write into any directory holding raw crime CSVs |
| **Three scripts could not execute** | Undefined `tr()`, reuse of a closed Spark connection, an assignment swallowed into a comment, hardcoded Spark part-file UUIDs | Retired to `experimental/`; replaced by scripts that run |

---

## 4. Decisions and their effect on scope

**Who** records whether the decision was the project owner's or proposed during the work.

| Decision | Who | Rationale | Effect on scope |
|---|---|---|---|
| Rewrite on `data.table`, retire Spark to `experimental/` | Owner | The Spark scripts could not run and wrote 7.6 GB of intermediates to produce <700 KB of aggregates | **Narrowed.** No Java/Spark/derby on the critical path. Spark retained for the issue 4.1 architecture write-up |
| Owner moves the raw data personally | Owner | 5.3 GB with no backup; a bulk move over the device bridge was the riskier option | Neutral |
| Map pre-2013 crime categories into **one continuous series** | Owner | Simpler frontend; alternative was an era-flagged split | **Narrowed.** Category comparisons crossing April 2013 need a footnote. Recorded in `SOURCES.md`; every row carries a `vocabulary` column |
| Take well-being and life expectancy from **ONS directly**, not the London Datastore | Proposed | The Datastore life-expectancy copy stops at **2008-2010** and is **OGL v2** : older than the ward file it was meant to replace | **Widened quality.** Longer series, current releases, OGL v3.0 on both |
| Include life expectancy **at 65** as well as at birth | Proposed | Same table, no extra cost, more informative for later-life inequality | **Widened.** 4 LE metrics instead of 2 |
| `coverage.json` carries `direction`, `scale`, `cadence`, `year_rule`, `boroughs_missing` | Proposed | Issue 1.9 asked only for years. The data has 3 scales, 2 cadences, 3 year rules and 2 directions | **Widened.** Beyond plan, but without it the frontend infers that falling anxiety is bad |
| Year conventions differ **deliberately** by metric | Proposed | A financial year and a 3-year rolling window are different objects | Neutral. Financial years → start year; rolling periods → end year. Exposed per metric so pairings can be stated |
| **No boundary simplification**; ONS pre-generalised product + coordinate rounding | Proposed | `st_simplify` moves each outline independently and opens slivers between neighbours; `rmapshaper` needs V8 | **Narrowed deps.** Drops `rmapshaper`/V8, adds `sf` |
| City of London declared a **permitted absence**, per metric | Proposed | ONS suppresses all 48 well-being cells `[u]` and publishes no life expectancy for ~8,000 residents | Neutral. Two metrics cover 32 boroughs, declared rather than silently mismatched |
| Fixture-based smoke test | Proposed | `parse()` accepts `tr()`, a closed connection and a swallowed assignment. Only execution finds them | **Widened.** Not in the plan; it is the control that prevents the original failure recurring |

**Net effect: Epic 1 got deeper, not wider.** No new epics, no new deliverables beyond
`coverage.json`'s extra fields and the test harness. Epics 2–4 are untouched.

---

## 5. Corrections to the review itself

Two claims in the original review were wrong and are corrected in place in
`codebase-review-issues.md`. Both were asserted from reasoning rather than tested against
the real repository : the same failure the review criticises elsewhere.

| Claim | Reality |
|---|---|
| "`npm ci` in `web/` fails, so CI has never passed" | False. npm walks up to the workspace root and finds the lockfile. Verified: `npm ci` and `npm run lint` both exit 0 from `web/` |
| "`parse()` alone catches `tr()`, the swallowed assignment and the stray paren" | False. All three are syntactically valid R. Only execution finds them : hence the smoke test |

---

## 6. State of the project

| Epic | Status |
|---|---|
| 0 : Repository & infrastructure | Done except CI (`ci.yml` not yet updated) and Vercel (not started) |
| **1 : Data pipeline** | **Complete.** Issues 1.1–1.10 all meet their acceptance criteria |
| 2 : API layer | Not started, unblocked |
| 3 : Frontend | Scaffold only |
| 4 : Narrative & release | Not started |

### Verification status

| Component | Verified against |
|---|---|
| Crime path (00–02) | **Real data** : 368 files, 16.7 M records, on the owner's machine |
| Income, IMD (10, 11) | **Real data** |
| Well-being, life expectancy (12, 13) | **Real ONS files**, staged and run |
| Unify (20) | Real income/IMD/well-being/LE; **synthetic crime** |
| Boundaries (03) | **Synthetic fixtures only** : real ONS boundary file not yet downloaded |
| Smoke test | 12 scripts, synthetic fixtures, both guards fire |
| QA | 31 checks, all passing |

**Outstanding run.** Boundaries have not been produced from real ONS data, and the unified
export has not been regenerated with the real crime series. One pass closes both:

```bash
Rscript pipeline/00_download_metrics.R      # now also fetches boundaries
Rscript pipeline/12_tidy_wellbeing.R
Rscript pipeline/13_tidy_life_expectancy.R
Rscript pipeline/03_borough_boundaries.R
Rscript pipeline/20_unify_metrics.R
Rscript pipeline/QA/01_QA.R
```

---

## 7. Open items

| Item | Blocks | Note |
|---|---|---|
| `.github/workflows/ci.yml` still targets `v2` only and has no R job | Issue 0.3 | Cannot be written by remote tooling (protected path) : must be pasted manually |
| `_to_delete/` at the repository root | : | Holds the three retired scripts and 8 superseded outputs. Remote tooling cannot delete |
| `crime.csv` and `crime_by_borough.csv` (7.6 GB) in `data/processed/` | : | Retired Spark intermediates. QA warns while they exist |
| Empty `pipeline/dimension/` directory | : | Needs manual removal |
| `projects-plan.md` Status flags are stale | : | Still describe the pre-rewrite state |
| Well-being ends 2022-23, one year short of the analysis window | : | Declared in `coverage.json`; not carried forward or interpolated |
| Income and IMD licences still recorded as *assumed* | : | Flagged as open in `SOURCES.md`; verify on the dataset pages |

---

## 8. Principles now enforced by the code

These are the standing rules the rewrite encodes. They exist because each one failed at
least once in the original.

1. **Failures exit non-zero.** `warning()` does not set an exit status under `Rscript`, so a
   broken run looked green.
2. **No silent fallbacks.** An unmapped crime type, a metric with no registry entry, or an
   undeclared missing borough all fail the run. A default `higher_is_better` is wrong for
   eleven of nineteen metrics.
3. **Coverage is checked per force, not pooled.** Pooled checking is what hid 33 missing months.
4. **One owner per fact.** A single exclusion ledger; other scripts read it rather than
   recomputing it.
5. **QA compares artefacts from different scripts**, so a disagreement is real information.
6. **Boroughs are matched on GSS code, never on name.**
7. **Nothing downstream hardcodes a year range** : `_config.R` and `coverage.json` own them.
8. **Raw data is never written to by the pipeline**, and the write path asserts it.
