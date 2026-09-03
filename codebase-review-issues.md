# Codebase Review : Issues & Fixes

**Reviewed:** `London_crime_project` @ 2026-08-12, against `plan-revision-issues.md` and `projects-plan.md`.
**Method:** read every R script, CI config, `.gitignore`, and both READMEs; then verified claims
against the actual data files (LSOA lookup, processed outputs, raw crime samples, QA logs).
Every numeric claim below was computed from the files in the repo, not inferred from code.

**Headline:** the plan is sound and the framing discipline (associative language, IMD-crime
exclusion, window limits) is genuinely good. The problem is that **the committed processed outputs
are wrong, and the committed scripts cannot regenerate them.** Building issues 1.5–1.9 on top of the
current `data/processed/` outputs would propagate three separate data-integrity faults into
`boroughs.json` and the frontend.

---

> **Corrections (2026-08-12, after implementation).** Two claims in the original review were wrong
> and are corrected in place below, marked **CORRECTED**. Both were asserted from reasoning rather
> than from a test against the real repo layout : the same failure the review criticises elsewhere.
>
> 1. **P2-1 : "`npm ci` in `web/` fails, so CI has never passed."** False. npm walks up from
>    `web/` and finds the workspace root's `package-lock.json`, so `npm ci` succeeds and hoists
>    into the root `node_modules/`. Verified against the real tree: `npm ci` and `npm run lint`
>    both exit 0 from `web/`. The original isolated test was invalid because it had no root
>    `package.json` declaring workspaces.
> 2. **P1-2 : "`parse()` alone catches `tr()`, the swallowed assignment, and the stray paren."**
>    False. All three are syntactically valid R; `parse()` accepts every retired script. Only
>    execution finds them, which is why the fix is a fixture-based smoke test, not a parse check.

Severity key: **P0** blocks correctness of any published number · **P1** blocks reproducibility or a
plan acceptance criterion · **P2** hygiene / risk.

---

## P0 : Data integrity

### P0-1. 33 of 184 Metropolitan monthly files are missing; borough rates for 2012–2015 are fiction

`pipeline/logs/rowcounts.log` records Metropolitan rows per year:

| Year | Met rows | Met files present | City rows | City files |
|---|---|---|---|---|
| 2011 | 1,223,176 | 12 | 8,458 | 12 |
| 2012 | 388,846 | 4 (01,03,05,10) | 7,522 | 12 |
| 2013 | 156,704 | 2 (02,03) | 6,613 | 12 |
| 2014 | 233,706 | 3 (03,08,12) | 6,511 | 12 |
| 2015 | 475,956 | 6 (02,04–08) | 6,185 | 12 |
| 2016+ | ~1.0–1.15 M | 12 | ~6–10 k | 12 |

Met rows per *present* file are stable at ~78–102 k across all years. The apparent 87% collapse in
2013 is missing files, not falling crime. City of London : complete for every month : shows no such
dip, which confirms it.

This is already baked into a committed output. `data/processed/crime_type_by_year_and_population.csv`,
Camden, `crime_rate_per_1000`:

```
2011  229.00   2012  72.23   2013  27.14   2014  40.86   2015  85.38   2016  175.37
```

Those numbers are not crime rates. Anything downstream : choropleth, year slider, scatterplot,
KPI "10-year change" (issue 3.7) : would be reporting file availability.

**Why it wasn't caught:** `pipeline/00_download.R` is the guard that exists precisely for this, and
it works. It just never sees the data : see P1-1.

**Fix**
1. Re-download the missing Met months from the data.police.uk archive (2012: 02,04,06–09,11,12;
   2013: 01,04–12; 2014: 01,02,04–07,09–11; 2015: 01,03,09–12).
2. Make `00_download.R` **fail the run**, not `warning()`, on any missing month : `warning()` does
   not set a non-zero exit status under `Rscript`, so a broken pipeline still looks green:
   ```r
   if (length(missing)) stop("Missing ", length(missing), " months: ", paste(missing, collapse = ", "))
   ```
3. Check coverage **per force**, not pooled. Pooled coverage passes as long as City of London has
   the month, which is exactly how 33 Met gaps stayed invisible.
4. If a month is genuinely unavailable upstream, record it in `SOURCES.md` and emit that
   borough-year as `NA` with a `partial` flag : never as a complete year. This is the same rule
   issue 1.9 already applies to 2025+; it needs to apply backwards too.

---

### P0-2. Crime-category rollup drops the pre-2013 taxonomy; "violence" is empty for 2011–2012

`categorize_crime()` in `pipeline/dimension/01_crime_by_LSOA.R` maps only the post-2013 category
vocabulary. The 2011–2013 files use the legacy one. Verified directly from the raw files:

- `2011-01`: `Anti-social behaviour, Burglary, Other crime, Robbery, Vehicle crime, Violent crime`
- `2013-02`: adds `Criminal damage and arson, Drugs, Other theft, Public disorder and weapons, Shoplifting`
- `2024-06`: `… Possession of weapons, Public order, Theft from the person, Violence and sexual offences`

`Violent crime` and `Public disorder and weapons` appear in **no** branch of the function, so they
fall through to `"No-category"`. The committed `crime_counts_by_crime_type.csv` shows the damage:

| Year | Violence and Sexual Offences | No-category | Other Crimes |
|---|---|---|---|
| 2011 | **0** | 159,369 | 297,619 |
| 2012 | **0** | 57,158 | 16,648 |
| 2013 | 419 | 24,402 | 3,285 |
| 2014 | 42,270 | 0 | 2,059 |
| 2019 | 226,413 | 0 | 10,183 |

The project's most analytically important category reads as zero for its first two years. Note also
`Other Crimes` at 297,619 in 2011 vs ~10,000 later : in 2011 `Other crime` was a genuine catch-all,
so even a corrected mapping leaves the category series **not comparable across the 2013 boundary**.

**Fix**
1. Add the legacy names to the mapping : `Violent crime` → Violence and Sexual Offences;
   `Public disorder and weapons` → split is impossible, so map to Public Order Offences and document
   that it also contains weapons offences pre-2013.
2. Replace the `if/else` chain + `sapply` with a lookup table joined on crime type. It vectorises,
   and more importantly it lets you assert completeness:
   ```r
   stopifnot(!any(out$Crime_Category == "No-category"))
   ```
   A `"No-category"` fallback that silently absorbs 159k records is the actual bug; the missing
   mapping is just what triggered it.
3. Add a documented `taxonomy_era` column (`legacy` ≤ 2013-03, `current` ≥ 2013-04) and either
   restrict category analysis to the current era or present the two eras as separate series. Add
   this as an acceptance criterion on issue 1.9 : the plan does not currently mention it.

---

### P0-3. Four scripts produce four different totals for the same quantity, and none reconcile

| Source | Claim |
|---|---|
| `logs/rowcounts.log` | 13,928,409 raw rows |
| `logs/dataQuality.log` (`QA/01_QA.R`) | 1,049,757 unmatched (**7.5%**), 12,963,297 matched |
| `logs/lsoa_lookup.log` (`00_LSAOlookup.R`) | 127,122 blank + 35,111 outside-London = 162,233 excluded → 13,766,176 |
| `crime_counts_by_year.csv` (`01_crime_by_LSOA.R`) | 13,788,645 counted → 139,764 excluded |
| Same file, borough-attributed only | 13,752,071 (excludes a null-borough bucket, below) |

Three different exclusion counts for one join. Issue 1.2's criterion : "lookup covers ≥99.5% of LSOA
codes appearing in the crime data" : resolves to **98.73%** (fails) on records, **99.64%** (passes)
on non-blank records, or **92.5%** (fails badly) per the QA log. It is currently marked **Done**.

Two contributing defects:

**(a) `QA/01_QA.R` joins on the wrong key.** Line 41 joins `LSOA_code` → `LSOA21CD` only. Most crime
records before 2021 carry 2011-vintage codes, which is the entire 7.5% "unmatched". Join against
`data/processed/lsoa_lookup.csv` (already vintage-harmonised) instead of re-deriving a single-vintage
join.

**(b) The QA assertion is a tautology and can never fail.**
```r
crime_summary   <- crime_joined |> filter(!is.na(LAD22CD)) |> group_by(...) |> summarise(count = n())
aggregated_total <- crime_summary |> summarise(total = sum(count))
difference       <- aggregated_total - matched_rows      # always 0 by construction
stopifnot(abs(difference_pct) <= 0.5)                    # therefore always passes
```
Summing group counts over the same filtered set returns the size of that set. `dataQuality.log`
duly records `Difference 0, Difference (%) 0`. The check has never tested anything. It also groups
by `Borough = LSOA_name`, which is the LSOA name, not the borough.

**Fix**
- Make the QA reconcile *independent* quantities: raw rows == borough-attributed + blank +
  outside-London + unmatched, asserted to equal zero residual; and separately assert
  `borough_attributed / (raw - blank) >= 0.995`.
- Pin the coverage denominator in `SOURCES.md` and restate issue 1.2's criterion unambiguously :
  "≥99.5% of records carrying a non-blank LSOA code". Then re-verify before leaving it as Done.
- Have one script own the exclusion accounting and have the others read its log, rather than three
  scripts each recomputing it.

---

### P0-4. A silent 34th "borough" of 36,574 records

`01_crime_by_LSOA.R` uses `left_join` to attach boroughs, so unmatched records survive with
`lad_nm = NA`. `crime_counts_by_year.csv` has 544 rows = **34** groups × 16 years: the 33 boroughs
plus a `NA` group totalling 36,574 records. Those records are then dropped without trace by the
population join in `01_LSOA_by_population.R` (462 rows = 33 × 14).

So one committed output over-counts London, the next silently under-counts it, and nothing logs the
difference. Issue 1.9's "validation asserts 33 boroughs" would pass on the second file while the
first is wrong.

**Fix:** after the join, split explicitly : write unmatched records to the exclusions log with
counts, then `stopifnot(uniqueN(lad_nm) == 33L, !anyNA(lad_nm))` before aggregating. Never let an
`NA` grouping key reach an output file.

---

## P1 : Reproducibility

### P1-1. The raw crime data is not where any script looks for it : and one script would delete it

`data/raw/crime/` is **empty**. The 368 monthly CSVs live in `data/processed/crime/`.

Every canonical script reads `data/raw/crime`: `00_download.R:2`, `00_LSAOlookup.R:9`,
`00_crime_rowcounts.R:9`, `01_crime_by_LSOA.R:22`, `QA/01_QA.R:8`. Run today, `00_download.R`
reports 184 missing months and `00_LSAOlookup.R` fails on an empty `rbindlist`. The documented
quickstart in `README.md` cannot work from a clean checkout.

Worse : `01_crime_by_LSOA.R:79-84`:
```r
spark_write_csv(single_file_tbl, path = str_c(getwd(), "/data/processed/crime"), mode = "overwrite")
```
`mode = "overwrite"` clears the target directory. That target is the only copy of ~5.3 GB of raw
crime CSVs. **Running the pipeline as documented destroys the source data.** `data/processed/crime/`
is also gitignored, so there is no recovery from the repo.

**Fix (do this before running anything else)**
1. Move the monthlies back: `data/processed/crime/*-street.csv` → `data/raw/crime/`.
2. Change the Spark write target to `data/processed/crime_hashed/` (or drop the intermediate
   entirely : see P1-3).
3. Add a guard at the top of any script that writes with `overwrite`:
   ```r
   stopifnot(!any(grepl("-street\\.csv$", list.files(out_path))))
   ```
4. Take a backup copy off the machine first. The 2012–2015 gap already shows this data is not
   trivially re-acquirable.

---

### P1-2. Several scripts cannot run at all

Concrete, unambiguous failures:

| File | Line | Defect |
|---|---|---|
| `00_QOL_tidy.R` | 1 | `install.packages(zoo)` : unquoted symbol. `Error: object 'zoo' not found` on line 1. Should be `install.packages("zoo")`, and package installation does not belong in a pipeline script at all. |
| `01_crime_by_LSOA.R` | 91→96 | `spark_disconnect(sc)` then `spark_read_csv(sc, ...)` on the closed connection. |
| `01_crime_by_LSOA.R` | 87, 126 | Hardcoded Spark part-file UUIDs (`part-00000-03c7566d-…`). Spark generates a fresh UUID every run, so the `file.rename` always fails on the second run. Use `list.files(dir, pattern = "^part-.*\\.csv$", full.names = TRUE)[1]`. |
| `01_crime_by_LSOA.R` | 133 | Misplaced paren: `str_c(getwd(), "/data/processed/crime_by_borough.csv", colClasses = "character")` : `colClasses` is consumed by `str_c`, producing the path `…/crime_by_borough.csvcharacter`. |
| `01_crime_by_LSOA.R` | 31 | `file.path(crime_files, "*-street\\.csv")` : Spark takes a **glob**, not a regex. `\\.` is a literal backslash and matches nothing. Same at `QA/01_QA.R:13`. |
| `01_LSOA_by_population.R` | 80 | The assignment was swallowed into a comment: `# Create new column Rate_per_1000crime_type_by_year_and_population <- …`. The `rename()` below is a dangling expression. |
| `01_LSOA_by_population.R` | 91 | `\|> tr(population = …)` : `tr` is not a function. |
| `01_LSOA_by_population.R` | 73→78 | `spark_disconnect(sc)` then reuse of `sc`. |
| `01_LSOA_by_population.R` | 97 | Uses `crime_with_borough`, which is never defined in this script. |
| `01_LSOA_by_population.R` | 54 | `tidyr::pivot_longer()` on a `tbl_spark` : not supported; needs `collect()` first. |
| `QA/01_QA.R` | 4 | `spark_install()` on every run. |
| `QA/01_QA.R` | 123-125 | `log_dir` written without `dir.create()`. |

`00_QOL_tidy.R` also cannot produce its own committed output. It sets `colnames(...) <- df[1, ]`
(line 59) and *then* extracts years from `names(...)` (line 62) : which by that point hold
`Code, Area, Number of Individuals, Mean £, …` with no year tokens. `years` is all-`NA`,
`na.locf` leaves it all-`NA`, and the `ifelse` on line 68 therefore returns unsuffixed labels. Yet
`London_average_income.csv` contains `Median_1999 … Median_2023`. The committed file was produced by
a different, interactive version of this code.

**Fix:** the honest framing is that `dimension/` and `00_QOL_tidy.R` are exploratory notebooks that
were committed as if they were pipeline steps. Move them to `pipeline/experimental/` alongside the
others, and write issues 1.5–1.8 as clean scripts. Then make it impossible to regress:

- **CORRECTED.** A parse check is *not* sufficient. `tr(...)`, the swallowed assignment and the
  misplaced paren are all syntactically valid R : `parse()` accepts every one of these scripts.
  Only running the code finds them. The fix is an R CI job that executes the pipeline against
  synthetic fixtures (no raw data needed), with a parse check as a cheap first gate.
- One connection per script, opened at the top, closed in `on.exit(spark_disconnect(sc))`.

---

### P1-3. Two 3.7–3.9 GB CSV intermediates for an aggregation that needs none

`01_crime_by_LSOA.R` writes `crime.csv` (3.68 GB) and `crime_by_borough.csv` (3.93 GB), both via
`sdf_repartition(partitions = 1)` : funnelling ~13.9 M rows through a single executor twice, to
produce files no downstream step needs. The final outputs are five aggregates totalling under 700 KB.

Related: lines 59-71 fabricate a `Crime_ID` for records that lack one using
`sha2(concat(rand(), current_timestamp()), 256)`. That is non-deterministic (different IDs every
run, so nothing is comparable across runs), and it invents identity for anti-social-behaviour
records that legitimately have no crime ID : 3.4 M of them by the script's own comment. Nothing in
the pipeline dedupes on `Crime_ID`, so the column serves no purpose.

**Fix:** read → join → aggregate → write only the aggregates. Drop the synthetic ID entirely. At
this volume `data.table` or DuckDB does the whole job in a few minutes without Spark, which also
removes the `sparklyr` / Java / `derby.log` surface : the plan's own issue 1.3 already lists DuckDB
as the alternative. Keep the Spark version in `experimental/` as the portfolio artefact; the
architecture narrative (issue 4.1) can reference it without it being the live path.

---

### P1-4. Plan conformance gaps in what is already built

Against `plan-revision-issues.md`:

- **1.6 : IMD Crime domain is still in the analysis output.** `IDMP_2015_n_2019.csv` (94 columns)
  contains `Crime_Average_score_2015/2019` plus all `Rank_*` and `Proportion_of_LSOAs_*` columns.
  The circularity exclusion and `imd_crime_validation.csv` are not implemented, and the rationale is
  not in `SOURCES.md`.
- **1.5 : income year convention is undocumented and inconsistent with 1.8.** The source columns are
  *financial* years (`1999-00` … `2023-24`); `00_QOL_tidy.R`'s regex takes the **start** year, so
  `Median_2011` is tax year 2011/12. Issue 1.8 assigns rolling life-expectancy periods to their
  **end** year. Two conventions, undocumented, about to be paired against calendar-year crime rates
  by issue 3.6's nearest-year rule. Pick one, state it in `SOURCES.md`, apply it to both.
  Also: the artefact columns are `NA_2023` **and** `NA_2023.1` (issue 1.5 names only the first), and
  the missing survey year is confirmed : the sequence runs 2007-08 → 2009-10.
- **1.7 : `well_being_probabilitye.csv` is ward-grain, unaggregated.** 663 ward rows written straight
  out with no borough aggregation and no population weighting. Its life-expectancy columns are
  `2005-2009` … `2009-13` : the coverage problem that motivated the whole revision. Filename typo too.
- **`decision.txt` contradicts the code and `SOURCES.md`.** It records the ward well-being file as
  dropped; `00_QOL_tidy.R` still processes it and `SOURCES.md` still lists it as *the* well-being
  source **and** as the life-expectancy source, with `Licence: not documented`. Issue 1.4 exists to
  fix exactly this. Also `SOURCES.md` names `Scores-Table 1.csv` as the expected raw file while the
  code reads `Data-Table 1.csv`.
- **1.9 : `coverage.json` doesn't exist yet**, so nothing can satisfy 2.1 / 3.4. Fine as sequencing,
  but note 3.4's "no hardcoded year lists" is the criterion most likely to be quietly violated first;
  make it a review checklist item on the frontend PR.
- Population is `Mid-YYYY` = mid-year estimate; crime is calendar-year. Denominator/numerator periods
  differ by six months. Harmless, but issue 1.9 should state it rather than leave it implicit.

---

## P2 : CI, repo hygiene, docs

### P2-1. CI does not cover the pipeline, and does not run on the branch the plan specifies

**CORRECTED.** The original review claimed `npm ci` fails in `web/` because the lockfile is at the
repo root. That is wrong: npm walks up, finds the workspace root, and hoists into the root
`node_modules/`. Verified against the real tree : `npm ci` and `npm run lint` both exit 0 from
`web/`, and `cache-dependency-path: package-lock.json` correctly resolves from the repo root
(`defaults.run` does not apply to `uses:` steps). The web job was fine.

What is actually wrong with `.github/workflows/ci.yml`:

- `on: pull_request: branches: [v2]` : only PRs targeting `v2` trigger, and there is no `push`
  trigger at all. Issue 0.3 specifies `main`.
- **The R pipeline is not in CI at all.** Nothing lints, parses or runs it. That is the reason
  three scripts that cannot execute were committed and stayed committed, and it is the only CI
  gap that would have prevented any of the P0/P1 findings.

**Fix:** add a second job that installs R + `data.table` + `jsonlite`, parse-checks every script,
and runs a smoke test against synthetic fixtures. Prefer explicit `--workspace web` invocations
over `defaults.run.working-directory` so the workspace relationship is visible in the workflow.
Add `main` to the triggers, and reconcile the status flags: `README.md` says CI is Done,
`projects-plan.md` issue 0.3 says Planned.

### P2-2. `.gitignore` and repo state

- `data/processed/crime/` is ignored : correct for Spark output, dangerous now that it holds the raw
  data (P1-1).
- Redundant duplicate rule `data/processed/crime` (line 15) alongside `data/processed/crime/`.
- `derby.log` exists at the repo root and in `data/`; only `data/derby.log` is ignored. Add
  `derby.log` and `**/derby.log`, and `.DS_Store` files are present throughout (the rule exists, so
  they may already be tracked from before it was added : worth a `git rm --cached` sweep).
- `pipeline/logs/*.log` are committed as CSVs with a `.log` extension. That is fine as an audit
  trail, but rename to `.csv` or write real newline-delimited logs; tooling and reviewers both
  assume `.log` is text.

### P2-3. Documentation overstates current state

`README.md` "Status → Done" lists "crime raw-file coverage validation" and "LSOA→borough lookup with
logged exclusions and 33-borough verification". Given P0-1 (33 missing months undetected), P0-3
(coverage unverified, three conflicting totals) and P1-1 (scripts point at an empty directory), none
of those are Done. The README's data table also advertises crime coverage `2011-01 – 2026-04`, which
is true of the *filenames present* but not of the Metropolitan series.

This matters more than usual here: the project's stated selling point is provenance discipline, so a
status claim that the data does not support is the most expensive kind of error in it. Rewrite the
status section against verified facts, and add the 2012–2015 gap to *Known limitations* until it is
filled.

---

## Suggested order of work

1. **Back up `data/processed/crime/` off-machine.** Then P1-1 (move raw data, fix the `overwrite`
   target). Nothing else is safe until this is done.
2. P0-1 : re-download the 33 Met months; make `00_download.R` fail hard, per force.
3. P0-3 / P0-4 : one honest exclusion ledger; replace the tautological QA assertion; re-verify
   issue 1.2 before leaving it Done.
4. P0-2 : fix the category mapping, assert no `"No-category"`, decide the pre-2013 era policy.
5. P1-2 / P1-3 : retire `dimension/` and `00_QOL_tidy.R` to `experimental/`; rewrite 1.5–1.8 as
   clean `data.table` scripts; wire `lintr` + a parse check into CI.
6. P2-1 : fix CI so it can actually go green, then re-derive every processed output from a clean
   checkout and diff against what is committed today.
7. Only then start issue 1.9. `boroughs.json` is the point where these faults become public.

---

## Two things worth keeping

The analytical judgement in `plan-revision-issues.md` is better than the code it governs : the IMD
crime-domain circularity, median-over-mean, ecological-inference caveat, and the refusal to
interpolate the 2008 gap are all correct calls that most projects at this scale get wrong. The
problem is purely that the implementation is several steps behind the plan.

And `00_download.R` is the right instinct: a coverage guard that would have caught the single most
serious defect in the repo. It just needs to point at the data and to fail loudly.
