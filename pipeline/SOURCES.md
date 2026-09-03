# Data Sources, Licences and Decisions

Every derived output in `data/processed/` is regenerated from the raw sources
below by the ordered scripts in `pipeline/`. Raw data is never committed; this
file is the recipe to rebuild it.

**Licence status is stated honestly.** "Verified" means the licence was read on
the dataset's own page. "Assumed" means it is the standard licence for that
publisher but has not been checked on the page : treat those as open items.

---

## Crime : UK Police street-level data

| | |
|---|---|
| Source | data.police.uk archive |
| URL | https://data.police.uk/data/archive/ |
| Forces | Metropolitan Police Service; City of London Police |
| Window | `CRIME_START` – `CRIME_END` in `pipeline/_config.R` (currently 2011-01 – 2026-04) |
| Licence | Open Government Licence v3.0 : **verified** |
| Expected files | `data/raw/crime/YYYY-MM-{metropolitan,city-of-london}-street.csv` |
| Consumed by | `00_download.R`, `00_crime_rowcounts.R`, `01_crime_by_borough.R` |

Acquisition is a manual bulk export from the archive page (the per-request API
is rate-limited and impractical over fifteen years). `00_download.R` verifies
the result rather than downloading it, and **fails** if any month is absent for
any force.

> **Resolved 2026-08-16.** 33 Metropolitan monthly files were missing across
> 2012–2015, which read as an 80% fall in crime rather than as absent files.
> All 368 files (184 months x 2 forces) are now present and `00_download.R`
> passes. Metropolitan volume for 2012–2015 went from 389k/157k/234k/476k rows
> to 1.13M/1.00M/948k/948k : 2.77 million records recovered. Kept here as the
> reason the coverage check is per force and fails hard: a pooled check passed
> throughout, because City of London was complete the whole time.

### Crime taxonomy

police.uk changed its category vocabulary in **April 2013**. Both vocabularies
are mapped into one continuous category series in `01_crime_by_borough.R`:

| Legacy name (to 2013-03) | Mapped to |
|---|---|
| `Violent crime` | Violence and Sexual Offences |
| `Public disorder and weapons` | Public Order Offences |

**The mapping makes the categories continuous. It does not make them
comparable.** Two things change composition at the boundary and no mapping can
repair them:

- `Public disorder and weapons` split into `Public order` **and** `Possession of
  weapons`. Assigning the whole legacy category to Public Order Offences moves
  pre-2013 weapons offences out of Drug and Weapon Offences.
- `Other crime` was a genuine catch-all in 2011 (297,619 records London-wide)
  and a small residual bucket afterwards (~10,000/yr).

Every output row carries a `vocabulary` column (`legacy_only`, `current_only`,
`both`) naming which side of the boundary that crime type's label belongs to. A
year containing both `legacy_only` and `current_only` records straddles the
change. Year-on-year category comparisons that cross 2013-04 should be
footnoted; total crime counts are unaffected.

### Exclusions

`01_crime_by_borough.R` classifies every record into exactly one bucket and
asserts the buckets sum to the raw total. `pipeline/logs/exclusions.log` holds
the counts by status and year.

| Status | Meaning |
|---|---|
| `attributed` | LSOA maps to one of the 33 boroughs : used |
| `blank` | no LSOA code (ungeocoded by the police) : excluded, not imputed |
| `outside_london` | valid ONS LSOA outside the 33 boroughs (boundary spillover) : excluded |
| `unmatched` | code matches no ONS LSOA in either vintage : excluded |

**Coverage denominator (resolves issue 1.2's ambiguity):** coverage is
`attributed / (all records − blank)`, i.e. the share of records *carrying an
LSOA code* that the lookup resolves. Blank codes are a police geocoding gap,
not a lookup failure. The threshold is ≥ 99.5% and it is enforced, not assumed.

---

## LSOA lookup : 2011/2021 LSOA to 2022 local authority

| | |
|---|---|
| Source | ONS Open Geography Portal |
| URL | https://geoportal.statistics.gov.uk/datasets/b9ca90c10aaa4b8d9791e9859a38ca67_0/explore |
| Licence | Open Government Licence v3.0 : **verified** |
| Expected file | `data/raw/LSAO_lookup/LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Exact_Fit_Lookup_for_EW_(V3).csv` |
| Consumed by | `00_LSAOlookup.R`, `01_crime_by_borough.R` |

Crime records carry 2011-vintage codes for most of the window and 2021-vintage
codes at the end, so the lookup is built from **both** columns. A single-vintage
join loses whichever era it does not cover : joining on `LSOA21CD` alone leaves
about 7.5% of records unmatched.

The file carries a UTF-8 byte-order mark; it is read with `encoding = "UTF-8"`
so the first column name is `LSOA11CD` and not `﻿LSOA11CD`.

132 London LSOAs split between the 2011 and 2021 vintages (up to 9 ways). No
London LSOA maps to more than one borough, which is asserted rather than
assumed : four such codes do exist elsewhere in England and Wales, and joining
on one would duplicate every crime record attached to it.

---

## Population : ONS mid-year estimates

| | |
|---|---|
| Source | ONS mid-year population estimates (MYE4) |
| URL | https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates |
| Coverage | mid-2011 to mid-2024 |
| Licence | Open Government Licence v3.0 : **verified** |
| Expected file | `data/raw/avg_population/MYE4-Table 1.csv` |
| Consumed by | `02_population_and_rates.R` |

The header row is located by content, not by a hardcoded skip count. Boroughs
are matched on **GSS code**, never on name.

Population is a **mid-year** estimate and crime is a **calendar year**, so
numerator and denominator are offset by six months. This is accepted and
recorded rather than corrected; it is stable across boroughs and years and so
does not affect cross-sectional comparison.

---

## Income : HMRC personal income by tax year

| | |
|---|---|
| Source | HMRC personal income statistics via the London Datastore |
| URL | https://data.london.gov.uk/dataset/average-income-of-tax-payers-borough-2g1nq |
| Coverage | 1999-00 to 2023-24 |
| Licence | Open Government Licence v3.0 : **assumed** (standard for UK government statistics; not yet read on the dataset page) |
| Expected file | `data/raw/personal_well_being/income-of-tax-payers/Total Income-Table 1.csv` |
| Consumed by | `10_tidy_income.R` |

**Median is the analysis metric.** Borough income is right-skewed and means are
dragged by high earners. Mean and taxpayer counts are retained as supplementary
rows, not dropped.

**Year convention: financial years are assigned to their START year.** Metric
year 2011 means tax year 2011/12. Well-being uses the same rule.

Life expectancy deliberately uses the **END** year of its rolling three-year
period. The two rules are not in conflict : a twelve-month accounting year and
a three-year rolling window are different things : but the difference is real
and is exposed per metric as `year_rule` in `coverage.json`, so issue 3.6 can
print which years it actually paired instead of implying they are the same.

The 2008-09 survey year is absent from the source (the sequence runs 2007-08 →
2009-10). It is recorded as a gap and **not** interpolated. Two unlabelled
trailing artefact columns are dropped and logged to
`pipeline/logs/income_dropped_columns.log`.

---

## IMD : Indices of Deprivation, borough domain summaries

| | |
|---|---|
| Source | MHCLG Indices of Deprivation via the London Datastore |
| URL | https://data.london.gov.uk/dataset/indices-of-deprivation-2l15g |
| Coverage | 2015 and 2019 snapshots |
| Licence | Open Government Licence v3.0 : **assumed** (not yet read on the dataset page) |
| Expected files | `data/raw/personal_well_being/ID {2015,2019} for London/Borough domain summaries-Table 1.csv` |
| Consumed by | `11_tidy_imd.R` |

**The IMD Crime domain is excluded from analysis.** It is constructed from
recorded crime, so using it to explain crime rates is circular. It is written to
`data/processed/imd_crime_validation.csv` and used only as an external check on
our police-derived rates. `11_tidy_imd.R` asserts it has not leaked into the
analysis output.

**Ranks and proportions are excluded.** IMD 2015 and IMD 2019 ranks are not
methodologically comparable, so a change in rank between snapshots is not a
change in deprivation. Average scores are retained.

**Domain scores are not on a common scale**, and two of them are legitimately
negative. Anything that colours or ranks across domains must respect this:

| Domain | Scale | Observed range |
|---|---|---|
| Income, Employment | proportion | 0.0 – 0.3 |
| Education, Skills and Training | score | 3.5 – 27.2 |
| Barriers to Housing and Services | score | 17.5 – 49.3 |
| Living Environment | score | 13.4 – 55.0 |
| Health Deprivation and Disability | standardised | −1.4 – 0.5 |
| Crime *(validation only)* | standardised | −1.7 – 1.0 |

Each output row carries its `scale_type` in `notes`. A blanket "score ≥ 0"
check is wrong and will fail on half of London.

---

## Well-being : ONS4 personal well-being by local authority

| | |
|---|---|
| Source | Office for National Statistics |
| Dataset | Personal well-being estimates by local authority, time-series edition, **version 4** |
| URL | https://www.ons.gov.uk/datasets/wellbeing-local-authority |
| Download | https://download.ons.gov.uk/downloads/datasets/wellbeing-local-authority/editions/time-series/versions/4.csv |
| Coverage | **2011-12 to 2022-23** (financial years) |
| Measures | life satisfaction, worthwhile, happiness, anxiety |
| Geography | UK, country, region, county, local and unitary authority |
| Released | 28 November 2023 |
| Licence | Open Government Licence v3.0 : **verified** on the dataset page 2026-08-16 |
| Expected file | `data/raw/wellbeing/ons4-wellbeing-local-authority-timeseries-v4.csv` |
| Consumed by | `00_download_metrics.R`, then `12_tidy_wellbeing.R` (issue 1.7) |

**Anxiety runs in the opposite direction to the other three.** Higher life
satisfaction, worthwhile and happiness scores are better; a higher anxiety
score is worse. Anything that ranks boroughs or colours a map across all four
must handle this, or three-quarters of the scale will read backwards.

**The series ends at 2022-23**, one year short of the 2011–2023 cross-metric
analysis window. ONS has published no later local-authority edition as of
2026-08-16. Well-being is therefore absent for 2023 in `coverage.json`; it is
not carried forward or interpolated.

**City of London has no well-being data at all.** All 48 of its cells (4
measures x 12 years) are marked `[u]` : sample too small to publish. The
metric covers **32 boroughs**, recorded in `coverage.json` under
`boroughs_missing` and listed in `pipeline/logs/wellbeing_suppressed.log`.
`12_tidy_wellbeing.R` declares this as a permitted absence and fails if any
other borough goes missing, or if City of London ever appears.

Only the `average-mean` estimate is used. The other four estimate types
(poor, fair, good, very good) are the proportion of people in each rating
band, not the borough's average score.

Financial years are assigned to their **start** year, matching the income rule
above: metric year 2011 means 2011-12.

---

## Life expectancy : ONS life expectancy for local areas of the UK

| | |
|---|---|
| Source | Office for National Statistics |
| Dataset | Life expectancy for local areas of the UK, edition "between 2001 to 2003 and 2022 to 2024" |
| URL | https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/healthandlifeexpectancies/datasets/lifeexpectancyforlocalareasoftheuk |
| Download | https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/healthandsocialcare/healthandlifeexpectancies/datasets/lifeexpectancyforlocalareasoftheuk/between2001to2003and2022to2024/lifeexpectancylocalareas.xlsx |
| Coverage | **2001-2003 to 2022-2024**, three-year rolling periods |
| Measures | life expectancy at birth **and at age 65**, male and female separately : four metrics |
| Geography | lower-tier local authorities (England), plus region and county |
| Released | 10 December 2025 |
| Licence | Open Government Licence v3.0 : **verified** on the dataset page 2026-08-16 |
| Expected file | `data/raw/life_expectancy/ons-lifeexpectancylocalareas-2022to2024.xlsx` |
| Consumed by | `00_download_metrics.R`, then `13_tidy_life_expectancy.R` (issue 1.8) |

**Period-to-year rule (issue 1.8): rolling periods are assigned to their END
year** : 2022-2024 becomes metric year 2024 : with the full period preserved in
`notes`. Note this **differs from the income and well-being rule**, which uses
the start year of a financial year. The two conventions are not in conflict
(one is a 3-year rolling window, the other a 12-month accounting year) but the
difference is deliberate and must be visible wherever the metrics are paired :
issue 3.6's nearest-available-year rule has to print the pairing on the chart.

**City of London is not in the source at all** : ONS does not publish life
expectancy for it. The metric covers **32 boroughs**, declared the same way as
well-being. `13_tidy_life_expectancy.R` also asserts that at-65 figures sit
below at-birth figures for every borough-year, which is the check that would
catch an age-group mix-up.

### Why ONS and not the London Datastore

Issue 1.4 named the London Datastore as the source for both series. Both
Datastore copies were checked on 2026-08-16 and rejected:

| Datastore dataset | Problem |
|---|---|
| `personal-well-being-borough-2r87d` | Apr 2011 – Mar 2019 only; last updated 2019 |
| `life-expectancy-at-birth-and-at-age-65-borough-23gm7` | 2000-2002 to 2008-2010, and **Open Government Licence v2**, not v3 |

Both are GLA re-publications of ONS data. Going to ONS directly gives a longer
series, a current release, and OGL v3.0 on both : which is also what makes the
"licence verified, not assumed" criterion pass rather than being waved through.

The life-expectancy Datastore file being *older* than the ward bundle it was
meant to replace is the reason this check happened before download rather than
after tidying.

---

## Retired : ward well-being bundle (secondary use only)

| | |
|---|---|
| Source | GLA, London ward well-being probability scores |
| URL | https://data.london.gov.uk/dataset/london-ward-well-being-scores-2k843 |
| Coverage | **2009–2013 only**, ward grain |
| Licence | Open Government Licence v3.0 : assumed |
| Status | **Not part of the canonical pipeline.** Retired to `pipeline/experimental/qol_tidy_original.R`. |

This file was previously cited as the source for **both** well-being and life
expectancy, under a single URL, with the licence undocumented. That is what
issue 1.4 exists to fix, and the two entries above replace it.

It is retained only for optional secondary features : unemployment, child
poverty, GCSE, PTAL, greenspace : all of which are **2011–2013 coverage** and
ward grain. If any of them is ever used, it must be aggregated ward→borough
with a **population-weighted** mean (never a plain mean), and the
export-mangled `Subjective_well_being_..._1/_2/_3` column labels must be
recovered from the source workbook first. No column with an unverified label
may enter an output.

---

## Borough boundaries : ONS Local Authority Districts

| | |
|---|---|
| Source | ONS Open Geography Portal (via data.gov.uk) |
| Default product | Local Authority Districts (**December 2022**) Boundaries UK **BGC** : Generalised (20m), clipped to the coastline |
| URL | https://www.data.gov.uk/dataset/287aef0c-ef71-488e-a01d-3775b2366764/local-authority-districts-december-2022-boundaries-uk-bgc |
| Fallback product | LAD (December 2023) UK **BUC** : Ultra generalised (500m), clipped |
| Fallback URL | https://www.data.gov.uk/dataset/d75f8904-5ebc-45de-b81d-e113c7bd1998/local-authority-districts-december-2023-boundaries-uk-buc |
| Licence | Open Government Licence v3.0 : **verified** on the data.gov.uk pages 2026-08-16 |
| Rights | Contains both Ordnance Survey and ONS Intellectual Property Rights |
| Expected file | `data/raw/boundaries/ons-lad-uk-{bgc,buc}.geojson` |
| Consumed by | `00_download_metrics.R`, `03_borough_boundaries.R` |

**No simplification is applied.** Issue 1.10 proposed `rmapshaper`; the file
size target is met instead by keeping only the 33 London features and
rounding coordinates to 6 decimal places (~0.1 m : far finer than a 20 m
generalisation, so nothing real is discarded). Both ONS products are
generalised across the whole UK coverage, so neighbouring boroughs still share
their edges exactly. Simplifying here with `sf::st_simplify` would move each
outline independently and open slivers between boroughs that no downstream
step could repair.

`pipeline/logs/boundaries.log` records the product, the coordinate precision,
the feature count, the output size and the CRS.

**Vintage.** The default boundary release is December 2022, matching the
LAD22 lookup. The fallback is December 2023. That difference is safe because
the script asserts the 33 GSS codes match `boroughs.json` **exactly** : if
they do, the vintages agree for London, which is checked rather than assumed.

**Projection.** ONS publishes in British National Grid (EPSG:27700); the
output is EPSG:4326 / RFC 7946 for MapLibre. Geometry validity is checked in
the source projection, before reprojection, because sf switches to spherical
`s2` semantics for lon/lat and rejects duplicated vertices that are perfectly
fine on a map. A bounding-box assertion catches the opposite failure : a file
that claims WGS84 while still carrying BNG eastings, which renders in the
North Sea and passes every other check.

---

## Unified export : boroughs.json and coverage.json

`20_unify_metrics.R` (issue 1.9) binds every `metrics_*.csv` into
`data/processed/boroughs.json`, and writes `data/processed/coverage.json`
alongside it.

`coverage.json` is the contract the frontend reads instead of hardcoding
anything. Per metric it declares:

| Field | Why it exists |
|---|---|
| `years`, `partial_years` | issue 3.4 : slider range, and years excluded from year-on-year comparison |
| `cadence` | `snapshot` (IMD) renders as discrete points, `annual` as a slider |
| `direction` | issue 3.7 : falling crime is good, falling life expectancy is not |
| `scale`, `unit` | IMD domains span proportion, score and standardised; a shared colour scale across them is wrong |
| `year_rule` | issue 3.6 : `calendar`, `financial_start`, `rolling_end` or `snapshot`, so a pairing can be explained |
| `boroughs_missing` | City of London, per metric, with no silent 32-vs-33 mismatch |

Every metric in the data must have a registry entry in `20_unify_metrics.R`.
A metric with no entry fails the run rather than inheriting a default : a
default `higher_is_better` applied to anxiety, crime, or any deprivation score
inverts the entire reading.

Array fields keep their shape at length 1: `partial_years` serialises as
`[2026]`, never as `2026`.

---

## Acquisition record

`pipeline/logs/acquisition.log` records, for every file
`00_download_metrics.R` fetches: the dataset name, URL, byte size, MD5 and the
UTC timestamp of the download. That answers "when did we pull this, and is it
still the same file" without relying on filesystem timestamps.

---

## Analysis window

Set in `pipeline/_config.R`; nothing downstream may hardcode a year range.

| Window | Value | Bound by |
|---|---|---|
| Cross-metric analysis | 2011 – 2023 | income (to 2023-24) |
| Crime rate trend | 2011 – 2024 | ONS population (to mid-2024) |
| Counts only, flagged | 2025 onward | no denominator published yet |

`02_population_and_rates.R` derives year completeness from the files present,
computes `months_present` per year, and refuses to publish a rate for any year
that is not twelve months (`coverage_flag`: `complete`, `partial_year`,
`no_denominator`). A four-month 2026 can never be presented as a full year. QA
asserts that no rate escapes for an incomplete year.

---

## Attribution

Contains public sector information licensed under the Open Government Licence
v3.0.
