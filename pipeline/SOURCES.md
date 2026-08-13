# Data Sources, Licences and Decisions

Every derived output in `data/processed/` is regenerated from the raw sources
below by the ordered scripts in `pipeline/`. Raw data is never committed; this
file is the recipe to rebuild it.

**Licence status is stated honestly.** "Verified" means the licence was read on
the dataset's own page. "Assumed" means it is the standard licence for that
publisher but has not been checked on the page — treat those as open items.

---

## Crime — UK Police street-level data

| | |
|---|---|
| Source | data.police.uk archive |
| URL | https://data.police.uk/data/archive/ |
| Forces | Metropolitan Police Service; City of London Police |
| Window | `CRIME_START` – `CRIME_END` in `pipeline/_config.R` (currently 2011-01 – 2026-04) |
| Licence | Open Government Licence v3.0 — **verified** |
| Expected files | `data/raw/crime/YYYY-MM-{metropolitan,city-of-london}-street.csv` |
| Consumed by | `00_download.R`, `00_crime_rowcounts.R`, `01_crime_by_borough.R` |

Acquisition is a manual bulk export from the archive page (the per-request API
is rate-limited and impractical over fifteen years). `00_download.R` verifies
the result rather than downloading it, and **fails** if any month is absent for
any force.

> **Known gap.** 33 Metropolitan monthly files are missing across 2012–2015
> (2012: 02, 04, 06–09, 11, 12 · 2013: 01, 04–12 · 2014: 01, 02, 04–07, 09–11 ·
> 2015: 01, 03, 09–12). Metropolitan volume in those years reads at 12–19% of
> normal while City of London — complete throughout — is flat, so this is file
> availability, not a fall in crime. Borough rates for 2012–2015 must not be
> published until these are re-downloaded. `00_download.R` fails on this
> condition by design; do not narrow the window to make it pass.

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
| `attributed` | LSOA maps to one of the 33 boroughs — used |
| `blank` | no LSOA code (ungeocoded by the police) — excluded, not imputed |
| `outside_london` | valid ONS LSOA outside the 33 boroughs (boundary spillover) — excluded |
| `unmatched` | code matches no ONS LSOA in either vintage — excluded |

**Coverage denominator (resolves issue 1.2's ambiguity):** coverage is
`attributed / (all records − blank)`, i.e. the share of records *carrying an
LSOA code* that the lookup resolves. Blank codes are a police geocoding gap,
not a lookup failure. The threshold is ≥ 99.5% and it is enforced, not assumed.

---

## LSOA lookup — 2011/2021 LSOA to 2022 local authority

| | |
|---|---|
| Source | ONS Open Geography Portal |
| URL | https://geoportal.statistics.gov.uk/datasets/b9ca90c10aaa4b8d9791e9859a38ca67_0/explore |
| Licence | Open Government Licence v3.0 — **verified** |
| Expected file | `data/raw/LSAO_lookup/LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Exact_Fit_Lookup_for_EW_(V3).csv` |
| Consumed by | `00_LSAOlookup.R`, `01_crime_by_borough.R` |

Crime records carry 2011-vintage codes for most of the window and 2021-vintage
codes at the end, so the lookup is built from **both** columns. A single-vintage
join loses whichever era it does not cover — joining on `LSOA21CD` alone leaves
about 7.5% of records unmatched.

The file carries a UTF-8 byte-order mark; it is read with `encoding = "UTF-8"`
so the first column name is `LSOA11CD` and not `﻿LSOA11CD`.

132 London LSOAs split between the 2011 and 2021 vintages (up to 9 ways). No
London LSOA maps to more than one borough, which is asserted rather than
assumed — four such codes do exist elsewhere in England and Wales, and joining
on one would duplicate every crime record attached to it.

---

## Population — ONS mid-year estimates

| | |
|---|---|
| Source | ONS mid-year population estimates (MYE4) |
| URL | https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates |
| Coverage | mid-2011 to mid-2024 |
| Licence | Open Government Licence v3.0 — **verified** |
| Expected file | `data/raw/avg_population/MYE4-Table 1.csv` |
| Consumed by | `02_population_and_rates.R` |

The header row is located by content, not by a hardcoded skip count. Boroughs
are matched on **GSS code**, never on name.

Population is a **mid-year** estimate and crime is a **calendar year**, so
numerator and denominator are offset by six months. This is accepted and
recorded rather than corrected; it is stable across boroughs and years and so
does not affect cross-sectional comparison.

---

## Income — HMRC personal income by tax year

| | |
|---|---|
| Source | HMRC personal income statistics via the London Datastore |
| URL | https://data.london.gov.uk/dataset/average-income-of-tax-payers-borough-2g1nq |
| Coverage | 1999-00 to 2023-24 |
| Licence | Open Government Licence v3.0 — **assumed** (standard for UK government statistics; not yet read on the dataset page) |
| Expected file | `data/raw/personal_well_being/income-of-tax-payers/Total Income-Table 1.csv` |
| Consumed by | `10_tidy_income.R` |

**Median is the analysis metric.** Borough income is right-skewed and means are
dragged by high earners. Mean and taxpayer counts are retained as supplementary
rows, not dropped.

**Year convention: financial years are assigned to their START year.** Metric
year 2011 means tax year 2011/12. This must be kept consistent with the
life-expectancy rule in issue 1.8 — which assigns rolling periods to their END
year — before the two are ever paired. They currently disagree; whichever
survives, both must use it.

The 2008-09 survey year is absent from the source (the sequence runs 2007-08 →
2009-10). It is recorded as a gap and **not** interpolated. Two unlabelled
trailing artefact columns are dropped and logged to
`pipeline/logs/income_dropped_columns.log`.

---

## IMD — Indices of Deprivation, borough domain summaries

| | |
|---|---|
| Source | MHCLG Indices of Deprivation via the London Datastore |
| URL | https://data.london.gov.uk/dataset/indices-of-deprivation-2l15g |
| Coverage | 2015 and 2019 snapshots |
| Licence | Open Government Licence v3.0 — **assumed** (not yet read on the dataset page) |
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

## Well-being and life expectancy — OPEN

The ward well-being bundle previously used for both metrics covers **2009–2013
only** and is ward-grain. It is retired to `pipeline/experimental/` and is
**not** part of the canonical pipeline (see `decision.txt`).

Plan issue 1.4 replaces it with two borough-level annual series:

- ONS4 personal well-being (life satisfaction, worthwhile, happiness, anxiety),
  2011/12 onwards
- Borough life expectancy, male and female, rolling annual

Neither is in the repository yet, so `metrics_wellbeing.csv` and
`metrics_life_expectancy.csv` do not exist. Licences must be **verified**, not
assumed, when they are acquired — the previous entry cited one ward-file URL as
the source for both metrics with the licence undocumented.

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
