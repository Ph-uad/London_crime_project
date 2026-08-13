# Pipeline

Ordered R scripts that turn raw sources in `data/raw/` into derived outputs in
`data/processed/`. Sources, licences and every analytical decision are recorded
in [`SOURCES.md`](SOURCES.md).

**Dependencies:** R ≥ 4.1 with `data.table` and `jsonlite`. No Spark, no Java.

```bash
sudo apt-get install r-base-core r-cran-data.table r-cran-jsonlite   # Debian/Ubuntu
# or, in R:  install.packages(c("data.table", "jsonlite"))
```

## Expected raw layout

```
data/raw/crime/                        YYYY-MM-{metropolitan,city-of-london}-street.csv
data/raw/LSAO_lookup/                  ONS LSOA(2011)->LSOA(2021)->LAD(2022) lookup
data/raw/avg_population/               MYE4-Table 1.csv
data/raw/personal_well_being/          income-of-tax-payers/, ID 2015 for London/, ID 2019 for London/
```

Raw crime CSVs belong in `data/raw/crime/` **only**. They must never sit under
`data/processed/`, which the pipeline writes to. `_common.R` detects that
situation and stops with instructions rather than reporting every month as
missing.

## Running

From the repository root, in order:

```bash
Rscript pipeline/00_download.R              # verify monthly coverage, per force
Rscript pipeline/00_crime_rowcounts.R       # per-year row totals + files-per-year
Rscript pipeline/00_LSAOlookup.R            # LSOA -> borough lookup, both vintages
Rscript pipeline/01_crime_by_borough.R      # join, exclusion ledger, aggregates
Rscript pipeline/02_population_and_rates.R  # population and rates per 1,000
Rscript pipeline/10_tidy_income.R           # income -> common long schema
Rscript pipeline/11_tidy_imd.R              # IMD scores -> common long schema
Rscript pipeline/QA/01_QA.R                 # cross-script reconciliation
```

Any script exiting non-zero means the run failed — do not use the outputs.
A `warning()` would not set an exit status, so failures here are `stop`-class
by design.

## Configuration

`_config.R` is the single source of truth for the acquisition window, the
forces checked, the analysis windows and every path. **Nothing downstream may
hardcode a year range.** Values can be overridden by environment variable,
which is how the smoke test runs against a twelve-month synthetic sample:

```bash
CRIME_START=2011-01 CRIME_END=2011-12 Rscript pipeline/00_download.R
```

## Outputs

| File | Grain |
|---|---|
| `lsoa_lookup.csv` | LSOA (both vintages) → borough |
| `crime_by_borough_year.csv` | borough × year, counts |
| `crime_by_borough_year_category.csv` | borough × year × category × type, with `vocabulary` |
| `borough_population.csv` | borough × year |
| `crime_rates_by_borough_year.csv` | borough × year, counts + rate + `coverage_flag` |
| `metrics_crime.csv` | common long schema |
| `metrics_income.csv` | common long schema (median, mean, taxpayers) |
| `metrics_imd.csv` | common long schema, 6 domains × 2 snapshots |
| `imd_crime_validation.csv` | common long schema — **validation only, never analysis** |

Common long schema: `borough_gss, borough_name, year, metric, value, source,
geography_native, notes`.

## Logs

`pipeline/logs/` is the audit trail and is committed.

| File | Written by |
|---|---|
| `crime_coverage.log` | `00_download.R` — months present/missing per force |
| `rowcounts.log` | `00_crime_rowcounts.R` — rows and **files** per year per force |
| `exclusions.log` | `01_crime_by_borough.R` — every record's bucket, by year |
| `crime_types.log` | `01_crime_by_borough.R` — vocabulary seen per year |
| `income_dropped_columns.log` | `10_tidy_income.R` — artefact columns dropped |
| `dataQuality.log` | `QA/01_QA.R` — every check, expected vs actual |

## Testing

```bash
Rscript pipeline/tests/smoke.R
```

Builds a synthetic 33-borough fixture in a temp directory, runs every script,
then deliberately removes a month and corrupts an aggregate to confirm the
guards fire. It needs no raw data, so CI runs it on every PR.

A parse check alone is not enough: `tr(...)`, a use of an undefined object and
a reused closed connection are all syntactically valid. Only execution finds
them, which is why this exists.

## `experimental/`

Exploratory scripts, not part of the canonical run and not expected to execute
as-is. `spark_01_crime_by_LSOA.R` and `spark_01_LSOA_by_population.R` are the
original sparklyr implementations, kept for the architecture write-up (issue
4.1). `qol_tidy_original.R` is the original combined tidy script.

Do not run `spark_01_crime_by_LSOA.R` against a live `data/` tree: it writes to
`data/processed/crime` with `mode = "overwrite"`, which clears that directory.
