# Data Sources

## Crime — UK Police street-level data
- **Source:** data.police.uk archive
- **URL:** https://data.police.uk/data/archive/
- **Forces selected:** Metropolitan Police Service; City of London Police
- **Date range:** 2011-01 to 2026-04
- **Licence:** Open Government Licence v3.0 (OGL-UK-3.0)
- **Expected raw files:** `data/raw/crime/*-street.csv`
- **Consumed by:** `pipeline/00_download.R`, `pipeline/00_LSAOlookup.R`, `pipeline/00_crime_rowcounts.R`, `pipeline/dimension/01_crime_by_LSOA.R`
- **Note:** The canonical crime acquisition script currently checks that raw CSV monthly coverage is present rather than downloading data.

## Income — Personal income by tax year
- **Source:** UK government personal tax and income statistics
- **URL:** https://data.london.gov.uk/dataset/average-income-of-tax-payers-borough-2g1nq
- **Licence:** Open Government Licence v3.0 (expected for UK government statistics)
- **Expected raw files:** `data/raw/personal_well_being/income-of-tax-payers/Total Income-Table 1.csv`
- **Consumed by:** `pipeline/00_QOL_tidy.R`

## Life expectancy — Borough-level life expectancy
- **Source:** external UK life expectancy CSV referenced by experimental scripts
- **URL:** https://data.london.gov.uk/dataset/london-ward-well-being-scores-2k843
- **Licence:** not documented in repo
- **Expected raw files:** `data/raw/life_expectancy/*` (not currently present in this checkout)
- **Consumed by:** `pipeline/experimental/male_life_expectancy.R`

## IMD — Index of Multiple Deprivation borough summaries
- **Source:** personal wellbeing / IMD borough summaries
- **URL:**  https://data.london.gov.uk/dataset/indices-of-deprivation-2l15g
- **Licence:** Open Government Licence v3.0 (expected for UK government statistics)
- **Expected raw files:**
  - `data/raw/personal_well_being/ID 2015 for London/IMD 2015-Table 1.csv`
  - `data/raw/personal_well_being/ID 2019 for London/IMD 2019-Table 1.csv`
- **Consumed by:** `pipeline/00_QOL_tidy.R`

## Wellbeing — London borough wellbeing scores
- **Source:** personal wellbeing borough scores
- **URL:**  https://data.london.gov.uk/dataset/london-ward-well-being-scores-2k843
- **Licence:** Open Government Licence v3.0 (expected for UK government statistics)
- **Expected raw files:**
  - `data/raw/personal_well_being/london-ward-well-being-probability-scores/Scores-Table 1.csv`
- **Consumed by:** `pipeline/00_QOL_tidy.R`

## LSOA lookup — 2011/2021 LSOA to 2022 local authority correspondence
- **Source:** ONS Open Geography Portal lookup
- **URL:** https://geoportal.statistics.gov.uk/datasets/b9ca90c10aaa4b8d9791e9859a38ca67_0/explore
- **Licence:** Open Government Licence v3.0 (ONS geography lookup data)
- **Expected raw file:** `data/raw/LSAO_lookup/LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Exact_Fit_Lookup_for_EW_(V3).csv`
- **Consumed by:** `pipeline/00_LSAOlookup.R`, `pipeline/dimension/01_crime_by_LSOA.R`
