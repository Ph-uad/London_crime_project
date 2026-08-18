# =============================================================
# _config.R — single source of truth for windows, paths and forces.
#
# Every downstream script and the exported coverage matrix read from
# here. Nothing else in the pipeline may hardcode a year range.
#
# Values can be overridden by environment variable, which is how the
# test fixtures run against a three-month sample without editing code.
# =============================================================

cfg_env <- function(name, default) {
  v <- Sys.getenv(name, unset = NA_character_)
  if (is.na(v) || !nzchar(v)) default else v
}

# ---- Crime acquisition window (must match pipeline/SOURCES.md) ----
CRIME_START <- cfg_env("CRIME_START", "2011-01")
CRIME_END   <- cfg_env("CRIME_END",   "2026-04")

# Forces whose monthly files must ALL be present. Coverage is checked per
# force: a pooled check passes whenever any one force supplies the month,
# which is how 33 missing Metropolitan months went unnoticed.
CRIME_FORCES <- strsplit(
  cfg_env("CRIME_FORCES", "metropolitan,city-of-london"), ",", fixed = TRUE
)[[1]]

# ---- Analysis windows (see SOURCES.md "Analysis window" decision) ----
# Cross-metric analysis is bounded by the shortest denominator.
ANALYSIS_START <- as.integer(cfg_env("ANALYSIS_START", "2011"))
ANALYSIS_END   <- as.integer(cfg_env("ANALYSIS_END",   "2023"))
# Crime rates run one year further, to the population-estimate limit.
TREND_END      <- as.integer(cfg_env("TREND_END",      "2024"))

# ---- Geography ----
LONDON_BOROUGH_N <- 33L
LONDON_GSS_PREFIX <- "^E09"

# ---- Paths (all relative to the repository root) ----
RAW_DIR       <- "data/raw"
PROC_DIR      <- "data/processed"
LOG_DIR       <- "pipeline/logs"

CRIME_RAW_DIR <- file.path(RAW_DIR, "crime")
LOOKUP_RAW    <- file.path(
  RAW_DIR, "LSAO_lookup",
  paste0("LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)",
         "_Exact_Fit_Lookup_for_EW_(V3).csv")
)
POP_RAW       <- file.path(RAW_DIR, "avg_population", "MYE4-Table 1.csv")
INCOME_RAW    <- file.path(RAW_DIR, "personal_well_being",
                           "income-of-tax-payers", "Total Income-Table 1.csv")
IMD_RAW <- c(
  "2015" = file.path(RAW_DIR, "personal_well_being", "ID 2015 for London",
                     "Borough domain summaries-Table 1.csv"),
  "2019" = file.path(RAW_DIR, "personal_well_being", "ID 2019 for London",
                     "Borough domain summaries-Table 1.csv")
)

# ---- Issue 1.4: annual well-being and life expectancy --------------------
# Both come from ONS directly, NOT from the London Datastore mirrors. The
# Datastore copies were checked on 2026-08-16 and rejected:
#   personal-well-being-borough-2r87d   stops at 2018-19, last updated 2019
#   life-expectancy-...-borough-23gm7   stops at 2008-2010, and is OGL v2
# Licences below were read on the ONS dataset pages, not assumed.
WELLBEING_RAW <- file.path(RAW_DIR, "wellbeing",
                           "ons4-wellbeing-local-authority-timeseries-v4.csv")
LIFEEXP_RAW   <- file.path(RAW_DIR, "life_expectancy",
                           "ons-lifeexpectancylocalareas-2022to2024.xlsx")

# Overridable so the acquisition script can be tested against file:// fixtures.
WELLBEING_URL <- cfg_env(
  "WELLBEING_URL",
  paste0("https://download.ons.gov.uk/downloads/datasets/",
         "wellbeing-local-authority/editions/time-series/versions/4.csv")
)
LIFEEXP_URL <- cfg_env(
  "LIFEEXP_URL",
  paste0("https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/",
         "healthandsocialcare/healthandlifeexpectancies/datasets/",
         "lifeexpectancyforlocalareasoftheuk/between2001to2003and2022to2024/",
         "lifeexpectancylocalareas.xlsx")
)

LOOKUP_OUT <- file.path(PROC_DIR, "lsoa_lookup.csv")

# ---- Issue 1.10: borough boundaries --------------------------------------
# ONS Open Geography Portal, Local Authority Districts boundaries. Licences
# read on the data.gov.uk dataset pages 2026-08-16: OGL v3.0 on both.
#
# Two generalisation levels, tried in order. BGC is the quality choice; BUC is
# the fallback if the London subset will not fit the 500 KB budget. Both are
# generalised BY ONS across the whole coverage, so adjacent boroughs still
# share edges exactly. We never simplify ourselves — a non-topology-preserving
# simplify opens slivers between neighbouring boroughs, which on a choropleth
# shows up as hairline gaps you cannot fix afterwards.
BOUNDARY_GEN <- cfg_env("BOUNDARY_GEN", "BGC")     # "BGC" (20m) or "BUC" (500m)
BOUNDARY_ITEMS <- list(
  BGC = list(item = "995533eee7e44848bf4e663498634849",
             vintage = "December 2022",
             detail = "Generalised (20m), clipped to the coastline"),
  BUC = list(item = "79a4e87783be4b6bbb96ddad6dda52a3",
             vintage = "December 2023",
             detail = "Ultra generalised (500m), clipped to the coastline")
)
boundary_url <- function(gen = BOUNDARY_GEN) {
  paste0("https://open-geography-portalx-ons.hub.arcgis.com/api/download/v1/",
         "items/", BOUNDARY_ITEMS[[gen]]$item, "/geojson?layers=0")
}
BOUNDARY_URL <- cfg_env("BOUNDARY_URL", boundary_url())
BOUNDARY_RAW <- file.path(RAW_DIR, "boundaries",
                          paste0("ons-lad-uk-", tolower(BOUNDARY_GEN),
                                 ".geojson"))
BOUNDARY_OUT <- file.path(PROC_DIR, "london.geojson")

# Issue 1.10's budget, and the coordinate precision used to meet it.
# 6 decimal places is ~0.1 m at London's latitude — far finer than a 20 m
# generalisation, so rounding to it discards nothing real.
BOUNDARY_MAX_BYTES <- as.numeric(cfg_env("BOUNDARY_MAX_BYTES", "512000"))
BOUNDARY_COORD_DP  <- as.integer(cfg_env("BOUNDARY_COORD_DP", "6"))

# ---- Common long schema (issues 1.5–1.9) ----
LONG_SCHEMA <- c("borough_gss", "borough_name", "year", "metric", "value",
                 "source", "geography_native", "notes")
