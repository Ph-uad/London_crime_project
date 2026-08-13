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

LOOKUP_OUT <- file.path(PROC_DIR, "lsoa_lookup.csv")

# ---- Common long schema (issues 1.5–1.9) ----
LONG_SCHEMA <- c("borough_gss", "borough_name", "year", "metric", "value",
                 "source", "geography_native", "notes")
