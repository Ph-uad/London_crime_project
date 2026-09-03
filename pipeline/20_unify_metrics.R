# =============================================================
# 20_unify_metrics.R : bind every tidied metric into one export, plus the
# coverage matrix. Implements plan issue 1.9.
#
# Two artefacts:
#
#   boroughs.json   the observations: borough x year x metric x value
#   coverage.json   what exists, and what each metric MEANS : which years,
#                   which boroughs, which direction is "good", what scale the
#                   values are on, and how the year was derived
#
# coverage.json is the contract that lets the frontend stop guessing. Issue
# 3.4 forbids hardcoded year lists; issue 3.6 has to print which years it
# paired; issue 3.7 needs to know that falling crime is good but falling life
# expectancy is not. All of that is answered here rather than re-derived in
# TypeScript.
#
# Every metric must have a registry entry below. A metric appearing in the
# data with no entry FAILS the run : the same rule as the crime-type mapping.
# A silent default is how "higher is better" gets applied to anxiety.
#
# Writes data/processed/boroughs.json, data/processed/coverage.json
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))
suppressPackageStartupMessages(library(jsonlite))

banner("20_unify_metrics")

# ---- Metric registry -----------------------------------------------------
# cadence   annual   -> continuous year slider
#           snapshot -> discrete selectable points (issue 3.4)
# year_rule how the metric's year was derived, so pairings can be explained
M <- function(metric, label, cadence, direction, scale, year_rule, unit)
  data.table(metric, label, cadence, direction, scale, year_rule, unit)

METRIC_META <- rbindlist(list(
  M("crime_rate_per_1000", "Crime rate per 1,000 residents", "annual",
    "higher_is_worse", "rate", "calendar", "crimes per 1,000"),
  M("crime_count", "Recorded crimes", "annual",
    "higher_is_worse", "count", "calendar", "crimes"),

  M("income_median", "Median income", "annual",
    "higher_is_better", "currency", "financial_start", "GBP"),
  M("income_mean", "Mean income", "annual",
    "higher_is_better", "currency", "financial_start", "GBP"),
  M("income_taxpayers", "Taxpayers", "annual",
    "neutral", "count", "financial_start", "individuals"),

  M("imd_income_score", "IMD income deprivation", "snapshot",
    "higher_is_worse", "proportion", "snapshot", "proportion"),
  M("imd_employment_score", "IMD employment deprivation", "snapshot",
    "higher_is_worse", "proportion", "snapshot", "proportion"),
  M("imd_education_skills_and_training_score", "IMD education deprivation",
    "snapshot", "higher_is_worse", "score", "snapshot", "score"),
  M("imd_health_deprivation_and_disability_score", "IMD health deprivation",
    "snapshot", "higher_is_worse", "standardised", "snapshot", "z-like"),
  M("imd_barriers_to_housing_and_services_score", "IMD housing barriers",
    "snapshot", "higher_is_worse", "score", "snapshot", "score"),
  M("imd_living_environment_score", "IMD living environment deprivation",
    "snapshot", "higher_is_worse", "score", "snapshot", "score"),

  M("wellbeing_life_satisfaction", "Life satisfaction", "annual",
    "higher_is_better", "rating", "financial_start", "mean 0-10"),
  M("wellbeing_worthwhile", "Feeling things done are worthwhile", "annual",
    "higher_is_better", "rating", "financial_start", "mean 0-10"),
  M("wellbeing_happiness", "Happiness yesterday", "annual",
    "higher_is_better", "rating", "financial_start", "mean 0-10"),
  # The one that breaks the pattern.
  M("wellbeing_anxiety", "Anxiety yesterday", "annual",
    "higher_is_worse", "rating", "financial_start", "mean 0-10"),

  M("life_expectancy_birth_male", "Life expectancy at birth, male", "annual",
    "higher_is_better", "years", "rolling_end", "years"),
  M("life_expectancy_birth_female", "Life expectancy at birth, female",
    "annual", "higher_is_better", "years", "rolling_end", "years"),
  M("life_expectancy_65_male", "Life expectancy at 65, male", "annual",
    "higher_is_better", "years", "rolling_end", "years"),
  M("life_expectancy_65_female", "Life expectancy at 65, female", "annual",
    "higher_is_better", "years", "rolling_end", "years")
))
check(!anyDuplicated(METRIC_META$metric), "duplicate entry in METRIC_META.")

SCALE_RANGE <- list(rate = c(0, 5000), count = c(0, 1e7),
                    currency = c(0, 1e6), proportion = c(0, 1),
                    score = c(0, 100), standardised = c(-5, 5),
                    rating = c(0, 10), years = c(0, 120))

# ---- Load ----------------------------------------------------------------
metric_files <- list.files(PROC_DIR, pattern = "^metrics_.*\\.csv$",
                           full.names = TRUE)
check(length(metric_files) > 0L,
      "no metrics_*.csv in ", PROC_DIR, ". Run the tidy scripts first.")
message("Inputs: ", paste(basename(metric_files), collapse = ", "))

obs <- rbindlist(lapply(metric_files, function(f) {
  d <- fread(f, colClasses = list(character = c("borough_gss", "borough_name",
                                                "metric")),
             showProgress = FALSE)
  check(identical(names(d), LONG_SCHEMA),
        basename(f), " does not conform to the long schema.\n       Expected: ",
        paste(LONG_SCHEMA, collapse = ", "), "\n       Got:      ",
        paste(names(d), collapse = ", "))
  d[, source_file := basename(f)]
  d
}))

# imd_crime_validation.csv is deliberately NOT matched by the glob above. If
# it ever is, the circularity the whole exclusion exists to prevent is back.
check(!any(grepl("crime", obs$metric) & grepl("imd", obs$metric)),
      "an IMD crime metric reached the unified export. It is validation-only ",
      "and must never enter analysis (SOURCES.md, IMD section).")

# Crime counts, including the years with no denominator, come from the rates
# table rather than the long export : issue 1.9 wants 2025+ counts present but
# flagged, and a count is not a rate.
rates_path <- file.path(PROC_DIR, "crime_rates_by_borough_year.csv")
check(file.exists(rates_path), "run 02_population_and_rates.R first.")
rates <- fread(rates_path,
               colClasses = list(character = c("borough_gss", "borough_name")),
               showProgress = FALSE)
counts <- rates[, .(borough_gss, borough_name, year, metric = "crime_count",
                    value = as.numeric(crimes),
                    source = "UK Police street-level crime (Met + City of London)",
                    geography_native = "LSOA aggregated to borough",
                    notes = paste0("calendar year; ", months_present,
                                   " months of data"),
                    source_file = "crime_rates_by_borough_year.csv")]
obs <- rbind(obs, counts)

# Which years are not a full twelve months : derived, never hardcoded.
partial_years <- sort(rates[months_present < 12L, unique(year)])

# ---- Registry conformance ------------------------------------------------
unknown <- setdiff(unique(obs$metric), METRIC_META$metric)
check(!length(unknown),
      "metric(s) with no METRIC_META entry: ", paste(unknown, collapse = ", "),
      ".\n       Add them above with cadence, direction, scale and year rule. ",
      "Do not let a default apply : 'higher is better' is wrong for anxiety, ",
      "crime and every IMD domain.")
unused <- setdiff(METRIC_META$metric, unique(obs$metric))
if (length(unused)) {
  message("Registry entries with no data (not yet produced): ",
          paste(unused, collapse = ", "))
}
ok("every metric present has registry metadata")

# ---- Window --------------------------------------------------------------
# Cross-metric analysis is bounded by the shortest denominator. Crime counts
# run past it, flagged, so a trend can be shown without implying comparability.
in_window <- obs[metric == "crime_count" |
                   (year >= ANALYSIS_START & year <= TREND_END)]
trimmed <- obs[!(metric == "crime_count") &
                 (year < ANALYSIS_START | year > TREND_END),
               .(rows = .N, years = paste0(min(year), "-", max(year))),
               by = metric]
if (nrow(trimmed)) {
  message("\nTrimmed to the ", ANALYSIS_START, "-", TREND_END, " window ",
          "(available in source, outside the window):")
  print(trimmed)
}
gone <- setdiff(unique(obs$metric), unique(in_window$metric))
check(!length(gone),
      "the ", ANALYSIS_START, "-", TREND_END, " window removes every ",
      "observation of: ", paste(gone, collapse = ", "),
      ".\n       A snapshot metric whose only years fall outside the window ",
      "vanishes silently otherwise. Widen the window in _config.R or drop ",
      "the metric deliberately.")
obs <- in_window

# ---- Validation ----------------------------------------------------------
boroughs <- unique(fread(LOOKUP_OUT, colClasses = "character",
                         showProgress = FALSE)[, .(borough_gss, borough_name)])
setorder(boroughs, borough_name)

check(!anyNA(obs$value), sum(is.na(obs$value)), " observations have no value.")
check(!anyNA(obs$borough_gss), "observations with an NA borough code.")
check(setequal(unique(obs$borough_gss), boroughs$borough_gss),
      "the union of all metrics covers ", uniqueN(obs$borough_gss),
      " boroughs, expected ", LONDON_BOROUGH_N, ".")
ok("union of all metrics covers all ", LONDON_BOROUGH_N, " boroughs")

obs <- merge(obs, METRIC_META[, .(metric, scale)], by = "metric")
for (s in unique(obs$scale)) {
  rng <- SCALE_RANGE[[s]]
  check(!is.null(rng), "no range defined for scale '", s, "'.")
  bad <- obs[scale == s & (value < rng[1] | value > rng[2])]
  check(nrow(bad) == 0L,
        nrow(bad), " value(s) outside the '", s, "' range [", rng[1], ", ",
        rng[2], "], e.g. ", bad[1, paste0(metric, " ", borough_name, " ",
                                          year, " = ", value)])
}
ok("all values inside their scale's range")
obs[, scale := NULL]

# ---- Coverage matrix -----------------------------------------------------
cov_rows <- obs[, .(years = list(sort(unique(year))),
                    boroughs = list(sort(unique(borough_gss))),
                    n_obs = .N), by = metric]
cov_rows <- merge(METRIC_META, cov_rows, by = "metric")

metrics_json <- lapply(seq_len(nrow(cov_rows)), function(i) {
  r <- cov_rows[i]
  have <- r$boroughs[[1]]
  miss <- setdiff(boroughs$borough_gss, have)
  yrs <- r$years[[1]]
  list(
    label = r$label, cadence = r$cadence, direction = r$direction,
    scale = r$scale, unit = r$unit, year_rule = r$year_rule,
    # I() forces a JSON array. Without it auto_unbox turns a one-element
    # vector into a bare scalar, so `partial_years` would be [2026] for one
    # year and 2026 for another : a contract every client would get wrong.
    years = I(yrs),
    partial_years = I(if (r$metric == "crime_count")
      intersect(partial_years, yrs) else integer(0)),
    boroughs_covered = length(have),
    boroughs_missing = I(if (length(miss))
      lapply(miss, function(g)
        list(gss = g, name = boroughs[borough_gss == g, borough_name]))
      else list()),
    observations = r$n_obs,
    source = obs[metric == r$metric, source[1]]
  )
})
names(metrics_json) <- cov_rows$metric

stamp <- format(Sys.time(), tz = "UTC", "%Y-%m-%dT%H:%M:%SZ")
window_json <- list(
  analysis_start = ANALYSIS_START, analysis_end = ANALYSIS_END,
  trend_end = TREND_END,
  note = paste0("Cross-metric analysis is bounded by the shortest ",
                "denominator. crime_count runs past it; years listed in ",
                "partial_years are not twelve months and must never be ",
                "compared year-on-year.")
)

coverage <- list(generated_utc = stamp, window = window_json,
                 boroughs = I(lapply(seq_len(nrow(boroughs)), function(i)
                   list(gss = boroughs$borough_gss[i],
                        name = boroughs$borough_name[i]))),
                 metrics = metrics_json)

write_json_out <- function(x, path) {
  assert_not_raw_data(dirname(path)); ensure_dir(dirname(path))
  write_json(x, path, auto_unbox = TRUE, digits = NA, null = "null")
  kb <- file.size(path) / 1024
  message("  ->  ", path, "  (", format(round(kb, 1), nsmall = 1), " KB)")
  file.size(path)
}

cov_bytes <- write_json_out(coverage, file.path(PROC_DIR, "coverage.json"))

# ---- Observations --------------------------------------------------------
setorder(obs, borough_name, metric, year)
payload <- list(
  generated_utc = stamp,
  window = window_json,
  schema = I(c("borough_gss", "year", "metric", "value")),
  note = "Per-metric meaning, direction, scale and coverage live in coverage.json.",
  boroughs = I(lapply(seq_len(nrow(boroughs)), function(i)
    list(gss = boroughs$borough_gss[i], name = boroughs$borough_name[i]))),
  observations = obs[, .(borough_gss, year, metric, value)]
)
b_bytes <- write_json_out(payload, file.path(PROC_DIR, "boroughs.json"))

# ---- Self-consistency ----------------------------------------------------
back <- fromJSON(file.path(PROC_DIR, "boroughs.json"))
back_obs <- as.data.table(back$observations)
check(nrow(back_obs) == nrow(obs),
      "boroughs.json round-trips to ", nrow(back_obs), " rows, wrote ",
      nrow(obs), ".")
cov_back <- fromJSON(file.path(PROC_DIR, "coverage.json"))
for (m in names(cov_back$metrics)) {
  declared <- sort(as.integer(cov_back$metrics[[m]]$years))
  actual <- sort(unique(back_obs[metric == m, year]))
  check(identical(declared, as.integer(actual)),
        "coverage.json declares years for '", m,
        "' that do not match boroughs.json.")
}
ok("coverage.json and boroughs.json agree on every metric's years")

# Array-shape contract: a one-element list must still serialise as an array.
shape <- fromJSON(file.path(PROC_DIR, "coverage.json"), simplifyVector = FALSE)
for (m in names(shape$metrics)) {
  for (fld in c("years", "partial_years", "boroughs_missing")) {
    check(is.list(shape$metrics[[m]][[fld]]),
          "coverage.json field '", fld, "' for metric '", m,
          "' serialised as a scalar, not an array. Clients would break on ",
          "the single-element case.")
  }
}
check(is.list(shape$boroughs) && is.list(shape$window) == FALSE ||
        is.list(shape$boroughs),
      "coverage.json 'boroughs' is not an array.")
ok("coverage.json array fields keep their shape at length 1")

check(b_bytes < 1024^2,
      "boroughs.json is ", round(b_bytes / 1024^2, 2),
      " MB, over the 1 MB budget in issue 1.9.")
ok("combined export ", round((b_bytes + cov_bytes) / 1024, 1), " KB, under 1 MB")

message("\n", nrow(obs), " observations, ", uniqueN(obs$metric), " metrics, ",
        ANALYSIS_START, "-", max(obs$year), ".")
print(cov_rows[, .(metric, cadence, direction,
                   years = vapply(years, function(y)
                     paste0(min(y), "-", max(y)), character(1)),
                   boroughs = vapply(boroughs, length, integer(1)))])
