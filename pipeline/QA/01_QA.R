# =============================================================
# QA/01_QA.R : cross-script reconciliation and schema conformance.
#
# The previous version could not fail. It computed
#   aggregated_total <- sum of group counts over rows already filtered to
#                       !is.na(LAD22CD)
#   difference       <- aggregated_total - matched_rows
# which is a quantity minus itself, then asserted the difference was under
# 0.5%. dataQuality.log duly recorded "Difference 0" on every run. It also
# joined on LSOA21CD alone (producing a spurious 7.5% unmatched rate) and
# grouped by LSOA_name under the column alias `Borough`.
#
# This version compares quantities produced by DIFFERENT scripts reading the
# source independently : 00_crime_rowcounts.R counts raw file rows,
# 01_crime_by_borough.R classifies and aggregates them : so a disagreement
# is real information. Every check can fail, and a failure exits non-zero.
#
# Writes pipeline/logs/dataQuality.log
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("QA : reconciliation")

results <- data.table(check = character(), expected = character(),
                      actual = character(), status = character())
record <- function(name, expected, actual, pass) {
  results <<- rbind(results, data.table(
    check = name, expected = as.character(expected),
    actual = as.character(actual), status = if (pass) "PASS" else "FAIL"))
  message(if (pass) "  ok  " else "  FAIL ", name,
          if (pass) "" else paste0("  (expected ", expected,
                                   ", got ", actual, ")"))
  pass
}

need <- function(p, who) {
  check(file.exists(p), "missing '", p, "'. Run ", who, " first.")
  p
}

rowcounts <- fread(need(file.path(LOG_DIR, "rowcounts.log"),
                        "00_crime_rowcounts.R"), showProgress = FALSE)
ledger <- fread(need(file.path(LOG_DIR, "exclusions.log"),
                     "01_crime_by_borough.R"), showProgress = FALSE)
by_year <- fread(need(file.path(PROC_DIR, "crime_by_borough_year.csv"),
                      "01_crime_by_borough.R"),
                 colClasses = list(character = c("borough_gss", "borough_name",
                                                 "year")),
                 showProgress = FALSE)
rates <- fread(need(file.path(PROC_DIR, "crime_rates_by_borough_year.csv"),
                    "02_population_and_rates.R"),
               colClasses = list(character = c("borough_gss", "borough_name",
                                               "year")),
               showProgress = FALSE)

pass <- TRUE
`%||%` <- function(a, b) if (is.null(a)) b else a

# --- 1. Two independent reads of the raw archive must agree ---------------
raw_counted <- rowcounts[, sum(rows)]
raw_ledger  <- ledger[, sum(records)]
pass <- record("raw total: rowcounts vs exclusion ledger",
               format(raw_counted, big.mark = ","),
               format(raw_ledger, big.mark = ","),
               raw_counted == raw_ledger) && pass

# --- 2. Per-year agreement, which a single global total can hide ----------
rc_year <- rowcounts[, .(raw = sum(rows)), by = .(year = as.character(year))]
lg_year <- ledger[, .(ledger = sum(records)), by = .(year = as.character(year))]
yr <- merge(rc_year, lg_year, by = "year", all = TRUE)
bad_years <- yr[is.na(raw) | is.na(ledger) | raw != ledger]
pass <- record("raw total per year", "0 mismatched years",
               paste0(nrow(bad_years), " mismatched"),
               nrow(bad_years) == 0L) && pass
if (nrow(bad_years)) print(bad_years)

# --- 3. Attributed records must equal the aggregate ------------------------
attributed <- ledger[status == "attributed", sum(records)]
agg <- by_year[, sum(crimes)]
pass <- record("attributed records vs borough-year aggregate",
               format(attributed, big.mark = ","),
               format(agg, big.mark = ","), attributed == agg) && pass

# --- 4. Coverage against issue 1.2 ----------------------------------------
blank <- ledger[status == "blank", sum(records)]
blank <- if (length(blank) && !is.na(blank)) blank else 0L
coverage <- attributed / (raw_ledger - blank)
pass <- record("lookup coverage (non-blank denominator)", ">= 99.500%",
               sprintf("%.3f%%", 100 * coverage), coverage >= 0.995) && pass

# --- 5. Geography ---------------------------------------------------------
for (nm in c("crime_by_borough_year", "crime_rates_by_borough_year")) {
  d <- if (nm == "crime_by_borough_year") by_year else rates
  n <- uniqueN(d$borough_gss)
  pass <- record(paste0(nm, ": borough count"), LONDON_BOROUGH_N, n,
                 n == LONDON_BOROUGH_N && !anyNA(d$borough_gss)) && pass
}

# --- 6. Rates are only published for complete years -----------------------
leaked <- rates[coverage_flag != "complete" & !is.na(crime_rate_per_1000), .N]
pass <- record("no rate published for an incomplete year", 0, leaked,
               leaked == 0L) && pass
neg <- rates[!is.na(crime_rate_per_1000) & crime_rate_per_1000 <= 0, .N]
pass <- record("no non-positive crime rate", 0, neg, neg == 0L) && pass

# --- 7. Long-schema conformance of every metric export --------------------
metric_files <- list.files(PROC_DIR, pattern = "^(metrics_|imd_crime_).*\\.csv$",
                           full.names = TRUE)
pass <- record("metric export files present", ">= 1", length(metric_files),
               length(metric_files) > 0L) && pass
for (f in metric_files) {
  d <- fread(f, nrows = 5L, showProgress = FALSE)
  pass <- record(paste0(basename(f), ": long schema"),
                 paste(LONG_SCHEMA, collapse = ","),
                 paste(names(d), collapse = ","),
                 identical(names(d), LONG_SCHEMA)) && pass
  full <- fread(f, showProgress = FALSE)
  pass <- record(paste0(basename(f), ": no NA borough or value"), 0,
                 sum(is.na(full$borough_gss)) + sum(is.na(full$value)),
                 !anyNA(full$borough_gss) && !anyNA(full$value)) && pass
}

# --- 8. Unified export agrees with the metric CSVs ------------------------
bj <- file.path(PROC_DIR, "boroughs.json")
cj <- file.path(PROC_DIR, "coverage.json")
if (file.exists(bj) && file.exists(cj)) {
  suppressPackageStartupMessages(library(jsonlite))
  b <- fromJSON(bj); cv <- fromJSON(cj, simplifyVector = FALSE)
  bo <- as.data.table(b$observations)
  pass <- record("boroughs.json under the 1 MB budget", "< 1024 KB",
                 paste0(round(file.size(bj) / 1024), " KB"),
                 file.size(bj) < 1024^2) && pass
  pass <- record("boroughs.json covers all boroughs", LONDON_BOROUGH_N,
                 uniqueN(bo$borough_gss),
                 uniqueN(bo$borough_gss) == LONDON_BOROUGH_N) && pass
  pass <- record("every metric in boroughs.json is described in coverage.json",
                 0, length(setdiff(unique(bo$metric), names(cv$metrics))),
                 length(setdiff(unique(bo$metric), names(cv$metrics))) == 0L) && pass
  # A direction of "higher_is_better" silently applied to anxiety, crime or a
  # deprivation score is the single most consequential metadata error here.
  MUST_BE_WORSE <- c("wellbeing_anxiety", "crime_rate_per_1000", "crime_count")
  wrong <- Filter(function(m) !is.null(cv$metrics[[m]]) &&
                    !identical(cv$metrics[[m]]$direction, "higher_is_worse"),
                  MUST_BE_WORSE)
  pass <- record("anxiety and crime are marked higher_is_worse", 0,
                 length(wrong), length(wrong) == 0L) && pass
  imd_wrong <- Filter(function(m) grepl("^imd_", m) &&
                        !identical(cv$metrics[[m]]$cadence, "snapshot"),
                      names(cv$metrics))
  pass <- record("IMD metrics are marked snapshot, not annual", 0,
                 length(imd_wrong), length(imd_wrong) == 0L) && pass
  leaked_imd <- grep("^imd_crime", unique(bo$metric), value = TRUE)
  pass <- record("no IMD crime domain in the unified export", 0,
                 length(leaked_imd), length(leaked_imd) == 0L) && pass
} else {
  message("\nNote: boroughs.json / coverage.json not present : ",
          "run 20_unify_metrics.R to include them in QA.")
}

# --- 9. Borough boundaries ------------------------------------------------
gj <- file.path(PROC_DIR, "london.geojson")
if (file.exists(gj)) {
  suppressPackageStartupMessages(library(jsonlite))
  fc <- fromJSON(gj, simplifyVector = FALSE)
  feats <- fc$features
  gss <- vapply(feats, function(f) f$properties$borough_gss %||% NA_character_,
                character(1))
  pass <- record("london.geojson feature count", LONDON_BOROUGH_N,
                 length(feats), length(feats) == LONDON_BOROUGH_N) && pass
  pass <- record("london.geojson under the 500 KB budget", "< 500 KB",
                 paste0(round(file.size(gj) / 1024), " KB"),
                 file.size(gj) <= 512000) && pass
  if (exists("by_year")) {
    pass <- record("london.geojson GSS codes match the crime aggregate", 0,
                   length(setdiff(unique(by_year$borough_gss), gss)),
                   length(setdiff(unique(by_year$borough_gss), gss)) == 0L) && pass
  }
  # Coordinates must be WGS84 degrees. British National Grid eastings here
  # would render the map in the North Sea and nothing else would catch it.
  flat <- unlist(lapply(feats, function(f) f$geometry$coordinates))
  pass <- record("london.geojson coordinates are WGS84 degrees",
                 "within London bbox",
                 sprintf("%.1f..%.1f", min(flat), max(flat)),
                 min(flat) > -1 && max(flat) < 52) && pass
} else {
  message("\nNote: london.geojson not present : run 03_borough_boundaries.R ",
          "to include it in QA.")
}

# --- Superseded outputs (warning, not a failure) --------------------------
# The retired scripts wrote these. They are not regenerated by the current
# pipeline, so a stale copy on disk is a trap: it looks like current output.
SUPERSEDED <- c("crime_counts_by_year.csv", "crime_counts_by_crime_type.csv",
                "crime_counts_by_crime_subcategory_type.csv",
                "crime_type_by_year_and_population.csv", "london_population.csv",
                "IDMP_2015_n_2019.csv", "London_average_income.csv",
                "well_being_probabilitye.csv", "crime.csv",
                "crime_by_borough.csv")
stale <- SUPERSEDED[file.exists(file.path(PROC_DIR, SUPERSEDED))]
if (length(stale)) {
  message("\nWARNING: ", length(stale), " superseded output(s) still in ",
          PROC_DIR, ".\n  They were produced by the retired scripts and are ",
          "NOT regenerated by this pipeline.\n  Delete them so nothing ",
          "downstream reads a stale file:\n    ",
          paste(stale, collapse = "\n    "))
}

# --- Report ---------------------------------------------------------------
message("")
print(results[, .(status, check,
                  result = ifelse(status == "PASS", actual,
                                  paste0(actual, " (expected ", expected, ")")))],
      nrows = Inf)
write_log(results, "dataQuality.log")

failed <- results[status == "FAIL"]
if (nrow(failed)) {
  fail(nrow(failed), " of ", nrow(results), " QA checks failed. See ",
       file.path(LOG_DIR, "dataQuality.log"), ".")
}
ok("all ", nrow(results), " QA checks passed")
