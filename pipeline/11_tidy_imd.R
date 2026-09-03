# =============================================================
# 11_tidy_imd.R : IMD borough domain scores to the common long schema.
# Implements plan issue 1.6. Replaces the deprivation section of the retired
# 00_QOL_tidy.R, whose output (IDMP_2015_n_2019.csv, 94 columns) kept every
# rank and proportion column and, critically, kept the Crime domain.
#
# The IMD Crime domain is BUILT FROM recorded crime. Using it to explain
# crime rates is circular, so it is excluded from the analysis output and
# written to imd_crime_validation.csv instead, where it serves only as an
# external check on our police-derived rates.
#
# Ranks are excluded from analysis: IMD 2015 and IMD 2019 ranks are not
# methodologically comparable, so a change in rank between the two snapshots
# is not a change in deprivation. Average scores are retained.
#
# Writes data/processed/metrics_imd.csv
#        data/processed/imd_crime_validation.csv
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("11_tidy_imd")

check(file.exists(LOOKUP_OUT), "run 00_LSAOlookup.R first.")
boroughs <- unique(fread(LOOKUP_OUT, colClasses = "character",
                         showProgress = FALSE)[, .(borough_gss, borough_name)])

ANALYSIS_DOMAINS <- c("Income", "Employment", "Education, Skills and Training",
                      "Health Deprivation and Disability",
                      "Barriers to Housing and Services", "Living Environment")
CRIME_DOMAIN <- "Crime"

# IMD domain scores are NOT on a common scale, and two of them are legitimately
# negative. Anything that colours or ranks across domains has to know this.
#   proportion   Income, Employment            0-1
#   score        Education, Barriers, Living   roughly 0-100
#   standardised Health, Crime                 z-like, negative is less deprived
# The envelopes below are sanity bounds: a breach means a shifted column or a
# units change in the source, not a real borough.
DOMAIN_SCALE <- data.table(
  domain = c("Income", "Employment", "Education, Skills and Training",
             "Barriers to Housing and Services", "Living Environment",
             "Health Deprivation and Disability", "Crime"),
  scale_type = c("proportion", "proportion", "score", "score", "score",
                 "standardised", "standardised"),
  lo = c(0, 0, 0, 0, 0, -5, -5),
  hi = c(1, 1, 100, 100, 100, 5, 5)
)

read_snapshot <- function(year, path) {
  check(file.exists(path), "IMD ", year, " file not found at '", path, "'.")
  d <- fread(path, colClasses = "character", encoding = "UTF-8",
             showProgress = FALSE)
  setnames(d, trimws(names(d)))

  code_col <- grep("^Local Authority District code", names(d), value = TRUE)[1]
  check(!is.na(code_col),
        "no 'Local Authority District code' column in the IMD ", year, " file.")

  # Keep only "<Domain> - Average score". Everything else : average rank,
  # rank of average rank, proportion of LSOAs in the most deprived decile,
  # scale, and every rank thereof : is excluded from analysis.
  score_cols <- grep(" - Average score$", names(d), value = TRUE)
  check(length(score_cols) > 0L,
        "no '- Average score' columns found in the IMD ", year, " file.")

  long <- melt(d, id.vars = code_col, measure.vars = score_cols,
               variable.name = "column", value.name = "value")
  setnames(long, code_col, "borough_gss")
  long[, domain := trimws(sub(" - Average score$", "", as.character(column)))]
  long[, column := NULL]
  long[, borough_gss := trimws(borough_gss)]
  long[, value := as.numeric(value)]
  long[, year := as.integer(year)]
  long[!is.na(value) & grepl(LONDON_GSS_PREFIX, borough_gss)]
}

all_scores <- rbindlist(lapply(names(IMD_RAW),
                               function(y) read_snapshot(y, IMD_RAW[[y]])))

found <- sort(unique(all_scores$domain))
message("Domains found: ", paste(found, collapse = " | "))
missing <- setdiff(c(ANALYSIS_DOMAINS, CRIME_DOMAIN), found)
check(!length(missing),
      "expected IMD domain(s) not found: ", paste(missing, collapse = ", "),
      ". Found: ", paste(found, collapse = ", "))

# ---- Crime domain: validation only ---------------------------------------
crime_dom <- all_scores[domain == CRIME_DOMAIN]
crime_out <- merge(crime_dom, boroughs, by = "borough_gss")[
  , .(borough_gss, borough_name, year,
      metric = "imd_crime_score", value,
      source = "MHCLG Indices of Deprivation, borough domain summaries",
      geography_native = "borough (LSOA-derived)",
      notes = paste0("VALIDATION ONLY : excluded from analysis. The IMD ",
                     "Crime domain is constructed from recorded crime, so ",
                     "using it to explain crime rates is circular. ",
                     "Scale: standardised (negative = less deprived)."))]
assert_london_boroughs(crime_out$borough_gss, "IMD crime validation")
setorder(crime_out, borough_name, year)
write_out(as_long(crime_out), file.path(PROC_DIR, "imd_crime_validation.csv"))

# ---- Analysis domains ----------------------------------------------------
analysis <- all_scores[domain %in% ANALYSIS_DOMAINS]
check(!any(analysis$domain == CRIME_DOMAIN),
      "the Crime domain leaked into the analysis output.")

slug <- function(x) paste0("imd_", tolower(gsub("[^a-z0-9]+", "_",
                                                tolower(x))), "_score")
analysis <- merge(analysis, DOMAIN_SCALE, by = "domain")
out <- merge(analysis, boroughs, by = "borough_gss")[
  , .(borough_gss, borough_name, year,
      metric = slug(domain), value,
      source = "MHCLG Indices of Deprivation, borough domain summaries",
      geography_native = "borough (LSOA-derived)",
      notes = paste0(domain, " domain, average score. Scale: ", scale_type,
                     ". Snapshot year : not a time series. 2015 and 2019 ",
                     "ranks are not methodologically comparable; scores used ",
                     "instead."))]

assert_london_boroughs(out$borough_gss, "IMD analysis")
check(out[, .N, by = .(year, metric)][, all(N == LONDON_BOROUGH_N)],
      "not every domain-year has all ", LONDON_BOROUGH_N, " boroughs.")
expected_rows <- LONDON_BOROUGH_N * length(ANALYSIS_DOMAINS) * length(IMD_RAW)
check(nrow(out) == expected_rows,
      "expected ", expected_rows, " rows (", LONDON_BOROUGH_N, " boroughs x ",
      length(ANALYSIS_DOMAINS), " domains x ", length(IMD_RAW),
      " snapshots), got ", nrow(out), ".")
# Domain-aware range check. A blanket "value >= 0" would be wrong: the Health
# and Crime domains are standardised and are negative for half of London.
breach <- analysis[value < lo | value > hi | !is.finite(value)]
check(nrow(breach) == 0L,
      nrow(breach), " IMD score(s) outside the documented envelope for their ",
      "domain, e.g. ", breach[1, paste0(domain, " = ", value, " (expected ",
                                        lo, " to ", hi, ")")],
      ". This usually means a shifted column or a units change in the source.")
ok("all scores inside their documented per-domain envelope")
ok(length(ANALYSIS_DOMAINS), " domains x ", length(IMD_RAW),
   " snapshots x ", LONDON_BOROUGH_N, " boroughs; Crime domain excluded")

setorder(out, borough_name, year, metric)
write_out(as_long(out), file.path(PROC_DIR, "metrics_imd.csv"))
