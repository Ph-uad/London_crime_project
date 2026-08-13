# =============================================================
# 10_tidy_income.R — HMRC personal income to the common long schema.
# Implements plan issue 1.5. Replaces the income section of the retired
# 00_QOL_tidy.R, which could not reproduce its own committed output: it
# overwrote the column names with the label row and only then tried to read
# years out of those names, so the year vector was all-NA.
#
# Year convention: the source labels FINANCIAL years ("2011-12"). Each is
# assigned to its START year, so metric year 2011 means tax year 2011/12.
# This is recorded in SOURCES.md and must stay consistent with the
# life-expectancy rule in issue 1.8 before the two are paired.
#
# Median is the analysis metric — borough income is right-skewed and means
# are dragged by high earners. Mean and taxpayer counts are retained as
# supplementary rows, not dropped.
#
# Writes data/processed/metrics_income.csv
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("10_tidy_income")

check(file.exists(INCOME_RAW), "income file not found at '", INCOME_RAW, "'.")
check(file.exists(LOOKUP_OUT), "run 00_LSAOlookup.R first.")
boroughs <- unique(fread(LOOKUP_OUT, colClasses = "character",
                         showProgress = FALSE)[, .(borough_gss, borough_name)])

raw <- fread(INCOME_RAW, header = FALSE, colClasses = "character",
             fill = TRUE, encoding = "UTF-8", showProgress = FALSE)

label_row <- which(trimws(raw[[1]]) == "Code")[1]
check(!is.na(label_row) && label_row > 1L,
      "could not locate the 'Code' label row in ", INCOME_RAW, ".")

labels <- trimws(as.character(unlist(raw[label_row])))

# The financial-year label sits above the first of each 3-column block; carry
# it forward across the block. (The retired script used zoo::na.locf; base R
# covers it without the dependency.)
src <- trimws(as.character(unlist(raw[label_row - 1L])))
fy <- character(length(src))
last <- NA_character_
for (i in seq_along(src)) {
  if (nzchar(src[i])) last <- src[i]
  fy[i] <- last
}

spec <- data.table(idx = seq_along(labels), label = labels, fy = fy)
spec[, year := suppressWarnings(as.integer(sub("^((19|20)\\d{2}).*$", "\\1", fy)))]

# Match on the label stem only. The source writes "Mean £" / "Median £", and
# matching the currency symbol makes the join hostage to file encoding.
stem <- toupper(trimws(gsub("[^A-Za-z ]", "", labels)))
spec[, metric := fifelse(startsWith(stem, "NUMBER OF INDIVIDUALS"),
                         "income_taxpayers",
                  fifelse(startsWith(stem, "MEDIAN"), "income_median",
                   fifelse(startsWith(stem, "MEAN"), "income_mean",
                           NA_character_)))]

keep <- spec[!is.na(metric) & !is.na(year)]
drop <- spec[idx > 2L & (is.na(metric) | is.na(year))]
if (nrow(drop)) {
  message("Dropping ", nrow(drop), " artefact column(s): ",
          paste(unique(paste0(drop$label, " [", drop$fy, "]")), collapse = ", "))
  write_log(drop[, .(idx, label, fy)], "income_dropped_columns.log")
}
check(nrow(keep) > 0L, "no usable income columns identified.")

# Data begins after the label row; blank spacer rows are dropped.
dat <- raw[(label_row + 1L):.N]
setnames(dat, paste0("V", seq_len(ncol(dat))))
dat <- dat[trimws(V1) != ""]

long <- rbindlist(lapply(seq_len(nrow(keep)), function(i) {
  col <- paste0("V", keep$idx[i])
  data.table(borough_gss = trimws(dat[["V1"]]),
             raw_value   = dat[[col]],
             year        = keep$year[i],
             metric      = keep$metric[i])
}))

long[, value := as.numeric(gsub("[^0-9.-]", "", raw_value))]
long <- long[!is.na(value)]
long[, raw_value := NULL]

# Restrict to the 33 boroughs by GSS code. The source also carries London,
# regional and England totals; matching on code rather than name keeps those
# out without a name-normalisation step that could silently drop a borough.
long <- merge(long, boroughs, by = "borough_gss")
assert_london_boroughs(long$borough_gss, "income")

fy_lookup <- unique(keep[, .(year, fy)])
long <- merge(long, fy_lookup, by = "year")
long[, `:=`(
  source = "HMRC personal income by tax year (London Datastore)",
  geography_native = "borough",
  notes = paste0("financial year ", fy, " assigned to start year; ",
                 fifelse(metric == "income_median",
                         "analysis metric", "supplementary"))
)]
long[, fy := NULL]

years <- sort(unique(long$year))
gaps <- setdiff(seq(min(years), max(years)), years)
if (length(gaps)) {
  message("Survey years absent from the source (recorded, not interpolated): ",
          paste(gaps, collapse = ", "))
}

med <- long[metric == "income_median"]
check(nrow(med) > 0L, "no median income rows produced.")
check(med[, all(value > 0)], "non-positive median income value.")
check(med[year >= ANALYSIS_START,
          .N, by = year][, all(N == LONDON_BOROUGH_N)],
      "median income is not present for all ", LONDON_BOROUGH_N,
      " boroughs in every year from ", ANALYSIS_START, ".")
ok("median income complete for 33 boroughs, ",
   min(med$year), "-", max(med$year))

setorder(long, borough_name, year, metric)
write_out(as_long(long), file.path(PROC_DIR, "metrics_income.csv"))
