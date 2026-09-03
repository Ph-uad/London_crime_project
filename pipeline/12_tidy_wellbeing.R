# =============================================================
# 12_tidy_wellbeing.R : ONS4 personal well-being to the common long schema.
# Implements plan issue 1.7.
#
# Source shape: the ONS "v4" cube. One row per
# (period x geography x measure x estimate-type). We take the
# `average-mean` estimate only : the other four estimate types (poor, fair,
# good, very good) are the proportion of people in each rating band, not the
# borough's average score.
#
# Two things that must not be lost downstream:
#
#   ANXIETY RUNS THE OTHER WAY. Higher life satisfaction, worthwhile and
#   happiness are better; higher anxiety is worse. The direction is carried in
#   `notes` and in coverage.json so a choropleth cannot colour all four the
#   same way by accident.
#
#   CITY OF LONDON HAS NO DATA. Every one of its 48 borough-year-measure cells
#   is marked [u] : sample too small to publish. It is declared as a permitted
#   absence rather than dropped quietly.
#
# The ward well-being bundle plays no part here (see SOURCES.md, "Retired").
# Nothing in this script aggregates wards, so the population-weighting trap
# issue 1.7 warns about does not arise.
#
# Writes data/processed/metrics_wellbeing.csv
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("12_tidy_wellbeing")

check(file.exists(WELLBEING_RAW),
      "well-being file not found at '", WELLBEING_RAW,
      "'. Run 00_download_metrics.R first.")
check(file.exists(LOOKUP_OUT), "run 00_LSAOlookup.R first.")
boroughs <- unique(fread(LOOKUP_OUT, colClasses = "character",
                         showProgress = FALSE)[, .(borough_gss, borough_name)])

raw <- fread(WELLBEING_RAW, colClasses = "character", showProgress = FALSE)

NEEDED <- c("v4_3", "Data marking", "Lower limit", "Upper limit", "yyyy-yy",
            "administrative-geography", "Geography", "MeasureOfWellbeing",
            "wellbeing-estimate")
missing_cols <- setdiff(NEEDED, names(raw))
check(!length(missing_cols),
      "the ONS cube layout has changed : missing column(s): ",
      paste(missing_cols, collapse = ", "), ".\n       Found: ",
      paste(names(raw), collapse = ", "))

# Higher is better for three of the four. Anxiety is the exception.
DIRECTION <- c("Life satisfaction" = "higher_is_better",
               "Worthwhile"        = "higher_is_better",
               "Happiness"         = "higher_is_better",
               "Anxiety"           = "higher_is_worse")
METRIC <- c("Life satisfaction" = "wellbeing_life_satisfaction",
            "Worthwhile"        = "wellbeing_worthwhile",
            "Happiness"         = "wellbeing_happiness",
            "Anxiety"           = "wellbeing_anxiety")

d <- raw[`wellbeing-estimate` == "average-mean" &
           grepl(LONDON_GSS_PREFIX, `administrative-geography`)]
check(nrow(d) > 0L,
      "no London rows with estimate 'average-mean'. Estimate types present: ",
      paste(unique(raw$`wellbeing-estimate`), collapse = ", "))

found <- sort(unique(d$MeasureOfWellbeing))
check(setequal(found, names(METRIC)),
      "unexpected well-being measures. Expected ",
      paste(names(METRIC), collapse = ", "), "; found ",
      paste(found, collapse = ", "))

setnames(d, c("administrative-geography", "yyyy-yy", "Data marking"),
         c("borough_gss", "period", "marking"))
d[, value := as.numeric(v4_3)]
d[, lo := suppressWarnings(as.numeric(`Lower limit`))]
d[, hi := suppressWarnings(as.numeric(`Upper limit`))]

# Financial year -> START year, matching the income rule in 10_tidy_income.R.
# See SOURCES.md; this deliberately differs from the life-expectancy rule.
d[, year := as.integer(substr(period, 1L, 4L))]

# Suppressed cells are recorded and dropped, never imputed.
suppressed <- d[is.na(value)]
if (nrow(suppressed)) {
  by_area <- suppressed[, .(cells = .N, years = uniqueN(year),
                            markings = paste(sort(unique(marking)),
                                             collapse = " ")),
                        by = .(borough_gss, Geography)][order(-cells)]
  message("Suppressed cells (dropped, not imputed): ", nrow(suppressed))
  print(by_area)
  write_log(suppressed[, .(borough_gss, Geography, period, MeasureOfWellbeing,
                           marking)],
            "wellbeing_suppressed.log")
}
d <- d[!is.na(value)]

d <- merge(d, boroughs, by = "borough_gss")
CITY_OF_LONDON <- "E09000001"
absent <- assert_metric_boroughs(
  d$borough_gss, "well-being", boroughs$borough_gss,
  allow_missing = CITY_OF_LONDON,
  reason = "ONS marks every City of London cell [u]: sample too small to publish"
)

long <- d[, .(borough_gss, borough_name, year,
              metric = unname(METRIC[MeasureOfWellbeing]),
              value,
              source = paste0("ONS personal well-being estimates by local ",
                              "authority (time series v4)"),
              geography_native = "borough",
              notes = paste0(MeasureOfWellbeing, ", mean rating 0-10, ",
                             "financial year ", period,
                             " assigned to start year; ",
                             unname(DIRECTION[MeasureOfWellbeing]),
                             "; 95% CI ", lo, "-", hi))]

check(long[, all(value >= 0 & value <= 10)],
      "well-being mean outside the 0-10 rating scale: ",
      long[value < 0 | value > 10][1, paste0(metric, " = ", value)])

expected_boroughs <- LONDON_BOROUGH_N - length(absent)
per_year <- long[, .N, by = .(year, metric)]
check(per_year[, all(N == expected_boroughs)],
      "not every measure-year has ", expected_boroughs, " boroughs. Worst: ",
      per_year[which.min(N), paste0(metric, " ", year, " has ", N)])
ok(uniqueN(long$metric), " measures x ", uniqueN(long$year), " years x ",
   expected_boroughs, " boroughs")
message("Coverage: ", min(long$year), "-", max(long$year),
        " (financial years ", min(d$period), " to ", max(d$period), ")")

setorder(long, borough_name, year, metric)
write_out(as_long(long), file.path(PROC_DIR, "metrics_wellbeing.csv"))
