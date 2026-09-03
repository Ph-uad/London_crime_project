# =============================================================
# 13_tidy_life_expectancy.R : ONS life expectancy to the common long schema.
# Implements plan issue 1.8.
#
# PERIOD -> YEAR RULE: rolling three-year periods are assigned to their END
# year. "2022 to 2024" becomes metric year 2024, with the full period kept in
# `notes`. The alternative : midpoint year : was rejected because the end year
# is what a reader assumes when a dashboard says "2024", and because it keeps
# the most recent estimate at the most recent year rather than pushing it back.
#
# This DIFFERS from the income and well-being rule, which assigns a financial
# year to its start year. The two are not in conflict: one is a three-year
# rolling window, the other a twelve-month accounting year. The difference is
# recorded in SOURCES.md and must stay visible wherever metrics are paired :
# issue 3.6 prints the pairing on the chart for exactly this reason.
#
# Four metrics: {male, female} x {at birth, at age 65}. "At birth" is the
# headline measure; at-65 is carried because it is in the same table and is
# the more informative one for later-life health inequality.
#
# City of London is absent from the source entirely : too few residents for
# ONS to publish. Declared, not silently dropped.
#
# Reading a 36 MB xlsx needs `readxl`, the pipeline's only dependency beyond
# data.table and jsonlite. ONS publishes this table in no other format.
#
# Writes data/processed/metrics_life_expectancy.csv
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

if (!requireNamespace("readxl", quietly = TRUE)) {
  fail("package 'readxl' is required to read the ONS life-expectancy ",
       "workbook.\n       install.packages(\"readxl\")   # or: apt-get ",
       "install r-cran-readxl")
}

banner("13_tidy_life_expectancy")

check(file.exists(LIFEEXP_RAW),
      "life-expectancy file not found at '", LIFEEXP_RAW,
      "'. Run 00_download_metrics.R first.")
check(file.exists(LOOKUP_OUT), "run 00_LSAOlookup.R first.")
boroughs <- unique(fread(LOOKUP_OUT, colClasses = "character",
                         showProgress = FALSE)[, .(borough_gss, borough_name)])

# Sheet "1" is the tidy table; rows 1-5 are title and note lines, so the
# header is row 6. Located by content rather than hardcoded, because ONS
# adds and removes note lines between editions.
SHEET <- "1"
probe <- readxl::read_excel(LIFEEXP_RAW, sheet = SHEET, col_names = FALSE,
                            n_max = 20L, .name_repair = "minimal",
                            progress = FALSE)
hdr_row <- which(vapply(seq_len(nrow(probe)),
                        function(i) identical(as.character(probe[i, 1][[1]]),
                                              "Period"), logical(1)))[1]
check(!is.na(hdr_row),
      "could not find the header row (first cell 'Period') in sheet '", SHEET,
      "' of ", LIFEEXP_RAW, ".")

message("Reading sheet '", SHEET, "' (header at row ", hdr_row,
        ") : this takes a minute for a 36 MB workbook ...")
raw <- as.data.table(readxl::read_excel(
  LIFEEXP_RAW, sheet = SHEET, skip = hdr_row - 1L, col_types = "text",
  progress = FALSE))

NEEDED <- c("Period", "Area code", "Area name", "Sex", "Age group",
            "Life expectancy", "Lower confidence interval",
            "Upper confidence interval")
missing_cols <- setdiff(NEEDED, names(raw))
check(!length(missing_cols),
      "the ONS workbook layout has changed : missing column(s): ",
      paste(missing_cols, collapse = ", "), ".\n       Found: ",
      paste(names(raw), collapse = ", "))

# "<1" is life expectancy at birth; "65 to 69" is the at-65 figure.
AGE <- c("<1" = "birth", "65 to 69" = "65")
d <- raw[grepl(LONDON_GSS_PREFIX, `Area code`) & `Age group` %chin% names(AGE)]
check(nrow(d) > 0L,
      "no London rows for age groups ", paste(names(AGE), collapse = " / "),
      ". Age groups present: ",
      paste(head(sort(unique(raw$`Age group`)), 8), collapse = ", "))

check(setequal(unique(d$Sex), c("Male", "Female")),
      "expected Sex values Male and Female; found ",
      paste(unique(d$Sex), collapse = ", "))

setnames(d, c("Area code", "Life expectancy"), c("borough_gss", "le"))
d[, value := as.numeric(le)]
d[, lo := suppressWarnings(as.numeric(`Lower confidence interval`))]
d[, hi := suppressWarnings(as.numeric(`Upper confidence interval`))]

# --- Period -> end year ---------------------------------------------------
d[, end_year := suppressWarnings(as.integer(sub("^.*\\bto\\s+(\\d{4})$", "\\1",
                                                Period)))]
bad <- d[is.na(end_year), unique(Period)]
check(!length(bad),
      "could not read an end year from period label(s): ",
      paste(head(bad, 3), collapse = ", "),
      ". Expected the form 'YYYY to YYYY'.")
span <- d[, .(n = uniqueN(Period)), by = end_year][n > 1L]
check(nrow(span) == 0L,
      "two different periods map to the same end year (",
      span[1, end_year], "), so the assignment is not one-to-one.")
ok("period -> end year: ", d[, uniqueN(Period)], " periods, ",
   d[, min(end_year)], "-", d[, max(end_year)])

dropped <- d[is.na(value)]
if (nrow(dropped)) {
  message("Rows with no published figure (dropped, not imputed): ",
          nrow(dropped))
  write_log(dropped[, .(borough_gss, `Area name`, Period, Sex, `Age group`)],
            "life_expectancy_suppressed.log")
}
d <- d[!is.na(value)]

d <- merge(d, boroughs, by = "borough_gss")
CITY_OF_LONDON <- "E09000001"
absent <- assert_metric_boroughs(
  d$borough_gss, "life expectancy", boroughs$borough_gss,
  allow_missing = CITY_OF_LONDON,
  reason = "ONS does not publish life expectancy for City of London (~8,000 residents)"
)

long <- d[, .(borough_gss, borough_name, year = end_year,
              metric = paste0("life_expectancy_", AGE[`Age group`], "_",
                              tolower(Sex)),
              value,
              source = "ONS life expectancy for local areas of the UK",
              geography_native = "borough",
              notes = paste0("life expectancy at ",
                             fifelse(AGE[`Age group`] == "birth", "birth",
                                     "age 65"),
                             ", ", tolower(Sex), ", years; rolling period ",
                             Period, " assigned to END year; ",
                             "higher_is_better; 95% CI ", lo, "-", hi))]

check(long[, all(value > 0 & value < 120)],
      "implausible life expectancy value: ",
      long[value <= 0 | value >= 120][1, paste0(metric, " = ", value)])
# At-65 figures are remaining years of life, so they must be well below
# at-birth figures for the same borough-year. A silent age-group mix-up would
# not otherwise show up.
wide <- dcast(long, borough_gss + year ~ metric, value.var = "value")
if (all(c("life_expectancy_birth_male", "life_expectancy_65_male")
        %chin% names(wide))) {
  check(wide[!is.na(life_expectancy_birth_male) &
               !is.na(life_expectancy_65_male),
             all(life_expectancy_65_male < life_expectancy_birth_male)],
        "at-65 life expectancy is not below at-birth for every borough-year : ",
        "the age groups have been mixed up.")
  ok("at-65 figures are remaining-years, below at-birth as expected")
}

expected_boroughs <- LONDON_BOROUGH_N - length(absent)
per <- long[, .N, by = .(year, metric)]
check(per[, all(N == expected_boroughs)],
      "not every metric-year has ", expected_boroughs, " boroughs. Worst: ",
      per[which.min(N), paste0(metric, " ", year, " has ", N)])
ok(uniqueN(long$metric), " metrics x ", uniqueN(long$year), " years x ",
   expected_boroughs, " boroughs")
message("Coverage: end years ", min(long$year), "-", max(long$year),
        " (periods ", d[, min(Period)], " to ", d[, max(Period)], ")")

setorder(long, borough_name, year, metric)
write_out(as_long(long), file.path(PROC_DIR, "metrics_life_expectancy.csv"))
