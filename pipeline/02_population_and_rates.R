# =============================================================
# 02_population_and_rates.R : borough population and crime rates per 1,000.
#
# Replaces pipeline/dimension/01_LSOA_by_population.R (retired to
# experimental/), which could not run: it called an undefined `tr()`, reused
# a disconnected Spark connection, referenced an undefined object, had an
# assignment swallowed into a comment, and ran tidyr::pivot_longer on a
# tbl_spark. Its output columns were also mislabelled : `lsoa_code` held a
# borough GSS code and `lsoa_name` held the string "London Borough".
#
# Boroughs are matched on GSS code, not on name. Name matching silently
# drops a borough the day a source spells one differently.
#
# Writes data/processed/borough_population.csv
#        data/processed/crime_rates_by_borough_year.csv
#        data/processed/metrics_crime.csv   (common long schema)
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("02_population_and_rates")

check(file.exists(POP_RAW), "population file not found at '", POP_RAW, "'.")
check(file.exists(LOOKUP_OUT), "run 00_LSAOlookup.R first.")

lookup <- fread(LOOKUP_OUT, colClasses = "character", showProgress = FALSE)
boroughs <- unique(lookup[, .(borough_gss, borough_name)])

# ---- Population ----------------------------------------------------------
# Find the header row rather than hardcoding a skip count: ONS moves the
# preamble between releases, and a wrong skip shifts every column silently.
# readLines rather than fread : the preamble rows are ragged, and fread has to
# commit to a column count before it has seen the real header.
peek <- readLines(POP_RAW, n = 40L, warn = FALSE)
hdr <- which(grepl('^"?Code"?\\s*,', peek))[1]
check(!is.na(hdr),
      "could not find the header row (first field 'Code') in ", POP_RAW,
      ". First lines:\n       ", paste(head(peek, 3), collapse = "\n       "))

pop <- fread(POP_RAW, skip = hdr - 1L, header = TRUE, colClasses = "character",
             showProgress = FALSE)
setnames(pop, trimws(names(pop)))
year_cols <- grep("^Mid[- ]?\\d{4}$", names(pop), value = TRUE)
check(length(year_cols) > 0L,
      "no 'Mid-YYYY' columns found in ", POP_RAW,
      ". Columns: ", paste(head(names(pop), 8), collapse = ", "))

pop <- pop[grepl(LONDON_GSS_PREFIX, Code)]
pop_long <- melt(pop, id.vars = "Code", measure.vars = year_cols,
                 variable.name = "year_col", value.name = "population")
pop_long[, year := sub("^Mid[- ]?", "", as.character(year_col))]
pop_long[, population := as.numeric(gsub("[, ]", "", population))]
pop_long[, year_col := NULL]
setnames(pop_long, "Code", "borough_gss")

pop_long <- merge(pop_long, boroughs, by = "borough_gss", all.x = TRUE)
assert_london_boroughs(pop_long$borough_gss, "population")
check(!anyNA(pop_long$population),
      sum(is.na(pop_long$population)), " borough-years have no population ",
      "estimate after parsing. Check the thousands separators in ", POP_RAW, ".")
check(pop_long[, all(population > 0)], "population contains non-positive values.")
ok("population parsed for ", uniqueN(pop_long$year), " years")

setcolorder(pop_long, c("borough_gss", "borough_name", "year", "population"))
setorder(pop_long, borough_name, year)
write_out(pop_long, file.path(PROC_DIR, "borough_population.csv"))

# ---- Year completeness, derived from the files (never hardcoded) ---------
months <- data.table(month = month_of(crime_files()))
months[, year := substr(month, 1L, 4L)]
year_cov <- months[, .(months_present = uniqueN(month)), by = year][order(year)]
year_cov[, complete_year := months_present == 12L]

# ---- Rates ---------------------------------------------------------------
crime_path <- file.path(PROC_DIR, "crime_by_borough_year.csv")
check(file.exists(crime_path), "run 01_crime_by_borough.R first.")
crime <- fread(crime_path, colClasses = list(character = c("borough_gss",
                                                           "borough_name",
                                                           "year")),
               showProgress = FALSE)

rates <- merge(crime, pop_long[, .(borough_gss, year, population)],
               by = c("borough_gss", "year"), all.x = TRUE)
rates <- merge(rates, year_cov, by = "year", all.x = TRUE)

rates[, crime_rate_per_1000 := round(crimes / population * 1000, 2)]
rates[, coverage_flag := fifelse(is.na(population), "no_denominator",
                          fifelse(!complete_year, "partial_year", "complete"))]
# A rate computed from a partial year is not a rate. Keep the count, drop the
# rate, and say why : rather than publishing a number that reads as a fall.
rates[coverage_flag != "complete", crime_rate_per_1000 := NA_real_]

assert_london_boroughs(rates$borough_gss, "crime rates")
check(rates[, sum(crimes)] == crime[, sum(crimes)],
      "the population join changed the crime total from ",
      format(crime[, sum(crimes)], big.mark = ","), " to ",
      format(rates[, sum(crimes)], big.mark = ","), ".")
check(rates[!is.na(crime_rate_per_1000), all(crime_rate_per_1000 > 0)],
      "non-positive crime rate produced.")
ok("rates computed; crime totals preserved through the join")

setcolorder(rates, c("borough_gss", "borough_name", "year", "crimes",
                     "population", "crime_rate_per_1000", "months_present",
                     "coverage_flag"))
rates[, complete_year := NULL]
setorder(rates, borough_name, year)
write_out(rates, file.path(PROC_DIR, "crime_rates_by_borough_year.csv"))

message("\nYears by coverage flag:")
print(rates[, .(boroughs = .N), by = .(year, coverage_flag)][order(year)])

# ---- Common long schema (feeds issue 1.9) --------------------------------
long <- rates[!is.na(crime_rate_per_1000),
              .(borough_gss, borough_name, year,
                metric = "crime_rate_per_1000",
                value  = crime_rate_per_1000,
                source = "UK Police street-level crime (Met + City of London)",
                geography_native = "LSOA aggregated to borough",
                notes  = paste0("calendar year; denominator = ONS mid-year ",
                                "estimate; ", months_present, " months of data"))]
write_out(as_long(long), file.path(PROC_DIR, "metrics_crime.csv"))
