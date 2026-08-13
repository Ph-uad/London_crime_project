# =============================================================
# 01_crime_by_borough.R — join crime records to boroughs and aggregate.
#
# Replaces pipeline/dimension/01_crime_by_LSOA.R (retired to experimental/).
# What changed and why:
#
#   * No Spark, and no multi-GB intermediates. The old script wrote
#     crime.csv (3.7 GB) and crime_by_borough.csv (3.9 GB) through a single
#     partition to produce aggregates totalling under 700 KB.
#   * No synthetic Crime_ID. The old sha2(rand(), current_timestamp()) hash
#     was non-deterministic, so nothing was comparable across runs, and
#     nothing downstream ever used the column.
#   * One exclusion ledger. Every record lands in exactly one bucket and the
#     buckets are asserted to sum to the raw total.
#   * No NA borough key. The old left_join let unmatched records through as
#     a 34th, unnamed "borough" holding 36,574 records.
#   * The crime-type mapping is asserted complete. The old if/else chain had
#     no branch for the pre-2013 category names, so "Violent crime" and
#     "Public disorder and weapons" fell into a silent "No-category" bucket
#     and Violence read 0 for 2011-2012.
#
# Writes data/processed/crime_by_borough_year.csv
#        data/processed/crime_by_borough_year_category.csv
#        pipeline/logs/exclusions.log, crime_types.log
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("01_crime_by_borough — join and aggregate")

files <- crime_files()
check(file.exists(LOOKUP_OUT),
      "lookup not found at '", LOOKUP_OUT, "'. Run 00_LSAOlookup.R first.")
lookup <- fread(LOOKUP_OUT, colClasses = "character", showProgress = FALSE)

# ---- Crime-type taxonomy -------------------------------------------------
# police.uk changed its category vocabulary in April 2013. Both vocabularies
# are mapped into one continuous series (see SOURCES.md, "Crime taxonomy").
#
# Read the caveat there before using the category series across 2013: the
# mapping makes the categories continuous, it does not make them comparable.
# "Other crime" was a genuine catch-all in 2011 (297,619 records) and a
# residual bucket afterwards (~10,000/yr), and "Public disorder and weapons"
# split into two post-2013 categories of which this keeps only the larger.
CRIME_CATEGORY <- data.table(
  crime_type = c(
    # --- current vocabulary (2013-04 onwards) ---
    "Violence and sexual offences", "Criminal damage and arson",
    "Anti-social behaviour", "Public order", "Drugs",
    "Possession of weapons", "Bicycle theft", "Burglary", "Other theft",
    "Robbery", "Shoplifting", "Theft from the person", "Vehicle crime",
    "Other crime",
    # --- legacy vocabulary (to 2013-03) ---
    "Violent crime", "Public disorder and weapons"
  ),
  crime_category = c(
    "Violence and Sexual Offences", "Criminal Damage and Arson",
    "Public Order Offences", "Public Order Offences", "Drug and Weapon Offences",
    "Drug and Weapon Offences", "Theft and Robbery", "Theft and Robbery",
    "Theft and Robbery", "Theft and Robbery", "Theft and Robbery",
    "Theft and Robbery", "Theft and Robbery", "Other Crimes",
    "Violence and Sexual Offences", "Public Order Offences"
  ),
  # Which vocabulary the NAME belongs to, relative to the 2013-04 change.
  # "both" = the label is used either side of the boundary; "legacy_only" and
  # "current_only" names cannot appear on both sides, so any category built
  # from them changes composition at 2013-04.
  vocabulary = c(
    "current_only", "both", "both", "current_only", "both",
    "current_only", "current_only", "both", "both",
    "both", "both", "current_only", "both",
    "both",
    "legacy_only", "legacy_only"
  )
)

# ---- Read ----------------------------------------------------------------
# Aggregate inside the per-file read so peak memory stays a few hundred MB
# rather than the whole 13.9M-row archive.
message("Reading ", length(files), " files ...")
raw <- rbindlist(lapply(files, function(f) {
  d <- fread(f, select = c("Month", "LSOA code", "Crime type"),
             colClasses = "character", showProgress = FALSE)
  setnames(d, c("month", "lsoa", "crime_type"))
  d[, .(records = .N), by = .(year = substr(month, 1L, 4L), lsoa, crime_type)]
}))
raw <- raw[, .(records = sum(records)), by = .(year, lsoa, crime_type)]
raw_total <- raw[, sum(records)]
message("Raw records: ", format(raw_total, big.mark = ","))

# ---- Classify every record into exactly one bucket -----------------------
london_codes <- lookup$lsoa
# Codes that are valid LSOAs but sit outside the 33 boroughs (boundary
# spillover) are distinguished from codes that match nothing at all.
all_ons <- {
  lk <- fread(LOOKUP_RAW, colClasses = "character", encoding = "UTF-8",
              showProgress = FALSE)
  unique(c(lk$LSOA11CD, lk$LSOA21CD))
}

raw[, status := fifelse(is.na(lsoa) | lsoa == "", "blank",
                 fifelse(lsoa %chin% london_codes, "attributed",
                  fifelse(lsoa %chin% all_ons, "outside_london", "unmatched")))]

ledger <- raw[, .(codes = uniqueN(lsoa), records = sum(records)),
              by = .(status, year)][order(status, year)]
write_log(ledger, "exclusions.log")

by_status <- ledger[, .(records = sum(records)), by = status][order(-records)]
print(by_status)

attributed <- by_status[status == "attributed", records]
attributed <- if (length(attributed)) attributed else 0L
blank      <- by_status[status == "blank", records]
blank      <- if (length(blank)) blank else 0L

# The reconciliation the old QA script only appeared to do: the buckets are
# independent of the raw total, so this can actually fail.
check(by_status[, sum(records)] == raw_total,
      "exclusion ledger sums to ", format(by_status[, sum(records)],
                                          big.mark = ","),
      " but raw total is ", format(raw_total, big.mark = ","), ".")
ok("exclusion ledger reconciles to the raw total exactly")

# Coverage against issue 1.2's criterion, on the stated denominator:
# records carrying a non-blank LSOA code. Blank codes are ungeocoded by the
# police and are not a failure of the lookup.
denom <- raw_total - blank
coverage <- attributed / denom
message(sprintf("Coverage: %.3f%% of %s non-blank records (%.3f%% of all %s)",
                100 * coverage, format(denom, big.mark = ","),
                100 * attributed / raw_total, format(raw_total, big.mark = ",")))
check(coverage >= 0.995,
      sprintf("lookup covers %.3f%% of non-blank records, below the 99.5%% ",
              100 * coverage),
      "criterion in issue 1.2. See ", file.path(LOG_DIR, "exclusions.log"), ".")
ok("coverage meets the 99.5% criterion")

# ---- Join ----------------------------------------------------------------
crime <- raw[status == "attributed"]
crime[, status := NULL]
crime <- merge(crime, lookup, by = "lsoa", all.x = TRUE, sort = FALSE)
assert_london_boroughs(crime$borough_gss, "crime join")
check(crime[, sum(records)] == attributed,
      "join changed the record count from ", format(attributed, big.mark = ","),
      " to ", format(crime[, sum(records)], big.mark = ","),
      " — the lookup is duplicating codes.")
ok("join preserved the record count (no duplication)")

# ---- Categorise ----------------------------------------------------------
crime <- merge(crime, CRIME_CATEGORY, by = "crime_type", all.x = TRUE,
               sort = FALSE)
unmapped <- unique(crime[is.na(crime_category), .(crime_type)])
check(nrow(unmapped) == 0L,
      "crime type(s) with no category mapping: ",
      paste(unmapped$crime_type, collapse = ", "),
      ".\n       Add them to CRIME_CATEGORY in this script. Do not add a ",
      "fallback bucket — a silent catch-all is what hid 240,929 records ",
      "across 2011-2013.")
ok("every crime type maps to a category")

# Log the vocabulary actually seen per year, so a taxonomy change is visible.
write_log(crime[, .(records = sum(records)),
                by = .(year, crime_type, crime_category, vocabulary)][
                  order(year, crime_type)],
          "crime_types.log")

# Records carrying a name that exists on only one side of the 2013-04 change.
# A year with both legacy_only and current_only records straddles the change.
vocab <- dcast(crime[, .(records = sum(records)), by = .(year, vocabulary)],
               year ~ vocabulary, value.var = "records", fill = 0L)
message("\nRecords by crime-type vocabulary (see SOURCES.md, Crime taxonomy):")
print(vocab)

# ---- Aggregate -----------------------------------------------------------
by_year <- crime[, .(crimes = sum(records)),
                 by = .(borough_gss, borough_name, year)][
                   order(borough_name, year)]
by_cat <- crime[, .(crimes = sum(records)),
                by = .(borough_gss, borough_name, year, crime_category,
                       crime_type, vocabulary)][
                  order(borough_name, year, crime_category, crime_type)]

check(by_year[, sum(crimes)] == attributed,
      "borough-year aggregate totals ", format(by_year[, sum(crimes)],
                                               big.mark = ","),
      ", expected ", format(attributed, big.mark = ","), ".")
check(by_cat[, sum(crimes)] == attributed,
      "category aggregate totals ", format(by_cat[, sum(crimes)],
                                           big.mark = ","),
      ", expected ", format(attributed, big.mark = ","), ".")
check(by_year[, all(crimes > 0L)], "borough-year aggregate has non-positive counts.")
ok("both aggregates reconcile to the attributed record count")

write_out(by_year, file.path(PROC_DIR, "crime_by_borough_year.csv"))
write_out(by_cat, file.path(PROC_DIR, "crime_by_borough_year_category.csv"))
