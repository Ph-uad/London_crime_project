# =============================================================
# 00_LSOAlookup.R : build the LSOA -> borough lookup, harmonised across the
# 2011 and 2021 code vintages.
#
# Change from the previous version: this script no longer reads the crime
# files. It builds the complete London lookup from the ONS correspondence
# table alone, and record-level exclusion accounting moved to
# 01_crime_by_borough.R. Previously both scripts computed exclusions and
# reported different totals for the same join.
#
# Writes data/processed/lsoa_lookup.csv
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("00_LSOAlookup : borough lookup")

check(file.exists(LOOKUP_RAW),
      "ONS lookup not found at '", LOOKUP_RAW, "'. See pipeline/SOURCES.md.")

# encoding = "UTF-8" strips the byte-order mark; without it the first column
# reads as "﻿LSOA11CD" and every reference to LSOA11CD fails.
lk <- fread(LOOKUP_RAW, colClasses = "character", encoding = "UTF-8",
            showProgress = FALSE)

check(all(c("LSOA11CD", "LSOA21CD", "LAD22CD", "LAD22NM") %in% names(lk)),
      "lookup is missing expected columns. Found: ",
      paste(names(lk), collapse = ", "))

# One map covering BOTH code vintages. Crime records carry 2011-vintage codes
# for most of the window and 2021-vintage codes at the end; a single-vintage
# join silently loses whichever era it does not cover.
both <- unique(rbindlist(list(
  lk[, .(lsoa = LSOA11CD, borough_gss = LAD22CD, borough_name = LAD22NM)],
  lk[, .(lsoa = LSOA21CD, borough_gss = LAD22CD, borough_name = LAD22NM)]
)))[lsoa != "" & !is.na(lsoa)]

london <- both[grepl(LONDON_GSS_PREFIX, borough_gss)]

# A code appearing under two boroughs would duplicate every crime record
# joined to it. It does not happen inside London today (four such codes exist
# elsewhere in England and Wales), so assert it rather than assume it.
dupes <- london[, .N, by = lsoa][N > 1L]
check(nrow(dupes) == 0L,
      nrow(dupes), " LSOA code(s) map to more than one London borough, ",
      "e.g. ", paste(head(dupes$lsoa, 3), collapse = ", "),
      ". Joining on these would duplicate crime records.")
ok("every LSOA code maps to exactly one borough")

assert_london_boroughs(london$borough_gss, "lsoa_lookup")

setorder(london, borough_name, lsoa)
write_out(london, LOOKUP_OUT)

message("\nCodes: ", format(nrow(london), big.mark = ","),
        " (both vintages) across ", uniqueN(london$borough_name), " boroughs.")
message("Vintage split: ",
        sum(london$lsoa %in% lk$LSOA11CD), " appear as 2011 codes, ",
        sum(london$lsoa %in% lk$LSOA21CD), " as 2021 codes.")
