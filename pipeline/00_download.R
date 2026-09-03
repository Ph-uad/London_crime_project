# =============================================================
# 00_download.R : verify that the raw crime archive is complete.
#
# This is the guard that should have caught the 33 missing Metropolitan
# months in 2012-2015. Two changes make it actually do that:
#
#   1. Coverage is checked PER FORCE. A pooled check passes whenever any
#      one force supplies the month; City of London was complete for every
#      month, so it masked every Metropolitan gap.
#   2. A gap now stops the run. `warning()` does not set a non-zero exit
#      status under Rscript, so the previous version reported a broken
#      dataset and still looked like a successful run.
#
# It verifies rather than downloads: the archive is a manual bulk export.
# See pipeline/SOURCES.md for the acquisition steps.
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("00_download : raw crime coverage")

ensure_dir(CRIME_RAW_DIR)
files <- crime_files()

expected <- expected_months()
present  <- data.table(month = month_of(files), force = force_of(files))

message("Window ", CRIME_START, " to ", CRIME_END,
        " (", length(expected), " months x ", length(CRIME_FORCES), " forces)")

report <- rbindlist(lapply(CRIME_FORCES, function(f) {
  have <- unique(present[force == f, month])
  data.table(force = f,
             expected = length(expected),
             present  = length(intersect(expected, have)),
             missing  = length(setdiff(expected, have)),
             missing_months = paste(setdiff(expected, have), collapse = " "))
}))

print(report[, .(force, expected, present, missing)])
write_log(report, "crime_coverage.log")

# Files outside the declared window are a documentation error, not a gap.
extra <- setdiff(unique(present$month), expected)
if (length(extra)) {
  message("\nNote: ", length(extra), " month(s) present but outside the ",
          "declared window: ", paste(sort(extra), collapse = ", "),
          "\n      Update CRIME_START/CRIME_END in pipeline/_config.R and ",
          "SOURCES.md, or remove the files.")
}

if (sum(report$missing) > 0L) {
  for (i in seq_len(nrow(report))) {
    if (report$missing[i] > 0L) {
      message("\n", report$force[i], ": ", report$missing[i], " missing\n  ",
              report$missing_months[i])
    }
  }
  fail(sum(report$missing), " monthly file(s) missing across ",
       sum(report$missing > 0L), " force(s). Full list in ",
       file.path(LOG_DIR, "crime_coverage.log"), ".\n",
       "       Do not build borough rates from an incomplete archive: a ",
       "missing month reads as a fall in crime.\n",
       "       Re-download from https://data.police.uk/data/archive/, or if ",
       "a month is genuinely unavailable upstream,\n",
       "       record it in SOURCES.md and narrow the window in ",
       "pipeline/_config.R.")
}

ok("all ", length(expected), " months present for all ",
   length(CRIME_FORCES), " forces")
