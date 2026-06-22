library(stringr)
# =============================================================
# 00_download.R — Crime data acquisition (documented + verified)
#
# MANUAL STEPS (custom download cannot be scripted from the form):
#   1. Go to https://data.police.uk/data/
#   2. Set date range: [START YYYY-MM] to [END YYYY-MM]
#   3. Under "Forces", tick ONLY:
#        - Metropolitan Police Service
#        - City of London Police
#   4. Tick "Include crime data". Download and unzip into data/raw/
#
# This script verifies the unzipped result is complete.
# =============================================================

raw_dir <- "data/raw/london_crime"
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

# Define the window you documented in SOURCES.md
start_month <- "2016-01"   # <-- set to your real start
end_month   <- "2026-04"   # <-- set to your real end

# Build the list of months you EXPECT
expected <- format(
  seq(as.Date(paste0(start_month, "-01")),
      as.Date(paste0(end_month,   "-12")),
      by = "month"),
  "%Y-%m"
)

# Find the months you ACTUALLY have (from street-file names)
files  <- list.files(raw_dir, pattern = "-street\\.csv$",
                     recursive = TRUE, full.names = TRUE)
present <- unique(str_extract(basename(files), "^\\d{4}-\\d{2}"))

missing <- setdiff(expected, present)

if (length(missing) == 0) {
  message("OK: all ", length(expected), " months present.")
} else {
  warning("Missing ", length(missing), " months:\n",
          paste(missing, collapse = ", "))
}