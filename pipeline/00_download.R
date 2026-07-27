library(stringr)
raw_dir <- "data/raw/crime"
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

# Define the window you documented in SOURCES.md
# This script verifies the raw crime CSVs are present for the expected months.
start_month <- "2011-01"   # <-- set to your real start
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