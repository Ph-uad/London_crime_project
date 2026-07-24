# =============================================================
# 01_rowcounts.R — Per-year row totals as a sanity check.
# Writes a log to pipeline/logs/rowcounts.log
# =============================================================

library(data.table)
library(stringr)

raw_dir <- "data/raw/crime"
log_dir <- "pipeline/logs"
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

files <- list.files(raw_dir, pattern = "-street\\.csv$",
                    recursive = TRUE, full.names = TRUE)

# Count data rows per file (fast: read one column only)
counts <- data.table(
  file  = files,
  year  = str_extract(basename(files), "^\\d{4}"),
  force = ifelse(grepl("city-of-london", files), "City of London", "Metropolitan"),
  rows  = vapply(files, \(f) nrow(fread(f, select = 1L)), integer(1))
)

# Aggregate to per-year, per-force totals
by_year <- counts[, .(rows = sum(rows)), by = .(year, force)][order(year, force)]

# Print and log
print(by_year)
log_path <- file.path(log_dir, "rowcounts.log")
fwrite(by_year, log_path)
message("Total rows: ", format(sum(counts$rows), big.mark = ","),
        " across ", length(files), " files. Log -> ", log_path)