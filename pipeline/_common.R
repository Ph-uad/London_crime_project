# =============================================================
# _common.R — shared helpers. Source this (it sources _config.R) at the
# top of every pipeline script.
#
# Design rules encoded here:
#   * a failed check stops the run with a non-zero exit status. `warning()`
#     does not set one under Rscript, so a broken pipeline still looks green.
#   * nothing writes into a directory that holds raw source data.
#   * every excluded record is counted in a ledger, never silently dropped.
# =============================================================

suppressPackageStartupMessages(library(data.table))

.pipeline_dir <- function() {
  # Works whether sourced from the repo root or from another script.
  if (dir.exists("pipeline")) "pipeline" else "."
}
source(file.path(.pipeline_dir(), "_config.R"))

# ---- Failure -------------------------------------------------------------

# Stop with a clean, actionable message and a non-zero exit status.
fail <- function(...) {
  msg <- paste0(...)
  message("\nFAIL: ", msg, "\n")
  quit(save = "no", status = 1L)
}

check <- function(condition, ...) {
  if (!isTRUE(condition)) fail(...)
  invisible(TRUE)
}

ok <- function(...) message("  ok  ", ...)

# ---- Directories and safe writes ----------------------------------------

ensure_dir <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

# Refuse to write into a directory that contains raw crime CSVs.
# The retired Spark script used mode = "overwrite" on a path that had become
# the only copy of the raw data; this makes that class of mistake impossible.
assert_not_raw_data <- function(path) {
  if (dir.exists(path)) {
    if (length(list.files(path, pattern = "-street\\.csv$", recursive = TRUE))) {
      fail("refusing to write to '", path, "': it contains raw crime CSVs.")
    }
  }
  invisible(TRUE)
}

write_out <- function(dt, path) {
  assert_not_raw_data(dirname(path))
  ensure_dir(dirname(path))
  fwrite(dt, path)
  message("  ->  ", path, "  (", format(nrow(dt), big.mark = ","), " rows)")
  invisible(path)
}

write_log <- function(dt, filename) {
  ensure_dir(LOG_DIR)
  path <- file.path(LOG_DIR, filename)
  fwrite(dt, path)
  message("  log ", path)
  invisible(path)
}

# ---- Crime file discovery -----------------------------------------------

# Raw crime lived in data/processed/crime/ at one point, where the pipeline
# would have overwritten it. Detect that and say so, rather than reporting
# every month as missing.
crime_files <- function(dir = CRIME_RAW_DIR) {
  f <- list.files(dir, pattern = "-street\\.csv$", recursive = TRUE,
                  full.names = TRUE)
  if (!length(f)) {
    stray <- list.files(PROC_DIR, pattern = "-street\\.csv$",
                        recursive = TRUE, full.names = TRUE)
    if (length(stray)) {
      fail("no crime CSVs in '", dir, "', but ", length(stray),
           " were found under '", PROC_DIR, "'.\n",
           "       Raw source data must not live in data/processed/ — the ",
           "pipeline writes there.\n",
           "       Move them:  mv ", PROC_DIR, "/crime/*-street.csv ", dir, "/")
    }
    fail("no '*-street.csv' files under '", dir,
         "'. See pipeline/SOURCES.md for how to acquire them.")
  }
  f
}

expected_months <- function(start = CRIME_START, end = CRIME_END) {
  format(seq(as.Date(paste0(start, "-01")),
             as.Date(paste0(end, "-01")), by = "month"), "%Y-%m")
}

force_of <- function(paths) {
  b <- basename(paths)
  fifelse(grepl("city-of-london", b, fixed = TRUE), "city-of-london",
          "metropolitan")
}

month_of <- function(paths) substr(basename(paths), 1L, 7L)

# ---- Geography assertions ------------------------------------------------

assert_london_boroughs <- function(gss, what = "output") {
  u <- sort(unique(gss[!is.na(gss)]))
  check(!anyNA(gss),
        what, " contains ", sum(is.na(gss)), " rows with no borough code. ",
        "Unmatched records must go to the exclusion ledger, not into an ",
        "output with an NA key.")
  check(all(grepl(LONDON_GSS_PREFIX, u)),
        what, " contains non-London codes: ",
        paste(head(u[!grepl(LONDON_GSS_PREFIX, u)], 5), collapse = ", "))
  check(length(u) == LONDON_BOROUGH_N,
        what, " covers ", length(u), " boroughs, expected ",
        LONDON_BOROUGH_N, ".")
  ok(what, ": ", LONDON_BOROUGH_N, " London boroughs, no NA keys")
  invisible(TRUE)
}

# Not every metric is published for all 33 boroughs. ONS suppresses or omits
# small-population areas — City of London (~8,000 residents) has no life
# expectancy at all and every well-being year is marked [u].
#
# A metric therefore declares which boroughs it is allowed to be missing, and
# why. Missing a borough that was NOT declared is a failure; declaring one that
# turns out to be present is also a failure, because the reason has gone stale.
assert_metric_boroughs <- function(gss, what, all_gss,
                                   allow_missing = character(), reason = "") {
  check(!anyNA(gss), what, " contains rows with an NA borough code.")
  u <- unique(gss)
  check(all(grepl(LONDON_GSS_PREFIX, u)),
        what, " contains non-London codes: ",
        paste(head(setdiff(u, grep(LONDON_GSS_PREFIX, u, value = TRUE)), 5),
              collapse = ", "))

  missing <- setdiff(all_gss, u)
  undeclared <- setdiff(missing, allow_missing)
  check(!length(undeclared),
        what, " is missing ", length(undeclared), " borough(s) with no ",
        "declared reason: ", paste(undeclared, collapse = ", "),
        ".\n       Either the source changed or the filter is wrong. Do not ",
        "add them to allow_missing without checking which.")

  stale <- intersect(allow_missing, u)
  check(!length(stale),
        what, " declares ", paste(stale, collapse = ", "), " as missing, but ",
        "the data now contains ", if (length(stale) > 1) "them" else "it",
        ". Remove the exception — the documented reason is out of date.")

  if (length(missing)) {
    message("  note  ", what, ": ", length(u), "/", length(all_gss),
            " boroughs. Missing by design: ", paste(missing, collapse = ", "),
            if (nzchar(reason)) paste0(" — ", reason) else "")
  } else {
    ok(what, ": all ", length(all_gss), " boroughs")
  }
  invisible(sort(missing))
}

# ---- Long-schema helper --------------------------------------------------

as_long <- function(dt) {
  missing <- setdiff(LONG_SCHEMA, names(dt))
  check(!length(missing),
        "long-schema output is missing column(s): ",
        paste(missing, collapse = ", "))
  setcolorder(dt, LONG_SCHEMA)
  dt[, ..LONG_SCHEMA]
}

banner <- function(x) message("\n== ", x, " ", strrep("=", max(0, 58 - nchar(x))))
