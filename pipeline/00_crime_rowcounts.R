# =============================================================
# 00_crime_rowcounts.R : per-year row totals as a sanity check.
#
# The log now carries a `files` column alongside `rows`. Reading rows alone
# invites the wrong conclusion: Metropolitan rows fell 87% in 2013, which
# looks like a collapse in crime and was actually ten missing files.
# rows_per_file is stable (~78-102k) whenever coverage is complete, so the
# two columns together make a gap self-evident.
#
# Writes pipeline/logs/rowcounts.log
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("00_crime_rowcounts : per-year totals")

files <- crime_files()

counts <- data.table(
  file  = files,
  year  = substr(basename(files), 1L, 4L),
  force = force_of(files),
  rows  = vapply(files, function(f) nrow(fread(f, select = 1L,
                                               showProgress = FALSE)),
                 integer(1))
)

by_year <- counts[, .(files = .N, rows = sum(rows)), by = .(year, force)][
  order(year, force)
][, rows_per_file := round(rows / files)]

print(by_year)
write_log(by_year, "rowcounts.log")

# Flag years whose per-file volume departs sharply from the force's median.
# This is a smell test, not a gate: 00_download.R owns the hard coverage check.
by_year[, med := median(rows_per_file), by = force]
odd <- by_year[rows_per_file < 0.5 * med | rows_per_file > 2 * med]
if (nrow(odd)) {
  message("\nUnusual per-file volume (investigate before trusting these years):")
  print(odd[, .(year, force, files, rows, rows_per_file, median = med)])
}

message("\nTotal rows: ", format(sum(counts$rows), big.mark = ","),
        " across ", length(files), " files.")
