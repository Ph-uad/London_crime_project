# =============================================================
# 00_download_metrics.R — acquire the well-being and life-expectancy series
# (issue 1.4) and the borough boundaries (issue 1.10).
#
# Why these two sources and not the ones the issue names:
#
#   The issue says "from the London Datastore". Both Datastore copies were
#   checked on 2026-08-16 and are unusable for a recent analysis:
#     personal-well-being-borough-2r87d  — Apr 2011 to Mar 2019, last updated
#                                          2019. Better than the ward file's
#                                          2013 cutoff, still four years short.
#     life-expectancy-...-borough-23gm7  — 2000-2002 to 2008-2010, and it is
#                                          Open Government Licence **v2**,
#                                          not v3 like everything else here.
#   Both are GLA re-publications of ONS data, so this goes to ONS directly:
#   longer series, current releases, and OGL v3.0 on both.
#
# Licences were READ on the ONS dataset pages, not assumed — see SOURCES.md.
#
# Downloads are recorded in pipeline/logs/acquisition.log with size, MD5 and
# UTC timestamp, so "when did we pull this and what exactly did we get" is
# answerable months later.
#
#   Rscript pipeline/00_download_metrics.R
#
# Needs outbound network access. If your machine is behind a proxy that
# blocks ons.gov.uk, download the two URLs by hand into the paths this
# script reports and re-run it — it verifies whatever is already on disk.
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

banner("00_download_metrics — well-being and life expectancy")

TARGETS <- list(
  list(name = "ONS4 personal well-being (local authority, time series)",
       url  = WELLBEING_URL,
       path = WELLBEING_RAW,
       kind = "csv",
       min_bytes = 1000000L,
       covers = "2011-12 to 2022-23"),
  list(name = "Life expectancy for local areas of the UK",
       url  = LIFEEXP_URL,
       path = LIFEEXP_RAW,
       kind = "xlsx",
       min_bytes = 1000000L,
       covers = "2001-2003 to 2022-2024, three-year rolling"),
  list(name = paste0("ONS Local Authority District boundaries UK ",
                     BOUNDARY_GEN, " (", BOUNDARY_ITEMS[[BOUNDARY_GEN]]$vintage,
                     ")"),
       url  = BOUNDARY_URL,
       path = BOUNDARY_RAW,
       kind = "geojson",
       min_bytes = 100000L,
       covers = BOUNDARY_ITEMS[[BOUNDARY_GEN]]$detail)
)

# A blocked proxy or an expired link often returns an HTML error page with a
# 200 status. Checking the magic bytes catches that; checking only the size
# does not.
sniff <- function(path) {
  con <- file(path, "rb"); on.exit(close(con))
  head <- readBin(con, "raw", n = 512L)     # enough to see past a BOM/preamble
  if (length(head) >= 2L && identical(head[1:2], as.raw(c(0x50, 0x4b)))) {
    return("xlsx")          # PK.. — any zip container, which xlsx is
  }
  txt <- tolower(rawToChar(head[head != as.raw(0)]))
  if (grepl("<!doctype|<html|<head|<body", txt)) return("html")
  "text"
}

fetch <- function(t) {
  ensure_dir(dirname(t$path))
  if (file.exists(t$path)) {
    message("  have  ", t$path, " — verifying, not re-downloading")
  } else {
    message("  get   ", t$name, "\n        ", t$url)
    ok_dl <- tryCatch({
      utils::download.file(t$url, t$path, mode = "wb", quiet = TRUE)
      TRUE
    }, error = function(e) {
      message("        download failed: ", conditionMessage(e))
      FALSE
    }, warning = function(w) {
      message("        download warning: ", conditionMessage(w))
      file.exists(t$path)
    })
    if (!ok_dl || !file.exists(t$path)) {
      if (file.exists(t$path)) unlink(t$path)
      fail("could not download '", t$name, "'.\n",
           "       URL:  ", t$url, "\n",
           "       Save it to:  ", t$path, "\n",
           "       Then re-run this script — it verifies files already on disk.")
    }
  }

  size <- file.size(t$path)
  got  <- sniff(t$path)
  if (got == "html") {
    unlink(t$path)
    fail("'", t$name, "' returned an HTML page, not data. The link has moved ",
         "or a proxy intercepted it.\n       Open ", t$url,
         " in a browser and check.")
  }
  want <- if (t$kind == "xlsx") "xlsx" else "text"   # geojson is text
  check(identical(got, want),
        "'", t$name, "' looks like ", got, ", expected ", want, ".")
  check(size >= t$min_bytes,
        "'", t$name, "' is only ", format(size, big.mark = ","),
        " bytes; expected at least ",
        format(t$min_bytes, big.mark = ",", scientific = FALSE),
        ". Likely a truncated or placeholder file.")

  ok(basename(t$path), "  ", format(round(size / 1e6, 1), nsmall = 1), " MB  ",
     t$covers)
  data.table(dataset = t$name, path = t$path, url = t$url,
             bytes = size, md5 = unname(tools::md5sum(t$path)),
             downloaded_utc = format(Sys.time(), tz = "UTC",
                                     "%Y-%m-%dT%H:%M:%SZ"))
}

manifest <- rbindlist(lapply(TARGETS, fetch))

log_path <- file.path(LOG_DIR, "acquisition.log")
if (file.exists(log_path)) {
  prev <- fread(log_path, colClasses = "character", showProgress = FALSE)
  prev <- prev[!path %chin% manifest$path]      # keep history for other files
  manifest <- rbind(prev, manifest, fill = TRUE)
}
write_log(manifest, "acquisition.log")

message("\nAcquired ", length(TARGETS), " dataset(s). Record of what was ",
        "pulled, when, and its MD5 is in ", log_path, ".")
message("Next: 12_tidy_wellbeing.R (1.7), 13_tidy_life_expectancy.R (1.8), ",
        "03_borough_boundaries.R (1.10).")
