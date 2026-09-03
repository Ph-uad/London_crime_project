# =============================================================
# pipeline/tests/smoke.R : end-to-end smoke test on synthetic fixtures.
#
# Why this exists: the retired scripts shipped as if they worked. They did
# not : 01_LSOA_by_population.R called an undefined tr(), reused a closed
# Spark connection and had an assignment swallowed into a comment. None of
# that is a syntax error, so `parse()` accepts all three; only executing the
# code finds them.
#
# Raw data is gitignored, so this builds its own miniature London: 33
# boroughs, two forces, both crime-type vocabularies, and deliberate
# exclusion cases. It runs in a temp directory and touches nothing real.
#
#   Rscript pipeline/tests/smoke.R
# =============================================================

suppressPackageStartupMessages(library(data.table))
set.seed(11)

repo <- normalizePath(".")
stopifnot(dir.exists(file.path(repo, "pipeline")))
root <- file.path(tempdir(), paste0("smoke-", as.integer(runif(1, 1e6, 9e6))))
dir.create(file.path(root, "data", "raw"), recursive = TRUE)
invisible(file.copy(file.path(repo, "pipeline"), root, recursive = TRUE))

BOROUGHS <- data.table(
  gss  = sprintf("E090000%02d", 1:33),
  name = paste("Test Borough", sprintf("%02d", 1:33))
)
PER_BOROUGH <- 4L

# ---- ONS LSOA lookup -----------------------------------------------------
lk <- BOROUGHS[, .(i = seq_len(PER_BOROUGH)), by = .(gss, name)]
lk[, lsoa11 := sprintf("E01%06d", .I)]
lk[, lsoa21 := lsoa11]                       # unchanged codes (CHGIND "U")
lk[seq(1, .N, by = 7), lsoa21 := sprintf("E01%06d", 900000 + .I)]  # splits
lookup_dir <- file.path(root, "data", "raw", "LSAO_lookup")
dir.create(lookup_dir, recursive = TRUE)
fwrite(lk[, .(LSOA11CD = lsoa11, LSOA11NM = "L", LSOA21CD = lsoa21,
              LSOA21NM = "L", CHGIND = "U", LAD22CD = gss, LAD22NM = name,
              LAD22NMW = "", ObjectId = .I)],
       file.path(lookup_dir,
                 paste0("LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_",
                        "District_(2022)_Exact_Fit_Lookup_for_EW_(V3).csv")))

# ---- Crime: both vocabularies, plus exclusion cases ----------------------
LEGACY  <- c("Anti-social behaviour", "Burglary", "Other crime", "Robbery",
             "Vehicle crime", "Violent crime", "Public disorder and weapons")
CURRENT <- c("Anti-social behaviour", "Burglary", "Other crime", "Robbery",
             "Vehicle crime", "Violence and sexual offences", "Public order",
             "Possession of weapons", "Bicycle theft", "Theft from the person")
crime_dir <- file.path(root, "data", "raw", "crime")
dir.create(crime_dir, recursive = TRUE)
months <- sprintf("2011-%02d", 1:12)
city <- lk[gss == "E09000001", lsoa11]
met  <- lk[gss != "E09000001", lsoa11]

for (m in months) {
  types <- if (m <= "2011-06") LEGACY else CURRENT   # vocabulary change mid-run
  for (f in c("metropolitan", "city-of-london")) {
    codes <- if (f == "metropolitan") met else city
    n <- if (f == "metropolitan") 600L else 40L
    d <- data.table(`Crime ID` = "", Month = m, `Reported by` = f,
                    `Falls within` = f, Longitude = "", Latitude = "",
                    Location = "", `LSOA code` = sample(codes, n, TRUE),
                    `LSOA name` = "L",
                    `Crime type` = sample(types, n, TRUE),
                    `Last outcome category` = "", Context = "")
    if (f == "metropolitan") {
      # blank (ungeocoded), valid-but-outside-London, and unmatched codes
      d <- rbind(d, d[1:3][, `LSOA code` := c("", "E01099999", "E01777777")])
    }
    fwrite(d, file.path(crime_dir, sprintf("%s-%s-street.csv", m, f)))
  }
}
# A code that is a real ONS LSOA outside London, so "outside_london" is
# distinguishable from "unmatched".
extra <- fread(file.path(lookup_dir, list.files(lookup_dir)[1]))
extra <- rbind(extra, data.table(LSOA11CD = "E01099999", LSOA11NM = "L",
                                 LSOA21CD = "E01099999", LSOA21NM = "L",
                                 CHGIND = "U", LAD22CD = "E08000001",
                                 LAD22NM = "Elsewhere", LAD22NMW = "",
                                 ObjectId = 99999))
fwrite(extra, file.path(lookup_dir, list.files(lookup_dir)[1]))

# ---- ONS mid-year population (7 preamble rows, then the header) ----------
pop_dir <- file.path(root, "data", "raw", "avg_population")
dir.create(pop_dir, recursive = TRUE)
pop_path <- file.path(pop_dir, "MYE4-Table 1.csv")
writeLines(c("MYE4: Population estimates", "preamble", "preamble", "preamble",
             "preamble", "preamble", "preamble"), pop_path)
pop <- BOROUGHS[, .(Code = gss, Name = name, Geography = "London Borough",
                    `Mid-2011` = format(200000 + 1000 * .I, big.mark = ","),
                    `Mid-2012` = format(201000 + 1000 * .I, big.mark = ","))]
fwrite(pop, pop_path, append = TRUE, col.names = TRUE)

# ---- HMRC income (financial-year row above a label row) ------------------
inc_dir <- file.path(root, "data", "raw", "personal_well_being",
                     "income-of-tax-payers")
dir.create(inc_dir, recursive = TRUE)
fys <- c("2011-12", "2012-13")
hdr1 <- c("", "", unlist(lapply(fys, function(y) c(y, "", ""))))
hdr2 <- c("Code", "Area",
          rep(c("Number of Individuals", "Mean £", "Median £"),
              length(fys)))
body <- BOROUGHS[, {
  vals <- unlist(lapply(seq_along(fys), function(k)
    c(format(50000 + 100 * .I, big.mark = ","),
      format(30000 + 100 * .I, big.mark = ","),
      format(24000 + 100 * .I, big.mark = ","))))
  as.list(c(gss, name, vals))
}, by = seq_len(nrow(BOROUGHS))][, -1]
inc <- rbind(as.list(hdr1), as.list(hdr2), as.list(rep("", length(hdr1))),
             setNames(body, names(body)), use.names = FALSE)
fwrite(inc, file.path(inc_dir, "Total Income-Table 1.csv"), col.names = FALSE)

# ---- IMD borough domain summaries ---------------------------------------
DOMAINS <- c("Income", "Employment", "Education, Skills and Training",
             "Health Deprivation and Disability", "Crime",
             "Barriers to Housing and Services", "Living Environment")
for (y in c("2015", "2019")) {
  d <- data.table(code = BOROUGHS$gss, nm = BOROUGHS$name)
  setnames(d, c(sprintf("Local Authority District code (%s)", y),
                sprintf("Local Authority District name (%s)", y)))
  for (dom in DOMAINS) {
    v <- switch(dom,
                "Income" = , "Employment" = round(runif(33, 0.05, 0.35), 3),
                "Health Deprivation and Disability" = ,
                "Crime" = round(runif(33, -1.5, 1.0), 2),
                round(runif(33, 5, 50), 1))
    d[[paste0(dom, " - Average score")]] <- v
    d[[paste0(dom, " - Average rank")]] <- sample(33)   # must be excluded
  }
  dir <- file.path(root, "data", "raw", "personal_well_being",
                   sprintf("ID %s for London", y))
  dir.create(dir, recursive = TRUE)
  fwrite(d, file.path(dir, "Borough domain summaries-Table 1.csv"))
}

# ---- ONS4 well-being cube (v4 layout, with a suppressed borough) ---------
wb_dir <- file.path(root, "data", "raw", "wellbeing")
dir.create(wb_dir, recursive = TRUE)
MEASURES <- c("Life satisfaction", "Worthwhile", "Happiness", "Anxiety")
wb <- CJ(period = c("2011-12", "2012-13"), gss = BOROUGHS$gss,
         MeasureOfWellbeing = MEASURES, sorted = FALSE)
wb <- merge(wb, BOROUGHS[, .(gss, name)], by = "gss")
wb[, `wellbeing-estimate` := "average-mean"]
wb[, value := round(runif(.N, 6.5, 8.0), 2)]
wb[MeasureOfWellbeing == "Anxiety", value := round(runif(.N, 2, 4), 2)]
# City of London suppressed in every cell, exactly as ONS publishes it
wb[gss == "E09000001", value := NA_real_]
wb[, marking := fifelse(is.na(value), "[u]", NA_character_)]
wb_out <- wb[, .(v4_3 = value, `Data marking` = marking,
                 `Lower limit` = value - 0.2, `Upper limit` = value + 0.2,
                 `yyyy-yy` = period, Time = period,
                 `administrative-geography` = gss, Geography = name,
                 `measure-of-wellbeing` = tolower(gsub(" ", "-",
                                                       MeasureOfWellbeing)),
                 MeasureOfWellbeing,
                 `wellbeing-estimate`, Estimate = "Average (mean)")]
# proportion rows the script must ignore
wb_out <- rbind(wb_out, copy(wb_out)[, `:=`(`wellbeing-estimate` = "good",
                                            Estimate = "Good")])
fwrite(wb_out, file.path(wb_dir, "ons4-wellbeing-local-authority-timeseries-v4.csv"))

# ---- Life expectancy workbook (sheet "1", header on row 6) ---------------
if (requireNamespace("writexl", quietly = TRUE)) {
  le_dir <- file.path(root, "data", "raw", "life_expectancy")
  dir.create(le_dir, recursive = TRUE)
  le <- CJ(Period = c("2009 to 2011", "2010 to 2012"),
           gss = setdiff(BOROUGHS$gss, "E09000001"),   # City of London absent
           Sex = c("Male", "Female"),
           `Age group` = c("<1", "65 to 69", "20 to 24"), sorted = FALSE)
  le <- merge(le, BOROUGHS[, .(gss, name)], by = "gss")
  le[, le_val := fifelse(`Age group` == "<1", runif(.N, 78, 85),
                  fifelse(`Age group` == "65 to 69", runif(.N, 18, 24),
                          runif(.N, 55, 62)))]
  body <- le[, .(Period, Country = "England", `Area type` = "Local Areas",
                 `Area code` = gss, `Area name` = name, Sex, `Sex code` = "1",
                 `Age group`, `Age code` = "1",
                 `Life expectancy` = round(le_val, 1),
                 `Lower confidence interval` = round(le_val - 0.4, 1),
                 `Upper confidence interval` = round(le_val + 0.4, 1))]
  pad <- as.data.table(matrix("", nrow = 5L, ncol = ncol(body)))
  setnames(pad, names(body))
  pad[1, 1] <- "Sheet 1: Life expectancy for local areas"
  hdr <- as.data.table(as.list(names(body))); setnames(hdr, names(body))
  sheet <- rbind(pad, hdr, body[, lapply(.SD, as.character)])
  writexl::write_xlsx(list(`1` = sheet),
                      file.path(le_dir,
                                "ons-lifeexpectancylocalareas-2022to2024.xlsx"),
                      col_names = FALSE)
} else {
  message("NOTE: writexl not installed : skipping the life-expectancy fixture ",
          "and 13_tidy_life_expectancy.R.")
}

# ---- Borough boundaries (British National Grid, plus non-London districts) --
if (requireNamespace("sf", quietly = TRUE)) {
  suppressPackageStartupMessages(library(sf))
  bdir <- file.path(root, "data", "raw", "boundaries")
  dir.create(bdir, recursive = TRUE)
  cell <- function(i) {
    x0 <- 500000 + (i %% 6) * 6000; y0 <- 175000 + (i %/% 6) * 6000
    ring <- rbind(c(x0, y0), c(x0 + 5000, y0), c(x0 + 5000, y0 + 5000),
                  c(x0, y0 + 5000), c(x0, y0))
    st_polygon(list(ring))
  }
  codes <- c(BOROUGHS$gss, "E08000001", "W06000001")
  nms   <- c(BOROUGHS$name, "Elsewhere", "Somewhere")
  bnd <- st_sf(LAD22CD = codes, LAD22NM = nms,
               geometry = st_sfc(lapply(seq_along(codes) - 1L, cell),
                                 crs = 27700))
  st_write(bnd, file.path(bdir, "ons-lad-uk-bgc.geojson"),
           driver = "GeoJSON", quiet = TRUE)
} else {
  message("NOTE: sf not installed : skipping 03_borough_boundaries.R.")
}

# ---- Run -----------------------------------------------------------------
# The window must span the IMD snapshot years (2015, 2019) as well as the
# annual fixtures, or 20_unify_metrics.R correctly refuses to drop a whole
# snapshot metric on the floor.
env <- c("CRIME_START=2011-01", "CRIME_END=2011-12",
         "ANALYSIS_START=2011", "ANALYSIS_END=2019", "TREND_END=2019")

owd <- setwd(root); on.exit(setwd(owd), add = TRUE)

SCRIPTS <- c("pipeline/00_download.R", "pipeline/00_crime_rowcounts.R",
             "pipeline/00_LSAOlookup.R", "pipeline/01_crime_by_borough.R",
             "pipeline/02_population_and_rates.R", "pipeline/10_tidy_income.R",
             "pipeline/11_tidy_imd.R", "pipeline/12_tidy_wellbeing.R",
             if (requireNamespace("sf", quietly = TRUE))
               "pipeline/03_borough_boundaries.R",
             if (requireNamespace("writexl", quietly = TRUE) &&
                 requireNamespace("readxl", quietly = TRUE))
               "pipeline/13_tidy_life_expectancy.R",
             "pipeline/20_unify_metrics.R", "pipeline/QA/01_QA.R")
SCRIPTS <- Filter(Negate(is.null), SCRIPTS)

failures <- character()
expect <- function(label, want, got, log = NULL) {
  if (identical(as.integer(want), as.integer(got))) {
    message("  ok    ", label)
  } else {
    message("  FAIL  ", label, "  (expected exit ", want, ", got ", got, ")")
    if (!is.null(log)) message(paste0("        ", tail(log, 14), collapse = "\n"))
    failures <<- c(failures, label)
  }
}

message("\n== smoke: full run must succeed ", strrep("=", 28))
for (s in SCRIPTS) {
  out <- suppressWarnings(system2("Rscript", s, stdout = TRUE, stderr = TRUE,
                                  env = env))
  st <- attr(out, "status"); if (is.null(st)) st <- 0L
  expect(s, 0L, st, out)
}

message("\n== smoke: guards must fire ", strrep("=", 34))
held <- file.path(root, "data", "raw", "crime", "2011-05-metropolitan-street.csv")
invisible(file.rename(held, paste0(held, ".held")))
out <- suppressWarnings(system2("Rscript", "pipeline/00_download.R",
                                stdout = TRUE, stderr = TRUE, env = env))
st <- attr(out, "status"); if (is.null(st)) st <- 0L
expect("00_download rejects a missing month", 1L, st)
invisible(file.rename(paste0(held, ".held"), held))

agg <- file.path(root, "data", "processed", "crime_by_borough_year.csv")
d <- fread(agg); d[1, crimes := crimes + 500L]; fwrite(d, agg)
out <- suppressWarnings(system2("Rscript", "pipeline/QA/01_QA.R",
                                stdout = TRUE, stderr = TRUE, env = env))
st <- attr(out, "status"); if (is.null(st)) st <- 0L
expect("QA rejects an aggregate that does not reconcile", 1L, st)

setwd(owd)
unlink(root, recursive = TRUE)

if (length(failures)) {
  message("\nSMOKE TEST FAILED: ", length(failures), " check(s)\n")
  quit(save = "no", status = 1L)
}
message("\nSmoke test passed: ", length(SCRIPTS),
        " scripts ran clean and both guards fired.\n")
