# =============================================================
# 00_LSOAlookup.R — Build LSOA→borough lookup, harmonise 2011/2021,
# log unmatched codes, and report coverage.
# =============================================================


library(data.table)

crime_files <- list.files("data/raw/london_crime", pattern = "-street\\.csv$",
                          recursive = TRUE, full.names = TRUE)
lookup_path <- "data/raw/LSAO_lookup/LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Exact_Fit_Lookup_for_EW_(V3).csv"
log_dir <- "pipeline/logs"; out_dir <- "data/processed"
dir.create(log_dir, showWarnings = FALSE); dir.create(out_dir, showWarnings = FALSE)


## 1. One borough map covering BOTH code vintages
lk <- fread(lookup_path, colClasses = "character")
borough_map <- unique(rbindlist(list(
  lk[, .(lsoa = LSOA11CD, lad_cd = LAD22CD, lad_nm = LAD22NM)],
  lk[, .(lsoa = LSOA21CD, lad_cd = LAD22CD, lad_nm = LAD22NM)]
)))[lsoa != ""]
## London-only map (E09 = the 33 London boroughs)
london_map <- borough_map[grepl("^E09", lad_cd)]

## 2. Read every crime LSOA code ONCE, count per code in a single pass
all_codes <- rbindlist(lapply(crime_files, \(f) fread(f, select = "LSOA code", colClasses = "character")))
setnames(all_codes, "LSOA code", "lsoa")
code_counts <- all_codes[, .N, by = lsoa]
total_records  <- code_counts[, sum(N)]

## 3. Classify every distinct code into four states
code_counts[, status := fifelse(lsoa == "",                     "blank",
                        fifelse(lsoa %in% london_map$lsoa,       "london",
                        fifelse(lsoa %in% borough_map$lsoa,      "outside_london",
                                                                 "unmatched")))]

## 4. Coverage — report BOTH ways your criterion could be read
total_records  <- code_counts[, sum(N)]
london_records <- code_counts[status == "london", sum(N)]

message(sprintf("London record coverage: %.2f%%", 100 * london_records / total_records))
print(code_counts[, .(records = sum(N)), by = status])   # see the split

## 5. Log ONLY genuine problems, with real counts
fwrite(code_counts[status != "london"][order(-N)],
       file.path(log_dir, "lsoa_lookup.log"))

## 6. The deliverable: borough lookup for codes that actually appear
used <- london_map[lsoa %in% code_counts[status == "london", lsoa]]
fwrite(used, file.path(out_dir, "lsoa_lookup.csv"))
message("Boroughs in output: ", uniqueN(used$lad_nm), " (must be 33)")



crime_files <- list.files("data/raw/london_crime", pattern = "-street\\.csv$",
                          recursive = TRUE, full.names = TRUE)
length(crime_files)                       # how many street files total?
sum(grepl("metropolitan",  crime_files))  # Met files found?  -> almost certainly 0
sum(grepl("city-of-london", crime_files)) # City files found? -> ~11