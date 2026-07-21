if (!requireNamespace("lazyeval", quietly = TRUE)) {
  install.packages("lazyeval")
}


library(sparklyr)
library(dplyr)

spark_install(version = "3.4.0")

sc <- spark_connect(master = "local", version = "3.4.0")

crime_files <- normalizePath("data/raw/crime")

crime <- spark_read_csv(
  sc,
  name = "crime",
  path = file.path(crime_files, "*-street\\.csv"),
  header = TRUE,
  infer_schema = TRUE
)

#-------------------------
# Raw row count
#-------------------------
crime_count <- crime |>
  tally() |>
  collect() |>
  pull(n)

#-------------------------
# Read lookup
#-------------------------
lsoa_lookup <- paste0(
  "data/raw/LSAO_lookup/LSOA_(2011)_to_LSOA_(2021)",
  "_to_Local_Authority_District_(2022)_Exact_Fit_",
  "Lookup_for_EW_(V3).csv"
)

lk <- spark_read_csv(
  sc,
  lsoa_lookup,
  options = list(
    header = TRUE,
    inferSchema = FALSE
  )
)

crime_joined <- crime |>
  left_join(lk, by = c("LSOA_code" = "LSOA21CD"))

#-------------------------
# QA counts
#-------------------------
unmatched_rows <- crime_joined |>
  filter(is.na(LAD22CD)) |>
  tally() |>
  collect() |>
  pull(n)

matched_rows <- crime_joined |>
  filter(!is.na(LAD22CD)) |>
  tally() |>
  collect() |>
  pull(n)

#-------------------------
# Aggregate
#-------------------------
crime_summary <- crime_joined |>
  filter(!is.na(LAD22CD)) |>
  group_by(
    Borough = LSOA_name,
    Month,
    Crime_type
  ) |>
  summarise(
    count = n(),
    .groups = "drop"
  )

aggregated_total <- crime_summary |>
  summarise(total = sum(count)) |>
  collect() |>
  pull(total)

#-------------------------
# QA metrics
#-------------------------
difference <- aggregated_total - matched_rows

difference_pct <- round(
  difference / matched_rows * 100,
  4
)

#-------------------------
# QA report
#-------------------------
qa_report <- tibble(
  Metric = c(
    "Raw rows",
    "Unmatched rows",
    "Matched rows",
    "Aggregated total",
    "Difference",
    "Difference (%)"
  ),
  Value = c(
    crime_count,
    unmatched_rows,
    matched_rows,
    aggregated_total,
    difference,
    difference_pct
  )
)

# Pretty print
qa_report |>
  mutate(
    Value = format(Value, big.mark = ",", scientific = FALSE)
  ) |>
  print(n = Inf)

# Pass/fail
stopifnot(abs(difference_pct) <= 0.5)

library(stringr)
library(data.table)
# log it
log_dir <- "pipeline/logs"
log_path <- file.path(log_dir, "dataQuality.log")
fwrite(qa_report, log_path)
