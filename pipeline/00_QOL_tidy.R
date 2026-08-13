install.packages(zoo)
# Required packages: sparklyr, dplyr, tidyverse, stringr, tidyr, zoo
library(zoo)
library(dplyr)
library(tidyr)
library(stringr)
library(sparklyr)
library(tidyverse)


# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0")

# Pick and clean deprivation data:
deprivation_summary_2015 <- read.csv("data/raw/personal_well_being/ID 2015 for London/Borough domain summaries-Table 1.csv")
deprivation_summary_2019 <- read.csv("data/raw/personal_well_being/ID 2019 for London/Borough domain summaries-Table 1.csv")

# Remove columns with all NA values
deprivation_summary_2015 <- deprivation_summary_2015 |>
  select(where(~ !all(is.na(.))))
deprivation_summary_2019 <- deprivation_summary_2019 |>
  select(where(~ !all(is.na(.))))

# Combine FIRST, then sort the final result
combined_deprivation <- deprivation_summary_2015 |>
  left_join(
    deprivation_summary_2019, 
    by = c("Local.Authority.District.code..2013." = "Local.Authority.District.code..2019."),
    suffix = c("_2015", "_2019")
  )

# Replace ever "." with "_"
names(combined_deprivation) <- gsub("\\.+", "_", names(combined_deprivation))

combined_deprivation <- combined_deprivation |>
  select(
    Local_Authority_District_code_2013_,
    Local_Authority_District_name_2013_,
    Local_Authority_District_name_2019_,
    everything()
  ) |>
  rename(
   district_code = Local_Authority_District_code_2013_,
   district_name =  Local_Authority_District_name_2019_,
  )

combined_deprivation <- combined_deprivation |>
  select(-Local_Authority_District_name_2013_)

output_path <- "data/processed/IDMP_2015_n_2019.csv"
combined_deprivation |>
  write.csv(output_path, row.names = FALSE)



# income-of-tax-payers
london_average_income_by_borough <- read.csv("data/raw/personal_well_being/income-of-tax-payers/Total Income-Table 1.csv")

colnames(london_average_income_by_borough) <- london_average_income_by_borough[1, ]

row_labels <- as.character(london_average_income_by_borough[1, ])
years <- names(london_average_income_by_borough) |> 
  str_extract("(19|20)\\d{2}") |>       # Extract 4-digit years (e.g., 1999, 2000)
  na.locf(na.rm = FALSE)                # Forward-fill the years over NA values
clean_labels <- row_labels |>
  str_remove_all("\\s*£") |>            # Strip out " £" symbols
  str_replace_all("\\s+", "_")          # Change spaces to underscores
new_headers <- ifelse(
  clean_labels %in% c("Code", "Area") | is.na(years),
  clean_labels,
  paste0(clean_labels, "_", years)
)
names(london_average_income_by_borough) <- new_headers
london_average_income_by_borough <- london_average_income_by_borough[-1, ]

output_path <- "data/processed/London_average_income.csv"
london_average_income_by_borough |>
  write.csv(output_path, row.names = FALSE)



# London well being probability 
well_being_probability <- read.csv("data/raw/personal_well_being/london-ward-well-being-probability-scores/Data-Table 1.csv")
well_being_probability <- well_being_probability[-1, ]
names(well_being_probability) <- gsub("\\.+", "_", names(well_being_probability))

view(well_being_probability)

output_path <- "data/processed/well_being_probabilitye.csv"
well_being_probability |>
  write.csv(output_path, row.names = FALSE)
