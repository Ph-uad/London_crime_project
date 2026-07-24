if (!requireNamespace("lazyeval", quietly = TRUE)) {
  install.packages("lazyeval")
}
line_length_linter(length = 100L, ignore_string_bodies = TRUE)
object_length_linter(length = 50L)

# Goal
## Create a reliable mapping from each crime record's LSOA to its borough.
# Tasks
# 1. Load the LSOA → borough lookup file.
# 2. Support both LSOA 2011 and LSOA 2021 codes if required.
# 3. Remove duplicate mappings.
# 4. Validate that every LSOA maps to only one borough.



spark_install(version = "3.4.0") # pick a recent Spark version
library(sparklyr)
library(dplyr)
library(tidyverse)
library(digest)

sc <- spark_connect(master = "local", version = "3.4.0")


# Path in variables
crime_files <- normalizePath("data/raw/crime")
lsoa_lookup <- paste0("data/raw/LSAO_lookup/LSOA_(2011)_to_LSOA_(202",
                      "1)_to_Local_Authority_District_(2022)_Exact_Fit_L",
                      "ookup_for_EW_(V3).csv")


# Assigning data-tables to DataFrames
lk <- spark_read_csv(sc, lsoa_lookup, colClasses = "character")
crime <- spark_read_csv(sc,
  name = "crime", path = file.path(crime_files, "*-street\\.csv"),
  header = TRUE, infer_schema = TRUE
)

head(lk, 5)
count(lk)
count(crime)
head(crime, 10)

glimpse(crime)


# Check for missing Crime_IDs and frequency of crime types
crime |>
  filter(is.na(Crime_ID)) |>
  group_by(Crime_type) |>
  summarise(count = n()) |>
  arrange(desc(count)) |>
  print(n = Inf)

# 1 Anti-social behaviour 3434827
# 2 Other crime 294211
# 3 Violent crime 138420
# 4 Vehicle crime 92751
# 5 Burglary 87478
# 6 Other theft 46888
# 7 Robbery 35172
# 8 Criminal damage and arson 17117
# 9 Drugs 13984
# 10 Shoplifting 9407
# 11 Public disorder and weapons 8203
# Result mostly Ant-social Behaviour

# Verdict:
# generate new 64-character ID for crimes without ID

# 1. Update the Spark table using native Spark SQL functions
crime <- crime |>
  mutate(
    Crime_ID = if_else(
      is.na(Crime_ID),

      # Spark SQL function: sha2(string, bits). 256 bits = 64 hex characters.
      # concat() mixes random strings and timestamps for absolute uniqueness.
      sha2(concat(rand(), current_timestamp()), 256),

      # Keep original ID if it is not NA
      Crime_ID
    )
  )

crime |> view()

sdf_register(crime, "crime_hashed")

# make sure the all in one partition
single_file_tbl <- crime |>
  sdf_repartition(partitions = 1)

spark_write_csv(
  single_file_tbl,
  path = str_c(getwd(), "/data/processed/crime"),
  mode = "overwrite",
  header = TRUE
)

file.rename(
  from = str_c(getwd(),"/data/processed/crime/part-00000-03c7566d-5596-4833-87c1-67c386073e27-c000.csv"),
  to = str_c(getwd(),"/data/processed/crime.csv")
)

spark_disconnect(sc)

crime_files <- "data/processed/crime.csv"
lookup_path <- "data/processed/lsoa_lookup.csv"

crime <- spark_read_csv(sc, crime_files, colClasses = "character")
lsoa_lookup <- spark_read_csv(sc, lookup_path, colClasses = "character")


glimpse(crime)
glimpse(lsoa_lookup)
# Check for empty LSOA codes in the crime data and print the results
crime |>
  filter(is.na(`LSOA_code`) | `LSOA_code` == "") |>
  select(`LSOA_code`, `LSOA_name`, `Longitude`, `Latitude`, `Location`) |>
  collect() |>
  view(n = Inf)
# 139,764 missing of 13,000,000 records (1.07%) missing. 
# small enough to ignore, as our main concern has to do with borough mapping
# Therefore drop rows with missing LSOA codes
crime <- crime |>
  filter(!is.na(`LSOA_code`) & `LSOA_code` != "")
# Run the filter again to confirm that there are no missing LSOA codes

# Join crime with lsoa_lookup on LSOA_code to get borough information
crime_with_borough <- crime |>
  left_join(lsoa_lookup, by = c("LSOA_code" = "lsoa"))

glimpse(crime_with_borough)

single_file_tbl <- crime_with_borough |>
  sdf_repartition(partitions = 1)

spark_write_csv(
  single_file_tbl,
  path = str_c(getwd(),"/data/joining/")
)

getwd()
file.rename(
  from = str_c(getwd(), "/data/joining/part-00000-f4d1e7f8-9d1f-4d7a-bfd3-71607ebdc88f-c000.csv"),
  to = str_c(getwd(), "/data/processed/crime_by_borough.csv")
)

# Aggregate crime counts
# Deliverable
# | Borough | Year | Crime Count |
crime_with_borough <- spark_read_csv(sc, str_c(getwd(),"/data/processed/crime_by_borough.csv", colClasses = "character"))

crime_with_borough <- crime_with_borough |>
  mutate(year = substr(`Month`, 1, 4))

glimpse(crime_with_borough)

crime_counts_by_year <- crime_with_borough |>
  group_by(`lad_nm`,`year`) |>
  summarise(crime_type_count = n()) |>
  arrange(Borough = `lad_nm`, Year = `year`) |>
  collect() |>
  view(n = Inf)

# Save the aggregated crime counts by borough and year to a CSV file
output_path <- "data/processed/crime_counts_by_year.csv"
crime_counts_by_year |>
  write.csv(output_path, row.names = FALSE)


# Aggregate crime counts by category
# Deliverable
# | Borough | Year | Crime Category | Count |
crime_counts_by_year_and_category <- crime_with_borough |>
  group_by(`lad_nm`,`year`, `Crime_type`) |>
  summarise(crime_type_count = n()) |>
  arrange(`lad_nm`, `year`, Crime_Subcategory = `Crime_type`) |>
  select(
    Borough = `lad_nm`,
    Year = `year`,
    Crime_Subcategory = `Crime_type`,
    Crime_Type_Count = crime_type_count
  ) |>
  collect() |>
  view(n = Inf)

# Add Main category feature
# Create categories for crime types
violence_and_sexual_offences <- c("Violence and sexual offences")
criminal_damage_and_arson <- c("Criminal damage and arson")
public_order_offences <- c("Anti-social behaviour", "Public order")
drug_and_weapon_offences <- c("Drugs", "Possession of weapons")
theft_and_robbery <- c(
  "Bicycle theft",
  "Burglary",
  "Other theft",
  "Robbery",
  "Shoplifting",
  "Theft from the person",
  "Vehicle crime"
)
other_crimes <- c("Other crime")

# Function to categorize crime types into broader categories
categorize_crime <- function(crime) {
  if (crime %in% violence_and_sexual_offences) {
    return("Violence and Sexual Offences")
  } else if (crime %in% criminal_damage_and_arson) {
    return("Criminal Damage and Arson")
  } else if (crime %in% public_order_offences) {
    return("Public Order Offences")
  } else if (crime %in% drug_and_weapon_offences) {
    return("Drug and Weapon Offences")
  } else if (crime %in% theft_and_robbery) {
    return("Theft And Robbery")
  } else if (crime %in% other_crimes) {
    return("Other Crimes")
  } else {
    return("No-category")
  }
}

# Apply function to crime_counts_by_year_and_category to create a new column for crime categories
crime_counts_by_crime_type <- crime_counts_by_year_and_category |>
  mutate(crime_category = sapply(`Crime_Subcategory`, categorize_crime)) |>
  select(Borough, Year, Crime_Subcategory, Crime_Subcategory_Count = `Crime_Type_Count`, Crime_Category = `crime_category`) |>
  collect() |> view(n = Inf)
  
output_path <- "data/processed/crime_counts_by_crime_subcategory_type.csv"
crime_counts_by_crime_type |>
  write.csv(output_path, row.names = FALSE)

# Create a freq/count feature for each crime category 
crime_counts_by_crime_type <- crime_counts_by_crime_type |>
  group_by(Borough, Year, Crime_Category) |>
  summarise(Crime_Category_Count = sum(Crime_Subcategory_Count)) |>
  arrange(Borough, Year, Crime_Category) |>
  collect() |> view(n = Inf)

# Save the aggregated crime counts by borough, year, and category to a CSV file
output_path <- "data/processed/crime_counts_by_crime_type.csv"
crime_counts_by_crime_type |>
  write.csv(output_path, row.names = FALSE)


