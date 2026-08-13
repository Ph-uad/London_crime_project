library(tidyverse)
library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local", version = "3.4.0")


# Path in variables
population_path <- "data/raw/avg_population/MYE4-Table 1.csv"
lsoa_path <- "data/processed/lsoa_lookup.csv"


# Assigning data-tables to DataFrames
population <- spark_read_csv(sc, population_path, colClasses = "character", skip = 6)

# take out the first 6 rows which are descriptive and not part of the data
population <- population |>
  collect() |>
  slice(-(1:6))

# Set first row as column names
colnames(population) <- population[1, ]
population <- population[-1, ]

# Remove NA columns
population <- population[, !is.na(names(population))]


# Get unique LSOA names
london_lsoa <- spark_read_csv(sc, lsoa_path, colClasses = 'character') |>
 collect() |>
 select(Borough = "lad_nm") |>
 distinct()


london_population <- population |>
    filter(`Name` %in% london_lsoa$Borough)



# Write the cleaned data to a CSV file
write_csv(london_population, "data/processed/london_population.csv")

# Rearrange columns to
# | Borough | Population | Year |


# turn every column with "Mid" in the name into a an column called "Year"  and the value be the four digit value that follows "Mid" in the column name
london_population <- spark_read_csv(sc, "data/processed/london_population.csv")
crime_counts_by_year <- spark_read_csv(sc, "data/processed/crime_counts_by_year.csv")

library(tidyr)

london_population_by_year_and_borough <- pivot_longer(
  london_population,
  cols = starts_with("Mid"),     # Selects Mid2024, Mid2023, Mid2022
  names_to = "Year",             # Name of your new column
  names_prefix = "Mid",          # Optional: turns "Mid2024" into "2024"
  values_to = "Population"       # Name for the actual data values
)

# Join crime counts by year and borough name 
crime_type_by_year_and_population <- london_population_by_year_and_borough |>
  left_join(crime_counts_by_year, by = c("Name" = "lad_nm", "Year" = "year")) |>
  collect()



# Write the cleaned data to CSV file
write_csv(crime_type_by_year_and_population, "data/processed/crime_type_by_year_and_population.csv")


spark_disconnect(sc)

# Calculate crime rates
# rate_per_1000

crime_type_by_year_and_population <- spark_read_csv(sc,"data/processed/crime_type_by_year_and_population.csv", colClasses = "character")

# Create new column Rate_per_1000crime_type_by_year_and_population <- crime_type_by_year_and_population |>
  rename(
    lsoa_code = Code,
    borough = Name,
    lsoa_name = Geography,
    year = Year
  )

trial <- transform(crime_type_by_year_and_population, population = gsub(",", "", population))

crime_type_by_year_and_population <- crime_type_by_year_and_population |>
  tr(
    population = as.numeric(gsub(",", "", population)),
  )



crime_counts_by_year <- crime_with_borough |>
  group_by(`lad_nm`,`year`) |>
  summarise(crime_type_count = n()) |>
  arrange(Borough = `lad_nm`, Year = `year`) |>
  collect()


# Calculate crime rates
# rate_per_1000

crime_type_by_year_and_population <- spark_read_csv(sc,"data/processed/crime_type_by_year_and_population.csv", colClasses = "character")

crime_type_by_year_and_population <- transform(
  crime_type_by_year_and_population,
  Population = as.numeric(gsub(",", "", Population))
)

crime_type_by_year_and_population <- crime_type_by_year_and_population |>
  rename(
    lsoa_code = Code,
    borough = Name,
    lsoa_name = Geography,
    year = Year,
    population = Population
  )

crime_type_by_year_and_population <- crime_type_by_year_and_population |>
  mutate(
    crime_rate_per_1000 = round((crime_type_count / population) * 1000, 2)
  )

# Write the cleaned data to CSV file
write.csv(crime_type_by_year_and_population, "data/processed/crime_type_by_year_and_population.csv")
