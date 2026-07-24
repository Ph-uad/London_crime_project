library(sparklyr)
spark_install(version = "3.4.0") # pick a recent Spark version
library(dplyr)
library(tidyverse)


# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0")

spark_disconnect_all()

# Pick and clean deprivation data:
deprivation_summary_2015 <- spark_read_csv(
    sc,
    name = "deprivation_summary_2019",
    path = "data/raw/personal_well_being/ID 2015 for London/Borough domain summaries-Table 1.csv",
    header = TRUE,
    infer_schema = TRUE
)

deprivation_summary_2019 <- spark_read_csv(
    sc,
    name = "deprivation_summary_2015",
    path = "data/raw/personal_well_being/ID 2019 for London/Borough domain summaries-Table 1.csv",
    header = TRUE,
    infer_schema = TRUE
)

glimpse(deprivation_summary_2015)
glimpse(deprivation_summary_2019)

deprivation_summary_2015 <- deprivation_summary_2015 |>
    rename(
       district_name = Local_Authority_District_name_2013  
    )

deprivation_summary_2019 <- deprivation_summary_2019 |>
    rename(
       district_name = Local_Authority_District_name_2019 
    )


# Combine FIRST, then sort the final result
combined_deprivation <- deprivation_summary_2015 |> 
  left_join(
    deprivation_summary_2019, 
    by = "district_name", 
    suffix = c("_2015", "_2019")
  ) |> 
  arrange(district_name) # <-- Move it here


glimpse(combined_deprivation)
