library(sparklyr)
library(dplyr) 
library(tidyverse)  

# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0") 

london_crime_data <- spark_read_csv(
  sc,
  name = "london_crime_data",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/london crime/london_crime.csv",
  header = TRUE,
  infer_schema = TRUE, 
)

london_crime_data %>% glimpse()

london_crime_data <- london_crime_data %>% 
  rename(
    "time" = Month
  )

dim_time <- london_crime_data %>%
  select(time)

dim_time %>% view()

dim_time <- dim_time %>%
  mutate(
    year = year(time),
    month = month(time),
    quarter = quarter(time)
  )

dim_time <- dim_time %>% 
  select(-time)

dim_time %>% distinct(year) %>% print(n = Inf)

dim_time <- dim_time %>%
  mutate(
    time_period =  sql("concat('Q', cast(quarter as string), ' ', cast(year as string))")
  )

dim_time <- dim_time %>%
  mutate(
    time_id = sql("ROW_NUMBER() OVER (ORDER BY time_period)"),
  )

dim_time <- dim_time %>%
  distinct(time_period, year, month, quarter)

dim_time <- sdf_with_sequential_id(dim_time, id = "time_id") %>%
  arrange(time_period)

dim_time %>% view()

dim_time <- dim_time %>%
  select(time_id, everything())

dim_time_spark <- copy_to(sc, dim_time, "dim_time", overwrite = TRUE)
write.csv(dim_time, "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_time3.csv", row.names = FALSE)

# spark_write_csv(dim_time, path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data", overwrite = TRUE)

# file.rename(
#   from = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/part-00000-c9ff188c-20fa-4662-9196-e6c0db7ccfff-c000.csv",
#   to   = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_borough.csv"
# )


spark_disconnect(sc)


