library(sparklyr)
library(dplyr) 
library(tidyverse)  


# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0") 


crime_data <- spark_read_csv(
  sc,
  name = "crime_data",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/london crime/part-00000-7a7833fd-b3e1-4170-9a3d-f204008465a2-c000.csv",
  header = TRUE,
  infer_schema = TRUE, 
)

crime_data %>% count()

dim_borough <- crime_data %>%
  select(LSOA_name) %>%
  distinct() %>%
  arrange(LSOA_name) %>%
  collect() %>%  # Bring data into memory
  mutate(borough_id = row_number())

# London Region by boroughs
central_london <- c("Camden", "City of London", "Islington", "Kensington and Chelsea", "Lambeth", 
                    "Southwark", "Westminster", "Wandsworth", "Hammersmith and Fulham", "Tower Hamlets")

north_london <- c("Barnet", "Enfield", "Haringey", "Waltham Forest")

east_london <- c("Barking and Dagenham", "Bexley", "Greenwich", "Hackney", "Havering", "Lewisham", 
                 "Newham", "Redbridge", "Tower Hamlets")

south_london <- c("Bromley", "Croydon", "Kingston upon Thames", "Merton", "Richmond upon Thames", 
                  "Sutton", "Wandsworth")

west_london <- c("Brent", "Ealing", "Hammersmith and Fulham", "Harrow", "Hillingdon", "Hounslow", 
                 "Richmond upon Thames")

# Inner and Outer London boroughs
inner_london <- c(
  "Camden", "City of London", "Greenwich", "Hackney", "Hammersmith and Fulham", 
  "Islington", "Kensington and Chelsea", "Lambeth", "Lewisham", "Newham", 
  "Southwark", "Tower Hamlets", "Wandsworth", "Westminster", "Haringey"
)

outer_london <- c(
  "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley", "Croydon", 
  "Ealing", "Enfield", "Harrow", "Havering", "Hillingdon", "Hounslow", 
  "Kingston upon Thames", "Merton", "Redbridge", "Richmond upon Thames", 
  "Sutton", "Waltham Forest"
)

categorize_subregion <- function(borough) {
    if (borough %in% central_london) {
      return("Central London")
    } else if (borough %in% north_london) {
      return("North London")
    } else if (borough %in% east_london) {
      return("East London")
    } else if (borough %in% south_london) {
      return("South London")
    } else if (borough %in% west_london) {
      return("West London")
    } else {
      return("Unknown")
    }
}

categorize_london_section <- function(borough) {
  if (borough %in% inner_london) {
    return("Inner London")
  } else if (borough %in% outer_london) {
    return("Outer London")
  } else {
    return("Unknown")
  }
}

dim_borough %>% view()

dim_borough <- dim_borough %>%
  rename(
    borough_name = LSOA_name
  )

dim_borough <- dim_borough %>%
  mutate(sub_region = sapply(borough_name, categorize_subregion))

dim_borough <- dim_borough %>% 
  mutate(london_section = sapply(borough_name, categorize_london_section))

dim_borough %>% glimpse()

dim_borough %>% view()

dim_borough_spark <- dim_borough %>%
  select(borough_name) %>%
  distinct() %>%
  sdf_repartition(partitions = 1)

dim_borough_spark <- copy_to(sc, dim_borough, "dim_borough", overwrite = TRUE)
spark_write_csv(dim_borough_spark, path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data", overwrite = TRUE)

file.rename(
  from = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/part-00000-c9ff188c-20fa-4662-9196-e6c0db7ccfff-c000.csv",
  to   = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_borough.csv"
)


spark_disconnect(sc)
