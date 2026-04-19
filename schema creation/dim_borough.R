library(sparklyr)
library(dplyr) 
library(tidyverse)  


# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0") 

crime_data <- spark_read_csv(
  sc,
  name = "crime_data",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/london crime/london_crime.csv",
  header = TRUE,
  infer_schema = TRUE, 
)


dim_time <- spark_read_csv(
  sc,
  name = "dim_time",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_time.csv",
  header = TRUE,
  infer_schema = TRUE, 
)

dim_time %>% count()

crime_data %>% count()

dim_borough <- crime_data %>% 
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

dim_borough %>% count()

dim_borough %>% view()

# dim_borough_spark <- dim_borough %>%
#   select(borough_name) %>%
#   distinct() %>%
#   sdf_repartition(partitions = 1)

dim_borough_spark <- copy_to(sc, dim_borough, "dim_borough", overwrite = TRUE)
spark_write_csv(dim_borough_spark, path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data", overwrite = TRUE)

file.rename(
  from = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/part-00000-c9ff188c-20fa-4662-9196-e6c0db7ccfff-c000.csv",
  to   = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_borough.csv"
)


imd_data_2015 <- spark_read_csv(
  sc,
  name = "imd_data_2015",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/data/ID 2015 for London/Borough summary measures-Table 1.csv",
  header = TRUE,
  infer_schema = TRUE, 
)

dim_borough <- spark_read_csv(
  sc,
  path ="/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_borough.csv",
  name = "dim_borough",
  header = TRUE,
  infer_schema = TRUE,
)

imd_data_2015 %>% glimpse()

imd_data_2015 <- imd_data_2015 %>%
  rename(
    borough_name = Local_Authority_District_name_2013,
    deprivation_score_2015	 = IMD__Average_score     
  )

dim_borough <- dim_borough %>%  
  left_join(select(imd_data_2015,borough_name, deprivation_score_2015), by = "borough_name")

imd_data_2019 <- spark_read_csv(
  sc,
  name = "imd_data_2019",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/data/ID 2019 for London/Borough summary measures-Table 1.csv",
  header = TRUE,
  infer_schema = TRUE, 
)

imd_data_2019 <- imd_data_2019 %>%
  rename(
    borough_name = Local_Authority_District_name_2019,
    deprivation_score_2019	 = IMD__Average_score     
  )

dim_borough <- dim_borough %>%  
  left_join(select(imd_data_2019, borough_name, deprivation_score_2019), by = "borough_name")

dim_borough %>% view()

dim_borough <-  dim_borough %>%
  select(borough_id, borough_name, sub_region, london_section, deprivation_score_2015, deprivation_score_2019)

dim_borough_spark <- copy_to(sc, dim_borough, "dim_borough", overwrite = TRUE)
spark_write_csv(dim_borough_spark, path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/a", overwrite = TRUE)

file.rename(
  from = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/a/part-00000-e654ff88-6f23-41d8-88df-e08ae5b2e430-c000.csv",
  to   = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_boroughs.csv"
)

spark_disconnect(sc)
