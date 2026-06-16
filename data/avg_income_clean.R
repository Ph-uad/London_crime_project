library(sparklyr)
spark_install(version = "3.4.0") # pick a recent Spark version
library(dplyr) 
library(tidyverse)

# install.packages("janitor")
library(janitor) 


sc <- spark_connect(master = "local", version = "3.4.0") 

avg_income  <- spark_read_csv(
  sc,
  name = "avg_income",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/data/cleaned/Income dataset /Total Income-Table 1.csv",
  header = TRUE,
  infer_schema = TRUE
)


avg_income %>% view ()
avg_income %>% glimpse()


avg_income <- avg_income %>%
  rename(
    borough_name = `_c1`,
    "14-15" = `_c45`,
    "15-16" = `_c48`,
    "16-17" = `_c51`,
    "17-18" = `_c54`,
    "18-19" = `_c57`,
    "19-20" = `_c60`,
    "20-21" = `_c63`,
    "21-22" = `_c66`,
    "22-23" = `_c69`,
  )

avg_income %>% glimpse()

avg_income <- avg_income %>%
  select(borough_name, "14-15", "15-16", "16-17", "17-18", "18-19","19-20","20-21", "21-22","22-23")

avg_income %>% view()

avg_income %>%
  group_by_all() %>%
  tally() %>%
  filter(n > 1)


avg_income <- avg_income %>%
  select(where(~ !is.logical(.) || !all(is.na(.)))) 



avg_income %>% view()

london_boroughs <- list("Barnet", "Bexley", "Brent", "Bromley", "Croydon", "Ealing",
                        "Enfield", "Greenwich", "Havering", "Hillingdon", "Hounslow",
                        "Islington", "Kensington and Chelsea", "Kingston upon Thames",
                        "Lambeth", "Lewisham", "Merton", "Newham", "Redbridge", "Richmond upon Thames",
                        "Sutton", "Tower Hamlets", "Waltham Forest", "Wandsworth", "Westminster",
                        "Barking and Dagenham", "Harrow", "Haringey", "City of London","London", "Camden", "Hackney",
                        "Hammersmith and Fulham", "Southwark")

avg_income <- avg_income %>%
  filter(!borough_name %!in% london_boroughs)

avg_income %>% view()

single_file_tbl <- avg_income %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/avg_income",
  mode  = "overwrite",
  header = TRUE
)

file.rename(
  from = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/avg_income/part-00000-551c553d-4fc4-4b53-89da-27a1bf8d1377-c000.csv",
  to   = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/avg_income/avg_income.csv"
)


spark_disconnect(sc)
