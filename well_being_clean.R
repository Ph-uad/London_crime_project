library(sparklyr)
spark_install(version = "3.4.0") # pick a recent Spark version
library(dplyr) 
library(tidyverse)

install.packages("janitor")
library(janitor) 


# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0")

df_local <- read_csv("/Users/tsumizu/Desktop/personal-well-being-borough/Summary - Mean Scores-Table 1.csv")

well_being <- copy_to(sc, df_local, "well_being", overwrite = TRUE)

well_being %>% view ()


df_local2 <- read_csv("/Users/tsumizu/Desktop/personal-well-being-borough/Summary - Mean Scores-Table 1.csv") %>%
  janitor::clean_names()

# Copy to Spark
well_being2 <- copy_to(sc, df_local2, "well_being", overwrite = TRUE)

# Preview cleaned headers
colnames(well_being2)
well_being2 %>% view()

well_being <- well_being2 %>% 
  rename( 
    code = 1,
    Area = 2,
    life_satisfaction_11_12 = life_satisfaction,
    life_satisfaction_12_13 = 4,
    life_satisfaction_13_14 = 5,
    life_satisfaction_14_15 = 6,
    life_satisfaction_15_16 = 7,
    life_satisfaction_16_17 = 8,
    life_satisfaction_17_18 = 9,
    life_satisfaction_18_19 = 10,
    worthwhile_11_12 = worthwhile,
    worthwhile_12_13 = 13,
    worthwhile_13_14 = 14,
    worthwhile_14_15 = 15,
    worthwhile_15_16 = 16,
    worthwhile_16_17 = 17,
    worthwhile_17_18 = 18,
    worthwhile_18_19 = 19,
    happiness_11_12 = happiness,
    happiness_12_13 = 22,
    happiness_13_14 = 23,
    happiness_14_15 = 24,
    happiness_15_16 = 25,
    happiness_16_17 = 26,
    happiness_17_18 = 27,
    happiness_18_19 = 28,
    anxeity_11_12 =  anxiety,
    anxeity_12_13 = 31,
    anxeity_13_14 = 32,
    anxeity_14_15 = 33,
    anxeity_15_16 = 34,
    anxeity_16_17 = 35,
    anxeity_17_18 = 36,
    anxeity_18_19 = 37,
  )

well_being %>% head()
well_being %>% glimpse()


df_ <- read_csv("/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/well_being/well_being.csv")
df_clean <- well_being %>%
  janitor::clean_names()

df_clean <- df_clean %>%
  select(where(~ !is.logical(.) || !all(is.na(.)))) 

well_being <- copy_to(sc, df_clean, "well_being", overwrite = TRUE)

well_being %>% view()

well_being <- well_being %>% 
  filter(code != "Code")

well_being <- well_being %>%
  rename(
    worthwhile_18_19 = x19  
  )

well_being %>% glimpse()


try_mutate <- well_being %>%
  mutate_all("x", 0)

london_boroughs <- list("Barnet", "Bexley", "Brent", "Bromley", "Croydon", "Ealing",
                        "Enfield", "Greenwich", "Havering", "Hillingdon", "Hounslow",
                        "Islington", "Kensington and Chelsea", "Kingston upon Thames",
                        "Lambeth", "Lewisham", "Merton", "Newham", "Redbridge", "Richmond upon Thames",
                        "Sutton", "Tower Hamlets", "Waltham Forest", "Wandsworth", "Westminster",
                        "Barking and Dagenham", "Harrow", "Haringey", "City of London","london", "Camden", "Hackney",
                        "Hammersmith and Fulham", "Southwark")



well_being <- well_being %>%
  filter(!area %!in% london_boroughs)

well_being %>% glimpse()

well_being <- well_being %>%
  select(-x11, -x20, -x29)



well_being <- read_csv("/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/well_being/well_being.csv")

well_being <- well_being %>%
  rename(
    borough_name =area
  )

well_being %>% glimpse()


single_file_tbl <- well_being %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/well_being",
  mode  = "error",
  header = TRUE
)


spark_disconnect(sc)

