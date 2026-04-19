library(sparklyr)
spark_install(version = "3.4.0") # pick a recent Spark version
library(dplyr) 
library(tidyverse)

# install.packages("janitor")
library(janitor) 

# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0") 

male_life_expectancy  <- spark_read_csv(
  sc,
  name = "life_expectancy",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/data/ukppp22ex 2/males period ex-Table 1.csv",
  header = TRUE,
  infer_schema = TRUE
)

male_life_expectancy %>% view()

male_life_expectancy <- male_life_expectancy %>%
  rename(
    # borough_name = Local_Authority,
    "2014" = `_c34`,
    "2015" = `_c35`,
    "2016" = `_c36`,
    "2017" = `_c37`,
    "2018" = `_c38`,
    "2019" = `_c39`,
    "2020" = `_c40`,
    "2021" = `_c41`,
    "2022" = `_c42`,
    "2023" = `_c43`,
    "2024" = `_c44`,
    "2025" = `_c45`,
  )

male_life_expectancy %>% glimpse()

male_life_expectancy <- male_life_expectancy %>% 
  select(borou)
