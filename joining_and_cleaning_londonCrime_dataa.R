# install SparklyR
# install.packages("sparklyr");
# install.packages("tidyverse");

#Install (or point to) a Spark distribution
#sparklyr can install Spark for you:
library(sparklyr)
spark_install(version = "3.4.0") # pick a recent Spark version


# A grammar of Data Manipulation 
library(dplyr) 
library(tidyverse)

# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0")

getwd()
list.files("/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/data/cleaned/London data set")

dir_path <- normalizePath("/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/data/cleaned/London data set")

#load data into the variable london_crime 
#and preview it after  

london_crime  <- spark_read_csv(
  sc,
  name = "crime_data",
  path = file.path(dir_path, "*.csv"),
  header = TRUE,
  infer_schema = TRUE
)


london_crime  %>% head()


london_crime %>% filter(is.na(Context)) %>% view()



# imputation 
# date normalization


#checking null Crime_ID for relationship 
london_crime %>%
  filter(is.na(Crime_ID)) %>%
  distinct(Crime_type) %>%
  print()


#generate new ID for crimes without ID due to ommision of crime type 
london_crime <- london_crime %>%
  mutate(
    Crime_ID = if_else(is.na(Crime_ID),uuid(),Crime_ID)
  )

london_crime %>% view()

london_crime %>% 
  distinct(LSOA_name) %>%
  view(n = Inf)

#mutate LSAO name by removing the last four digit in the LSAO names 
london_crime <- london_crime %>%
  mutate(LSOA_name = sql("substring(LSOA_name, 1, length(LSOA_name) - 5)"))

london_crime %>% select(LSOA_name) %>% print(n=5)


#filter rows with borough names not in london 
london_boroughs <- c  ("Barnet", "Bexley", "Brent", "Bromley", "Croydon", "Ealing",
                        "Enfield", "Greenwich", "Havering", "Hillingdon", "Hounslow",
                        "Islington", "Kensington and Chelsea", "Kingston upon Thames",
                        "Lambeth", "Lewisham", "Merton", "Newham", "Redbridge", "Richmond upon Thames",
                        "Sutton", "Tower Hamlets", "Waltham Forest", "Wandsworth", "Westminster",
                        "Barking and Dagenham", "Harrow", "Haringey", "City of London", "Camden", "Hackney",
                        "Hammersmith and Fulham", "Southwark")

london_crime <- london_crime %>%
  filter(!LSOA_name %!in% london_boroughs)


london_crime %>% count()



#chain column‑dropping and row‑filtering in one pipeline
london_crime <- london_crime  %>%
  select(-Longitude, -Latitude) %>%
  filter(
    !is.na(LSOA_code),
    LSOA_code != "",
    !is.na(LSOA_name),
    LSOA_name != ""
  )

london_crime %>% count()


#to filter 
# and view if they all got joined by 
london_crime %>%
  filter(year(Month) == 2020) %>%
  head(10) %>%     
  collect()  

#make sure the all in one partition 
single_file_tbl <- london_crime %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/london crime",
  mode  = "overwrite",
  header = TRUE
)


spark_disconnect(sc)



# Splitting dataset's time to month year and time period (Year and quarter)

london_crime_data <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/london crime/london_crime.csv",
  infer_schema = TRUE,
  header = TRUE,
  name = "London_Crime_Data"
)

london_crime_data <- london_crime_data %>%
  rename(
    time = Month
  )

london_crime_data <- london_crime_data %>%
  mutate(
    year = year(time),
    month = month(time),
    quarter = quarter(time)
  )


london_crime_data <- london_crime_data %>%
  mutate(
    time_period =  sql("concat('Q', cast(quarter as string), ' ', cast(year as string))")
  )

london_crime_data %>% view()

london_crime_data <- london_crime_data %>%
  select(-Last_outcome_category, -Context)

london_crime_data <- london_crime_data %>%
  mutate(
    time = substr(time, 1,7)
  )

london_crime_data %>% view()

#make sure the all in one partition 
single_file_tbl <- london_crime_data %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/London_crime_by_period",
  mode  = "overwrite",
  header = TRUE
)

file.rename(
  from = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/London_crime_by_period/part-00000-335b5cac-4bcb-40eb-a30f-6dfbf351341d-c000.csv",
  to = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/london crime/London_crime_by_period.csv"
)
