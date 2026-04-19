
library(tidyverse)
library(dplyr)
library(sparklyr)

sc <- spark_connect(master = "local", version = "3.4.0")

dim_crime_type <- spark_read_csv(
  sc,
  name ="crime_type",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/london crime/part-00000-7a7833fd-b3e1-4170-9a3d-f204008465a2-c000.csv",
  header = TRUE,
  infer_schema = TRUE
)

crime_type_and_count <- dim_crime_type %>%
  group_by(Crime_type) %>%
  summarise(count = n()) %>%
  arrange(desc(count)) %>%
  collect()


print(crime_type_and_count)

crime_type_and_count <- crime_type_and_count  %>% 
  rename(
    crime_type_freq = count,
    crime_type = Crime_type
         )

crime_subcategory_id <- crime_type_and_count %>% 
  select(crime_subcategory, crime_subcategory_freq) %>%
  distinct() %>%
  arrange(crime_subcategory) %>%
  mutate(crime_type_id = row_number())

crime_subcategory_id %>% print()


violence_and_sexual_offences <- c("Violence and sexual offences")
criminal_damage_and_arson <- c("Criminal damage and arson") 
public_order_offences <-  c("Anti-social behaviour", "Public order" )
drug_and_weapon_offences <- c("Drugs", "Possession of weapons")
theft_and_robbery <- c("Bicycle theft", "Burglary", "Other theft", "Robbery" , "Shoplifting", "Theft from the person", "Vehicle crime")
other_crimes <- c("Other crime")


categorize_crime <- function(crime) {
  if (crime %in% violence_and_sexual_offences) {
    return("Violence and Sexual Offences")
  } else if (crime %in% criminal_damage_and_arson) {
    return("Criminal Damageccand Arson")
  } else if (crime %in% public_order_offences) {
    return("Public Order Offences")
  } else if (crime %in% drug_and_weapon_offences) {
    return("Drug and Weapon Offences")
  } else if (crime %in% theft_and_robbery) {
    return("Theft And Robbery")
  } else if (crime %in% other_crimes) {
    return("Other Crimes")
  } else {
    return("Unknown")
  }
}

crime_subcategory_id <- crime_subcategory_id %>%
  mutate(crime_category = sapply(crime_subcategory, categorize_crime))


crime_type <- crime_subcategory_id  %>% 
  rename(
    crime_type = crime_subcategory,
    crime_type_freq = crime_subcategory_freq, 
  )

crime_type <- crime_type %>% 
  select(crime_type_id, everything())
 
crime_type %>% print()

dim_crime_type_spark <- copy_to(sc, crime_type, "crime_type", overwrite = TRUE)
spark_write_csv(dim_crime_type_spark, path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_crime_type", overwrite = TRUE)

file.rename(
  from = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_crime_type/part-00000-326b7f8e-1d8b-4c98-8b21-cdbdac0a2349-c000.csv",
  to   = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_crime_type.csv"
)

spark_disconnect(sc)

