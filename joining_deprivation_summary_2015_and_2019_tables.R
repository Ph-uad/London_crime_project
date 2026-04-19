#Install (or point to) a Spark distribution
#sparklyr can install Spark for you:
library(sparklyr)
spark_install(version = "3.4.0") # pick a recent Spark version
library(dplyr) 
library(tidyverse)


# (A) Local Spark
sc <- spark_connect(master = "local", version = "3.4.0")

deprivation_summary_2019  <- spark_read_csv(
  sc,
  name = "deprivation_summary_2019",
  path = "/Users/tsumizu/Downloads/ID 2019 /Borough domain summaries-Table 1.csv",
  header = TRUE,
  infer_schema = TRUE
)

deprivation_summary_2015  <- spark_read_csv(
  sc,
  name = "deprivation_summary_2015",
  path = "/Users/tsumizu/Downloads/ID 2015/Borough domain summaries-Table 1.csv",
  header = TRUE,
  infer_schema = TRUE
)


deprivation_summary_2019 %>% glimpse()

#to get a list of the dataset header
deprivation_summary_2019 %>% glimpse()

deprivation_summary_2019 <-  deprivation_summary_2019 %>%
  select( Local_Authority_District_name_2019  ,Income__Average_score,Income__Proportion_of_LSOAs_in_most_deprived_10_nationally,
         Employment__Average_score,Employment__Proportion_of_LSOAs_in_most_deprived_10_nationally,Education_Skills_and_Training__Average_score, Education_Skills_and_Training__Proportion_of_LSOAs_in_most_deprived_10_nationally,
         Health_Deprivation_and_Disability__Average_score, Health_Deprivation_and_Disability__Proportion_of_LSOAs_in_most_deprived_10_nationally, Crime__Average_score,Crime__Proportion_of_LSOAs_in_most_deprived_10_nationally, 
         Living_Environment__Average_score, Living_Environment__Proportion_of_LSOAs_in_most_deprived_10_nationally)

deprivation_summary_2019 %>% head()
deprivation_summary_2015 %>% glimpse()



deprivation_summary_2015 <- deprivation_summary_2015 %>%
  rename(
    district_name =  Local_Authority_District_name_2013,
    avg_income_score_2015 =  Income__Average_score ,
    # income_proportion_of_LSOAs_in_most_deprived_10_nationally_2015= Income__Proportion_of_LSOAs_in_most_deprived_10_nationally,
    # avg_employment_score_2015= Employment__Average_score,
    # employment_proportion_of_LSOAs_in_most_deprived_10_nationally_2015 = Employment__Proportion_of_LSOAs_in_most_deprived_10_nationally,
    # avg_education_skills_and_training_score_2015 = Education_Skills_and_Training__Average_score ,
    # education_skills_and_training_proportion_of_LSOAs_in_most_deprived_10_nationally_2015 = Education_Skills_and_Training__Proportion_of_LSOAs_in_most_deprived_10_nationally ,
    # avg_health_deprivation_and_disability_score_2015 = Health_Deprivation_and_Disability__Average_score ,
    # health_deprivation_and_disability_of_LSOAs_in_most_deprived_10_nationally_2015= Health_Deprivation_and_Disability__Proportion_of_LSOAs_in_most_deprived_10_nationally,
    # avg_crime_average_score_2015 = Crime__Average_score ,
    # crime_average_score_of_LSOAs_in_most_deprived_10_nationally_2015 = Crime__Proportion_of_LSOAs_in_most_deprived_10_nationally ,
    # avg_living_environment_score_2015 = Living_Environment__Average_score  ,
    # living_environment_score_of_LSOAs_in_most_deprived_10_nationally_2015 = Living_Environment__Proportion_of_LSOAs_in_most_deprived_10_nationally,
  )
  
deprivation_summary_2019 <- deprivation_summary_2019 %>%
  rename(
    district_name= Local_Authority_District_name_2019 ,
    avg_income_score_2019 =  Income__Average_score ,
    income_proportion_of_LSOAs_in_most_deprived_10_nationally_2019= Income__Proportion_of_LSOAs_in_most_deprived_10_nationally,
    avg_employment_score_2019= Employment__Average_score,
    employment_proportion_of_LSOAs_in_most_deprived_10_nationally_2019 = Employment__Proportion_of_LSOAs_in_most_deprived_10_nationally,
    avg_education_skills_and_training_score_2019 = Education_Skills_and_Training__Average_score ,
    education_skills_and_training_proportion_of_LSOAs_in_most_deprived_10_nationally_2019 = Education_Skills_and_Training__Proportion_of_LSOAs_in_most_deprived_10_nationally ,
    avg_health_deprivation_and_disability_score_2019 = Health_Deprivation_and_Disability__Average_score ,
    health_deprivation_and_disability_of_LSOAs_in_most_deprived_10_nationally_2019= Health_Deprivation_and_Disability__Proportion_of_LSOAs_in_most_deprived_10_nationally,
    avg_crime_average_score_2019 = Crime__Average_score,
    crime_average_score_of_LSOAs_in_most_deprived_10_nationally_2019 = Crime__Proportion_of_LSOAs_in_most_deprived_10_nationally,
    avg_living_environment_score_2019 = Living_Environment__Average_score,
    living_environment_score_of_LSOAs_in_most_deprived_10_nationally_2019 = Living_Environment__Proportion_of_LSOAs_in_most_deprived_10_nationally,
  )

deprivation_summary_2015 %>% view()
deprivation_summary_2019 %>% view()


deprivation_summary <- deprivation_summary_2015 %>%
  inner_join(deprivation_summary_2019, by = "district_name")

deprivation_summary %>% view()

single_file_tbl <- deprivation_summary %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/deprivation 15&19",
  mode  = "overwrite",
  header = TRUE
)


spark_disconnect(sc)

  # select(-Income__Rank_of_average_rank, -Income__Rank_of_average_score, -Income__Rank_of_Proportion_of_LSOAs_in_most_deprived_10_nationally, 
  #        -Income__Rank_of_scale, -`_c19`, -Education_Skills_and_Training__Average_rank, -Education_Skills_and_Training__Rank_of_average_rank, 
  #        -Education_Skills_and_Training__Rank_of_proportion_of_LSOAs_in_most_deprived_10_nationally, -Education_Skills_and_Training__Rank_of_average_score,
  #        -`_c26`, -Health_Deprivation_and_Disability__Average_rank, -Health_Deprivation_and_Disability__Rank_of_average_rank, -Health_Deprivation_and_Disability__Rank_of_average_score
  #        -Health_Deprivation_and_Disability__Rank_of_proportion_of_LSOAs_in_most_deprived_10_nationally, -`_c33`, -Crime__Average_rank, -Crime__Rank_of_average_rank,
  #        -Crime__Rank_of_average_score, -Crime__Rank_of_proportion_of_LSOAs_in_most_deprived_10_nationally, -`c40`, -Barriers_to_Housing_and_Services__Average_rank, 
  #        -Barriers_to_Housing_and_Services__Rank_of_average_rank, - Barriers_to_Housing_and_Services__Rank_of_average_score, -Barriers_to_Housing_and_Services__Rank_of_average_score,) %>%
  
  
  
  
  
  
  
  
  