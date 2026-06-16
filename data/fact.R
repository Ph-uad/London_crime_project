# 
install.packages("sparklyr")
spark_install(version = "3.1.2")

library(sparklyr)
library(dplyr) 
library(tidyverse)  

spark_disconnect_all()
sc <- spark_connect(master = "local")

df <- sdf_copy_to(sc, data.frame(value = rnorm(1000)), overwrite = TRUE)

# df %>% glimpse()
# fact_table %>% glimpse()


time <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_time.csv",
  name = "time",
  infer_schema = TRUE,
  header = TRUE,
)

borough <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_boroughs.csv",
  name = "borough",
  infer_schema = TRUE,
  header = TRUE,
)

dim_crime_type <- spark_read_csv(
  sc,
  path = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_crime_type.csv',
  name = "dim_crime_type",
  infer_schema = TRUE,
  header = TRUE,
)


wellbeing <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/well_being/well_being.csv",
  name = "being",
  infer_schema = TRUE,
  header = TRUE,
)

income <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/avg_income/avg_income.csv",
  name = "income",
  infer_schema = TRUE,
  header = TRUE,
)

london_crime <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/clean data/london crime/london_crime.csv",
  name = "london_crime",
  infer_schema = TRUE,
  header = TRUE,
)

london_crime %>% glimpse()
time %>% glimpse()
wellbeing %>% glimpse()
deprivation %>% glimpse()
income %>% glimpse()


london_crime <- london_crime %>%
  select(-Context, -Last_outcome_category)

london_crime %>% glimpse()

wellbeing <- wellbeing %>% 
  rename(
    borough_name = area
  )

# Making little table to dynamically join wellbeing values by years to london crime to make fact table
wellbeing_2015 <-  wellbeing %>%
  select(
    borough_name,
    worthwhile = worthwhile_14_15,
    life_satisfaction = life_satisfaction_14_15,
    happiness = happiness_14_15,
    anxiety = anxeity_14_15,
  )


wellbeing_2016 <-  wellbeing %>%
  select(
    borough_name,
    worthwhile = worthwhile_15_16,
    life_satisfaction = life_satisfaction_15_16,
    happiness = happiness_15_16,
    anxiety = anxeity_15_16,
  )

wellbeing_2017 <-  wellbeing %>%
  select(
    borough_name,
    worthwhile = worthwhile_16_17,
    life_satisfaction = life_satisfaction_16_17,
    happiness = happiness_16_17,
    anxiety = anxeity_16_17,
  )

wellbeing_2018 <- wellbeing %>%
  select(
    borough_name,
    worthwhile = worthwhile_17_18,
    life_satisfaction = life_satisfaction_17_18,
    happiness = happiness_17_18,
    anxiety = anxeity_17_18,
  )

wellbeing_2019 <- wellbeing %>%
  select(
    borough_name,
    worthwhile = worthwhile_18_19,
    life_satisfaction = life_satisfaction_18_19,
    happiness = happiness_18_19,
    anxiety = anxeity_18_19,
  )

wellbeing_2015 <- wellbeing_2015 %>%
  mutate(
    year = 2015
  )

wellbeing_2016 <- wellbeing_2016 %>%
  mutate(
    year = 2016
  )

wellbeing_2017 <- wellbeing_2017 %>%
  mutate(
    year = 2017
  )

wellbeing_2018 <- wellbeing_2018 %>%
  mutate(
    year = 2018
  )

wellbeing_2019 <- wellbeing_2019 %>%
  mutate(
    year = 2019
  )

wellbeing_2019 %>% glimpse()
 

all_wellbeing <- sdf_bind_rows(
  wellbeing_2015,
  wellbeing_2016,
  wellbeing_2017,
  wellbeing_2018,
  wellbeing_2019
)

all_wellbeing %>% glimpse()

london_crime <- london_crime %>%
  mutate(
      year = as.integer(substr(Month, 1, 4))
  ) %>%
  rename(
    borough_name = LSOA_name
  )

london_crime %>% glimpse()

fact_table %>% glimpse()

fact_table <- london_crime %>%
  left_join(
    select(all_wellbeing, year, borough_name,life_satisfaction, worthwhile, anxiety, happiness ),
    by = c("year", "borough_name")
  )

fact_table %>% filter(life_satisfaction >= 1) %>% glimpse()

income %>% glimpse()

avg_income_2015 <-  income %>%
  select(
    borough_name,
    avg_income = `1415`,
  ) %>% 
  mutate(
    year = 2015
  )

avg_income_2016 <-  income %>%
  select(
    borough_name,
    avg_income = `1516`,
  )  %>% 
  mutate(
    year = 2016
  )

avg_income_2017 <-  income %>%
  select(
    borough_name,
    avg_income = `1617`,
  ) %>% 
  mutate(
    year = 2017
  )

avg_income_2018 <- income %>%
  select(
    borough_name,
    avg_income = `1718`,
  ) %>% 
  mutate(
    year = 2018
  )

avg_income_2019 <- income %>%
  select(
    borough_name,
    avg_income = `1819`,
  ) %>% 
  mutate(
    year = 2019
  )

avg_income_2020 <- income %>%
  select(
    borough_name,
    avg_income = `1920`,
  ) %>% 
  mutate(
    year = 2020
  )

avg_income_2021 <- income %>%
  select(
    borough_name,
    avg_income = `2021`,
  )  %>% 
  mutate(
    year = 2021
  )

avg_income_2022 <- income %>%
  select(
    borough_name,
    avg_income = `2122`,
  )  %>% 
  mutate(
    year = 2022
  )

avg_income_2023 <- income %>%
  select(
    borough_name,
    avg_income = `2223`,
  ) %>% 
  mutate(
    year = 2023
  )

all_avg_income <- sdf_bind_rows(
  avg_income_2015,
  avg_income_2016,
  avg_income_2017,
  avg_income_2018,
  avg_income_2019,
  avg_income_2020,
  avg_income_2021,
  avg_income_2022,
  avg_income_2023
)

avg_income_2015 %>% glimpse()

fact_table <- fact_table %>%
  left_join(
    select(all_avg_income, year,borough_name, avg_income),
    by = c("year", "borough_name")
  )

fact_table %>% glimpse()




deprivation %>% glimpse()

deprivation_15 <- borough %>%
  select(
    deprivation_index = deprivation_score_2015,
    borough_name
  )%>%
  mutate(
    year = 2015 
  )

deprivation_19 <- borough %>%
  select(
    deprivation_index = deprivation_score_2019,
    borough_name
  )%>%
  mutate(
    year = 2019
  )

deprivation_15 %>% glimpse()

all_deprivation <- sdf_bind_rows(
  deprivation_15,
  deprivation_19
)

fact_table <- fact_table %>%
  left_join(
    select(all_deprivation, year,borough_name, deprivation_index),
    by = c("year", "borough_name")
  )

fact_table %>% filter(year == 2015) %>%  glimpse()



time %>% glimpse()

fact_table <- fact_table %>%
  rename(
    date = Month
  )

fact_table <- fact_table %>%
  mutate(
    month = month(date)
  )

fact_table <- fact_table %>%
  left_join(
    select(time, year, month, time_id),
    by = c("year", "month")
  )

fact_table %>% filter(month == 5, year == 2022) %>%  glimpse()

borough %>% glimpse()

fact_table <- fact_table %>%
  left_join(
    select(borough, borough_name, borough_id),
    by = "borough_name"
  )

fact_table %>% filter(borough_name == "Croydon") %>%  glimpse()

dim_crime_type %>% glimpse()

fact_table <- fact_table %>%
  rename(
    crime_type = Crime_type
  )%>%
  left_join(
    select(dim_crime_type, crime_type, crime_type_id),
    by = "crime_type"
  )

fact_table %>% filter(crime_type == "Bicycle theft") %>%  glimpse()
 

dim_crime_type %>% count()



# df <- sdf_copy_to(sc, data.frame(value = rnorm(15000000)), overwrite = TRUE)
df <- sdf_copy_to(sc, data.frame(value = rnorm(100000)), overwrite = TRUE)

df_with_id <- sdf_with_sequential_id(fact_table, id = "id_num") %>%
    mutate(id_num = id_num + 1) %>%
  mutate(
    fact_id =  sql("concat('Z-', lpad(cast(id_num as string), 7, '0'))")
  )


df_with_id %>% glimpse()



stage_fact_data <- stage_fact_data %>%
  distinct() %>%
  arrange(year) %>%
  mutate(fact_id = row_number())
  

final_fact_table <- df_with_id

final_fact_table %>% glimpse()
  
fact_data %>% glimpse()

final_fact_table <-  final_fact_table %>%
  select(
    fact_id,
    borough_id,
    time_id ,
    crime_type_id,
    deprivation_index,
    happiness,
    anxiety,
    worthwhile,
    life_satisfaction,
    avg_income,
  )

 library(dplyr)

final_fact_table <- final_fact_table %>%
  mutate(across(everything(), ~ifelse(is.na(.), 0, .)))

library(dplyr)

final_fact_table <- final_fact_table %>%
  mutate(income_clean = regexp_replace(avg_income, ",", ""))

final_fact_table %>% glimpse()
final_fact_table <- final_fact_table %>%
  rename(
    avg_income = income_clean
  )


final_fact_table %>%
  group_by(fact) %>%
  tally() %>%
  filter(n > 1)

single_file_tbl <- final_fact_table %>%
  sdf_repartition(partitions = 6)

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_data",
  mode  = "overwrite",
  header = TRUE
)


stage_fact_data <- fact_table





london_crime %>% 
  distinct(LSOA_name) %>% print(n = Inf)






























fact_data <- spark_read_csv(
  sc,
  name = "fact_data",
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact/part-00000-f5a45036-5f4d-4596-adc7-c675ad313fe3-c000.csv",
  header = TRUE,
  infer_schema = TRUE, 
)

fact_data <- fact_data %>%
  select(Month, Location, borough_name, Crime_type, year)

fact_data %>% filter(year == 2015) %>% glimpse()
avg_income_2015 %>% glimpse()



fact_id
borough_id
time_id 
crime_type_id
avg_income
deprivation_index
life_satisfaction
happiness
anxiety
worthwhile

wellbeing2015 %>% glimpse()

all_wellbeing <- sdf_bind_rows(
  wellbeing2015,
  wellbeing_2016,
  wellbeing_2017,
  wellbeing2018,
  wellbeing_2019,
)

fact_table <- fact_table %>%
  left_join(
    select(all_wellbeing, year,borough_name,life_satisfaction ),
    by = c("year", "borough_name")
  )


fact_data <- fact_data %>%
  left_join(
    select(all_avg_income, avg_income, year, borough_name),
    by = c("year", "borough_name")
  )

fact_data %>% filter(year == 2023) %>% glimpse()

single_file_tbl <- fact_data %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact",
  mode  = "overwrite",
  header = TRUE
)

file.rename(
  from = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact/part-00000-a0364691-a1ea-4789-bd0d-97b49f28d313-c000.csv',
  to = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table.csv"
)

spark_disconnect(sc)
sc <- spark_connect(master = "local")

fact_table <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table.csv",
  header = TRUE,
  infer_schema = TRUE,
  name = "fact_table"
)

dim_borough <- spark_read_csv(
  sc,
  path = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_boroughs.csv',
  header = TRUE,
  infer_schema = TRUE,
  name = "dim_borough"
)

fact_table <- fact_table %>%
  left_join(select(dim_borough, borough_id, borough_name), by= "borough_name")

fact_table %>% filter(borough_id ==5 ) %>%  glimpse()

imd_index_2015 <- dim_borough %>%
  select(borough_id, deprivation = deprivation_score_2015) %>%
  mutate(year = 2015)

fact_table %>%glimpse()
  

imd_index_2019 <- dim_borough %>%
  select(borough_id, deprivation = deprivation_score_2019) %>%
  mutate(year = 2019)

# fact_table <- fact_table %>%
#   select(-deprivation_x, deprivation_y, -deprivation_x_x, -deprivation_y_y)

imd_index <- sdf_bind_rows(
  imd_index_2019,
  imd_index_2015
)

fact_table <-fact_table %>%
  left_join(
    select(imd_index, borough_id, year, deprivation),
    by = c("year", "borough_id")
  )

fact_table %>% filter( year == 2019) %>%  glimpse()

dim_crime <- spark_read_csv(
  sc,
  path = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_crime_type.csv',
  header = TRUE,
  infer_schema = TRUE,
  name = "dim_crime"
)

fact_table <- fact_table %>%
  rename(
    crime_type = Crime_type
  )

fact_table <-fact_table %>%
  left_join(
    select(dim_crime, crime_type, crime_type_id),
    by = "crime_type"
  )

fact_table %>% filter( year == 2019) %>%  glimpse()

single_file_tbl <- fact_data %>%
  sdf_repartition(partitions = 6)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact",
  mode  = "overwrite",
  header = TRUE
)

















# fact_table <- fact_table %>%
#   select(-crime_type, - )

dim_time <- spark_read_csv(
  sc,
  path = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_time.csv',
  header = TRUE,
  infer_schema = TRUE,
  name = "dim_time"
)

fact_table <- fact_table %>%
  mutate(
    year = year(Month),
    months = month(Month),
    quarter = quarter(Month)
  )

fact_table %>% glimpse()

fact_table <-fact_table %>%
  left_join(
    select(dim_time,year,month,  time_id),
    by = c("year", "month")
  )

fact_table <-fact_table %>%
  select(Location, borough_name, year, avg_income, borough_id, deprivation, crime_type_id )

fact_table <-fact_table %>%
  select(-life_satisfaction_x, -life_satisfaction_y)

fact_table <- fact_table %>%
  select(
    fact_id,
    borough_id,
    time_id ,
    crime_type_id,
    avg_income,
    deprivation_index,
    life_satisfaction,
    happiness,
    anxiety,
    worthwhile,
  )



colnames(fact_table)

fact_table %>% glimpse()


single_file_tbl <- fact_table %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact",
  mode  = "overwrite",
  header = TRUE
)

file.rename(
  from = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact/part-00000-5fe56459-667c-4cba-bda7-e92ce29b979d-c000.csv',
  to = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table.csv"
)


fact_data <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table.csv",
  infer_schema = TRUE,
  header = TRUE,
  name = "fact_table"
)



fact_data %>% filter(happiness >= 1) %>% glimpse()

fact_data <- fact_data %>%
  select(
    -time,
    -Location,
    -borough_name,
    -crime_type,
    -year,
  )


fact_data %>% filter(deprivation >= 35) %>% glimpse()

fact_data <- fact_data %>%
  rename(
    fact_id = crime_type_id
  )


single_file_tbl <- fact_data %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact",
  mode  = "overwrite",
  header = TRUE
)

file.rename(
  from = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact/part-00000-59487e19-f2dd-4ff5-8c4a-e089cebc33e5-c000.csv',
  to = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table.csv"
)

spark_disconnect(sc)



fact_data <- spark_read_csv(
  sc,
  path = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table.csv',
  infer_schema = TRUE,
  header = TRUE,
  name = "fact_table"
)

fact_data <- fact_data %>%
  mutate(avg_income = regexp_replace(avg_income, ",", "")
  )

single_file_tbl <- fact_data %>%
  sdf_repartition(partitions = 6)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact",
  mode  = "overwrite",
  header = TRUE
)

file.rename(
  from = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact/part-00000-edf46c14-e907-4e98-b165-753327980a93-c000.snappy.parquet',
  to = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table1.snappy.parquet"
)

file.rename(
  from = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact/part-00001-edf46c14-e907-4e98-b165-753327980a93-c000.snappy.parquet',
  to = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table2.snappy.parquet"
)

# %>%
  # select(-row_num)  # optionally remove the helper column

fact_data <- spark_read_csv(
  sc,
  path = '/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_boroughs.csv',
  infer_schema = TRUE,
  header = TRUE,
  name = "fact_table"
)

fact_data %>% glimpse()



# 
# wellbeing <- wellbeing %>% 
#   rename(
#     borough_name = area,
#     life_satisfaction_12 = life_satisfaction_11_12,
#     life_satisfaction_13 = life_satisfaction_12_13,
#     life_satisfaction_14 = life_satisfaction_13_14,
#     life_satisfaction_15 = life_satisfaction_14_15,
#     life_satisfaction_16 = life_satisfaction_15_16,
#     life_satisfaction_17 = life_satisfaction_16_17,
#     life_satisfaction_18 = life_satisfaction_17_18,
#     life_satisfaction_19 = life_satisfaction_18_19,
#     worthwhile_12 = worthwhile_11_12 ,
#     worthwhile_13 = worthwhile_12_13,
#     worthwhile_14 = worthwhile_13_14,
#     worthwhile_15 = worthwhile_14_15,
#     worthwhile_16 = worthwhile_15_16,
#     worthwhile_17 = worthwhile_16_17,
#     worthwhile_18 = worthwhile_17_18,
#     worthwhile_19 = worthwhile_18_19,
#     happiness_12 = happiness_11_12 ,
#     happiness_13 = happiness_12_13,
#     happiness_14 = happiness_13_14,
#     happiness_15 = happiness_14_15,
#     happiness_16 = happiness_15_16,
#     happiness_17 = happiness_16_17,
#     happiness_18 = happiness_17_18,
#     happiness_19 = happiness_18_19,
#     anxeity_12 = anxeity_11_12, 
#     anxiety_13 = anxeity_12_13,
#     anxiety_14 = anxeity_13_14,
#     anxiety_15 = anxeity_14_15,
#     anxiety_16 = anxeity_15_16,
#     anxiety_17 = anxeity_16_17,
#     anxiety_18 = anxeity_17_18,
#     anxiety_19 = anxeity_18_19,
#   )


# wellbeing <- wellbeing %>%
#   select(
#     borough_name,
#     life_satisfaction_15, life_satisfaction_16, life_satisfaction_17, life_satisfaction_18, life_satisfaction_19,
#     worthwhile_15, worthwhile_16, worthwhile_17, worthwhile_18, worthwhile_19,
#     happiness_15, happiness_16, happiness_17, happiness_18, happiness_19,
#     anxiety_15, anxiety_16, anxiety_17, anxiety_18, anxiety_19,
#   )
# 
# wellbeing <- wellbeing %>%
#   left_join(select(deprivation, borough_name, deprivation_score_2015, deprivation_score_2019), by = "borough_name")


# library(dplyr)
# library(rlang)
# 
# join_wellbeing_metric <- function(crime_data, well_being, metric_prefix) {
#   
#   crime_data_with_year <- crime_data %>%
#     mutate(year = as.integer(substr(Month, 1, 4)))
#   
#   joined_data <- crime_data_with_year %>%
#     left_join(well_being, by = "borough_name") %>%
#     mutate(
#       !!metric_prefix := case_when(
#         year == 2015 ~ !!sym(paste0(metric_prefix, "_15")),
#         year == 2016 ~ !!sym(paste0(metric_prefix, "_16")),
#         year == 2017 ~ !!sym(paste0(metric_prefix, "_17")),
#         year == 2018 ~ !!sym(paste0(metric_prefix, "_18")),
#         year == 2019 ~ !!sym(paste0(metric_prefix, "_19")),
#         TRUE ~ NA_real_
#       )
#     )
#   
#   return(joined_data)
# }

# final_crime_data <- join_wellbeing_metric(crime_data =  london_crime, well_being =  wellbeing , "life_satisfaction")

london_crime <- london_crime %>% 
  mutate(
    year = substr(Month ,1, 4)
  )

london_crime %>% glimpse()

wellbeing2015


final_crime_data <- london_crime %>% 
  left_join(
    select(wellbeing2015, year, borough_name,  
           worthwhile = worthwhile_15,
           life_satisfaction = life_satisfaction_15,
           happiness = happiness_15,
           anxiety = anxiety_15),
    by = c("year", "borough_name")
  )

final_crime_data %>% view()

check <- final_crime_data %>%
  filter(year == 2019, borough_name == "Bexley" )

income %>% glimpse()




check <- final_crime_data %>%
  filter(year == 2023, borough_name == "Harrow" )

check %>% view()


final_crime_data <- final_crime_data %>%
  select(-Context, -Last_outcome_category, -Reported_by, -Falls_within)

single_file_tbl <- final_crime_data %>%
  sdf_repartition(partitions = 1)  

spark_write_csv(
  single_file_tbl,
  path  = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact",
  mode  = "overwrite",
  header = TRUE
)

file.rename(
  from = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact/part-00000-f5a45036-5f4d-4596-adc7-c675ad313fe3-c000.csv",
  to = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table.csv"
)




# Borough ID change for name 

crime_data <- spark_read_csv(
  sc,
  path = "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/fact_table.csv",,
  name = "crime_data",
  infer_schema = TRUE,
  header = TRUE,
)
