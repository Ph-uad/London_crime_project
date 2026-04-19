# dim_QOL_Indicators

library(tidyverse)
library(dplyr)
library(sparklyr)

sc <- spark_connect(master = "local", version = "3.4.0")

# dim_qol_indicators <- spark_read_csv(
#   sc,
#   path = "",
#   
# )


dim_qol_indicators <- tibble(
  indicator_id = 1,
  indicator_name = "Avg Income",
  indicator_type = "Economic",
)

dim_qol_indicators <- bind_rows(
  dim_qol_indicators,
  tibble(
    indicator_id = c(2, 3, 4, 5, 6, 7),
    indicator_name = c("Life Expectancy", "Happiness", "Anxiety", "Life Satisfaction", "Worthwhile", "Deprivation Index"
),
    indicator_type = c("Health", "Well being", "Well being", "Well being", "Well being", "Economic"),
  )
)


dim_qol_indicators %>% view()


dim_qol_indicators <- copy_to(sc, dim_qol_indicators, "dim_qol_indicators", overwrite = TRUE)
write.csv(dim_qol_indicators, "/Users/tsumizu/Documents/School/Sem. 2/ADMP/Assignment/Assignment 2/schema_data/dim_qol_indicators.csv", row.names = FALSE)
