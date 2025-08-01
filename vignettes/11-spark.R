# Data generation for testing purposes
library(conflicted)
library(arrow)

library(data.table)
setDTthreads(32)

library(dtplyr)
library(dbplyr)
library(dplyr, warn.conflicts = FALSE)
conflicts_prefer(dplyr::lag, dplyr::lead)

library(collapse)

library(microbenchmark)

library(sparklyr)

# connect to Spark
config <- spark_config()

config$`sparklyr.shell.driver-memory` <- '64G'
# config$`spark.executor.instances` <- '4'

sc <- spark_connect(master = "local", config = config)

# spark_disconnect(sc)
# source("R/ftg.R")

# files with data -----------------------------------------------------------
path_1m <- "data/sample_1m"
path_5m <- "data/sample_5m"
path_50m <- "data/sample_50m"
path_500m <- "data/sample_500m"

# Using spark for poverty and gini --------------------------------------------

dta <- open_dataset("data/sample_5m/") |> collect()

# hh_tbl <- 
#   spark_read_parquet( 
#   sc, 
#   name = "hh_tbl", 
#   path = "C:/Users/wb532966/eb-local/pov-spark/data/sample_1m/country=1/part-0.parquet", 
#   # options = list(), 
#   repartition = 32, 
#   memory = TRUE, 
#   overwrite = TRUE, 
#   # columns = NULL, 
#   # schema = NULL, 
#   # ... 
# ) 

hh_tbl <- copy_to(sc, dta, "hh_tbl", overwrite = TRUE)
hh_tbl |> collect()
# pl <- 100

get_pov__dplyr <- function(dta, pl) {
  dta |> 
  group_by(country, year) |>
  arrange(country, year, welfare) |>
  mutate(
    poor = as.integer(welfare <= pl),
    hc = poor * weight,
    wmax = hc * (pl - welfare),
    cum_pop = cumsum(weight ) / sum(weight),
    cum_inc = cumsum(welfare * weight) / sum(welfare * weight)
  ) |> 
  mutate(
    cum_inc_lead = lead(cum_inc, default = 0),
    cum_inc_lag = lag(cum_inc, default = 0)
  ) |> 
  summarise(
    hc = sum(hc),
    weight = sum(weight),
    wmax = sum(wmax),
    gini1 = sum(cum_inc_lead * cum_pop, na.rm = T),
    gini2 = sum(cum_inc_lag * cum_pop, na.rm = T),
    .groups = "drop"
  ) |> 
  mutate(
    ftg0 = hc / weight,
    ftg1 = wmax / weight / pl,
    ftg2 = ftg1 ^ 2,
    gini = gini1 - gini2
  ) |> 
  select(country, year, hc, ftg0, ftg1, ftg2, gini)
}


hh_tbl  |>  get_pov__dplyr(250) |> collect()
hh_tbl |> collect() 
dta |> get_pov__dplyr(250) 

# spark_apply(hh_tbl, nrow, group_by = "country")

# For 1 mln
# microbenchmark(
#   get_pov__dplyr(dta, 250),
#   get_pov__dplyr(hh_tbl, 250),
#   times = 10
# )
# # Unit: milliseconds
# # expr      min       lq      mean    median        uq      max neval
# # get_pov__dplyr(dta, 250) 780.9183 798.0021  946.3031  814.0699  909.3288 1958.846    10
# # get_pov__dplyr(hh_tbl, 250) 979.8483 980.7436 1029.9332 1002.3629 1112.2390 1120.221    10

# For 2 mln
microbenchmark(
  get_pov__dplyr(dta, 250),
  get_pov__dplyr(hh_tbl, 250),
  times = 10
)
# Unit: milliseconds
# expr       min        lq     mean   median       uq      max neval
# get_pov__dplyr(dta, 250) 1356.1118 1452.6107 1489.009 1484.655 1499.684 1637.784    10
# get_pov__dplyr(hh_tbl, 250)  991.5161  992.6858 1141.504 1092.149 1267.975 1506.912    10
