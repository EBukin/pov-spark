# Calculating poverty efficiently 

library(conflicted)
library(arrow)

library(data.table)
setDTthreads(32)

library(dplyr, warn.conflicts = FALSE)

library(duckdb)
library(DBI)
library(dtplyr)
library(dbplyr)
library(duckplyr)

library(collapse)
conflicts_prefer(dplyr::filter(), dplyr::lag(), dplyr::lead())

library(microbenchmark)
library(glue)
# library(profvis)

# Source files -------------------------------------------------------------

source("R/ftg.R")

# Read data -------------------------------------------------------------

dta_1m <- open_dataset("data/sample_1m") |> collect()
dta_5m <- open_dataset("data/sample_5m") |> collect()
dta_50m <- open_dataset("data/sample_50m") |> collect()
# dta_500m <- open_dataset("data/sample_500m") 

# Correct Poverty and GINI ----------------------------------------------

## dplyr
dta_tbl <- dta_1m |> 
  # filter(country == 1, year == 1990 ) |> arrange(country, year, welfare) |>
  group_by(country, year) 

pov_cor <- get_pov__dplyr(dta_tbl, 250)
pov_alt <- get_pov_slow__dplyr(dta_tbl, 250)
pov_cor |> all.equal(pov_alt)
get_pov__dplyr_nogini(dta_tbl, 250) |> all.equal(pov_alt)
get_pov__dplyr_nopmax(dta_tbl, 250) |> all.equal(pov_alt)

# microbenchmark(
#   get_pov__dplyr(dta_tbl, 250),
#   get_pov__dplyr_nopmax(dta_tbl, 250),
#   get_pov__dplyr_nogini(dta_tbl, 250),
#   get_pov_slow__dplyr(dta_tbl, 250),
#   times = 10
# )
# # # Unit: milliseconds
# #                                 expr      min       lq      mean   median       uq      max neval
# #         get_pov__dplyr(dta_tbl, 250) 157.7437 162.2942 215.72470 165.6281 327.6836 339.6044    10
# #  get_pov__dplyr_nopmax(dta_tbl, 250) 158.6102 164.2333 241.87913 237.3550 320.0885 331.2409    10
# #  get_pov__dplyr_nogini(dta_tbl, 250)  92.0283  94.9571  99.34864  97.7462 106.8319 108.5998    10
# #    get_pov_slow__dplyr(dta_tbl, 250) 255.4480 261.4204 372.09485 412.9124 421.7446 441.0405    10


## DT ------------------------------------------------------------------------

get_pov__DT(qDT(dta_tbl), 250) |> as_tibble() |> all.equal(pov_cor)
get_pov__DT_nogini(qDT(dta_tbl), 250) |> as_tibble() |> all.equal(pov_cor)

## 5m Benchmarking DPLYR vs DT ---------------------------------------------

# dta <- open_dataset("data/sample_5m") |> collect()
# microbenchmark(
#   get_pov__dplyr(group_by(dta, country, year) , 250),
#   get_pov__dplyr_nogini(group_by(dta, country, year) , 250),
#   get_pov__DT(qDT(dta), 250),
#   get_pov__DT_nogini(qDT(dta), 250),
#   times = 3
# )
# # Unit: milliseconds
# #                                                      expr      min       lq     mean   median       uq       max neval
# #         get_pov__dplyr(group_by(dta, country, year), 250) 636.8596 678.2938 816.1343 719.7279 905.7716 1091.8153     3
# #  get_pov__dplyr_nogini(group_by(dta, country, year), 250) 301.2942 428.1590 474.1020 555.0239 560.5059  565.9878     3
# #                                get_pov__DT(qDT(dta), 250) 353.9678 354.4676 354.9408 354.9674 355.4273  355.8872     3
# #                         get_pov__DT_nogini(qDT(dta), 250) 161.8439 162.3442 163.9009 162.8446 164.9295  167.0143     3

## 50m Benchmarking DPLYR vs DT ---------------------------------------------

# dta <- open_dataset("data/sample_50m") |> collect()
# microbenchmark(
#   get_pov__dplyr(group_by(dta, country, year) , 250),
#   get_pov__dplyr_nogini(group_by(dta, country, year) , 250),
#   get_pov__DT(qDT(dta), 250),
#   get_pov__DT_nogini(qDT(dta), 250),
#   times = 3
# )
# # Unit: seconds
# #                                                      expr      min       lq     mean   median       uq      max neval
# #         get_pov__dplyr(group_by(dta, country, year), 250) 5.619309 5.756574 5.863884 5.893839 5.986172 6.078505     3
# #  get_pov__dplyr_nogini(group_by(dta, country, year), 250) 2.371264 2.383796 2.526175 2.396327 2.603630 2.810934     3
# #                                get_pov__DT(qDT(dta), 250) 3.443943 3.673975 4.015365 3.904006 4.301076 4.698146     3
# #                         get_pov__DT_nogini(qDT(dta), 250) 1.784891 1.907978 2.298829 2.031065 2.555797 3.080530     3

## 250m Benchmarking DPLYR vs DT ---------------------------------------------

# dta <- open_dataset("data/sample_250m") |> collect()
# microbenchmark(
#   get_pov__dplyr(group_by(dta, country, year) , 250),
#   get_pov__dplyr_nogini(group_by(dta, country, year) , 250),
#   get_pov__DT(qDT(dta), 250),
#   get_pov__DT_nogini(qDT(dta), 250),
#   times = 1
# )
# Unit: seconds
#                                                      expr       min        lq      mean    median        uq       max neval
#         get_pov__dplyr(group_by(dta, country, year), 250) 28.037560 28.037560 28.037560 28.037560 28.037560 28.037560     1
#  get_pov__dplyr_nogini(group_by(dta, country, year), 250) 11.074106 11.074106 11.074106 11.074106 11.074106 11.074106     1
#                                get_pov__DT(qDT(dta), 250) 17.991187 17.991187 17.991187 17.991187 17.991187 17.991187     1
#                         get_pov__DT_nogini(qDT(dta), 250)  8.387914  8.387914  8.387914  8.387914  8.387914  8.387914     1

## DuckDB -------------------------------------------------------------

duck_dr <- duckdb("data-duck-temp/test.duckdb")
con <- dbConnect(duck_dr)

# dbDisconnect(con, shutdown = TRUE)
dbGetQuery(conn = con, "SELECT current_setting('threads') AS threads")
dbGetQuery(conn = con, "SELECT current_setting('memory_limit') AS memlimit")

# # Load data in the DB ---------------------------------------------
# dbExecute(con, "
# CREATE TABLE sample_1m AS
#     SELECT * FROM read_parquet('data/sample_1m/**/*.parquet');
#     ")

# dbExecute(con, "
# CREATE TABLE sample_5m AS
#     SELECT * FROM read_parquet('data/sample_5m/**/*.parquet');
#     ")

# dbExecute(con, "
# CREATE TABLE sample_50m AS
#     SELECT * FROM read_parquet('data/sample_50m/**/*.parquet');
#     ")

    
# dbExecute(con, "
# CREATE TABLE sample_250m AS
#     SELECT * FROM read_parquet('data/sample_250m/**/*.parquet');
#     ")


## Simple calculations ------------------------------------------------------
db_dta <- tbl(con, "sample_5m")

get_pov__duckdb_nogini <- function(con, tbl_name, pl = 100) {
  tbl(con, tbl_name) |>
    group_by(country, year) |>
    get_pov__dplyr_nogini(pl) |>
    arrange(country, year) |>
    collect()    
}


get_pov__duckdb_gini <- function(con, tbl_name) {
  tbl(con, tbl_name) |>
    group_by(country, year) |>
    arrange(country, year, welfare) |>
    mutate(
      cum_pop = cumsum(weight) / sum(weight),
      cum_inc = cumsum(welfare * weight) / sum(welfare * weight),
      cum_lead = lead(cum_inc),
      cum_lag = lag(cum_inc)
    ) |>
    summarise(
      gini = sum(cum_lead * cum_pop) - sum(cum_lag * cum_pop),
      .groups = "drop"
    ) |>
    select(country, year, gini) |> 
    collect()
}

get_gini_sql <- function(tbl_name = "sample_50m", ending = "") {
glue(
  "WITH sorted AS (
  SELECT
    country,
    year,
    welfare,
    weight,
    SUM(weight) OVER (PARTITION BY country, year) AS total_weight,
    SUM(welfare * weight) OVER (PARTITION BY country, year) AS weighted_sum,
    ROW_NUMBER() OVER (PARTITION BY country, year ORDER BY welfare) AS rn,
    SUM(weight) OVER (
      PARTITION BY country, year
      
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_weight
  FROM {tbl_name}
),
gini_calc AS (
  SELECT
    country,
    year,
    welfare,
    weight,
    total_weight,
    weighted_sum,
    (cum_weight - weight / 2.0) / total_weight AS lorenz_x,
    (welfare * weight) / weighted_sum AS lorenz_y,
    rn
  FROM sorted
),
gini_with_lag AS (
  SELECT
    *,
    LAG(lorenz_x, 1, 0) OVER (PARTITION BY country, year ORDER BY rn) AS lorenz_x_prev
  FROM gini_calc
)
SELECT
  country,
  year,
  1 - SUM(lorenz_y * (lorenz_x - lorenz_x_prev)) AS gini
FROM gini_with_lag
GROUP BY country, year
  {ending}
  ;
  "
)
}

get_pov__duckdb_nogini(con, "sample_5m") |> 
  all.equal(get_pov__dplyr_nogini(group_by(dta_5m, country, year), 100))

get_pov__dplyr(group_by(dta_5m, country, year), 100) |> 
  select(country, year, gini) |> 
  all.equal(get_pov__duckdb_gini(con, "sample_5m"))

dc_tbl <- tbl(con, "sample_50m") |> group_by(country, year) 
    arrange(country, year, welfare)

dbGetQuery(con, get_gini_sql("sample_5m")) |> 
  as_tibble() |> 
  all.equal(get_pov__duckdb_gini(con, "sample_50m"))


dc_tbl |> show_query()

# dc_tbl |> get_pov_slow__dplyr(150)
# dc_tbl |> get_pov_slow2__dplyr(150)

# microbenchmark(
#   collect(get_pov__dplyr_nogini(dc_tbl, 250)),
#   collect(get_pov_slow__dplyr(dc_tbl, 250)),
#   collect(get_pov_slow2__dplyr(dc_tbl, 250)),
#   times = 2
# )

# # GIni calculation benchmarking ---------------------------------------------
microbenchmark(
  get_pov__duckdb_nogini(con, "sample_5m"),
  dbGetQuery(con, get_gini_sql("sample_5m")) ,
  get_pov__duckdb_gini(con, "sample_5m"),
  times = 2
)
# # Unit: milliseconds
# #                                        expr      min       lq      mean    median        uq       max neval
# #    get_pov__duckdb_nogini(con, "sample_5m")  232.229  232.229  234.9159  234.9159  237.6028  237.6028     2
# #  dbGetQuery(con, get_gini_sql("sample_5m")) 2995.951 2995.951 3092.1004 3092.1004 3188.2497 3188.2497     2
# #      get_pov__duckdb_gini(con, "sample_5m") 3736.323 3736.323 3788.2528 3788.2528 3840.1830 3840.1830     2


# microbenchmark(
#   get_pov__duckdb_nogini(con, "sample_50m"),
#   dbGetQuery(con, get_gini_sql("sample_50m")) ,
#   get_pov__duckdb_gini(con, "sample_50m"),
#   times = 1
# )
# # Unit: milliseconds
# #                                         expr        min         lq       mean     median         uq        max neval
# #    get_pov__duckdb_nogini(con, "sample_50m")   447.0285   447.0285   447.0285   447.0285   447.0285   447.0285     1
# #  dbGetQuery(con, get_gini_sql("sample_50m")) 42770.8498 42770.8498 42770.8498 42770.8498 42770.8498 42770.8498     1
# #      get_pov__duckdb_gini(con, "sample_50m") 43884.3592 43884.3592 43884.3592 43884.3592 43884.3592 43884.3592     1



# ## 5m Benchmarking DuckDB vs DPLYR vs DT --------------------------------------------
# microbenchmark(
#   get_pov__dplyr_nogini(group_by(dta_5m, country, year) , 250),
#   get_pov__DT_nogini(qDT(dta_5m), 250),
#   collect(get_pov__duckdb_nogini(con, "sample_5m", 250)),
#   times = 10
# )
# # Unit: milliseconds
# #                                                         expr      min       lq     mean   median       uq      max neval
# #  get_pov__dplyr_nogini(group_by(dta_5m, country, year), 250) 246.6971 263.3072 344.0957 303.9558 352.1064 672.4228    10
# #                         get_pov__DT_nogini(qDT(dta_5m), 250) 149.2261 155.8531 164.6822 166.1911 171.4148 184.1361    10
# #       collect(get_pov__duckdb_nogini(con, "sample_5m", 250)) 159.0060 163.2099 274.2674 203.9521 263.6038 701.2617    10


# ## 50m Benchmarking DuckDB vs DPLYR vs DT --------------------------------------------
# dta_50m <- open_dataset("data/sample_50m") |> collect()
# microbenchmark(
#   get_pov__dplyr_nogini(group_by(dta_50m, country, year), 250),
#   get_pov__DT_nogini(qDT(dta_50m), 250),
#   collect(get_pov__duckdb_nogini(con, "sample_50m", 250)),
#   times = 10
# )
# # Unit: milliseconds
# #                                                          expr       min        lq      mean    median        uq       max neval
# #  get_pov__dplyr_nogini(group_by(dta_50m, country, year), 250) 2570.4224 2808.1677 2957.2882 3037.6551 3147.3593 3190.0181    10
# #                         get_pov__DT_nogini(qDT(dta_50m), 250) 1790.0157 1887.2354 2126.3403 2129.4073 2358.3307 2486.9413    10
# #       collect(get_pov__duckdb_nogini(con, "sample_50m", 250))  197.9357  201.6341  250.3288  210.0115  219.8687  559.6356    10


# # ## 250m Benchmarking DuckDB vs DPLYR vs DT --------------------------------------------
# dta_250m <- open_dataset("data/sample_250m") |> collect()
# microbenchmark(
#   get_pov__dplyr_nogini(group_by(dta_250m, country, year) , 250),
#   get_pov__DT_nogini(qDT(dta_250m), 250),
#   collect(get_pov__duckdb_nogini(con, "sample_250m", 250)),
#   times = 5
# )
# # Unit: milliseconds
# #                                                           expr        min         lq       mean     median         uq      max neval
# #  get_pov__dplyr_nogini(group_by(dta_250m, country, year), 250) 11462.9905 11875.5468 11958.6597 12122.9275 12142.8200 12189.01     5
# #                         get_pov__DT_nogini(qDT(dta_250m), 250)  8152.6180  8697.7525  9067.9570  8977.1777  8996.3822 10515.85     5
# #       collect(get_pov__duckdb_nogini(con, "sample_250m", 250))   192.0631   195.0852   416.2085   209.4305   222.8341  1261.63     5

























