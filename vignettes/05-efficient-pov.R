# Calculating poverty efficiently 

library(conflicted)
library(arrow)

library(data.table)
setDTthreads(32)

library(dplyr, warn.conflicts = FALSE)

library(duckdb)

library(dtplyr)
library(dbplyr)
library(duckplyr)

library(collapse)
conflicts_prefer(dplyr::filter(), dplyr::lag(), dplyr::lead())

library(microbenchmark)
library(profvis)

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

# # Unit: milliseconds
# #                                 expr      min       lq     mean   median       uq      max neval
# #         get_pov__dplyr(dta_tbl, 250) 348.9707 369.5064 436.1559 381.3473 529.4359 557.0243    10
# #  get_pov__dplyr_nopmax(dta_tbl, 250) 324.4104 333.7826 376.2160 351.2255 373.9992 503.6767    10
# #  get_pov__dplyr_nogini(dta_tbl, 250) 165.2066 171.9551 196.2291 175.4228 196.1794 359.0436    10
# #    get_pov_slow__dplyr(dta_tbl, 250) 762.2433 936.8946 930.7522 942.8344 956.3956 980.2391    10


## DT ------------------------------------------------------------------------

get_pov__DT(qDT(dta_tbl), 250) |> as_tibble() |> all.equal(pov_cor)
get_pov__DT_nogini(qDT(dta_tbl), 250) |> as_tibble() |> all.equal(pov_cor)


## Benchmarking DPLYR vs DT ---------------------------------------------

# dta <- open_dataset("data/sample_5m") |> collect()
# microbenchmark(
#   get_pov__dplyr(group_by(dta, country, year) , 250),
#   get_pov__dplyr_nogini(group_by(dta, country, year) , 250),
#   get_pov__DT(qDT(dta), 250),
#   get_pov__DT_nogini(qDT(dta), 250),
#   times = 3
# )
# # Unit: milliseconds
# #                                                      expr      min        lq      mean    median        uq       max neval
# #         get_pov__dplyr(group_by(dta, country, year), 250) 954.4853 1042.2530 1080.2149 1130.0206 1143.0797 1156.1388     3
# #  get_pov__dplyr_nogini(group_by(dta, country, year), 250) 413.8668  500.8850  537.4365  587.9032  599.2214  610.5396     3
# #                                get_pov__DT(qDT(dta), 250) 661.3397  672.4404  785.9107  683.5410  848.1961 1012.8513     3
# #                         get_pov__DT_nogini(qDT(dta), 250) 226.5202  230.8212  284.1453  235.1222  312.9579  390.7935     3


# dta <- open_dataset("data/sample_50m") |> collect()
# microbenchmark(
#   get_pov__dplyr(group_by(dta, country, year) , 250),
#   get_pov__dplyr_nogini(group_by(dta, country, year) , 250),
#   get_pov__DT(qDT(dta), 250),
#   get_pov__DT_nogini(qDT(dta), 250),
#   times = 3
# )
# Unit: seconds
#                                                      expr       min        lq      mean    median        uq       max neval
#         get_pov__dplyr(group_by(dta, country, year), 250) 11.400559 12.382634 12.921231 13.364708 13.681567 13.998426     3
#  get_pov__dplyr_nogini(group_by(dta, country, year), 250)  4.459267  5.188055  5.989964  5.916842  6.755313  7.593784     3
#                                get_pov__DT(qDT(dta), 250)  4.739437  4.881952  5.356797  5.024466  5.665477  6.306488     3
#                         get_pov__DT_nogini(qDT(dta), 250)  2.041641  2.055975  2.163757  2.070308  2.224814  2.379321     3


## DuckDB -------------------------------------------------------------

# dta_dc <- dta_1m |> as_duckdb_tibble()

path_duckdb <- tempfile(fileext = ".duckdb")
con <- DBI::dbConnect(duckdb::duckdb(path_duckdb))
DBI::dbWriteTable(con, "data", dta_1m, overwrite = TRUE)

read_sql_duckdb("SELECT current_setting('memory_limit') AS memlimit")


DBI::dbSendQuery(con, "SET memory_limit = '32GB';")

dta_dc <- tbl(con, "data")
dta_dc |>
  group_by(country, year) |> 
  get_pov_fast__dplyr(100) |>
  collect() 
    explain()


db2 <- tbl(con, "read_parquet('data/sample_5m/**/*.parquet', hive_partitioning = true)") 


db2 |> 
  group_by(country, year) |> 
  get_pov__dplyr(100) |> 
  # show_query()
  collect() 



























