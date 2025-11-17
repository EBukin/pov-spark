# Calculating poverty efficiently 

library(conflicted)
library(arrow)

library(data.table)
setDTthreads(32)

library(dplyr, warn.conflicts = FALSE)
library(tidyr)

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
duck_dr <- duckdb("data-testing-duck/gmd-simple.duckdb")
con <- dbConnect(duck_dr)

# dbDisconnect(con, shutdown = TRUE)
dbGetQuery(conn = con, "SELECT current_setting('threads') AS threads")
dbGetQuery(conn = con, "SELECT current_setting('memory_limit') AS memlimit")

# # Load data in the DB ---------------------------------------------

# List all tables in the connection
dbListTables(con)

dta <- tbl(con, "dta") |> rename(country = code, welfare = welfppp)
dic <- 
  tbl(con, "dic") |> 
  collect() 


get_random_code <- function(n = 1, variable = "code") {  
  dic |>
    filter(variable == variable) |> 
    slice_sample(n = n) |> 
    pull(value) |> 
    as.integer()
}

get_random_code(10)

filter_dta <- function(dta, n = 10) {
  codes <- get_random_code(n, "code")
  dta |> filter(country %in% codes) 
}

test_vars <- c("country", "year", "school",  "marital", "urban", "male")

group_dta <- function(dta) {
  vars <- test_vars
  dta |> 
    group_by(across(all_of(vars)))
}

# DUCKDB: Random Filter + Group -------------------------------------------
set.seed(13)
dta |>
  filter_dta() |>
  group_dta() |>   
  get_pov__dplyr_nogini(pl = 10, groups = test_vars) |> 
  collect()

dta |>
  filter_dta() |>
  group_dta() |>   
  get_pov__dplyr_nogini(pl = 10, groups = test_vars) |>
  explain()

ftg_duckdb_random <- function(dta, n = 10) {
  dta |>
    filter_dta(n) |>
    group_dta() |>
    get_pov__dplyr_nogini(pl = 10, groups = test_vars) |>
    collect() |> 
    arrange(across(all_of(test_vars)))
}

set.seed(13)
ftg_duckdb_random(dta)

# DUCK+DT: Random Filter + Group -------------------------------------------
ftg_duckdb_DT_random <- function(dta, n = 10) {
  dta |>
    filter_dta(n) |>
    collect() |>
    qDT() |>
    get_pov__DT_nogini(pl = 10, groups = test_vars)
}

set.seed(13)
ftg_duckdb_DT_random(dta)

# Arrow+DT: Random Filter + Group -------------------------------------------
dta_arrow <- open_dataset("data-testing/gmd-simple") |> 
  rename(country = code, welfare = welfppp)

ftg_arrow_DT_random <- function(dta = dta_arrow, n = 10) {
  dta |> ftg_duckdb_DT_random(n)
}

set.seed(13)
ftg_arrow_DT_random(dta_arrow)

# Ginin Arrow+DT: Random Filter + Group --------------------------------------


# Benchmarking with different random samples (1 country all years) -------------------------------
microbenchmark(
  duckdb = ftg_duckdb_random(dta, 1),
  duckdb_dt = ftg_duckdb_DT_random(dta, 1),
  arrow_dt = ftg_arrow_DT_random(dta_arrow, 1),
  setup = set.seed(124),
  times = 5
)

#                      
# Unit: milliseconds
#       expr      min       lq      mean   median       uq      max neval
#     duckdb 317.7394 325.0528 326.89654 327.2598 328.7040 335.7267     5
#  duckdb_dt  97.7570  98.9363 101.17258 100.1780 101.2782 107.7134     5
#   arrow_dt  68.7067  69.9971  71.52462  70.1082  71.1632  77.6479     5

# Benchmarking with same seed in each expression (10 countries all years) -------------------------------
microbenchmark(
  duckdb = ftg_duckdb_random(dta, 10),
  duckdb_dt = ftg_duckdb_DT_random(dta, 10),
  arrow_dt = ftg_arrow_DT_random(dta_arrow, 10),
  setup = set.seed(124),
  times = 5
)

# Unit: milliseconds
#       expr       min       lq      mean    median        uq       max neval
#     duckdb  338.9231  340.329  352.1627  343.8456  357.2666  380.4493     5
#  duckdb_dt 2112.6583 2376.683 2411.5149 2487.4563 2497.5256 2583.2510     5
#   arrow_dt 1730.8332 1779.275 1778.9807 1782.1960 1784.8419 1817.7569     5


# Benchmarking with different random samples (25 countries all years) -------------------------------
microbenchmark(
  duckdb = ftg_duckdb_random(dta, 25),
  duckdb_dt = ftg_duckdb_DT_random(dta, 25),
  arrow_dt = ftg_arrow_DT_random(dta_arrow, 25),
  setup = set.seed(124),
  times = 5
)

# Unit: milliseconds
#       expr       min        lq      mean    median        uq       max neval
#     duckdb  338.6494  363.3158  363.7269  365.3257  370.1626  381.1808     5
#  duckdb_dt 2403.4391 2405.5444 2513.0811 2550.0267 2586.8690 2619.5265     5
#   arrow_dt 1530.9328 1743.5997 1723.4060 1748.7909 1793.7322 1799.9742     5


set.seed(124)
a <- ftg_duckdb_random(dta, 25) |> as.matrix()

set.seed(124)
b <- ftg_duckdb_DT_random(dta, 25) |> as.matrix()

set.seed(124)
c <- ftg_arrow_DT_random(dta_arrow, 25) |> as.matrix()

all.equal(a, c)
all.equal(b, c)
