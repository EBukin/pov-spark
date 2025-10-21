# Data generation for testing purposes

library(conflicted)
library(arrow)

library(data.table)

setDTthreads(32)
library(dtplyr)

library(dplyr, warn.conflicts = FALSE)

library(collapse)

library(duckdb)
library(duckplyr)
conflict_prefer("filter", "dplyr")

library(microbenchmark)
library(profvis)

# Read data
dta_1m <- open_dataset("data/sample_1m") 
dta_5m <- open_dataset("data/sample_5m") 
dta_50m <- open_dataset("data/sample_50m") 
dta_500m <- open_dataset("data/sample_500m") 


# Loading data  ---------------------------------------

load_data__DT <- function(od_dta) {
  as.data.table(collect(od_dta))
}

load_data__DT_key <- function(od_dta) {
  as.data.table(collect(od_dta), key = c("country", "year"))
}
# load_data__DT(dta_1m)

load_data__DT_c <- function(od_dta) {
  collapse::qDT(collect(od_dta))
}

# load_data__DT_c(dta_1m)

# load_data__ducdb <- function(path) {
#   tbl(
#     con,
#     paste0("read_parquet('", path, "/**/*.parquet', hive_partitioning = true)")
#   )
# }
# collect(load_data__ducdb("data/sample_1m"))

## Benchmark
microbenchmark(
  load_data__DT(dta_1m),
  load_data__DT(dta_5m),
  # load_data__DT(dta_50m),
  # load_data__DT(dta_500m),
  
  load_data__DT_key(dta_1m),
  load_data__DT_key(dta_5m),
  # load_data__DT_key(dta_50m),
  # load_data__DT_key(dta_500m),
  
  load_data__DT_c(dta_1m),
  load_data__DT_c(dta_5m),
  # load_data__DT_c(dta_50m),
  # load_data__DT_c(dta_500m),
  # collect(load_data__ducdb("data/sample_1m")),
  # collect(load_data__ducdb("data/sample_5m")),
  # collect(load_data__ducdb("data/sample_50m")),
  # collect(load_data__ducdb("data/sample_500m")),
  times = 3
)

# Filter data by country ---------------------------------------
flt_country__DT <- function(dta, country_id) {
  filter(dta, country %in% country_id)
}

flt_year__DT <- function(dta, years) {
  filter(dta, year %in% years)
}

flt_country__DT_c <- function(dta, country_id) {
  fsubset(dta, country %iin% country_id)
}

flt_year__DT_c <- function(dta, years) {
  fsubset(dta, year %iin% years)
}

flt_country__arrow <- function(dta, country_id) {
  dta |> filter(country %in% country_id)
}

flt_year__arrow <- function(dta, years) {
  dta |> filter(year %in% years)
}

# Benchmark
DT_1 <- load_data__DT_c(dta_1m)
DT_5 <- load_data__DT_c(dta_5m)
DT_50 <- load_data__DT_c(dta_50m)
DT_500 <- load_data__DT_c(dta_500m) # Slow for 500m

set.seed(1234)
country_set <- DT_1$country |> unique() |> sample(10)
years_set <- DT_1$year |> unique() |> sample(10)

# # flt_country__DT(DT_1, country_set)
# # flt_country__DT(DT_50, country_set)
# # flt_country__DT_c(DT_1, country_set)
# # flt_country__DT_c(DT_50, country_set)

# microbenchmark(
#   flt_country__DT(DT_1, country_set),
#   flt_country__DT(DT_5, country_set),
#   flt_country__DT(DT_50, country_set),
#   flt_country__DT(DT_500, country_set),
#   collect(flt_country__arrow(dta_1m, country_set)),
#   collect(flt_country__arrow(dta_5m, country_set)),
#   collect(flt_country__arrow(dta_50m, country_set)),
#   collect(flt_country__arrow(dta_500m, country_set)),
#   flt_country__DT_c(DT_1, country_set),
#   flt_country__DT_c(DT_5, country_set),
#   flt_country__DT_c(DT_50, country_set),
#   flt_country__DT_c(DT_500, country_set),
#   times = 3
# )

# microbenchmark(
#   flt_year__DT(flt_country__DT(DT_5, country_set), years_set),
#   flt_year__DT(flt_country__DT(DT_50, country_set), years_set),
#   flt_year__DT(flt_country__DT(DT_500, country_set), years_set),
#   flt_year__DT_c(flt_country__DT_c(DT_5, country_set), years_set),
#   flt_year__DT_c(flt_country__DT_c(DT_50, country_set), years_set),
#   flt_year__DT_c(flt_country__DT_c(DT_500, country_set), years_set),
#   collect(flt_year__arrow(flt_country__arrow(dta_5m, country_set), years_set)),
#   collect(flt_year__arrow(flt_country__arrow(dta_50m, country_set), years_set)),
#   collect(flt_year__arrow(flt_country__arrow(dta_500m, country_set), years_set)),
#   load_data__DT_c(flt_year__arrow(flt_country__arrow(dta_5m, country_set), years_set)),
#   load_data__DT_c(flt_year__arrow(flt_country__arrow(dta_50m, country_set), years_set)),
#   load_data__DT_c(flt_year__arrow(flt_country__arrow(dta_500m, country_set), years_set)),
#   times = 1
# )

# Group by calculations --------------------------------------------------------

group_by__DPL <- function(dta) {
  group_by(dta, country, year)
}

get_stats__DPL <- function(dta, pl) {
 dta |>
    mutate(
      poor = welfare <= pl,
      hc = poor * weight,
      cum_pop = cumsum(weight / sum(weight)),
      cum_inc = cumsum(welfare * weight) / sum(welfare * weight)
    ) |> 
    summarise(
      hc0 = sum(hc),
      ftg0 = sum(hc) / sum(weight),
      ftg1 = sum((pmax(pl - welfare, 0) / pl) * weight) / sum(weight),
      gini  =  sum(cum_inc[-1] * cum_pop[-n()]) - sum(cum_inc[-n()] * cum_pop[-1]),
      .groups = "drop"
    ) |> 
    mutate(ftg2 = ftg1 ^ 2) |> 
   select(country, year, hc = hc0, ftg0, ftg1, ftg2, gini)
}

get_stats__DPL_fgt <- function(dta, pl) {
 dta |>
    mutate(
      poor = welfare <= pl,
      hc = poor * weight,
      wmax = hc * (pl - welfare),
    ) |> 
    summarise(
      hc = sum(hc),
      weight = sum(weight),
      wmax = sum(wmax) ,
      .groups = "drop"
    ) |> 
   mutate(
    ftg0 = hc / weight,
    ftg1 = wmax / weight / pl,
    ftg2 = ftg1 ^ 2
   ) |> 
   select(country, year, hc, ftg0, ftg1, ftg2)
}

get_stats__DPL_gini <- function(dta, pl) {
 dta |>
    mutate(
      cum_pop = cumsum(weight / sum(weight)),
      cum_inc = cumsum(welfare * weight) / sum(welfare * weight)
    ) |> 
    summarise(
      hc = sum(hc),
      weight = sum(weight),
      wmax = sum(wmax) ,
      .groups = "drop"
    ) |> 
   mutate(
    ftg0 = hc / weight,
    ftg1 = wmax / weight / pl,
    ftg2 = ftg1 ^ 2
   ) |> 
   select(country, year, hc, ftg0, ftg1, ftg2)
}



get_stats__DPL_fgt_c <- function(dta, pl) {
 dta |>
    mutate(
      poor = welfare <= pl,
      hc = poor * weight,
      wmax = hc * (pl - welfare)
    ) |> 
    summarise(
      hc = fsum(hc),
      weight = fsum(weight),
      wmax = fsum(wmax)  ,
      .groups = "drop"
    ) |> 
   mutate(
      ftg0 = hc / weight,
      ftg1 = wmax / weight / pl,
      ftg2 = ftg1 ^ 2
    ) |> 
   select(country, year, hc, ftg0, ftg1, ftg2)
}

get_stats__DPL_fgt_cc <- function(dta, pl0) {
 dta |>
    fmutate(
      poor = welfare <= pl0,
      hc = poor * weight,
      wmax = hc * (pl0 - welfare)
    ) |> 
    fsummarise(
      hc = fsum(hc),
      weight = fsum(weight),
      wmax = fsum(wmax) 
    ) |> 
   fungroup() |> 
   fmutate(
      ftg0 = hc / weight,
      ftg1 = wmax / weight / pl0,
      ftg2 = ftg1 ^ 2
    ) |> 
   fselect(country, year, hc, ftg0, ftg1, ftg2)
}

get_stats__DPL_fgt_ccc <- function(dta, pl) {
 dta |>
    fmutate(
      poor = welfare <= pl,
      hc = poor * weight,
      wmax = hc * (pl - welfare)
    ) |> 
   collapg(
      FUN = fsum, 
      cols = c("hc", "wmax", "weight")
    ) |> 
   fmutate(
      ftg0 = hc / weight,
      ftg1 = wmax / weight / pl,
      ftg2 = ftg1 ^ 2
    ) |> 
   fselect(country, year, hc, ftg0, ftg1, ftg2)
}

# # Testing Dplyr solution
# test_dta_1 <- load_data__DT_c(dta_1m) |> as_tibble() |> group_by__DPL() 
# test_dta_50 <- load_data__DT_c(dta_50m) |> as_tibble() |> group_by__DPL() 

# get_stats__DPL(test_dta_1, 250)
# get_stats__DPL_fgt(test_dta_1, 250)
# get_stats__DPL_fgt_c(test_dta_1, 250)
# get_stats__DPL_fgt_cc(test_dta_1, 250)
# get_stats__DPL_fgt_ccc(test_dta_1, 250)

# get_stats__DPL(test_dta_1, 250) |> all.equal(get_stats__DPL_fgt(test_dta_1, 250))
# get_stats__DPL(test_dta_1, 250) |> all.equal(get_stats__DPL_fgt_c(test_dta_1, 250))
# get_stats__DPL(test_dta_1, 250) |> all.equal(get_stats__DPL_fgt_cc(test_dta_1, 250))
# get_stats__DPL(test_dta_1, 250) |> all.equal(get_stats__DPL_fgt_ccc(test_dta_1, 250))

# # 4 seconds for 50 mln
# microbenchmark(
#   get_stats__DPL(test_dta_1, 250),
#   get_stats__DPL_fgt(test_dta_1, 250),
#   get_stats__DPL_fgt_c(test_dta_1, 250),
#   get_stats__DPL_fgt_cc(test_dta_1, 250),
#   get_stats__DPL_fgt_ccc(test_dta_1, 250),
#   # get_stats__DPL(test_dta_50, 250),
#   # get_stats__DPL_fgt(test_dta_50, 250),
#   # get_stats__DPL_fgt_c(test_dta_50, 250),
#   get_stats__DPL_fgt_cc(test_dta_50, 250),
#   get_stats__DPL_fgt_ccc(test_dta_50, 250),  
#   times = 5
# )

## DT based solutions ----------------------------------------------------------
get_stats__DT_fgt <- function(dta, pl, group_by = c("country", "year")) {
  # browser()
  dta[,
    `:=`(c("poor", "hc", "wmax"), {
      poor <- welfare <= pl
      hc <- poor * weight
      wmax <- hc * (pl - welfare)
      .(poor, hc, wmax)
    }),
    by = group_by
  ] 
  
  dta <- 
    dta[, 
      lapply(.SD, sum, na.rm = TRUE), 
      keyby = group_by, 
      .SDcols = c("hc", "wmax", "weight")
    ]
  
  dta[,
    `:=`(c("ftg0", "ftg1", "ftg2"), {
      ftg0 <- hc / weight
      ftg1 <- wmax / weight / pl
      ftg2 <- ftg1 ^ 2
      .(ftg0, ftg1, ftg2)
    })
  ]

  dta[, .(country, year, hc, ftg0, ftg1, ftg2)]
}

get_stats__DT_fgt_c <- function(dta, pl, group_by = c("country", "year")) {
  # browser()
  dta[,
    `:=`(c("poor", "hc", "wmax"), {
      poor <- welfare <= pl
      hc <- poor * weight
      wmax <- hc * (pl - welfare)
      .(poor, hc, wmax)
    }),
    by = group_by
  ] 
  
  dta |> 
    gby(group_by) |> 
    gv(c("hc", "wmax", "weight")) |> 
    fsum() |> 
    fmutate(
      ftg0 = hc / weight,
      ftg1 = wmax / weight / pl,
      ftg2 = ftg1 ^ 2
    ) |> 
    fselect(country, year, hc, ftg0, ftg1, ftg2)
}


get_stats__DT_fgt_cc <- function(dta, pl, group_by = c("country", "year")) {
  dta[,
    `:=`(c("poor", "hc", "wmax"), {
      poor <- welfare <= pl
      hc <- poor * weight
      wmax <- hc * (pl - welfare)
      .(poor, hc, wmax)
    }),
    by = group_by
  ] 
  
  dta_out <- 
    dta |> 
    gby(group_by) |> 
    gv(c("hc", "wmax", "weight")) |> 
    fsum()

  dta_out[, `:=`(c("ftg0", "ftg1", "ftg2"), {
    ftg0 <- hc/weight
    ftg1 <- wmax/weight/250
    ftg2 <- ftg1^2
    .(ftg0, ftg1, ftg2)
  })]
  
  dta_out[, .(country, year, hc, ftg0, ftg1, ftg2)]
}

DT_1 <- load_data__DT_c(dta_1m)
load_data__DT(dta_1m) |> get_stats__DT_fgt(250) |> as_tibble() 

get_stats__DT_fgt(load_data__DT(dta_1m), 250) |> as_tibble() |> 
  all.equal(get_stats__DPL(test_dta_1, 250))

get_stats__DT_fgt(load_data__DT(dta_1m), 250) |> 
  all.equal(get_stats__DT_fgt_c(load_data__DT(dta_1m), 250))

get_stats__DT_fgt(load_data__DT(dta_1m), 250) |> 
  all.equal(get_stats__DT_fgt_cc(load_data__DT(dta_1m), 250))


## Big benchmark -------------------------------------------------------
# microbenchmark(
#   get_stats__DT_fgt(DT_1, 250),
#   get_stats__DT_fgt_c(DT_1, 250),  
#   get_stats__DT_fgt_cc(DT_1, 250),  
#   get_stats__DT_fgt(DT_5, 250),
#   get_stats__DT_fgt_c(DT_5, 250),  
#   get_stats__DT_fgt_cc(DT_5, 250),  
#   get_stats__DT_fgt(DT_50, 250),
#   get_stats__DT_fgt_c(DT_50, 250),  
#   get_stats__DT_fgt_cc(DT_50, 250),   
#   get_stats__DT_fgt(DT_500, 250),
#   get_stats__DT_fgt_c(DT_500, 250),  
#   get_stats__DT_fgt_cc(DT_500, 250),  
#   times = 10
# )
# # Unit: milliseconds
# #                               expr        min         lq        mean      median         uq        max neval
# #       get_stats__DT_fgt(DT_1, 250)    52.6033    55.3372    58.96415    57.16215    61.8859    71.3253    10
# #     get_stats__DT_fgt_c(DT_1, 250)    51.9351    53.0436   200.81502    55.83055    60.2284  1504.2736    10
# #    get_stats__DT_fgt_cc(DT_1, 250)    54.9755    57.2530    59.25590    58.52805    60.9183    64.4293    10
# #       get_stats__DT_fgt(DT_5, 250)   161.1649   185.0498   193.75953   196.96165   203.4144   227.5275    10
# #     get_stats__DT_fgt_c(DT_5, 250)   189.1876   200.1860   212.00211   215.00110   221.8400   236.3237    10
# #    get_stats__DT_fgt_cc(DT_5, 250)   189.4867   196.6378   212.52308   216.10850   230.1169   231.7793    10
# #      get_stats__DT_fgt(DT_50, 250)  1221.9380  1239.7485  2118.76418  1548.66355  3319.5247  3982.5006    10
# #    get_stats__DT_fgt_c(DT_50, 250)  1659.7307  1733.2672  2130.49904  1969.90520  2117.8033  3348.6601    10
# #   get_stats__DT_fgt_cc(DT_50, 250)  1508.5677  1648.3598  1734.90925  1706.98910  1792.6969  1967.0458    10
# #     get_stats__DT_fgt(DT_500, 250) 11310.4462 11857.1280 12667.72930 12234.78705 12890.0340 16379.4781    10
# #   get_stats__DT_fgt_c(DT_500, 250) 13531.8046 14323.8372 15625.11739 16095.32735 16496.6227 16962.4534    10
# #  get_stats__DT_fgt_cc(DT_500, 250) 14779.6420 15560.7501 16513.03321 16488.81920 17003.5040 19397.1607    10


## Coimpare DPL with DT ---------------------------------

get_stats__DT_fgt_c(DT_5, 250)
get_stats__DPL_fgt_cc(test_dta_50, 250)




get_stats__DT_fgt(load_data__DT(dta_1m), 250) |> as_tibble() |> 
  all.equal(get_stats__DPL(test_dta_1, 250))





get_stats__DT <- function(dta, pl) {
  dta[,
    `:=`(c("poor", "hc", "cum_pop", "cum_inc"), {
      poor <- (welfare <= pl)
      hc <- poor * weight
      cum_pop <- cumsum(weight / sum(weight))
      cum_inc <- cumsum(welfare * weight) / sum(welfare * weight)
      .(poor, hc, cum_pop, cum_inc, welfare, weight)
    }),
    # by = .(country, year)
  ] 

  DT_1_out <- 
    dta [,
      .(
        hc0 = sum(hc),
        ftg0 = sum(hc) / sum(weight),
        ftg1 = sum((pmax(pl - welfare, 0) / pl) * weight) / sum(weight),
        gini = sum(cum_inc[-1] * cum_pop[-.N]) -
          sum(cum_inc[-.N] * cum_pop[-1])
      ),
      keyby = .(country, year)
    ] 
  
  DT_1_out [, `:=`(ftg2 = ftg1^2)]

  return(DT_1_out)
}

group_by__DT_c <- function(dta) {
  fgroup_by(dta, country, year)
}

get_stats__DT_c <- function(dta, pl) {
  # dta$pl <- pl
  dta$zero <- 0
  dta[,
    `:=`(c("poor", "wmax"# , "hc"#, "cum_pop", "cum_inc"
  ), {
      poor <- (welfare <= pl) * weight
      wmax <- (fmax(pl - welfare, zero) / pl) * weight
      # hc <- poor * weight
      # cum_pop <- fcumsum(weight / fsum(weight))
      # cum_inc <- fcumsum(welfare * weight) / fsum(welfare * weight)
      .(poor, wmax)
    }),
    by = .(country, year)
  ] 
  
  DT_1_out <- 
    dta [,
      .(
        poor = fsum(poor),
        weight2 = fsum(weight),
        wmax = fsum(wmax),
        ftg1 = sum((pmax(pl - welfare, 0)) * weight) / sum(weight)  / pl#,
        # ftg0 = fsum(hc) / fsum(weight),
        # ftg1 = fsum(wmax) / fsum(weight)#,
        # gini = fsum(cum_inc[-1] * cum_pop[-.N]) - fsum(cum_inc[-.N] * cum_pop[-1])
      ),
      keyby = .(country, year)
    ] 
  
  DT_1_out [, `:=`(ftg0 = poor / weight2)]
  # DT_1_out [, `:=`(ftg1 = wmax / weight)]
  DT_1_out [, `:=`(ftg2 = ftg1^2)]

  return(DT_1_out)
}


DT_1 <- load_data__DT_c(dta_1m)
as_tibble(stats_dt_c(DT_1))


DT_1 <- load_data__DT_c(dta_1m)
as_tibble(stats_dt(DT_1))



# Testing
DT_1 <- load_data__DT_c(dta_1m)
# DT_5 <- load_data__DT_c(dta_5m)
# DT_50 <- load_data__DT_c(dta_50m)
# DT_500 <- load_data__DT_c(dta_500m) # Slow for 500m

stats_dpl <- \(dta) dta |> as_tibble() |> group_by__DPL() |> get_stats__DPL(250)
stats_dpl2 <- \(dta) dta |> as_tibble() |> group_by__DPL() |> get_stats__DPL_fgt(250)
stats_dtpl <- \(dta) dta  |> lazy_dt() |> group_by__DPL() |> get_stats__DPL(250) |> collect()
stats_dt <- \(dta) get_stats__DT(dta, 250)
stats_dt_c <- \(dta) get_stats__DT_c(dta, 250)

# print(
as_tibble(stats_dt(DT_1))
as_tibble(stats_dt_c(DT_1))
stats_dt_c(DT_1)

all.equal(stats_dpl(DT_1), stats_dtpl(DT_1))
all.equal(stats_dtpl(DT_1), as_tibble(stats_dt(DT_1)))
all.equal(as_tibble(stats_dt(DT_1)), as_tibble(stats_dt_c(DT_1)))


microbenchmark(
  stats_dpl(DT_1),
  stats_dtpl(DT_1),
  stats_dt(DT_1),
  stats_dt_c(DT_1),
  times = 3
)

microbenchmark(
  stats_dpl(DT_5),
  stats_dtpl(DT_5),
  stats_dt(DT_5),
  stats_dt_c(DT_5),
  times = 3
)



DT_1_out <- DT_1 |> lazy_dt() |> group_by__DT() |> get_stats__DPL(250) |> collect()
DT_1_out |> all.equal(DT_1)

DT_1 |> group_by__DT() |> get_stats__DT(250) 

DT_1 <- load_data__DT_c(dta_1m)



dta_dt <- dta_full |> collect() |> as.data.table() |> 
    select(-1) |> lazy_dt()

# pl <- 250

get_stats <- function(dta, pl) {
  dta |>
    mutate(
      poor =  as.integer(welfare <= pl),
      hc = poor * weight,
      cum_pop = cumsum(weight / sum(weight)),
      cum_inc = cumsum(welfare * weight) / sum(welfare * weight)
    ) |> 
    summarise(
      hc0 = sum(hc),
      ftg0 = sum(hc) / sum(weight),
      ftg1 = sum((pmax(pl - welfare, 0) / pl) * weight) / sum(weight)#,
      # gini2  =  sum(cum_inc[-1] * cum_pop[-n()]) - sum(cum_inc[-n()] * cum_pop[-1]),
    ) |> 
    mutate(ftg2 = ftg1 ^ 2) 
}

# Summarize dta_full by country and year
data_summary <- dta_full |> group_by(country, year) |> get_stats(250) |> collect()

setDTthreads(32)
aa <- profvis::profvis({
  dta_full |> group_by(country, year) |> get_stats(250) |> collect()
})

aa

# 
# library(duckdb)
# library(dbplyr)
# library(duckplyr)
# 
# con <- dbConnect(duckdb())
# duckdb_register(con, "dta", dta_full)
# 
# tbl(con, "dta") |> group_by(country, year) |> get_stats(250) |> collect()
# 
# bb <- profvis::profvis({
#   tbl(con, "dta") |> group_by(country, year) |>  get_stats(250) |> collect()
# })
# bb
  