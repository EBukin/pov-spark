library(arrow)
library(data.table)
library(collapse)

# 1 year data 13m 
# 
setDTthreads(32)

dta_test <- open_dataset("data/sample_1k") |> dplyr::collect() |> qDT()

data.table::setindexv(dta, c("country", "year", "gender", "area"))


gini_dt <- function(dta, ...) {
  dta[,
    `:=`(c("gini"), {
      cum_pop <- collapse::fcumsum(weight) / sum(weight)
      cum_inc <- collapse::fcumsum(welfare * weight) / sum(welfare * weight)
      gini1 = data.table::shift(
        cum_inc,
        n = 1,
        fill = NA_real_,
        type = "lead"
      ) *
        cum_pop
      gini2 = data.table::shift(cum_inc, n = 1, fill = NA_real_, type = "lag") *
        cum_pop
      gini = sum(gini1 - gini2, na.rm = TRUE)
      .(gini)
    }),
    keyby = .(country, year)
  ]
}




dta_1m <- open_dataset("data/sample_1m") |> dplyr::collect() |> qDT()
dta_1m_index <- copy(dta_1m)
data.table::setindexv(dta_1m_index, c("country", "year", "gender", "area"))

dta_5m <- open_dataset("data/sample_5m") |> dplyr::collect() |> qDT()
dta_5m_index <- copy(dta_5m)
data.table::setindexv(dta_5m_index, c("country", "year", "gender", "area"))


dta_50m <- open_dataset("data/sample_50m") |> dplyr::collect() |> qDT()
dta_50m_index <- copy(dta_50m)
data.table::setindexv(dta_50m_index, c("country", "year", "gender", "area"))


dta_150m <- open_dataset("data/sample_150m") |> dplyr::collect() |> qDT()
dta_150m_index <- copy(dta_150m)
data.table::setindexv(dta_150m_index, c("country", "year", "gender", "area"))

# Microbenchmarking
microbenchmark::microbenchmark(
  gini_dt(dta_test),
  # gini_dt(dta_1m),
  gini_dt(dta_1m_index),
  # gini_dt(dta_5m),
  gini_dt(dta_5m_index),
  # gini_dt(dta_50m),
  gini_dt(dta_50m_index),
  # gini_dt(dta_150m),
  gini_dt(dta_150m_index),
  times = 5
)

