

get_pov_slow__dplyr <- function(dta, pl) {
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
    cum_inc_lead = dplyr::lead(cum_inc, default = 0),
    cum_inc_lag = dplyr::lag(cum_inc, default = 0),
    gini1 = sum(cum_inc_lead * cum_pop, na.rm = T),
    gini2 = sum(cum_inc_lag * cum_pop, na.rm = T),
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

get_pov__dplyr <- function(dta, pl) {
  dta  |>
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


get_pov__dplyr_nopmax <- function(dta, pl) {
  dta  |>
  mutate(
    poor = welfare <= pl,
    hc = poor * weight,
    cum_pop = cumsum(weight / sum(weight)),
    cum_inc = cumsum(welfare * weight) / sum(welfare * weight)
  ) |> 
  summarise(
    hc0 = sum(hc),
    ftg0 = sum(hc) / sum(weight),
    ftg1 = sum(poor * (pl - welfare) * weight) / sum(weight) / pl,
    gini  =  sum(cum_inc[-1] * cum_pop[-n()]) - sum(cum_inc[-n()] * cum_pop[-1]),
    .groups = "drop"
  ) |> 
  mutate(ftg2 = ftg1 ^ 2) |> 
  select(country, year, hc = hc0, ftg0, ftg1, ftg2, gini)
}


get_pov__dplyr_nogini <- function(dta, pl) {
  dta  |>
  mutate(
    poor = as.integer(welfare <= pl),
    hc = poor * weight
  ) |> 
  summarise(
    hc0 = sum(hc),
    ftg0 = sum(hc) / sum(weight),
    ftg1 = sum(poor * (pl - welfare) * weight) / sum(weight) / pl,
    .groups = "drop"
  ) |> 
  mutate(ftg2 = ftg1 ^ 2) |> 
  select(country, year, hc = hc0, ftg0, ftg1, ftg2)
}



get_pov__DT <- function(dta, pl, groups = c("country", "year")) {
   dta[,
    `:=`(c("poor", "hc", "wmax", "gini1", "gini2"), {
      poor <- welfare <= pl
      hc <- poor * weight
      wmax <- hc * (pl - welfare)
      # wtot <- (welfare * weight)      
      cum_pop <- cumsum(weight) / sum(weight)
      cum_inc <- cumsum(welfare * weight) / sum(welfare * weight)
      gini1 = shift(cum_inc, n = 1, fill = NA_real_, type = "lead") * cum_pop
      gini2 = shift(cum_inc, n = 1, fill = NA_real_, type = "lag") * cum_pop
      .(poor, hc, wmax, gini1, gini2)
    }),
    by = groups
  ] 
  
  dta |> 
    gby(groups) |> 
    gv(c("hc", "wmax", "weight", "gini1", "gini2")) |> 
    fsum() |> 
    fmutate(
      ftg0 = hc / weight,
      ftg1 = wmax / weight / pl,
      ftg2 = ftg1 ^ 2,
      gini = gini1 - gini2
    ) |> 
    fselect(country, year, hc, ftg0, ftg1, ftg2, gini)
}

get_pov__DT_nogini <- function(dta, pl, groups = c("country", "year")) {
  dta[,
    `:=`(c("poor", "hc", "wmax"), {
      poor <- welfare <= pl
      hc <- poor * weight
      wmax <- hc * (pl - welfare)
      .(poor, hc, wmax) 
    }),
    by = groups
  ]   
  dta |> 
    gby(groups) |> 
    gv(c("hc", "wmax", "weight")) |> 
    fsum() |> 
    fmutate(
      ftg0 = hc / weight,
      ftg1 = wmax / weight / pl,
      ftg2 = ftg1 ^ 2
    ) |> 
    fselect(country, year, hc, ftg0, ftg1, ftg2) 
}


# get_ftg_gini__dplyr <- function(dta, pl) {
#  dta |>
#     mutate(
#       poor = welfare <= pl,
#       hc = poor * weight,
#       cum_pop = cumsum(weight / sum(weight)),
#       cum_inc = cumsum(welfare * weight) / sum(welfare * weight)
#     ) |> 
#     summarise(
#       hc0 = sum(hc),
#       ftg0 = sum(hc) / sum(weight),
#       ftg1 = sum((pmax(pl - welfare, 0) / pl) * weight) / sum(weight),
#       gini  =  sum(cum_inc[-1] * cum_pop[-n()]) - sum(cum_inc[-n()] * cum_pop[-1]),
#       .groups = "drop"
#     ) |> 
#     mutate(ftg2 = ftg1 ^ 2)
# }

