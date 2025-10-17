# Data generation for testing purposes
library(data.table)
library(dplyr)
library(purrr)
library(dtplyr)
library(arrow)

# Data setup -------------------------------------------------------------
set.seed(123)

# Gata generation template -----------------------------------------------

get_country_table_new <- function(
  n_total = 5 * 10^6,
  n_countries = 10L,
  n_years_country = 2:6,
  ...
) {
  # n_total <- 1 * 10^6 # Total number of observations in the dataset
  # n_years_country <- 2:6 # For each country number of years is sampled from this range
  n_pop_country <- 10:50 # For each country population is sampled from this range
  out <-
    data.table(
      country = as.integer(1:n_countries),
      n_obs = sample(10:250, n_countries, replace = TRUE),
      n_years = sample(n_years_country, n_countries, replace = TRUE),
      urban_range = runif(n_countries, 0.25, 0.9) #,
      # female_range = runif(n_countries, 0.45, 0.55),
      # total_pop = sample(n_pop_country, n_countries, replace = TRUE) * n_total
    )
  out |>
    mutate(
      n_obs = round(n_obs / sum(n_obs) * n_total),
      total_pop = n_obs * runif(nrow(out), 5, 15) * 1000
    )
}

# get_country_table_new(5*10^6, 150) |> pull(n_obs) |> sum()
  

# pop_exp <- 10^6
# size_deflator <- 1000
# n_countries <- 10

# # Function to simulate daata simulation template 
# get_country_table_old <- function(pop_exp = 10^6,
#                               size_deflator = 1000,
#                               n_countries = 10L) {
#   n_years <- sample(3:10, n_countries, replace = TRUE)
#   urban_range <- runif(n_countries, 0.25, 0.9)
#   n_obs <- sample(10:250, n_countries, replace = TRUE) * pop_exp / size_deflator
#   total_pop <- sample(3:250, n_countries, replace = TRUE) * pop_exp

#   data.table(
#     country = as.integer(1:n_countries),
#     n_obs = as.integer(n_obs),
#     n_years = as.integer(n_years),
#     urban_range = urban_range,
#     total_pop = as.integer(total_pop)
#   ) 
# }

get_country_data <- function(country_id, n_obs, n_years, urban_range, total_pop) {
  female_share <- runif(1, 0.45, 0.55)
  new_range <- urban_range + runif(1, -0.05, 0.05)
  all_wt <- runif(n_obs, 0, 1)
  years_range <- sample(1990:2025, n_years, replace = TRUE)
  if (length(years_range) > 1) {
    years_range_val <- sample(years_range, n_obs, replace = TRUE)
  } else {
    years_range_val <- rep(years_range, n_obs)
  }
  # browser()
  data.table(
    # country = country_id,
    year = years_range_val,
    welfare = runif(n_obs, 0, 1000),
    weight = all_wt / sum(all_wt) * total_pop,
    gender = sample(1:3, n_obs, replace = TRUE, prob = c(female_share, 0.96 - female_share, 0.04)),
    area = sample(1:3, n_obs, replace = TRUE, prob = c(new_range, 0.95 - new_range, 0.05))
  ) |> 
    arrange(year, welfare)
}

get_country_data(1L, 100000L, 3, 0.5, 1000000L) |> count(year)
get_country_table_new(5*10^6, 150)

# Small sample 1k -------------------------------------------------------------
set.seed(1234)

dta_full <- 
  get_country_table_new(1*10^3, 2, n_years_country = 1:2)[, {
  get_country_data(country, n_obs, n_years, urban_range, total_pop)
}, by = .(country)] |> 
  as_tibble() |>
  arrange(country, year, welfare, gender, area) |> 
  as.data.frame()

dta_test <- dta_full
# usethis::use_data(dta_test)

dta_test |> write_dataset("data/sample_1k")


# Small sample 1m -------------------------------------------------------------
set.seed(1234)

dta_full <- 
  get_country_table_new(1*10^6, 150)[, {
  get_country_data(country, n_obs, n_years, urban_range, total_pop)
}, by = .(country)] |> 
  arrange(country, year, welfare, gender, area)

dta_full |> group_by(country) |> write_dataset("data/sample_1m")


# Larger sample 5m -------------------------------------------------------------
set.seed(1234)

dta_full <- 
  get_country_table_new(5*10^6, 150)[, {
  get_country_data(country, n_obs, n_years, urban_range, total_pop)
}, by = .(country)] |> 
  arrange(country, year, welfare, gender, area)

dta_full |> 
  group_by(country) |>
  write_dataset("data/sample_5m")


# Larger sample 50m -------------------------------------------------------------
set.seed(1234)

dta_full <- 
  get_country_table_new(50*10^6, 150)[, {
  get_country_data(country, n_obs, n_years, urban_range, total_pop)
}, by = .(country)] |> 
  arrange(country, year, welfare, gender, area)

dta_full |> group_by(country) |> write_dataset("data/sample_50m")

# Larger sample 500m -------------------------------------------------------------
set.seed(1234)

dta_full <- 
  get_country_table_new(150*10^6, 150)[, {
  get_country_data(country, n_obs, n_years, urban_range, total_pop)
}, by = .(country)] |> 
  arrange(country, year, welfare, gender, area)

dta_full |> group_by(country) |> write_dataset("data/sample_150m")

  