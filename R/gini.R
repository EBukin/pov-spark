#' Gini coefficient
#'
#' Compute the Gini coefficient for microdata.
#' Soiurce: https://github.com/PIP-Technical-Team/wbpip/blob/PROD/R/md_compute_gini.R
#'
#' Given a vector of income or consumption values and their respective weights
#' `md_compute_gini()` computes the Gini coefficient for the distribution.
#'
#' inheritParams compute_pip_stats
#' 
#' @param weight numeric: A vector of population weights, optional, a vector
#' of 1s if not specified.
#' @examples
#' gini_collapse(welfare = 1:2000, weight = rep(1, 2000))
#' @return numeric
#' 
#' @importFrom collapse fcumsum fsum flag
#' 
#' @export
gini_collapse <- function(welfare, weight) {
  # Compute weighted welfare
  weighted_welfare <- welfare * weight
  weighted_welfare_lag <- collapse::flag(weighted_welfare, fill = 0)

  # Compute area under the curve using
  # Area of trapezoid = Base * Average height
  v <- (collapse::fcumsum(weighted_welfare_lag) + (weighted_welfare / 2)) * weight
  auc <- collapse::fsum(v) # Area Under the Curve

  # Compute Area Under the Lorenz Curve
  # Normalize auc so it is always between 0 and 0.5
  auc <- (auc / collapse::fsum(weight)) / collapse::fsum(weighted_welfare)

  # Compute Gini
  gini <- 1 - (2 * auc)

  return(gini)
}


#' @describeIn  gini_collapse Gini coefficient using base R functions
#' 
#' @examples
#' identical( gini_collapse(welfare = 1:2000, weight = rep(1, 2000)), gini_base(welfare = 1:2000, weight = rep(1, 2000)))
gini_base <- function(welfare, weight) {
  # Compute weighted welfare
  weighted_welfare <- welfare * weight
  weighted_welfare_lag <- lag(weighted_welfare)
  weighted_welfare_lag[1] <- 0
  # Compute area under the curve using
  # Area of trapezoid = Base * Average height
  v <- (cumsum(weighted_welfare_lag) + (weighted_welfare / 2)) * weight
  auc <- sum(v) # Area Under the Curve

  # Compute Area Under the Lorenz Curve
  # Normalize auc so it is always between 0 and 0.5
  auc <- (auc / sum(weight)) / sum(weighted_welfare)

  # Compute Gini
  gini <- 1 - (2 * auc)

  return(gini)
}



