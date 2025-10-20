
fgt_collapse <- function(
  welfare,
  weight = rep(1, length(welfare)),
  povline = collapse::fmedian(welfare, w = weight)/2,
  alpha = 0,
  ...
) {
  pov_status <- welfare < povline
  relative_distance <- 1 - (welfare / povline)
  collapse::fmean((pov_status) * (relative_distance)^alpha, w = weight)
}


fgt_base <- function(
  welfare,
  weight = rep(1, length(welfare)),
  povline = collapse::fmedian(welfare, w = weight)/2,
  alpha = 0,
  ...
) {
  pov_status <- welfare < povline
  relative_distance <- 1 - (welfare / povline)
  sum(((pov_status) * (relative_distance)^alpha) * weight) / sum(weight)
}

