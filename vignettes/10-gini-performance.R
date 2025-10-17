pkgload::load_all()

library(microbenchmark)


# Gini performance comparison for large samples
c(10^6, 10^7, 10^8) |>
  purrr::walk(function(n) {
    cat(n, "\n")
    welfare <- runif(n, 0, 100) |> sort()
    weight <- runif(n, 0, 1)

    mb <- microbenchmark(
      gini_collapse(welfare, weight),
      gini_base(welfare, weight),
      times = 10
    )

    print(mb)
  })

# 1e+06 
# Unit: milliseconds
#                            expr     min      lq     mean   median      uq     max neval
#  gini_collapse(welfare, weight) 45.8526 46.6774 52.15085 51.89315 56.9397 59.6991    10
#      gini_base(welfare, weight) 25.0043 44.7989 49.81986 49.35140 58.5303 66.3438    10
# 1e+07 
# Unit: milliseconds
#                            expr      min       lq     mean   median       uq      max neval
#  gini_collapse(welfare, weight) 160.8393 164.9465 355.2606 410.4524 455.9610 617.3568    10
#      gini_base(welfare, weight) 179.8570 192.9065 345.8680 353.9755 481.2676 552.8197    10
# 1e+08 
# Unit: seconds
#                            expr      min       lq     mean   median       uq      max neval
#  gini_collapse(welfare, weight) 1.382467 1.439544 1.487631 1.482680 1.529818 1.623435    10
#      gini_base(welfare, weight) 1.562783 1.642878 1.704111 1.701017 1.733276 1.865756    10