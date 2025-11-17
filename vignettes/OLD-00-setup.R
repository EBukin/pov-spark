# install.packages("sparklyr")
library(sparklyr)

# # Installing Spark ------------------------------------------------------------

# Run 
spark_install(version = "4.0")

# # If fails, dowlonad binary manually from 'https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz',
# # download.file(
# #   url = 'https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz',
# #   destfile = 'extra/spark-2.4.3-bin-hadoop2.7.tgz'
# #   # method = 'curl'
# # )
# # and install it manually with spark_install_tar(file)

spark_install_tar("extra/spark-4.0.0-bin-hadoop3.tgz")
# spark_installed_versions() 
# spark_uninstall("2.4.3", "2.7")

# # Test spark installation ------------------------------------------------------
library(sparklyr)

sc <- spark_connect(master = "local")

sparklyr::spark_disconnect(sc)
# library(data.table)
# library(dtplyr)
# library(arrow)
