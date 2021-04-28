# Databricks notebook source
import pandas as pd
import numpy as np
import json
import urllib3
import time
urllib3.disable_warnings()

# COMMAND ----------

#dataset = spark.read.format("csv").options(header='true', delimiter = ';').load("/FileStore/tables/RU_KONSOLIDIERUNG-3.csv")
#dataset = spark.read.format("csv").options(header='true', delimiter = ';').load("/FileStore/tables/Company_mapp.csv")
dataset = spark.read.format("csv").options(header='true', delimiter = ';').load("/FileStore/tables/qm_targets_v21_07_2020.csv")

# COMMAND ----------

display(dataset)

# COMMAND ----------

dbutils.fs.ls("abfss://rootaap@azweaappaapdatalake.dfs.core.windows.net/DIVI/apidata/")

# COMMAND ----------

save_location= "abfss://rootaap@azweaappaapdatalake.dfs.core.windows.net/DIVI/apidata/"
csv_location = save_location+"temp.folder"
file_location = save_location+"qm_targets.csv"

#spark_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").save(file_location)
dataset.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").save(csv_location)

file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------


