# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np

# COMMAND ----------

#predictor 1: patient characteristics

pred1 = spark.table("dua_058828_spa240.paper_4_demo_file_01_12_months")
pred1 = pred1.select('beneID','state','ageCat','age','sex','race','houseSize','fedPovLine','speakEnglish','married','UsCitizen','ssi','ssdi','tanf','disabled','state','county','medicaidMonths')
print(pred1.count())
print(pred1.printSchema())
pred1.show()

# COMMAND ----------

#predictor 2: SDOH features

pred3 = spark.table("dua_058828_spa240.paper_4_sdoh_notebook_02_12_months")
pred3 = pred3.drop('county')
print(pred3.count())
print(pred3.printSchema())
pred3.show()

# COMMAND ----------

#predictor 4: acute care predictors

pred4 = spark.table("dua_058828_spa240.paper_4_acute_care_predictors_12_months")
pred4 = pred4.drop('x', 'y_allcause', 'y_avoid', 'total_avoid_acute_visits')
print(pred4.count())
print(pred4.printSchema())
pred4.show()

# COMMAND ----------

#predictor 5: pharmacy predictors

pred5 = spark.table("dua_058828_spa240.paper_4_pharm_predictors_12_months")
pred5 = pred5.drop('x', 'y_pharm')
print(pred5.count())
print(pred5.printSchema())
pred5.show()

# COMMAND ----------

#predictor 7: dx / ccsr predictors

pred7 = spark.table("dua_058828_spa240.paper_4_dx_predictors_12_months")
print(pred7.count())
pred7 = pred7.withColumnRenamed("null", "ccsr_null")
print(pred7.printSchema())
pred7.show(1)

# COMMAND ----------

#predictor 8: proc code / betos predictors

pred8 = spark.table("dua_058828_spa240.paper_4_proc_code_predictors_12_months")
print(pred8.count())
pred8 = pred8.withColumnRenamed("null", "betos_null")
print(pred8.printSchema())
pred8.show(1)

# COMMAND ----------

#predictor 9: rx / ndc agg

pred9 = spark.table("dua_058828_spa240.paper_4_rx_predictors_12_months")
print(pred9.count())
pred9 = pred9.withColumnRenamed("null", "rx_null")
print(pred9.printSchema())
pred9.show(1)

# COMMAND ----------

#predictor 10: clinician specialty / cms code

pred10 = spark.table("dua_058828_spa240.paper_4_clinician_specialty_predictors_12_months")
print(pred10.count())
print(pred10.printSchema())
pred10.show(1)

# COMMAND ----------

# Assuming common_columns contains the common column names "beneID" and "state"

# Initialize the merged DataFrame with pred2
merged_df = pred1

# Loop through pred3 to pred10 and perform left join on common columns
for df in [pred3, pred4, pred5, pred7, pred8, pred9, pred10]:
    merged_df = merged_df.join(df, on=["beneID","state"], how='left')

# Show the merged DataFrame
print((merged_df.count(), len(merged_df.columns)))
print(merged_df.printSchema())

# COMMAND ----------

# Show the merged DataFrame
print((merged_df.count(), len(merged_df.columns)))
print(merged_df.printSchema())

# COMMAND ----------

merged_df.write.saveAsTable("dua_058828_spa240.paper_4_final_data_all_predictors_12_months", mode='overwrite')