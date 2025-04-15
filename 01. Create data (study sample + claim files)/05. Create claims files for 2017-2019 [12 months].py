# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np

# COMMAND ----------

# MAGIC %r
# MAGIC # Check if the glmnet package is installed
# MAGIC if (require(glmnet)) {
# MAGIC   print("Package 'glmnet' is installed.")
# MAGIC } else {
# MAGIC   print("Package 'glmnet' is NOT installed.")
# MAGIC }

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper_4_final_patient_sample_2017_2019_12_months")
df = df.select("beneID","state")
print((df.count(), len(df.columns)))

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

df.show()

# COMMAND ----------

# inpatient2019 = spark.table("dua_058828_spa240.inpatient2019")
# inpatient2019 = inpatient2019.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
# print((inpatient2019.count(), len(inpatient2019.columns)))
# inpatient2019 = inpatient2019.join(df, on=["beneID","state"], how="inner")
# print((inpatient2019.count(), len(inpatient2019.columns)))

# inpatient2018 = spark.table("dua_058828_spa240.inpatient2018_hedis_paper")
# inpatient2018 = inpatient2018.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
# print((inpatient2018.count(), len(inpatient2018.columns)))
# inpatient2018 = inpatient2018.join(df, on=["beneID","state"], how="inner")
# print((inpatient2018.count(), len(inpatient2018.columns)))

# inpatient2017 = spark.table("dua_058828_spa240.inpatient2017")
# inpatient2017 = inpatient2017.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
# print((inpatient2017.count(), len(inpatient2017.columns)))
# inpatient2017 = inpatient2017.join(df, on=["beneID","state"], how="inner")
# print((inpatient2017.count(), len(inpatient2017.columns)))

# inpatient2019.write.saveAsTable("dua_058828_spa240.paper_4_inpatient2019_12_months_new", mode='overwrite')
# inpatient2018.write.saveAsTable("dua_058828_spa240.paper_4_inpatient2018_12_months_new", mode='overwrite')
# inpatient2017.write.saveAsTable("dua_058828_spa240.paper_4_inpatient2017_12_months_new", mode='overwrite')

# COMMAND ----------

# inpatient2019 = spark.table("dua_058828_spa240.pharm2019")
# inpatient2019 = inpatient2019.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
# print((inpatient2019.count(), len(inpatient2019.columns)))
# inpatient2019 = inpatient2019.join(df, on=["beneID","state"], how="inner")
# print((inpatient2019.count(), len(inpatient2019.columns)))

# inpatient2018 = spark.table("dua_058828_spa240.pharm2018")
# inpatient2018 = inpatient2018.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
# print((inpatient2018.count(), len(inpatient2018.columns)))
# inpatient2018 = inpatient2018.join(df, on=["beneID","state"], how="inner")
# print((inpatient2018.count(), len(inpatient2018.columns)))

# inpatient2017 = spark.table("dua_058828_spa240.pharm2017")
# inpatient2017 = inpatient2017.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
# print((inpatient2017.count(), len(inpatient2017.columns)))
# inpatient2017 = inpatient2017.join(df, on=["beneID","state"], how="inner")
# print((inpatient2017.count(), len(inpatient2017.columns)))


# inpatient2019.write.saveAsTable("dua_058828_spa240.paper_4_pharm2019_12_months", mode='overwrite')
# inpatient2018.write.saveAsTable("dua_058828_spa240.paper_4_pharm2018_12_months", mode='overwrite')
# inpatient2017.write.saveAsTable("dua_058828_spa240.paper_4_pharm2017_12_months", mode='overwrite')

# COMMAND ----------

inpatient2019 = spark.table("dua_058828_spa240.otherServices2019")
inpatient2019 = inpatient2019.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
print((inpatient2019.count(), len(inpatient2019.columns)))
inpatient2019 = inpatient2019.join(df, on=["beneID","state"], how="inner")
print((inpatient2019.count(), len(inpatient2019.columns)))

inpatient2018 = spark.table("dua_058828_spa240.otherServices2018")
inpatient2018 = inpatient2018.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
print((inpatient2018.count(), len(inpatient2018.columns)))
inpatient2018 = inpatient2018.join(df, on=["beneID","state"], how="inner")
print((inpatient2018.count(), len(inpatient2018.columns)))

inpatient2017 = spark.table("dua_058828_spa240.otherServices2017")
inpatient2017 = inpatient2017.withColumnRenamed("BENE_ID", "beneID").withColumnRenamed("STATE_CD", "state")
print((inpatient2017.count(), len(inpatient2017.columns)))
inpatient2017 = inpatient2017.join(df, on=["beneID","state"], how="inner")
print((inpatient2017.count(), len(inpatient2017.columns)))


inpatient2019.write.saveAsTable("dua_058828_spa240.paper_4_otherservices2019_12_months", mode='overwrite')
inpatient2018.write.saveAsTable("dua_058828_spa240.paper_4_otherservices2018_12_months", mode='overwrite')
inpatient2017.write.saveAsTable("dua_058828_spa240.paper_4_otherservices2017_12_months", mode='overwrite')

# COMMAND ----------

