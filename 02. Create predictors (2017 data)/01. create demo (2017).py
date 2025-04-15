# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count, sum

# COMMAND ----------

df = spark.read.table("dua_058828_spa240.demo2017")
print((df.count(), len(df.columns)))
dfA = df.dropDuplicates(["beneID","state"])
dfA = dfA.select("beneID","state")
print((dfA.count(), len(dfA.columns)))

# COMMAND ----------

df.show()

# COMMAND ----------

sample = spark.read.table("dua_058828_spa240.paper_4_final_patient_sample_2017_2019_12_months_no_dead")
sample = sample.select("beneID","state","medicaidMonths")
sample.show()

# COMMAND ----------

final = sample.join(df, on=["beneID","state"], how="left")
final.show()

# COMMAND ----------

print(final.count())

# COMMAND ----------

# Count the number of rows in 'sample' that are missing corresponding values in 'df'
missing_count = final.filter(final['ageCat'].isNull()).count()

print(f"Number of rows in 'sample' missing a corresponding value in 'df': {missing_count}")

# COMMAND ----------

birth_date = spark.sql("select BENE_ID, STATE_CD, BIRTH_DT from extracts.tafr17.demog_elig_base")
birth_date.show()

# COMMAND ----------

birth_date = birth_date.withColumnRenamed("STATE_CD", "state")
birth_date = birth_date.withColumnRenamed("BENE_ID", "beneID")
birth_date.show()

# COMMAND ----------

from pyspark.sql.functions import max

# Convert BIRTH_DT to date type
birth_date = birth_date.withColumn("BIRTH_DT", birth_date["BIRTH_DT"].cast("date"))

# Group by beneID and state, and get the max BIRTH_DT as birthday
result_df = birth_date.groupBy("beneID", "state").agg(max("BIRTH_DT").alias("birthday"))

# Show the result
result_df.show()

# COMMAND ----------

final = final.join(result_df, on=["beneID","state"], how="left")
print(final.count())
final.show()

# COMMAND ----------

# Count the number of rows in 'sample' that are missing corresponding values in 'df'
missing_count = final.filter(final['birthday'].isNull()).count()

print(f"Number of rows in 'sample' missing a corresponding value in 'df': {missing_count}")

# COMMAND ----------

final.write.saveAsTable("dua_058828_spa240.paper_4_demo_file_01_12_months", mode='overwrite')

# COMMAND ----------

