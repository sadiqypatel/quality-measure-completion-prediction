# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count, sum

# COMMAND ----------

df = spark.read.table("dua_058828_spa240.demo2018")
print((df.count(), len(df.columns)))
dfA = df.dropDuplicates(["beneID","state"])
dfA = dfA.select("beneID","state")
print((dfA.count(), len(dfA.columns)))

# COMMAND ----------

df.show()

# COMMAND ----------

sample = spark.read.table("dua_058828_spa240.paper_4_final_patient_sample_both")
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

final.write.saveAsTable("dua_058828_spa240.paper_4_demo_file_01", mode='overwrite')

# COMMAND ----------

