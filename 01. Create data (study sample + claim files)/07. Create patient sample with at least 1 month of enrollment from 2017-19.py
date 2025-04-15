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

# df2019 = spark.table("dua_058828_spa240.paper_4_patient_sample_2019_atleast_1_month")
# df2019 = df2019.select("beneID","state")
# print((df2019.count(), len(df2019.columns)))

# df2018 = spark.table("dua_058828_spa240.paper_4_patient_sample_2018_atleast_1_month")
# df2018 = df2018.select("beneID","state")
# print((df2018.count(), len(df2018.columns)))

df = spark.table("dua_058828_spa240.paper_4_patient_sample_2017_atleast_1_month")
#df2017 = df2017 .select("beneID","state")
print((df.count(), len(df.columns)))

# Count the number of distinct values in a column
distinct_count_2017 = df.select(col("state")).distinct().count()
# distinct_count_2018 = df2018.select(col("state")).distinct().count()
# distinct_count_2019 = df2019.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count_2017)
# print("Distinct count:", distinct_count_2018)
# print("Distinct count:", distinct_count_2019)

# COMMAND ----------

df.show()

# COMMAND ----------

final = df
print((final.count(), len(final.columns)))

final = final.select("beneID","state").distinct()

# COMMAND ----------

final.show()

# COMMAND ----------

df = spark.read.table("dua_058828_spa240.demo2017")
print((df.count(), len(df.columns)))
dfA = df.dropDuplicates(["beneID","state"])
dfA = dfA.select("beneID","state")
print((dfA.count(), len(dfA.columns)))

# COMMAND ----------

df.show()

# COMMAND ----------

sample = final
sample.show()

# COMMAND ----------

final = sample.join(df, on=["beneID","state"], how="left")
final.show()

# COMMAND ----------

print(final.count())
print(sample.count())

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

final.write.saveAsTable("dua_058828_spa240.paper_4_demo_file_at_least_1_month", mode='overwrite')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

