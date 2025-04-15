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
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession

# COMMAND ----------

main = spark.table("dua_058828_spa240.paper_4_final_data_all_predictors_12_months")


# COMMAND ----------

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

#df = df.sample(withReplacement=False, fraction=0.5)
#print(df.count())
#df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Define the 75th percentile values for each variable
p75_values = {
    "saFacRate": 0.0008,
    "goodAirDays": 93.9,
    "urgentCareRate": 0.02,
    "aprnRate": 2.8,
    "mhTreatRate": 0.07,
    "popDensity": 107.8 
}

p25_povRate = 4.1
p25_gradRate = 7.4

# COMMAND ----------

# Create a new DataFrame with the modified values
df_1 = df.withColumn("saFacRate", F.when(df.saFacRate < p75_values["saFacRate"], p75_values["saFacRate"]).otherwise(df.saFacRate))

# COMMAND ----------

# Create a new DataFrame with the modified values
df_2 = df.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))

# COMMAND ----------

# Create a new DataFrame with the modified values
df_3 = df.withColumn("urgentCareRate", F.when(df.urgentCareRate < p75_values["urgentCareRate"], p75_values["urgentCareRate"]).otherwise(df.urgentCareRate))

# COMMAND ----------

# Create a new DataFrame with the modified values
df_4 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))

# COMMAND ----------

# Create a new DataFrame with the modified values
df_5 = df.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))

# COMMAND ----------

# Create a new DataFrame with the modified values
df_6 = df.withColumn("popDensity", F.when(df.popDensity < p75_values["popDensity"], p75_values["popDensity"]).otherwise(df.popDensity))

# COMMAND ----------

# Create a new DataFrame with the modified values
df_7 = df.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))

# COMMAND ----------

# Create a new DataFrame with the modified values
df_8 = df.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))

# COMMAND ----------

# MAGIC %md
# MAGIC categorical

# COMMAND ----------

column_name = 'fedPovLine'

# Filter out rows with "missing" and "200AndMore" values
df_filtered = df.filter(F.col(column_name) == "0To100")

# Get the first row with "0To100" value
first_0_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "0To100" value
df_updated = df_filtered.exceptAll(first_0_row).withColumn(column_name, F.lit("200AndMore"))

# Combine the updated rows with the rest of the DataFrame
df_9 = df.filter(F.col(column_name) != "0To100").unionByName(first_0_row).unionByName(df_updated)

# COMMAND ----------

# from pyspark.sql import functions as F

# # Categorical features
# categorical_features = ['fedPovLine']

# for feature in categorical_features:
#     print(f"Value counts for {feature}:")
#     df.groupBy(feature).count().orderBy(feature).show()
#     print()

# COMMAND ----------

column_name = 'ssdi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df_10 = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

# COMMAND ----------

# from pyspark.sql import functions as F

# # Categorical features
# categorical_features = ['ssdi']

# for feature in categorical_features:
#     print(f"Value counts for {feature}:")
#     df.groupBy(feature).count().orderBy(feature).show()
#     print()

# COMMAND ----------

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df_11 = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

# COMMAND ----------

# from pyspark.sql import functions as F

# # Categorical features
# categorical_features = ['ssi']

# for feature in categorical_features:
#     print(f"Value counts for {feature}:")
#     df.groupBy(feature).count().orderBy(feature).show()
#     print()

# COMMAND ----------

column_name = 'tanf'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df_12 = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

# COMMAND ----------

# from pyspark.sql import functions as F

# # Categorical features
# categorical_features = ['tanf']

# for feature in categorical_features:
#     print(f"Value counts for {feature}:")
#     df.groupBy(feature).count().orderBy(feature).show()
#     print()

# COMMAND ----------

column_name = 'speakEnglish'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "no")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("yes"))

# Combine the updated rows with the rest of the DataFrame
df_13 = df.filter(F.col(column_name) != "no").unionByName(first_no_row).unionByName(df_updated)

# COMMAND ----------

from pyspark.sql import functions as F

# Categorical features
categorical_features = ['tanf']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_1.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

from pyspark.sql import functions as F

# Categorical features
categorical_features = ['tanf']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_12.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

df_1.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_1", mode='overwrite')
df_2.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_2", mode='overwrite')
df_3.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_3", mode='overwrite')
df_4.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_4", mode='overwrite')
df_5.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_5", mode='overwrite')
df_6.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_6", mode='overwrite')
df_7.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_7", mode='overwrite')
df_8.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_8", mode='overwrite')
df_9.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_9", mode='overwrite')
df_10.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_10", mode='overwrite')
df_11.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_11", mode='overwrite')
df_12.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_12", mode='overwrite')
df_13.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_13", mode='overwrite')

# COMMAND ----------

test1 = spark.table("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_9")
test2 = spark.table("dua_058828_spa240.paper_4_final_data_all_predictors_12_months")

from pyspark.sql import functions as F

# Categorical features
categorical_features = ['fedPovLine']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    test1.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    test2.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

test1 = spark.table("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_10")
test2 = spark.table("dua_058828_spa240.paper_4_final_data_all_predictors_12_months")

from pyspark.sql import functions as F

# Categorical features
categorical_features = ['ssdi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    test1.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    test2.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

test1 = spark.table("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_file_13")
test2 = spark.table("dua_058828_spa240.paper_4_final_data_all_predictors_12_months")

from pyspark.sql import functions as F

# Categorical features
categorical_features = ['speakEnglish']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    test1.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    test2.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

