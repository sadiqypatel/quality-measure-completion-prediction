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

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

df_1 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_1 = df_1.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))

# COMMAND ----------

# Create a new DataFrame with the modified values
df_2 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_2 = df_2.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_2 = df_2.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

df_3 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_3 = df_3.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_3 = df_3.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_3 = df_3.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

df_4 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_4 = df_4.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_4 = df_4.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_4 = df_4.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_4 = df_4.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

df_5 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_5 = df_5.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_5 = df_5.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_5 = df_5.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_5 = df_5.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))

# COMMAND ----------

categorical_features = ['ssi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_4.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_5.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

df_6 = df.withColumn("saFacRate", F.when(df.saFacRate < p75_values["saFacRate"], p75_values["saFacRate"]).otherwise(df.saFacRate))
df_6 = df_6.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_6 = df_6.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_6 = df_6.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_6 = df_6.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_6 = df_6.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))

# COMMAND ----------

categorical_features = ['ssi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_5.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_6.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

df_7 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_7 = df_7.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_7 = df_7.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_7 = df_7.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_7 = df_7.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))
df_7 = df_7.withColumn("saFacRate", F.when(df.saFacRate < p75_values["saFacRate"], p75_values["saFacRate"]).otherwise(df.saFacRate))
df_7 = df_7.withColumn("urgentCareRate", F.when(df.urgentCareRate < p75_values["urgentCareRate"], p75_values["urgentCareRate"]).otherwise(df.urgentCareRate))

# COMMAND ----------

categorical_features = ['ssi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_6.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_7.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

df_8 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_8 = df_8.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_8 = df_8.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_8 = df_8.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_8 = df_8.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))
df_8 = df_8.withColumn("saFacRate", F.when(df.saFacRate < p75_values["saFacRate"], p75_values["saFacRate"]).otherwise(df.saFacRate))
df_8 = df_8.withColumn("urgentCareRate", F.when(df.urgentCareRate < p75_values["urgentCareRate"], p75_values["urgentCareRate"]).otherwise(df.urgentCareRate))
df_8 = df_8.withColumn("popDensity", F.when(df.popDensity < p75_values["popDensity"], p75_values["popDensity"]).otherwise(df.popDensity))

# COMMAND ----------

categorical_features = ['ssi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_7.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_8.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'tanf'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

df_9 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_9 = df_9.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_9 = df_9.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_9 = df_9.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_9 = df_9.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))
df_9 = df_9.withColumn("saFacRate", F.when(df.saFacRate < p75_values["saFacRate"], p75_values["saFacRate"]).otherwise(df.saFacRate))
df_9 = df_9.withColumn("urgentCareRate", F.when(df.urgentCareRate < p75_values["urgentCareRate"], p75_values["urgentCareRate"]).otherwise(df.urgentCareRate))
df_9 = df_9.withColumn("popDensity", F.when(df.popDensity < p75_values["popDensity"], p75_values["popDensity"]).otherwise(df.popDensity))


# COMMAND ----------

categorical_features = ['tanf']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_8.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_9.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['ssi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_8.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_9.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'tanf'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'ssdi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

df_10 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_10 = df_10.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_10 = df_10.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_10 = df_10.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_10 = df_10.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))
df_10 = df_10.withColumn("saFacRate", F.when(df.saFacRate < p75_values["saFacRate"], p75_values["saFacRate"]).otherwise(df.saFacRate))
df_10 = df_10.withColumn("urgentCareRate", F.when(df.urgentCareRate < p75_values["urgentCareRate"], p75_values["urgentCareRate"]).otherwise(df.urgentCareRate))
df_10 = df_10.withColumn("popDensity", F.when(df.popDensity < p75_values["popDensity"], p75_values["popDensity"]).otherwise(df.popDensity))

# COMMAND ----------

categorical_features = ['tanf']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_9.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_10.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['ssi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_9.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_10.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['ssdi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_9.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_10.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'tanf'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'ssdi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'speakEnglish'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "no")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("yes"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "no").unionByName(first_no_row).unionByName(df_updated)

df_11 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_11 = df_11.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_11 = df_11.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_11 = df_11.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_11 = df_11.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))
df_11 = df_11.withColumn("saFacRate", F.when(df.saFacRate < p75_values["saFacRate"], p75_values["saFacRate"]).otherwise(df.saFacRate))
df_11 = df_11.withColumn("urgentCareRate", F.when(df.urgentCareRate < p75_values["urgentCareRate"], p75_values["urgentCareRate"]).otherwise(df.urgentCareRate))
df_11 = df_11.withColumn("popDensity", F.when(df.popDensity < p75_values["popDensity"], p75_values["popDensity"]).otherwise(df.popDensity))

# COMMAND ----------

categorical_features = ['ssi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_10.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['tanf']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_10.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['ssdi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_10.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['speakEnglish']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_10.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

# Create a new DataFrame with the modified values

df = main.select(['beneID', 'state', 'fedPovLine', 'speakEnglish', 'ssi', 'ssdi', 'tanf', 'saFacRate', 'povRate', 'goodAirDays', 'urgentCareRate', 'aprnRate', 'mhTreatRate', 'popDensity', 'highSchoolGradRate'])

column_name = 'ssi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'tanf'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'ssdi'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "yes")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("no"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "yes").unionByName(first_no_row).unionByName(df_updated)

column_name = 'speakEnglish'

# Filter out rows with "missing" and "yes" values
df_filtered = df.filter(F.col(column_name) == "no")

# Get the first row with "no" value
first_no_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "no" value
df_updated = df_filtered.exceptAll(first_no_row).withColumn(column_name, F.lit("yes"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "no").unionByName(first_no_row).unionByName(df_updated)

column_name = 'fedPovLine'

# Filter out rows with "missing" and "200AndMore" values
df_filtered = df.filter(F.col(column_name) == "0To100")

# Get the first row with "0To100" value
first_0_row = df_filtered.limit(1)

# Update the values in the rest of the rows with "0To100" value
df_updated = df_filtered.exceptAll(first_0_row).withColumn(column_name, F.lit("200AndMore"))

# Combine the updated rows with the rest of the DataFrame
df = df.filter(F.col(column_name) != "0To100").unionByName(first_0_row).unionByName(df_updated)

df_12 = df.withColumn("aprnRate", F.when(df.aprnRate < p75_values["aprnRate"], p75_values["aprnRate"]).otherwise(df.aprnRate))
df_12 = df_12.withColumn("povRate", F.when(df.povRate > p25_povRate, p25_povRate).otherwise(df.povRate))
df_12 = df_12.withColumn("mhTreatRate", F.when(df.mhTreatRate < p75_values["mhTreatRate"], p75_values["mhTreatRate"]).otherwise(df.mhTreatRate))
df_12 = df_12.withColumn("highSchoolGradRate", F.when(df.highSchoolGradRate > p25_gradRate, p25_gradRate).otherwise(df.highSchoolGradRate))
df_12 = df_12.withColumn("goodAirDays", F.when(df.goodAirDays < p75_values["goodAirDays"], p75_values["goodAirDays"]).otherwise(df.goodAirDays))
df_12 = df_12.withColumn("saFacRate", F.when(df.saFacRate < p75_values["saFacRate"], p75_values["saFacRate"]).otherwise(df.saFacRate))
df_12 = df_12.withColumn("urgentCareRate", F.when(df.urgentCareRate < p75_values["urgentCareRate"], p75_values["urgentCareRate"]).otherwise(df.urgentCareRate))
df_12 = df_12.withColumn("popDensity", F.when(df.popDensity < p75_values["popDensity"], p75_values["popDensity"]).otherwise(df.popDensity))

# COMMAND ----------

categorical_features = ['ssi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_12.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['tanf']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_12.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['ssdi']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_12.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['speakEnglish']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_12.groupBy(feature).count().orderBy(feature).show()
    print()

categorical_features = ['fedPovLine']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_12.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

categorical_features = ['speakEnglish']

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_11.groupBy(feature).count().orderBy(feature).show()
    print()

for feature in categorical_features:
    print(f"Value counts for {feature}:")
    df_12.groupBy(feature).count().orderBy(feature).show()
    print()

# COMMAND ----------

df_1.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_1", mode='overwrite')
df_2.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_2", mode='overwrite')
df_3.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_3", mode='overwrite')
df_4.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_4", mode='overwrite')
df_5.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_5", mode='overwrite')
df_6.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_6", mode='overwrite')
df_7.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_7", mode='overwrite')
df_8.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_8", mode='overwrite')
df_9.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_9", mode='overwrite')
df_10.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_10", mode='overwrite')
df_11.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_11", mode='overwrite')
df_12.write.saveAsTable("dua_058828_spa240.paper4_original_df_changed_sdoh_mini_part4_file_12", mode='overwrite')

# COMMAND ----------

