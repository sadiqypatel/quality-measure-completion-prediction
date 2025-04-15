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

df = spark.table("dua_058828_spa240.demo2017")
print((df.count(), len(df.columns)))

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

# Group by 'beneID' and count the number of occurrences
df_count = df.groupBy("beneID").count()

# Filter out the 'beneID' values that occur more than once
df_count = df_count.filter(df_count["count"] == 1)

# Drop the 'count' column
df_count = df_count.drop("count")

# Join the original DataFrame with the filtered DataFrame to remove the duplicate rows
df = df.join(df_count, on="beneID", how="inner")

print((df.count(), len(df.columns)))

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

df = df.dropna(subset=['beneID','state'])
print((df.count(), len(df.columns)))

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

df.registerTempTable("connections")
df = spark.sql('''
SELECT distinct beneID, state, max(age) as age, max(disabled) as disabled
FROM connections
GROUP BY beneID, state;
''')

print((df.count(), len(df.columns)))

# COMMAND ----------

df = df.filter(~col("state").isin(['AL', 'AZ', 'CT', 'FL', 'GA', 'IA', 'ID', 'IN', 'KY', 'MA', 'MD', 'MI', 'MN', 'NC', 'NE', 'NH', 'NJ', 'NY', 'OH', 'OK', 'PA', 'RI', 'TN', 'TX', 'UT', 'VI', 'PR']))
print((df.count(), len(df.columns)))
df.show()

# Count the number of distinct values in a column
distinct_count = df.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

member = spark.table("dua_058828_spa240.elig2017")
member = member.select("beneID", "state", "dual","medEnroll")
member = member.withColumn("dualInd", when((col("dual").isin(['yes'])), lit(1)).otherwise(lit(0))).withColumn("medicaidMonths", when((col("medEnroll").isin(['yes'])), lit(1)).otherwise(lit(0)))
member.show(10)
print((member.count(), len(member.columns)))

# Count the number of distinct values in a column
distinct_count = member.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

member.registerTempTable("connections")
memberDf = spark.sql('''
SELECT beneID, state, sum(dualInd) as medicareMonths, sum(medicaidMonths) as medicaidMonths
FROM connections
GROUP BY beneID, state;
''')

print((memberDf.count(), len(memberDf.columns)))

# Count the number of distinct values in a column
distinct_count = memberDf.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

print((df.count(), len(df.columns)))
print((memberDf.count(), len(memberDf.columns)))

dfNew = df.join(memberDf, on=['beneID','state'], how='left')
print((dfNew.count(), len(dfNew.columns)))
dfNew.show(25)  

# Count the number of distinct values in a column
distinct_count = dfNew.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

#remove all duals
#1 -- remove members with any enrollment in medicare
#2 -- remove members who are 65 and older

dfNew = dfNew.filter(dfNew.medicareMonths ==0)

# Count the number of distinct values in a column
distinct_count = dfNew.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

dfNew = dfNew.filter(dfNew.age < 65)

# Count the number of distinct values in a column
distinct_count = dfNew.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

dfNew = dfNew.dropna(subset=["medicaidMonths"])

# Count the number of distinct values in a column
distinct_count = dfNew.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

print((dfNew.count(), len(dfNew.columns)))
dfNew.show(10)

# COMMAND ----------

# Value counts for the column 'medicaidMonths'
value_counts = dfNew.groupBy("medicaidMonths").count()

# Show the result
value_counts.show()

# COMMAND ----------

# Get the total number of rows
total_rows = dfNew.count()

# Value counts for the column 'medicaidMonths'
value_counts = dfNew.groupBy("medicaidMonths").count()

# Calculate the percentage of total rows
value_counts_with_percentage = value_counts.withColumn(
    "percentage",
    (col("count") / total_rows) * 100
)

# Sort the result by 'medicaidMonths' in ascending order
sorted_value_counts = value_counts_with_percentage.orderBy("medicaidMonths")

# Show the result
sorted_value_counts.show()

# COMMAND ----------

dfNew.write.saveAsTable("dua_058828_spa240.paper_4_patient_sample_2017_atleast_1_month", mode='overwrite')

# COMMAND ----------

# Filter out rows where medicaidMonths is less than 8
dfNew_filtered = dfNew.filter(dfNew.medicaidMonths == 12)

print((dfNew.count(), len(dfNew.columns)))
print((dfNew_filtered.count(), len(dfNew_filtered.columns)))
dfNew_filtered.show(10)

# COMMAND ----------

# # Filter out rows where medicaidMonths is less than 8
# dfNew_filtered = dfNew.filter(dfNew.medicaidMonths >= 8)

# print((dfNew.count(), len(dfNew.columns)))
# print((dfNew_filtered.count(), len(dfNew_filtered.columns)))
# dfNew_filtered.show(10)

# COMMAND ----------

dfNew_filtered.write.saveAsTable("dua_058828_spa240.paper_4_patient_sample_2017_12_months", mode='overwrite')

# COMMAND ----------

# dfNew_filtered.write.saveAsTable("dua_058828_spa240.paper_4_patient_sample_2018", mode='overwrite')