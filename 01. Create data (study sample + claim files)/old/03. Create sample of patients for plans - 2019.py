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

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper4_final_sample_plan_28_8_or_more")
df.show()

# COMMAND ----------

plan = spark.sql("select bene_id, state_cd, mc_plan_id_01_01, mc_plan_id_01_02, mc_plan_id_01_03, mc_plan_id_01_04, mc_plan_id_01_05, mc_plan_id_01_06, mc_plan_id_01_07, mc_plan_id_01_08, mc_plan_id_01_09, mc_plan_id_01_10, mc_plan_id_01_11, mc_plan_id_01_12 from extracts.tafr19.demog_elig_mngd_care")

plan.show()

# COMMAND ----------

df = df.select("beneID", "state", "medicaidMonths")
df = df.withColumnRenamed("state_cd", "state")
plan = plan.withColumnRenamed("state_cd", "state")
plan = plan.withColumnRenamed("bene_id", "beneID")

df.show(5)

# COMMAND ----------

print(df.count())
print(plan.count())

# COMMAND ----------

df_final = plan.join(df, how="inner", on=["beneID","state"])
print(df_final.count())
df_final.show()

# COMMAND ----------

# Perform value counts on column "abc"
value_counts = df_final.groupBy("medicaidMonths").count()

# Show the result
value_counts.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as sum_, count, desc
from functools import reduce

# Assuming you already have a Spark session initiated
spark = SparkSession.builder.appName("Percentage Rows with Nulls by State").getOrCreate()

# Columns to check for null values
columns_to_check = [f"mc_plan_id_01_{str(i).zfill(2)}" for i in range(1, 13)]

# Expression to sum null counts across specified columns
null_sum_expr = reduce(
    lambda a, b: a + b,
    [when(col(c).isNull(), 1).otherwise(0) for c in columns_to_check]
)

# Adding a new column 'null_count' that counts nulls in specified columns
df_final_with_null_count = df_final.withColumn('null_count', null_sum_expr)

# Filtering rows where 'null_count' is greater than 4
rows_with_excess_nulls = df_final_with_null_count.filter(col('null_count') > 4)

# Grouping by 'state' and counting rows with more than 4 nulls
excess_nulls_count_by_state = rows_with_excess_nulls.groupBy("state").agg(count("*").alias("excess_nulls_count"))

# Grouping by 'state' and counting total rows
total_count_by_state = df_final.groupBy("state").agg(count("*").alias("total_rows"))

# Joining the two datasets on 'state'
state_counts = total_count_by_state.join(excess_nulls_count_by_state, "state", "outer")

# Calculating the percentage of rows with excess nulls
state_counts_with_percentage = state_counts.withColumn(
    "percentage",
    (col("excess_nulls_count") / col("total_rows") * 100)
)

# Ordering by percentage in descending order
sorted_percentage_by_state = state_counts_with_percentage.orderBy(desc("percentage"))

# Showing the result
sorted_percentage_by_state.select("state", "percentage").show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, desc
from functools import reduce

# Assuming you already have a Spark session initiated
spark = SparkSession.builder.appName("Percentage Rows with Any Nulls by State").getOrCreate()

# Columns to check for null values
columns_to_check = [f"mc_plan_id_01_{str(i).zfill(2)}" for i in range(1, 13)]

# Expression to check if any of the specified columns contain nulls
any_null_expr = when(reduce(lambda a, b: a | b, [col(c).isNull() for c in columns_to_check]), 1).otherwise(0)

# Adding a new column 'any_null' that indicates if any nulls are present in specified columns
df_final_with_any_null = df_final.withColumn('any_null', any_null_expr)

# Filtering rows where 'any_null' is 1
rows_with_any_nulls = df_final_with_any_null.filter(col('any_null') == 1)

# Grouping by 'state' and counting rows with any nulls
any_nulls_count_by_state = rows_with_any_nulls.groupBy("state").agg(count("*").alias("any_nulls_count"))

# Grouping by 'state' and counting total rows
total_count_by_state = df_final.groupBy("state").agg(count("*").alias("total_rows"))

# Joining the two datasets on 'state'
state_counts = total_count_by_state.join(any_nulls_count_by_state, "state", "outer")

# Calculating the percentage of rows with any nulls
state_counts_with_percentage = state_counts.withColumn(
    "percentage",
    (col("any_nulls_count") / col("total_rows") * 100)
)

# Ordering by percentage in descending order
sorted_percentage_by_state = state_counts_with_percentage.orderBy(desc("percentage"))

# Showing the result
sorted_percentage_by_state.select("state", "percentage").show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, desc
from functools import reduce

# Assuming you already have a Spark session initiated
spark = SparkSession.builder.appName("Percentage Rows with Any Nulls by State for MedicaidMonths 12").getOrCreate()

# Columns to check for null values
columns_to_check = [f"mc_plan_id_01_{str(i).zfill(2)}" for i in range(1, 13)]

# Filtering DataFrame for medicaidMonths == 12
df_filtered = df_final.filter(col("medicaidMonths") == 12)

# Expression to check if any of the specified columns contain nulls
any_null_expr = when(reduce(lambda a, b: a | b, [col(c).isNull() for c in columns_to_check]), 1).otherwise(0)

# Adding a new column 'any_null' that indicates if any nulls are present in specified columns
df_filtered_with_any_null = df_filtered.withColumn('any_null', any_null_expr)

# Filtering rows where 'any_null' is 1
rows_with_any_nulls = df_filtered_with_any_null.filter(col('any_null') == 1)

# Grouping by 'state' and counting rows with any nulls
any_nulls_count_by_state = rows_with_any_nulls.groupBy("state").agg(count("*").alias("any_nulls_count"))

# Grouping by 'state' and counting total rows in the filtered DataFrame
total_count_by_state = df_filtered.groupBy("state").agg(count("*").alias("total_rows"))

# Joining the two datasets on 'state'
state_counts = total_count_by_state.join(any_nulls_count_by_state, "state", "outer")

# Calculating the percentage of rows with any nulls
state_counts_with_percentage = state_counts.withColumn(
    "percentage",
    (col("any_nulls_count") / col("total_rows") * 100)
)

# Ordering by percentage in descending order
sorted_percentage_by_state = state_counts_with_percentage.orderBy(desc("percentage"))

# Showing the result
sorted_percentage_by_state.select("state", "percentage").show()


# COMMAND ----------

#deelete rows with >4 null values

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from functools import reduce

# Assuming you already have a Spark session initiated
spark = SparkSession.builder.appName("Remove Rows with Many Nulls").getOrCreate()

# Columns to check for null values
columns_to_check = [f"mc_plan_id_01_{str(i).zfill(2)}" for i in range(1, 13)]

# Expression to sum null counts across specified columns
null_sum_expr = reduce(
    lambda a, b: a + b,
    [when(col(c).isNull(), 1).otherwise(0) for c in columns_to_check]
)

# Adding a new column 'null_count' that counts nulls in specified columns
df_final_with_null_count = df_final.withColumn('null_count', null_sum_expr)

# Filtering rows where 'null_count' is greater than 4
df_final_cleaned = df_final_with_null_count.filter(col('null_count') <= 4).drop("null_count")

# Showing the result
df_final_cleaned.show() 

# COMMAND ----------

print(df_final.count())
print(df_final_cleaned.count())

# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, when, array, array_distinct, size

# # Assuming you already have a Spark session initiated
# spark = SparkSession.builder.appName("Uniformity Check").getOrCreate()

# # Define the columns to check
# columns_to_check = [f"mc_plan_id_01_{str(i).zfill(2)}" for i in range(1, 13)]

# # Using array to gather the values and array_distinct to filter unique non-null values
# df_final_cleaned = df_final_cleaned.withColumn(
#     "uniformity_check",
#     when(
#         size(array_distinct(array(*[col(c) for c in columns_to_check if c is not None]))) == 1, 1
#     ).otherwise(0)
# )

# # Showing the result
# df_final_cleaned.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, size, array_distinct, array, expr

# List of column names to check
columns_to_check = [f"mc_plan_id_01_{str(i).zfill(2)}" for i in range(1, 13)]

# Define an array column, excluding nulls explicitly
non_null_array = array(*[col(c).alias(c) for c in columns_to_check])

# Filter out null values from this array
filtered_array = expr("filter(non_null_array, x -> x is not null)")

# Adding the 'uniformity_check' column to the DataFrame
df_final_cleaned = df_final.withColumn("non_null_array", non_null_array)\
                           .withColumn("filtered_array", filtered_array)\
                           .withColumn(
                               "uniformity_check",
                               when(
                                   (size(filtered_array) > 0) & 
                                   (size(array_distinct(filtered_array)) == 1),
                                   1
                               ).otherwise(0)
                            )

# Drop the intermediate columns if they are not needed
df_final_cleaned = df_final_cleaned.drop("non_null_array", "filtered_array")

df_final_cleaned.show()

# COMMAND ----------

# Perform value counts on column "abc"
value_counts = df_final_cleaned.groupBy("uniformity_check").count()

# Show the result
value_counts.show()

#31259858

# +----------------+--------+
# |uniformity_check|   count|
# +----------------+--------+
# |               1|22365085|
# |               0| 8948777|
# +----------------+--------+

# COMMAND ----------

#df_final_cleaned.show(500)

# COMMAND ----------

from pyspark.sql import functions as F

# Filter the DataFrame for the desired medicaidMonths values
filtered_df = df_final_cleaned.filter(
    df_final_cleaned.medicaidMonths.isin([8, 9, 10, 11, 12])
)

# Calculate the total count for each medicaidMonths value
total_counts = filtered_df.groupBy("medicaidMonths").count()

# Calculate the count of 'uniformity_check' == 1 for each medicaidMonths value
uniformity_counts = filtered_df.filter(
    filtered_df.uniformity_check == 1
).groupBy("medicaidMonths").count().withColumnRenamed("count", "uniformity_count")

# Join the total counts with the uniformity counts on 'medicaidMonths'
result = total_counts.join(
    uniformity_counts, "medicaidMonths", "inner"
)

# Calculate the percentage
result_with_percentage = result.withColumn(
    "percentage",
    F.col("uniformity_count") / F.col("count") * 100
)

# Select only necessary columns and order by medicaidMonths
final_result = result_with_percentage.select(
    "medicaidMonths", "percentage"
).orderBy("medicaidMonths")

# Show the result
final_result.show()


# COMMAND ----------

# Count occurrences of each value in 'medicaidMonths'
medicaid_months_counts = df_final_cleaned.groupBy("medicaidMonths").count()

# Show the result
medicaid_months_counts.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Calculate total rows in the DataFrame for normalization
total_rows = df_final_cleaned.count()

# Count occurrences of each value in 'medicaidMonths'
medicaid_months_counts = df_final_cleaned.groupBy("medicaidMonths").count()

# Calculate the percentage of total rows for each 'medicaidMonths' value
medicaid_months_percentage = medicaid_months_counts.withColumn(
    "percentage",
    (F.col("count") / total_rows) * 100
)

# Show the result with percentage
medicaid_months_percentage.show()

# COMMAND ----------

# Filter the DataFrame for 'uniformity_check' == 1
uniformity_check_counts = df_final_cleaned.filter(
    df_final_cleaned.uniformity_check == 1
).groupBy("medicaidMonths").count()

# Show the result
uniformity_check_counts.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Total number of rows in the DataFrame
total_count = df_final_cleaned.count()

# Counting rows where uniformity_check == 1
uniform_count = df_final_cleaned.filter(F.col("uniformity_check") == 1).count()

# Calculating the percentage of rows where uniformity_check == 1
percentage_uniform = (uniform_count / total_count) * 100

print(f"Percentage of rows with uniformity_check == 1: {percentage_uniform:.2f}%")

# COMMAND ----------

from pyspark.sql import functions as F

# Total number of rows in the DataFrame
total_count = df_final_cleaned.count()

# Count of rows where uniformity_check == 1 for each medicaidMonths
uniformity_counts = df_final_cleaned.groupBy("medicaidMonths").agg(
    F.sum(F.when(F.col("uniformity_check") == 1, 1).otherwise(0)).alias("uniform_count")
)

# Calculate the percentage of total rows where uniformity_check == 1 for each medicaidMonths
result = uniformity_counts.withColumn(
    "percentage_of_total",
    (F.col("uniform_count") / total_count) * 100
)

# Show the result
result.select("medicaidMonths", "percentage_of_total").show()

# COMMAND ----------

# Filtering out rows where uniformity_check == 0
df_filtered = df_final_cleaned.filter(df_final_cleaned.uniformity_check != 0)

# COMMAND ----------

# Perform value counts on column "abc"
value_counts = df_final_cleaned.groupBy("uniformity_check").count()

# Show the result
value_counts.show()

# COMMAND ----------

# Perform value counts on column "abc"
value_counts = df_filtered.groupBy("uniformity_check").count()

# Show the result
value_counts.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Calculate total rows in the DataFrame for normalization
total_rows = df_filtered.count()

# Count occurrences of each value in 'medicaidMonths'
medicaid_months_counts = df_filtered.groupBy("medicaidMonths").count()

# Calculate the percentage of total rows for each 'medicaidMonths' value
medicaid_months_percentage = medicaid_months_counts.withColumn(
    "percentage",
    (F.col("count") / total_rows) * 100
)

# Show the result with percentage
medicaid_months_percentage.show()

# COMMAND ----------

df_filtered.write.saveAsTable("dua_058828_spa240.paper4_final_sample_from_notebook_03", mode='overwrite')