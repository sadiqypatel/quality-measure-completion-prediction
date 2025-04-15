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

df = spark.table("dua_058828_spa240.paper4_final_sample_2018_from_notebook_07")
df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Create a new column 'plan_id' by picking the non-null value from the first column in the list that isn't null
# This works because all non-null values are the same as specified
df = df.withColumn("plan_id", F.coalesce(*[F.col(f"mc_plan_id_01_{str(i).zfill(2)}") for i in range(1, 13)]))

df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Count the number of null values in the 'plan_id' column
null_count = df.filter(F.col("plan_id").isNull()).count()

if null_count > 0:
    print(f"There are {null_count} null values in the 'plan_id' column.")
else:
    print("There are no null values in the 'plan_id' column.")

# COMMAND ----------

# List of columns to drop
columns_to_drop = [f"mc_plan_id_01_{str(i).zfill(2)}" for i in range(1, 13)]

# Dropping the specified columns from the DataFrame
df = df.drop(*columns_to_drop)

df.show()

# COMMAND ----------

plan = spark.table("dua_058828_spa240.paper4_final_state_plans_28_2018")
plan.show()

# COMMAND ----------

plan = plan.select("MC_PLAN_ID","MC_PLAN_NAME","STATE_CD","MC_PLAN_TYPE_CTGRY_CD","MC_PLAN_REIMBRSMT_TYPE_CTGRY_CD")
plan.show()

# COMMAND ----------

plan = plan.withColumnRenamed("STATE_CD", "state")
plan = plan.withColumnRenamed("MC_PLAN_NAME", "mc_plan")
plan = plan.withColumnRenamed("MC_PLAN_ID", "plan_id")
plan = plan.withColumnRenamed("MC_PLAN_TYPE_CTGRY_CD", "mc_plan_cat")
plan = plan.withColumnRenamed("MC_PLAN_REIMBRSMT_TYPE_CTGRY_CD", "mc_plan_reimb_cat")
plan.show()

# COMMAND ----------

from pyspark.sql.functions import countDistinct

# Count the total number of rows
total_rows = plan.count()

# Count the total number of unique 'plan_id'
unique_plan_ids = plan.select(countDistinct("plan_id")).collect()[0][0]

print("Total number of rows:", total_rows)
print("Total number of unique plan_id:", unique_plan_ids)

# COMMAND ----------

print(df.count())
merged = df.join(plan, on=["plan_id","state"], how="left")
print(merged.count())
merged.show()

# COMMAND ----------

# Check for multiple columns (say 'column1', 'column2' from df2)

from pyspark.sql.functions import col, isnull

total_rows = merged.count()

from pyspark.sql.functions import col, isnull

# List columns from df2 you want to check
columns_to_check = ['mc_plan', 'mc_plan_reimb_cat', 'mc_plan_cat']

# Iterate and calculate the percentage of missing values in each column
for column in columns_to_check:
    missing_count = merged.filter(isnull(col(column))).count()
    missing_percentage = (missing_count / total_rows) * 100
    print(f"Percentage of missing values in '{column}': {missing_percentage:.2f}%")

# COMMAND ----------

print(df.count())
print(merged.count())

clean_df = merged.dropna(how='all', subset= ['mc_plan', 'mc_plan_reimb_cat', 'mc_plan_cat'])
print(clean_df.count())

# COMMAND ----------

# Check for multiple columns (say 'column1', 'column2' from df2)

from pyspark.sql.functions import col, isnull

# Count missing values in a specific column from df2
missing_count = clean_df.filter(isnull(col('mc_plan'))).count()
print(f"Missing values in 'df2_column': {missing_count}")

# COMMAND ----------

print(clean_df.count())

# COMMAND ----------

# Count occurrences of each beneID
beneID_counts = clean_df.groupBy("beneID").agg(F.count("beneID").alias("count"))

# Filter to find beneIDs that appear more than once
repeated_beneIDs = beneID_counts.filter(F.col("count") > 1).select("beneID")

# COMMAND ----------

# Remove rows where beneID is repeated more than once
filtered_df = clean_df.join(repeated_beneIDs, "beneID", "left_anti")
print(filtered_df.count())

# COMMAND ----------

from pyspark.sql import functions as F

# Counting occurrences of each value including nulls in 'column_name'
value_counts = filtered_df.groupBy("mc_plan_reimb_cat").agg(F.count("*").alias("count"))

# To specifically ensure that null values are counted and displayed
# We create a new column that identifies nulls and then performs the grouping
value_counts_including_nulls = filtered_df.withColumn(
    "value_or_null",
    F.when(F.col("mc_plan_reimb_cat").isNull(), "NULL").otherwise(F.col("mc_plan_reimb_cat"))
).groupBy("value_or_null").agg(F.count("*").alias("count"))

# Show the result
value_counts_including_nulls.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Counting occurrences of each value including nulls in 'column_name'
value_counts = filtered_df.groupBy("mc_plan_cat").agg(F.count("*").alias("count"))

# To specifically ensure that null values are counted and displayed
# We create a new column that identifies nulls and then performs the grouping
value_counts_including_nulls = filtered_df.withColumn(
    "value_or_null",
    F.when(F.col("mc_plan_cat").isNull(), "NULL").otherwise(F.col("mc_plan_cat"))
).groupBy("value_or_null").agg(F.count("*").alias("count"))

# Show the result
value_counts_including_nulls.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Assuming df is your DataFrame and 'column_name' is the column to check for nulls
nulls_in_column = filtered_df.filter(F.col('mc_plan_reimb_cat').isNull())

# Show rows with nulls in 'column_name'
nulls_in_column.show()

# COMMAND ----------

from pyspark.sql.functions import when

# Create new columns based on the value of MC_PLAN_REIMBRSMT_TYPE_CD
filtered_df = filtered_df.withColumn("risk_cap", when(filtered_df["mc_plan_reimb_cat"] == 1, 1).otherwise(0)) \
       .withColumn("non_risk_cap", when(filtered_df["mc_plan_reimb_cat"] == 2, 1).otherwise(0)) \
       .withColumn("ffs", when(filtered_df["mc_plan_reimb_cat"] == 3, 1).otherwise(0)) \
       .withColumn("pccm", when(filtered_df["mc_plan_reimb_cat"] == 4, 1).otherwise(0)) \
       .withColumn("other", when(filtered_df["mc_plan_reimb_cat"] == 5, 1).otherwise(0)) 

# Show the updated DataFrame
filtered_df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Grouping by 'state' and performing multiple aggregations
aggregated_df = filtered_df.groupBy("state").agg(
    F.countDistinct("beneID").alias("unique_beneID_count"),   # Count unique beneID by state
    F.countDistinct("plan_id").alias("unique_plan_id_count"), # Count unique plan_id by state
    F.sum("risk_cap").alias("total_risk_cap"),                # Sum of risk_cap by state
    F.sum("non_risk_cap").alias("total_non_risk_cap"),        # Sum of non_risk_cap by state
    F.sum("ffs").alias("total_ffs"),                          # Sum of ffs by state
    F.sum("pccm").alias("total_pccm"),                        # Sum of pccm by state
    F.sum("other").alias("total_other")                       # Sum of other by state
)

# COMMAND ----------

# Show the result
aggregated_df.show(1000)

# COMMAND ----------

print(filtered_df.count())
filtered_df = filtered_df.filter(col("mc_plan_cat") == 1)
print(filtered_df.count())

# COMMAND ----------

from pyspark.sql import functions as F

# Grouping by 'state' and performing multiple aggregations
aggregated_df = filtered_df.groupBy("state").agg(
    F.countDistinct("beneID").alias("unique_beneID_count"),   # Count unique beneID by state
    F.countDistinct("plan_id").alias("unique_plan_id_count"), # Count unique plan_id by state
    F.sum("risk_cap").alias("total_risk_cap"),                # Sum of risk_cap by state
    F.sum("non_risk_cap").alias("total_non_risk_cap"),        # Sum of non_risk_cap by state
    F.sum("ffs").alias("total_ffs"),                          # Sum of ffs by state
    F.sum("pccm").alias("total_pccm"),                        # Sum of pccm by state
    F.sum("other").alias("total_other")                       # Sum of other by state
)

# COMMAND ----------

# Show the result
aggregated_df.show(1000)

# COMMAND ----------

filtered_df.write.saveAsTable("dua_058828_spa240.paper4_final_sample_2018_from_notebook_07_new", mode='overwrite')

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper4_final_sample_2018_from_notebook_07_new")
df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Grouping by 'state' and performing multiple aggregations
aggregated_df = df.groupBy("state").agg(
    F.countDistinct("beneID").alias("unique_beneID_count"),   # Count unique beneID by state
    F.countDistinct("plan_id").alias("unique_plan_id_count"), # Count unique plan_id by state
    F.sum("risk_cap").alias("total_risk_cap"),                # Sum of risk_cap by state
    F.sum("non_risk_cap").alias("total_non_risk_cap"),        # Sum of non_risk_cap by state
    F.sum("ffs").alias("total_ffs"),                          # Sum of ffs by state
    F.sum("pccm").alias("total_pccm"),                        # Sum of pccm by state
    F.sum("other").alias("total_other")                       # Sum of other by state
)

# COMMAND ----------

# Show the result
aggregated_df.show(1000)

# COMMAND ----------

