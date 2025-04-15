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

plan = spark.sql("select MC_PLAN_ID, MC_PLAN_NAME, STATE_CD, MC_PLAN_TYPE_CTGRY_CD, MC_PLAN_REIMBRSMT_TYPE_CD, MC_PLAN_REIMBRSMT_TYPE_CTGRY_CD, MC_ENT_PRFT_STUS_CD  from extracts.tafan18.mngd_care_plan_base")
plan.show()

# COMMAND ----------

from pyspark.sql.functions import when

# Create new columns based on the value of MC_PLAN_REIMBRSMT_TYPE_CD
plan = plan.withColumn("risk_cap", when(plan["MC_PLAN_REIMBRSMT_TYPE_CD"] == 1, 1).otherwise(0)) \
       .withColumn("non_risk_cap", when(plan["MC_PLAN_REIMBRSMT_TYPE_CD"] == 2, 1).otherwise(0)) \
       .withColumn("ffs", when(plan["MC_PLAN_REIMBRSMT_TYPE_CD"] == 3, 1).otherwise(0)) \
       .withColumn("pccm", when(plan["MC_PLAN_REIMBRSMT_TYPE_CD"] == 4, 1).otherwise(0)) \
       .withColumn("other", when(plan["MC_PLAN_REIMBRSMT_TYPE_CD"] == 5, 1).otherwise(0)) 

# Show the updated DataFrame
plan.show()

# COMMAND ----------

plan = plan.filter(~col("STATE_CD").isin(['RI', 'UT', 'AL', 'KY', 'OK', 'NY', 'MA', 'NH', 'CT', 'TN', 'MD', 'ID', 'MN', 'NJ', 'TX', 'NE', 'PR', 'VI']))
print((plan.count(), len(plan.columns)))
plan.show(200)

# Count the number of distinct values in a column
distinct_count = plan.select(col("STATE_CD")).distinct().count()

# Display the result
print("Distinct count:", distinct_count)

# COMMAND ----------

# Aggregate null counts by state

columns_to_check = ["MC_PLAN_REIMBRSMT_TYPE_CD", "MC_PLAN_REIMBRSMT_TYPE_CTGRY_CD", "MC_ENT_PRFT_STUS_CD"]  # Replace these with your actual column names

# Aggregate to find total count and null count per state for specified columns
null_percentage_by_state = plan.groupBy("state_cd").agg(
    *[(count(when(col(c).isNull() | isnan(col(c)), c)) / count(lit(1)) * 100).alias(c + "") for c in columns_to_check]
)

# Show the results
null_percentage_by_state.show(null_percentage_by_state.count(), False)

# COMMAND ----------

# Group by 'state' and sum each of the plan type columns
sum_df = plan.groupBy("state_cd").agg(
    sum(col("risk_cap")).alias("sum_risk_cap"),
    sum(col("non_risk_cap")).alias("sum_non_risk_cap"),
    sum(col("ffs")).alias("sum_ffs"),
    sum(col("pccm")).alias("sum_pccm"),
    sum(col("other")).alias("sum_other")
)

# Show the aggregated DataFrame
sum_df.show(n=sum_df.count(), truncate=False)

# COMMAND ----------

states_to_exclude = ['ME', 'MT', 'NC', 'SD', 'FL']   
plan_final = plan.filter(~plan.STATE_CD.isin(states_to_exclude))

state_counts = plan_final.groupBy("state_cd").count()

# Print the number of unique states
num_unique_states = state_counts.count()
print(f"Number of unique states: {num_unique_states}")
print((plan_final.count(), len(plan_final.columns)))

# COMMAND ----------

#plan_final.write.saveAsTable("dua_058828_spa240.paper4_final_state_plans", mode='overwrite')
plan_final.write.saveAsTable("dua_058828_spa240.paper4_final_state_plans_28_2018", mode='overwrite')

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper4_final_state_plans_28_2018")
df.show()

# COMMAND ----------

pa_plans = df.filter(df.STATE_CD == "PA")
distinct_plan_names = pa_plans.select("MC_PLAN_ID").distinct()
distinct_plan_names.show(distinct_plan_names.count(), truncate=False)