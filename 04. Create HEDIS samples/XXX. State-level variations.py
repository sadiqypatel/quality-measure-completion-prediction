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
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC 01a - Child & Adolescent

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_well_child_visits_2018_12_months")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('any_visit_2018').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 01b - Child & Adolescent

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_well_child_visits_2019_12_months")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('any_visit_2019').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 02. Lower back pain

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_lower_back_pain_02_final_12_months")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('low_back_pass').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
        expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 03. Prenatal & Postpartum care

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_post_partum_care_12_months_new")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('prenatal').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
        expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('postpartum').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
        expr("percentile_approx(percentage, 0.5)").alias('median'),

    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 05a-b. SPC Outcome 1 & 2

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_spc_both_outcomes_12_months_new3")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('anymed').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
            expr("percentile_approx(percentage, 0.5)").alias('median'),

    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

file = spark.table("dua_058828_spa240.paper_4_spc_both_outcomes_12_months_new4")
print(file.count())
file.show()

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('adherence_yes').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
                expr("percentile_approx(percentage, 0.5)").alias('median'),

    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 05a-b. SPD Outcome 1 & 2

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_spd_both_outcomes_12_months_new1")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file
#df = df.filter(df['anymed'] != 0)

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('anymed').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

file = spark.table("dua_058828_spa240.paper_4_spd_both_outcomes_12_months_new2")
print(file.count())
file.show()

# Define your DataFrame
df = file
df = df.filter(df['anymed'] != 0)

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('adherence_yes').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 06. PCR - Plan All-cause Readmission

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_pcr_outcome_12_months")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('readmission_yes').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC FUM30

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_fum30_outcome_12_months_new2")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('bh_visit').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC AMM 84 days

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_amm_outcome1_months_new")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('outcome1_yes').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC AMM 180 days

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_amm_both_outcomes_months_new")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('outcome2_yes').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

file = spark.table("dua_058828_spa240.paper_4_pbh_with_outcomes_new1")
print(file.count())
file.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round

# Define your DataFrame
df = file

# Aggregate by 'state'
aggregated_df = df.groupBy('state').agg(
    sum('outcome1_yes').alias('sum_wcv'),
    count('*').alias('total_rows')
)

# Calculate the percentage
result_df = aggregated_df.withColumn(
    'percentage',
    round((col('sum_wcv') / col('total_rows')) * 100, 1)
)

# Show the result

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, expr

# Initialize Spark session

# Define your DataFrame (assuming your DataFrame is named 'df' and has a column 'percentage')
df = result_df

# Calculate min, max, 25th, and 75th percentiles for 'percentage'
statistics_df = df.agg(
    min('percentage').alias('min_percentage'),
    max('percentage').alias('max_percentage'),
    expr("percentile_approx(percentage, 0.5)").alias('median'),
    expr("percentile_approx(percentage, 0.25)").alias('25th_percentile'),
    expr("percentile_approx(percentage, 0.75)").alias('75th_percentile')
)

# Show the result
statistics_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC