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
# MAGIC Denominator

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_demo_file_01_12_months")
denom = denom.select("beneID","state", "sex", "age", "birthday")
print(denom.count())

# COMMAND ----------

denom.show()

# COMMAND ----------

from pyspark.sql.functions import col, year

# Calculate age in 2018
denom = denom.withColumn('age_2018', 2018 - year(col('birthday')))

# Filter rows to keep only those between 3 and 20 years old
filtered_denom = denom.filter((col('age_2018') >= 3) & (col('age_2018') < 21))

# Show the result
filtered_denom.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Numerator

# COMMAND ----------

numerator = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
numerator.show()

# COMMAND ----------

numerator = numerator.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
print(numerator.count())
numerator.show(25)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("WellCareFilter").getOrCreate()

# Create DataFrame (assuming it's already loaded as 'numerator')

# Define the well-care codes
cpt_codes = ["99382", "99383", "99384", "99385", "99391","99392", "99393", "99394", "99395"]
hcpcs_codes = ["G0438", "G0439", "S0302", "S0610", "S0612", "S0613"]
icd10_codes = ["Z0000", "Z0001", "Z00121", "Z00129", "Z002", "Z003", "Z01411", "Z01419", "Z025", "Z761", "Z762"]

# Combine CPT and HCPCS codes into one list
procedure_codes = cpt_codes + hcpcs_codes

# Filter the DataFrame
numerator = numerator.filter(
    (col("LINE_PRCDR_CD").isin(procedure_codes)) &
    (col("DGNS_CD_1").isin(icd10_codes))
)

# Show the filtered DataFrame
print(numerator.count())
numerator.show()

# COMMAND ----------

from pyspark.sql.functions import col, count

print(numerator.count())

# COMMAND ----------

from pyspark.sql.functions import month

# Assuming your DataFrame is named 'df' and you have a column 'service_date'

# Create the 'service_month' column based on the 'service_date' column
numerator = numerator.withColumn("service_month", month(col("SRVC_BGN_DT")))

# Show the result to verify the new column
numerator.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum

# Create a pivot table to count visits per month for each beneID
pivot_df = numerator.groupBy(["beneID","state"]).pivot("service_month", list(range(1, 13))).count()

# Rename pivoted columns to month_1, month_2, ..., month_12
for i in range(1, 13):
    pivot_df = pivot_df.withColumnRenamed(str(i), f"month_{i}")

# Fill null values with 0 (no visits in that month)
pivot_df = pivot_df.fillna(0)

# Create columns to indicate if there was at least one visit in that month
for i in range(1, 13):
    pivot_df = pivot_df.withColumn(f"month_{i}", when(col(f"month_{i}") > 0, 1).otherwise(0))

# Create the total_visits column
pivot_df = pivot_df.withColumn("total_visits", 
                               col("month_1") + col("month_2") + col("month_3") + col("month_4") + 
                               col("month_5") + col("month_6") + col("month_7") + col("month_8") + 
                               col("month_9") + col("month_10") + col("month_11") + col("month_12"))

# Create the any_visit column
pivot_df = pivot_df.withColumn("any_visit", when(col("total_visits") > 0, 1).otherwise(0))

# Select only the required columns
final_df = pivot_df.select(
    "beneID", "state",
    *[f"month_{i}" for i in range(1, 13)],
    "total_visits",
    "any_visit"
)

# Show the result to verify
final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC start sensitivity analysis

# COMMAND ----------

sensitivity = final_df.join(filtered_denom, on=["beneID","state"], how="inner")
from pyspark.sql import functions as F

sensitivity = sensitivity.withColumn(
    'quarter',
    F.when((F.col('month_1') == 1) | (F.col('month_2') == 1) | (F.col('month_3') == 1), 1)
    .when((F.col('month_4') == 1) | (F.col('month_5') == 1) | (F.col('month_6') == 1), 2)
    .when((F.col('month_7') == 1) | (F.col('month_8') == 1) | (F.col('month_9') == 1), 3)
    .when((F.col('month_10') == 1) | (F.col('month_11') == 1) | (F.col('month_12') == 1), 4)
    .otherwise(None)
)

# Drop rows where quarter is None
sensitivity = sensitivity.filter(F.col('quarter').isNotNull())

# Aggregate by beneID and state, taking the minimum value for quarter
sensitivity = sensitivity.groupBy('beneID', 'state').agg(F.min('quarter').alias('min_quarter'))

sensitivity.show()

# Perform value count of the 'quarter' column
quarter_counts = sensitivity.groupBy('min_quarter').count()

# Calculate the total count for percentage calculation
total_count = sensitivity.count()

# Add a column for the percentage
quarter_percentages = quarter_counts.withColumn(
    'percentage',
    (F.col('count') / total_count) * 100
)

quarter_percentages.show()

# COMMAND ----------

# MAGIC %md
# MAGIC end sensitivity analysis

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, round

# Assuming your DataFrame is named 'final_df'

# Count the total number of rows in the DataFrame
total_rows = final_df.count()

# Calculate the percentage of total rows for each value in 'total_visits'
total_visits_percentage_df = final_df.groupBy("total_visits") \
                                     .agg(count("*").alias("count")) \
                                     .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for total_visits
total_visits_percentage_df.show()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = final_df.groupBy("any_visit") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

final_df = final_df.select("beneID", "state", "total_visits", "any_visit")
final_df = (final_df.withColumnRenamed('total_visits', 'total_visits_2018')
        .withColumnRenamed('any_visit', 'any_visit_2018'))

# COMMAND ----------

final_df.show()

# COMMAND ----------

final = filtered_denom.join(final_df, on=["beneID","state"], how="left").fillna(0)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, round

# Assuming your DataFrame is named 'final_df'

total_rows = final.count()

# Calculate the percentage of total rows for each value in 'total_visits'
total_visits_percentage_df = final.groupBy("total_visits_2018") \
                                     .agg(count("*").alias("count")) \
                                     .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for total_visits
total_visits_percentage_df.show()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = final.groupBy("any_visit_2018") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

final = final.select("beneID", "state", "total_visits_2018", "any_visit_2018")
final.show()

# COMMAND ----------

print(final.count())

# COMMAND ----------

# MAGIC %md
# MAGIC SAVE FILES

# COMMAND ----------

final.write.saveAsTable("dua_058828_spa240.paper_4_well_child_visits_2018_12_months_new", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC TEST AFTER THE FACT

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_well_child_visits_2018_12_months")
print(denom.count())

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("any_visit") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

