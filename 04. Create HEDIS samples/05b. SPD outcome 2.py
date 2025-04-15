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
# MAGIC DENOMINATOR

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_spd_outcome1_12_months")
print(denom.count())
denom.show()

# COMMAND ----------

outcome1_value = denom.groupBy("anymed").count()
outcome1_value.show()

# COMMAND ----------

# Remove rows where anymed is 0
denom = denom.filter(denom.anymed != 0)

# Show the result
print(denom.count())
denom.show()

# COMMAND ----------

# MAGIC %md
# MAGIC NUMERATOR

# COMMAND ----------

pharm = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
print(pharm.count())
pharm.show()

# COMMAND ----------

pharm = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
print(pharm.count())
pharm = pharm.select("beneID", "state", "RX_FILL_DT", "NDC", "DAYS_SUPPLY")
pharm.show()

# COMMAND ----------

pharm.registerTempTable("connections")

pharm = spark.sql('''

SELECT DISTINCT beneID, state, RX_FILL_DT, NDC, DAYS_SUPPLY

FROM connections;
''')

pharm = pharm.filter(col("NDC").isNotNull())
pharm.show(200)

# COMMAND ----------

import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#betos_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df = rx_df[rx_df['rxDcDrugName'].isin(['acarbose', 'miglitol', 'pramlintide [Symlin]', 'alogliptin | metformin', 'alogliptin | pioglitazone', 'canagliflozin | metformin [Invokamet]', 'dapagliflozin | metformin [Xigduo]', 'dapagliflozin | saxagliptin [Qtern]', 'empagliflozin | metformin [Synjardy]', 'empagliflozin | linagliptin | metformin [Trijardy]', 'ertugliflozin | metformin [Segluromet]', 'ertugliflozin | sitagliptin [Steglujan]', 'glimepiride | pioglitazone', 'glimepiride | pioglitazone [Duetact]', 'glipizide | metformin', 'glyburide | metformin', 'linagliptin | metformin [Jentadueto]', 'metformin | pioglitazone', 'metformin | pioglitazone [Actoplus Met]', 'metformin | rosiglitazone [Avandamet]', 'metformin | saxagliptin [Kombiglyze]','metformin | sitagliptin [Janumet]','insulin aspart human','insulin aspart human [Fiasp]','insulin aspart human [NovoLog]','insulin aspart protamine human | insulin aspart human','insulin aspart protamine human | insulin aspart human [NovoLog Mix]','insulin degludec [Tresiba]','insulin degludec | liraglutide [Xultophy]','insulin detemir [Levemir]','insulin glargine','insulin glargine [Basaglar]','insulin glargine [Lantus]','insulin glargine [Semglee]','insulin glargine [Toujeo]','insulin glargine | lixisenatide [Soliqua]','insulin glulisine human [Apidra]','insulin lispro','insulin lispro [Admelog]','insulin lispro [Humalog]','insulin lispro [Lyumjev]','insulin lispro | insulin lispro protamine human','insulin lispro | insulin lispro protamine human [Humalog Mix]','insulin regular human [Afrezza]','insulin regular human [Humulin R]','insulin regular human [Myxredlin]', 'metformin','metformin [Fortamet]','metformin [Glumetza]','metformin [Riomet]','metformin | pioglitazone','metformin | pioglitazone [Actoplus Met]','metformin | rosiglitazone [Avandamet]','metformin | saxagliptin [Kombiglyze]','metformin | sitagliptin [Janumet]', 'Dulaglutide','exenatide [Bydureon]','exenatide [Byetta]','liraglutide [Saxenda]','liraglutide [Victoza]','lixisenatide [Adlyxin]','semaglutide [Ozempic]','semaglutide [Rybelsus]','semaglutide [Wegovy]','canagliflozin [Invokana]','canagliflozin | metformin [Invokamet]','dapagliflozin [Farxiga]','dapagliflozin | metformin [Xigduo]','dapagliflozin | saxagliptin [Qtern]','empagliflozin [Jardiance]','empagliflozin | linagliptin [Glyxambi]','empagliflozin | linagliptin | metformin [Trijardy]','empagliflozin | metformin [Synjardy]','ertugliflozin [Steglatro]','ertugliflozin | metformin [Segluromet]','ertugliflozin | sitagliptin [Steglujan]','glimepiride','glimepiride [Amaryl]','glimepiride | pioglitazone','glimepiride | pioglitazone [Duetact]','glipizide','glipizide [Glucotrol]','glipizide | metformin','glyburide','glyburide [Glynase]','glyburide | metformin','tolazamide','pioglitazone [Actos]','rosiglitazone [Avandia]','alogliptin','alogliptin [Nesina]','alogliptin | metformin','alogliptin | metformin [Kazano]','alogliptin | pioglitazone','alogliptin | pioglitazone [Oseni]','linagliptin [Tradjenta]','linagliptin | metformin [Jentadueto]','saxagliptin [Onglyza]','sitagliptin [Januvia]'])]
rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
print(rx_df.count())
rx_df.show(1000)

# COMMAND ----------

from pyspark.sql.functions import col, substring

numerator = pharm.join(
    rx_df,
    on='ndc',
    how='inner'
)

print(numerator.count())

# COMMAND ----------

numerator = numerator.join(denom, on=['beneID', 'state'], how='inner')
print(numerator.count())
numerator.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Convert RX_FILL_DT to date type
numerator = numerator.withColumn('RX_FILL_DT', F.to_date('RX_FILL_DT', 'yyyy-MM-dd'))

# Ensure DAYS_SUPPLY is an integer
numerator = numerator.withColumn('DAYS_SUPPLY', F.col('DAYS_SUPPLY').cast('int'))

# Add DAYS_SUPPLY to RX_FILL_DT to create RX_FILL_DT_30
numerator = numerator.withColumn('RX_FILL_DT_30', F.date_add('RX_FILL_DT', F.col('DAYS_SUPPLY')))

# Show the result
numerator.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Define the end of the year 2018
end_of_2018 = '2018-12-31'

# Aggregate the data
aggregated_df = numerator.groupBy('beneID', 'state').agg(
    F.min('RX_FILL_DT').alias('min_RX_FILL_DT'),
    F.max('RX_FILL_DT_30').alias('max_RX_FILL_DT'),
    F.sum('DAYS_SUPPLY').alias('total_DAYS_SUPPLY'),
    (F.datediff(F.lit(end_of_2018), F.min('RX_FILL_DT'))).alias('diff_last_day_2018_RX_FILL_DT'),
    (F.abs(F.datediff(F.min('RX_FILL_DT'), F.max('RX_FILL_DT_30')))).alias('abs_diff_min_max_RX_FILL_DT')
)

# Show the result
aggregated_df.show()

# COMMAND ----------

# Calculate the percentage
aggregated_df = aggregated_df.withColumn(
    'adherance',
    F.round((F.col('total_DAYS_SUPPLY') / F.col('abs_diff_min_max_RX_FILL_DT')) * 100, 1)
)

aggregated_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC No requirements on min date

# COMMAND ----------

# Calculate statistics
percentiles = aggregated_df.select(
    F.min("adherance").alias("min"),
    F.max("adherance").alias("max"),
    F.mean("adherance").alias("mean"),
    F.expr('percentile_approx(adherance, 0.25)').alias('25th_percentile'),
    F.expr('percentile_approx(adherance, 0.5)').alias('median'),
    F.expr('percentile_approx(adherance, 0.75)').alias('75th_percentile'),
    F.expr('percentile_approx(adherance, 0.90)').alias('90th_percentile'),
    F.expr('percentile_approx(adherance, 0.95)').alias('95th_percentile'),
    F.expr('percentile_approx(adherance, 0.99)').alias('99th_percentile')
)

# Show the calculated statistics
percentiles.show()

# COMMAND ----------

# Count the total number of rows in the aggregated DataFrame
total_count = aggregated_df.count()

# Filter the rows where adherence is 80.0 or greater
adherence_filtered_df = aggregated_df.filter(F.col('adherance') >= 80.0)

# Count the number of rows with adherence 80.0 or greater
adherence_count = adherence_filtered_df.count()

# Calculate the percentage
percentage = (adherence_count / total_count) * 100

# Display the result
print(f'Percentage of rows with adherence 80.0 or greater: {percentage:.2f}%')

# COMMAND ----------

# MAGIC %md
# MAGIC Requiring 61 or more days after index medication (min med date)

# COMMAND ----------

# Filter rows where 'diff_last_day_2018_RX_FILL_DT' is 61 or more
filtered_df = aggregated_df.filter(col('diff_last_day_2018_RX_FILL_DT') >= 61)

# Calculate statistics
percentiles = filtered_df.select(
    F.min("adherance").alias("min"),
    F.max("adherance").alias("max"),
    F.mean("adherance").alias("mean"),
    F.expr('percentile_approx(adherance, 0.25)').alias('25th_percentile'),
    F.expr('percentile_approx(adherance, 0.5)').alias('median'),
    F.expr('percentile_approx(adherance, 0.75)').alias('75th_percentile'),
    F.expr('percentile_approx(adherance, 0.90)').alias('90th_percentile'),
    F.expr('percentile_approx(adherance, 0.95)').alias('95th_percentile'),
    F.expr('percentile_approx(adherance, 0.99)').alias('99th_percentile')
)

# Show the calculated statistics
percentiles.show()

# COMMAND ----------

# Count the total number of rows in the aggregated DataFrame
total_count = filtered_df.count()

# Filter the rows where adherence is 80.0 or greater
adherence_filtered_df = filtered_df.filter(F.col('adherance') >= 80.0)

# Count the number of rows with adherence 80.0 or greater
adherence_count = adherence_filtered_df.count()

# Calculate the percentage
percentage = (adherence_count / total_count) * 100

# Display the result
print(f'Percentage of rows with adherence 80.0 or greater: {percentage:.2f}%')

# COMMAND ----------

# MAGIC %md
# MAGIC create data set with both outcomes

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_spd_outcome1_12_months")
aggregated_df = aggregated_df.select("beneID", "state", "adherance")
denom = denom.filter(denom.anymed != 0)
print(denom.count())
print(aggregated_df.count())
aggregated_df.show()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

result_df = aggregated_df.withColumn("adherence_yes", F.when(F.col("adherance") >= 80, 1).otherwise(0))
result_df.show()

# COMMAND ----------

print(result_df.count())

# COMMAND ----------

result_df.show()

# COMMAND ----------

final = denom.join(result_df, on=['beneID',"state"], how='left')
print(denom.count())
print(final.count())

# COMMAND ----------

null_count = final.filter(final["adherence_yes"].isNull()).count()
total_count = final.count()
null_percentage = (null_count / total_count) * 100
print(f"Percentage of rows with null 'adherence_yes': {null_percentage:.2f}%")

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit

final = final.withColumn("adherence_yes", coalesce(col("adherence_yes"), lit(0)))

# COMMAND ----------

null_count = final.filter(final["adherence_yes"].isNull()).count()
total_count = final.count()
null_percentage = (null_count / total_count) * 100
print(f"Percentage of rows with null 'adherence_yes': {null_percentage:.2f}%")

# COMMAND ----------

final.show()

# COMMAND ----------

from pyspark.sql import functions as F

total_count = final.count()
adherence_yes_counts = final.groupBy("adherence_yes").count().withColumn("percentage", F.col("count") / total_count * 100)
adherence_yes_counts.show()

# COMMAND ----------

final_filtered = final.select("beneID","state","anymed",'adherance',"adherence_yes")
final_filtered.show()

# COMMAND ----------

final_filtered.write.saveAsTable("dua_058828_spa240.paper_4_spd_both_outcomes_12_months_new2", mode='overwrite')

# COMMAND ----------

