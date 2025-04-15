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
# MAGIC PATIENT DEMOGRAPHICS

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_demo_file_01_12_months")
denom = denom.select("beneID","state", "sex", "age", "birthday")
print(denom.count())

# COMMAND ----------

denom.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Define the measurement year
measurement_year = 2018

# Calculate the age as of Dec 31 of the measurement year
denom = denom.withColumn("age", F.floor(F.datediff(F.lit(f"{measurement_year}-12-31"), F.col("birthday")) / 365.25))

# Filter the rows based on the criteria
eligible_population = denom.filter(
    (F.col("age").between(18, 100))
)

eligible_population.show()

# COMMAND ----------

print(eligible_population.count())

# COMMAND ----------

# MAGIC %md
# MAGIC EXCLUSIONS [ALREADY DONE]
# MAGIC
# MAGIC - DIED
# MAGIC - HOSPICE CARE

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY MEMBERS WITH AMI HOSPTIALIZATIONS: 
# MAGIC - MEASUREMENT PERIOD IS 2018: JULY 1, 2017-JUNE 30, 2018

# COMMAND ----------

inpat2017 = spark.table("dua_058828_spa240.paper_4_inpatient2017_12_months_new")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

# COMMAND ----------

inpat2017 = inpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT", "SRVC_END_DT","DGNS_CD_1","PRCDR_CD_1")
inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","SRVC_END_DT","DGNS_CD_1","PRCDR_CD_1")
inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","SRVC_END_DT","DGNS_CD_1","PRCDR_CD_1")

# Use reduce to apply union to all DataFrames in the list
hospital = inpat2017.union(inpat2018)

# COMMAND ----------

hospital.show()

# COMMAND ----------

from pyspark.sql.functions import when

# Define the list of myocardial infarction codes
mi_codes = [
    'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129', 'I213', 
    'I214', 'I219', 'I21A1', 'I21A9', 'I220', 'I221', 'I222', 'I228', 'I229'
]

# Add ami_yes column based on the condition
hospital = hospital.withColumn('ami_yes', when(col('DGNS_CD_1').isin(mi_codes), 1).otherwise(0))

# COMMAND ----------

values = hospital.groupBy("ami_yes").count()
values.show()

# COMMAND ----------

hospital = hospital.filter(col('ami_yes') == 1)
# Rename columns
hospital = hospital.withColumnRenamed('SRVC_BGN_DT', 'StartDate') \
       .withColumnRenamed('SRVC_END_DT', 'EndDate') 
hospital.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum as cumsum, when, row_number
from pyspark.sql.window import Window

def episodesOfCare(df):
    # Define window specifications for calculating lag values, cumulative sum, and row number
    beneID_state_window = Window.partitionBy("beneID", "state").orderBy("StartDate", "EndDate")
    beneID_state_window_cumsum = Window.partitionBy("beneID", "state").orderBy("StartDate", "EndDate").rowsBetween(Window.unboundedPreceding, 0)

    # Calculate lag values for StartDate and EndDate columns
    df = df.withColumn("prev_StartDate", lag("StartDate").over(beneID_state_window))
    df = df.withColumn("prev_EndDate", lag("EndDate").over(beneID_state_window))

    # Calculate row number within each group
    df = df.withColumn("row_num", row_number().over(beneID_state_window))

    # Define conditions for new episode and overlap types
    new_episode_condition = (col("StartDate") > col("prev_EndDate") + 1) | col("prev_EndDate").isNull()
    regular_overlap_condition = (col("StartDate") <= col("prev_EndDate") + 1) & (col("EndDate") > col("prev_EndDate"))
    same_start_date_condition = (col("StartDate") == col("prev_StartDate")) & (col("EndDate") < col("prev_EndDate"))
    embedded_condition = (col("StartDate") > col("prev_StartDate")) & (col("EndDate") < col("prev_EndDate"))
    perfect_overlap_condition = (col("StartDate") == col("prev_StartDate")) & (col("EndDate") == col("prev_EndDate"))

    # Assign new episode flag based on condition
    df = df.withColumn("new_episode_flag", new_episode_condition.cast("int"))

    # Calculate episode numbers using cumulative sum
    df = df.withColumn("episode", cumsum("new_episode_flag").over(beneID_state_window_cumsum))

    df = df.withColumn("ovlp", 
                   when(col("row_num") == 1, "1.First")
                   .when(new_episode_condition, "2.New Episode")
                   .when(regular_overlap_condition, "3.Regular Overlap")
                   .when(same_start_date_condition, "5.Same Start Date (SRO)")
                   .when(embedded_condition, "6.Embedded")
                   .when(perfect_overlap_condition, "7.Perfect Overlap"))

    # Drop unnecessary columns
    df = df.drop("prev_StartDate", "prev_EndDate", "new_episode_flag", "row_num")

    return df
  
# Convert 'StartDate' and 'EndDate' columns to date type
df = hospital.withColumn("StartDate", col("StartDate").cast("date"))
df = df.withColumn("EndDate", col("EndDate").cast("date"))

# Apply the episodesOfCare function
result_df = episodesOfCare(df)

# Sort the DataFrame by beneID, state, StartDate, and EndDate
#result_df = result_df.orderBy("beneID", "state", "StartDate", "EndDate")

# Show the result
result_df.show(200)

# COMMAND ----------

from pyspark.sql import functions as F

# Aggregating by beneID, state, episode
aggregated_df = result_df.groupBy("beneID", "state", "episode").agg(
    F.min("StartDate").alias("min_StartDate"),
    F.max("EndDate").alias("max_EndDate"),
    F.max("ami_yes").alias("ami")
)

aggregated_df.show()

# COMMAND ----------

aggregated_df = aggregated_df.withColumn("max_EndDate", col("max_EndDate").cast("date"))

# Filter rows where max_EndDate is between July 1, 2017, and June 30, 2018
aggregated_df = aggregated_df.filter((col('max_EndDate') >= '2017-07-01') & (col('max_EndDate') <= '2018-06-30'))

# Show the results to verify the filtering
aggregated_df.show(100)

# COMMAND ----------

print(aggregated_df.count())
unique_combinations_count = aggregated_df.select('beneID', 'state').distinct().count()
print("Total number of unique beneID, state combinations:", unique_combinations_count)

# COMMAND ----------

df = aggregated_df.withColumn("max_EndDate", col("max_EndDate").cast("date"))

# Define the window specification
window_spec = Window.partitionBy("beneID", "state").orderBy("max_EndDate")

# Add a row number column to identify the earliest date
df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the earliest date for each unique beneID, state combination
result_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

# Show the results to verify correct application
print(result_df.count())
result_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, date_add

# Add new columns 'index_date' and 'index_plus_180'
result_df = result_df.withColumn('index_date', col('max_EndDate')) \
                     .withColumn('index_plus_180', date_add(col('max_EndDate'), 180))

# Show the results to verify correct application
result_df.show()

# COMMAND ----------

result_df.write.saveAsTable("dua_058828_spa240.paper_9_amm_outcome_12_months", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY MEDICATIONS FOR AMI

# COMMAND ----------


import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#betos_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df = rx_df[rx_df['rxDcTherapeuticClass'].isin(['HMG-CoA Reductase Inhibitor',
'Angiotensin Converting Enzyme Inhibitor',
'Angiotensin 2 Receptor Blocker | Thiazide Diuretic',
'Angiotensin 2 Receptor Blocker',
'Beta-Adrenergic Blocker',
'Anti-coagulant',
'Antiarrhythmic',
'Antifibrinolytic Agent',
'Aldosterone Antagonist',
'Aldosterone Antagonist | Thiazide Diuretic',
'Calcium Channel Blocker',
'Loop Diuretic',
'Cardiac Glycoside',
'Potassium Binder',
'Platelet Aggregation Inhibitor',
'Potassium Channel Blocker',
'Alpha-1 Adrenergic Agonist',
'Dihydropyridine Calcium Channel Blocker',
'Dihydropyridine Calcium Channel Blocker | HMG-CoA Reductase Inhibitor',
'Angiotensin Converting Enzyme Inhibitor | Thiazide Diuretic',
'Nitrate Vasodilator',
'Potassium',
'Angiotensin Converting Enzyme Inhibitor | Calcium Channel Blocker',
'Low Molecular Weight Heparin',
'Angiotensin Converting Enzyme Inhibitor | Dihydropyridine Calcium Channel Blocker',
'Angiotensin 2 Receptor Blocker | Dihydropyridine Calcium Channel Blocker',
'Renin Inhibitor | Thiazide Diuretic',
'Angiotensin 2 Receptor Blocker | Dihydropyridine Calcium Channel Blocker | Thiazide Diuretic',
'Thiazide Diuretic | beta-Adrenergic Blocker',
'Dietary Cholesterol Absorption Inhibitor | HMG-CoA Reductase Inhibitor',
'Anti-coagulant | Calculi Dissolution Agent',
'Thiazide Diuretic',
'Thiazide-like Diuretic',
'Vasodilator',
'Potassium-sparing Diuretic | Thiazide Diuretic',
'Thiazide-like Diuretic | beta-Adrenergic Blocker',
'Potassium-sparing Diuretic',
'Renin Inhibitor'])]

rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
print(rx_df.count())
rx_df.show(1000)

# COMMAND ----------

# import pandas as pd
# rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
# #betos_df.head()
# rx_df = spark.createDataFrame(rx_df)
# rx_df = rx_df[rx_df['rxDcDrugName'].isin(['carvedilol','carvedilol [Coreg]','labetalol','labetalol [Trandate]','nadolol','nadolol [Corgard]','pindolol','propranolol','propranolol [Hemangeol]','propranolol [Inderal]','propranolol [InnoPran]','timolol','timolol [Betimol]','timolol [Istalol]','timolol [Timoptic]','sotalol','sotalol [Betapace]','sotalol [Sotylize]','acebutolol','atenolol','atenolol [Tenormin]','atenolol | chlorthalidone','atenolol | chlorthalidone [Tenoretic]','betaxolol','betaxolol [Betoptic S]','bisoprolol','bisoprolol | hydrochlorothiazide','bisoprolol | hydrochlorothiazide [Ziac]','metoprolol','metoprolol [Kapspargo]','metoprolol [Lopressor]','metoprolol [Toprol]','nebivolol','nebivolol [Bystolic]','atenolol | chlorthalidone','atenolol | chlorthalidone [Tenoretic]','bendroflumethiazide | nadolol','bisoprolol | hydrochlorothiazide','bisoprolol | hydrochlorothiazide [Ziac]','hydrochlorothiazide | metoprolol','hydrochlorothiazide | metoprolol [Dutoprol]','hydrochlorothiazide | metoprolol [Lopressor HCT]','hydrochlorothiazide | metoprolol tartrate'])]
# rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
# rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
# print(rx_df.count())
# rx_df.show(1000)

# COMMAND ----------

pharm2017 = spark.table("dua_058828_spa240.paper_4_pharm2017_12_months")
pharm2018 = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
pharm = pharm2018.union(pharm2017)
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

from pyspark.sql.functions import col, substring

ami_meds = pharm.join(
    rx_df,
    on='ndc',
    how='inner'
)

print(pharm.count())
print(ami_meds.count())
ami_meds.show()

# COMMAND ----------

# MAGIC %md
# MAGIC FIND MEDS IN 180 day window

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, lit, date_sub

denom = spark.table("dua_058828_spa240.paper_9_amm_outcome_12_months")

# Add new column with the min date
denom = denom.withColumn("index_date2", col("min_StartDate"))

# Subtract 45 days from StartDate and create new column index_date3
denom = denom.withColumn("index_date3", date_sub(col("min_StartDate"), 45))

# Show result
denom.show()

# COMMAND ----------

ami_meds = ami_meds.join(denom, on=["beneID","state"], how="inner")
ami_meds.show()

# COMMAND ----------

# Show the results to verify correct application
ami_meds =ami_meds.drop("ndc", "ami")
#dep_meds.show()

# COMMAND ----------

# Filter the DataFrame where rx_fill_dt is less than index_date_plus_84
ami_meds = ami_meds.filter((col('RX_FILL_DT') < col('index_plus_180')) & (col('index_date') <= col('RX_FILL_DT')))
ami_meds = ami_meds.orderBy(['beneID', 'state', 'RX_FILL_DT'], ascending=[True, True, True])

# Show the results to verify correct application
ami_meds.show(150)

# COMMAND ----------

distinct = ami_meds.select("beneID","state", "RX_FILL_DT", "DAYS_SUPPLY").distinct()
#distinct = ami_meds.select("beneID","state", "RX_FILL_DT", "DAYS_SUPPLY")

# COMMAND ----------

# Sum days_supply by beneID and state
from pyspark.sql.functions import col, date_add, sum as spark_sum
distinct_agg = distinct.groupBy('beneID', 'state').agg(spark_sum('DAYS_SUPPLY').alias('outcome1'))

# COMMAND ----------

# Add outcome1_yes column based on the condition
distinct_agg = distinct_agg.withColumn('outcome1_yes', when(col('outcome1') >= 135, 1).otherwise(0))
distinct_agg.show()

# COMMAND ----------

denom = denom.join(distinct_agg, on=["beneID","state"], how="left").fillna(0)
print(denom.count())

# COMMAND ----------

denom.show()

# COMMAND ----------

value_outcome1 = denom.groupBy("outcome1_yes").count()
value_outcome1.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("outcome1_yes") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC quick checks

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

# Calculate min and max dates
result = denom.agg(min("index_date").alias("min_date"), max("index_date").alias("max_date"))

# Show result
result.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

# Calculate min and max dates
result = denom.agg(min("index_plus_180").alias("min_date"), max("index_plus_180").alias("max_date"))

# Show result
result.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

# Calculate min and max dates
result = pharm.agg(min("RX_FILL_DT").alias("min_date"), max("RX_FILL_DT").alias("max_date"))

# Show result
result.show()

# COMMAND ----------

denom.show(200)

# COMMAND ----------

# MAGIC %md
# MAGIC save file

# COMMAND ----------

denom.show()

# COMMAND ----------

denom = denom.drop("episode", "ami", "index_date2", "index_date3")
denom.show()

# COMMAND ----------

denom.write.saveAsTable("dua_058828_spa240.paper_4_pbh_with_outcomes_new1", mode='overwrite')