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
# MAGIC Pregnancy

# COMMAND ----------

outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

# COMMAND ----------

outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2019 = outpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")

inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")

inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2019 = inpat2019.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

# COMMAND ----------

from functools import reduce
# Use reduce to apply union to all DataFrames in the list
all_claims = outpat2018.union(outpat2019).union(inpat2018).union(inpat2019)

# Show the result
all_claims.show()

# COMMAND ----------

# MAGIC %md
# MAGIC identify pregnancies

# COMMAND ----------

codes = [
    'O80', 'Z370', 'Z372', 'Z373', 'Z3750', 'Z3751', 'Z3752', 'Z3753', 
    'Z3754', 'Z3759', 'Z3760', 'Z3761', 'Z3762', 'Z3763', 'Z3764', 'Z3769', 
    'Z3800', 'Z3801', 'Z381', 'Z382', 'Z3830', 'Z3831', 'Z384', 'Z385', 
    'Z3861', 'Z3862', 'Z3863', 'Z3864', 'Z3865', 'Z3866', 'Z3868', 'Z3869', 
    'Z387', 'Z388', '59400', '59612', '59409', '59612'
]

# Filter the DataFrame
all_claims = all_claims.filter(all_claims.LINE_PRCDR_CD.isin(codes) | all_claims.DGNS_CD_1.isin(codes))

# Show the result
all_claims.show(500)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Define the date range
start_date = "2018-10-08"
end_date = "2019-10-07"

# Filter the DataFrame
filtered_df = all_claims.filter((col("SRVC_BGN_DT") >= start_date) & (col("SRVC_BGN_DT") <= end_date))

# Count rows outside the date range
outside_date_range_count = all_claims.filter((col("SRVC_BGN_DT") < start_date) | (col("SRVC_BGN_DT") > end_date)).count()

# Show the count of rows outside the date range
print(f"Number of rows not falling between {start_date} and {end_date}: {outside_date_range_count}")

# Count rows outside the date range
outside_date_range_count = filtered_df.filter((col("SRVC_BGN_DT") < start_date) | (col("SRVC_BGN_DT") > end_date)).count()

# Show the count of rows outside the date range
print(f"Number of rows not falling between {start_date} and {end_date}: {outside_date_range_count}")

# Show the filtered DataFrame
filtered_df.show()

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# Define a window specification, partitioned by beneID and state, ordered by srvc_bgn_dt
window_spec = Window.partitionBy("beneID", "state").orderBy("srvc_bgn_dt")

# Add a row number column based on the window specification
filtered_df_with_row_num = filtered_df.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the rows with row_num = 1 (earliest srvc_bgn_dt for each beneID and state)
earliest_records_df = filtered_df_with_row_num.filter(col("row_num") == 1).drop("row_num")

# Show the result
earliest_records_df.show()

# COMMAND ----------

from pyspark.sql.functions import count

# Group by beneID and state, and count the number of rows in each group in the earliest_records_df
grouped_df = earliest_records_df.groupBy("beneID", "state").agg(count("*").alias("row_count"))

# Filter to find combinations with more than one row
multiple_rows_df = grouped_df.filter(col("row_count") > 1)

# Count the number of such combinations
multiple_rows_count = multiple_rows_df.count()

# Show the result
print(f"Number of beneID, state combinations with more than one row: {multiple_rows_count}")

# Optionally, show the combinations with more than one row
multiple_rows_df.show()

# COMMAND ----------

print(earliest_records_df.count())

# COMMAND ----------

sample = spark.table("dua_058828_spa240.paper_4_demo_file_01_12_months")
sample = sample.select("beneID","state", "sex", "age", "birthday")

print(earliest_records_df.count())
earliest_records_df = sample.join(earliest_records_df, on=["beneID","state"], how="inner")
print(earliest_records_df.count())
earliest_records_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC prenatal visists

# COMMAND ----------

from functools import reduce

outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2019 = outpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")

inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")

inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2019 = inpat2019.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

# Use reduce to apply union to all DataFrames in the list
all_claims = outpat2018.union(outpat2019).union(inpat2018).union(inpat2019)

# Show the result
all_claims.show()

# COMMAND ----------

# Define the list of codes for prenatal visits
prenatal_codes = [
    '99500', '0500F', '05001F', '05002F', 
    'H1000', 'H1001', 'H1002', 'H1003', 'H1004'
]

# Add the prenatal column based on the condition
all_claims = all_claims.withColumn(
    "prenatal1", 
    when(col("LINE_PRCDR_CD").isin(prenatal_codes), 1).otherwise(0)
)

# Show the result

# Count values for prenatal1
prenatal1_counts = all_claims.groupBy("prenatal1").count()
prenatal1_counts.show()

all_claims.show()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Define the list of codes for prenatal visits and prenatal bundled services
prenatal_codes = [
    '99500', '0500F', '05001F', '05002F', 
    'H1000', 'H1001', 'H1002', 'H1003', 'H1004',
    '59400', '59425', '59426', '59510', '59610', '59618', 'H1005'
]

# Add the prenatal column based on the condition
all_claims = all_claims.withColumn(
    "prenatal2", 
    when(col("LINE_PRCDR_CD").isin(prenatal_codes), 1).otherwise(0)
)

# Count values for prenatal2
prenatal2_counts = all_claims.groupBy("prenatal2").count()
prenatal2_counts.show()

# Show the result
all_claims.show()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Define the list of CPT and HCPCS codes for office visits, telephone visits, and e-visits/virtual check-ins
office_visit_codes = [
    '99201', '99202', '99203', '99204', '99205', '99211', '99212', '99213', '99214', '99215',
    '99241', '99242', '99243', '99244', '99245'
]

# Define the list of pregnancy-related ICD-10 codes, removing dots
pregnancy_related_icd10_codes = [
    'O0900', 'O0901', 'O0902', 'O0903',  # O09.00–O09.03
    'O0910', 'O0911', 'O0912', 'O0913',  # O09.10–O09.13
    'O09211', 'O09212', 'O09213'         # O09.211–O09.213
]

all_claims = all_claims.withColumn(
    "prenatal3",
    when(
        (col("DGNS_CD_1").isin(pregnancy_related_icd10_codes)),
        1
    ).otherwise(0)
)

# Count values for prenatal2
prenatal3_counts = all_claims.groupBy("prenatal3").count()
prenatal3_counts.show()

# Show the result
all_claims.show()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Define the list of CPT and HCPCS codes for office visits, telephone visits, and e-visits/virtual check-ins
office_visit_codes = [
    '99201', '99202', '99203', '99204', '99205', '99211', '99212', '99213', '99214', '99215',
    '99241', '99242', '99243', '99244', '99245'
]

# Define the list of pregnancy-related ICD-10 codes, removing dots
pregnancy_related_icd10_codes = [
    'O0900', 'O0901', 'O0902', 'O0903',  # O09.00–O09.03
    'O0910', 'O0911', 'O0912', 'O0913',  # O09.10–O09.13
    'O09211', 'O09212', 'O09213'         # O09.211–O09.213
]

all_claims = all_claims.withColumn(
    "prenatal3",
    when(
        (col("LINE_PRCDR_CD").isin(office_visit_codes)) | 
        (col("DGNS_CD_1").isin(pregnancy_related_icd10_codes)),
        1
    ).otherwise(0)
)

# Count values for prenatal2
prenatal3_counts = all_claims.groupBy("prenatal3").count()
prenatal3_counts.show()

# Show the result
all_claims.show()

# COMMAND ----------

# Drop rows where prenatal1, prenatal2, and prenatal3 all equal to 0
prenatal = all_claims.filter(~((col("prenatal1") == 0) & (col("prenatal2") == 0) & (col("prenatal3") == 0)))

# Show the result
print(prenatal.count())
prenatal.show()

# COMMAND ----------

# Count values for prenatal1
prenatal1_counts = prenatal.groupBy("prenatal1").count()
prenatal1_counts.show()

# Count values for prenatal2
prenatal2_counts = prenatal.groupBy("prenatal2").count()
prenatal2_counts.show()

# Count values for prenatal3
prenatal3_counts = prenatal.groupBy("prenatal3").count()
prenatal3_counts.show()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Create the new column 'prenatal' based on the conditions
prenatal = prenatal.withColumn(
    "prenatal", 
    when((col("prenatal1") == 1) | (col("prenatal2") == 1) | (col("prenatal3") == 1), 1).otherwise(0)
)

# Count values for prenatal3
prenatal4_counts = prenatal.groupBy("prenatal").count()
prenatal4_counts.show()

prenatal = prenatal.drop("prenatal1", "prenatal2", "prenatal3")


# Show the result
prenatal.show()

# COMMAND ----------

earliest_records_df.show()

# COMMAND ----------

# Drop the columns prenatal1, prenatal2, and prenatal3
earliest_records_df = earliest_records_df.drop("DGNS_CD_1", "LINE_PRCDR_CD", "CLM_ID")
earliest_records_df = earliest_records_df.withColumnRenamed("SRVC_BGN_DT", "index_date")
earliest_records_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff

# Perform a left join of df2 with df1 on beneID and state
df_combined = prenatal.join(earliest_records_df, ["beneID", "state"], how="inner")

# Show the result
df_combined.show()

# COMMAND ----------

# Filter visits that occurred between 280 and 176 days before index_date
filtered_visits = df_combined.filter(
    (datediff(col("index_date"), col("SRVC_BGN_DT")) >= 176) &
    (datediff(col("index_date"), col("SRVC_BGN_DT")) <= 280)
)

# Show the result
filtered_visits = filtered_visits.withColumn("date_difference", datediff(col("index_date"), col("SRVC_BGN_DT")))
filtered_visits.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC START SENSITIVITY ANALYSIS  

# COMMAND ----------

from pyspark.sql import functions as F

# Assuming your DataFrame is named df
filtered_visits = filtered_visits.withColumn(
    "semester", 
    F.when(F.month(F.col("SRVC_BGN_DT")).between(1, 6), 1).otherwise(2)
)

from pyspark.sql import functions as F

# Perform value count for the 'semester' column
semester_count = filtered_visits.groupBy("semester").count()

# Count the total number of rows in the dataset
total_count = filtered_visits.count()

# Calculate the percentage for each semester value
semester_percentage = semester_count.withColumn(
    "percentage", (F.col("count") / total_count) * 100
)

# Show the result with count and percentage
semester_percentage.show()

# COMMAND ----------

# MAGIC %md
# MAGIC END SENSITIVITY ANALYSIS

# COMMAND ----------

# Select distinct rows based on beneID, state, and prenatal
prenatal_final = filtered_visits.select("beneID", "state", "prenatal").distinct()

# Show the result
prenatal_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Postpartum

# COMMAND ----------

from functools import reduce

outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2019 = outpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")

inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")

inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2019 = inpat2019.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

# Use reduce to apply union to all DataFrames in the list
all_claims = outpat2018.union(outpat2019).union(inpat2018).union(inpat2019)

# Show the result
all_claims.show()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Define the list of CPT and HCPCS codes for postpartum visits
postpartum_cpt_codes = [
    '57170', '58300', '59430', '99501', '0503F', '59400', '59410', '59510', 
    '59515', '59610', '59614', '59618', '59622'
]

postpartum_hcpcs_codes = ['G0101']

# Define the list of ICD-10 codes for postpartum visits, removing dots
postpartum_icd10_codes = [
    'Z01411', 'Z01419', 'Z0142', 'Z30430', 'Z391', 'Z392'
]

# Define the list of CPT and HCPCS codes for cervical cytology
cervical_cytology_cpt_codes = [
    '88141', '88142', '88143', '88147', '88148', '88150', '88152', '88153', 
    '88164', '88165', '88166', '88167', '88174', '88175'
]

cervical_cytology_hcpcs_codes = [
    'G0123', 'G0124', 'G0141', 'G0143', 'G0144', 'G0145', 'G0147', 'G0148', 
    'P3000', 'P3001', 'Q0091'
]

# Combine all the codes into one list for easy checking
all_postpartum_codes = postpartum_cpt_codes + postpartum_hcpcs_codes + cervical_cytology_cpt_codes + cervical_cytology_hcpcs_codes

# Add the postpartum column based on the conditions
all_claims = all_claims.withColumn(
    "postpartum",
    when(
        (col("LINE_PRCDR_CD").isin(all_postpartum_codes)) | 
        (col("DGNS_CD_1").isin(postpartum_icd10_codes)),
        1
    ).otherwise(0)
)

# Show the result
all_claims.show()

# COMMAND ----------

# Count values for prenatal3
postpartum_counts = all_claims.groupBy("postpartum").count()
postpartum_counts.show()

# COMMAND ----------

# Drop rows where postpartum is 0
postpartum = all_claims.filter(col("postpartum") != 0)

# Count values for prenatal3
postpartum_counts = postpartum.groupBy("postpartum").count()
postpartum_counts.show()

# Show the result
postpartum.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff

# Perform a left join of df2 with df1 on beneID and state
df_combined = postpartum.join(earliest_records_df, ["beneID", "state"], how="inner")

# Show the result
df_combined.show()

# COMMAND ----------

# Add a new column with the date difference in days
df_combined = df_combined.withColumn("date_difference", datediff(col("SRVC_BGN_DT"), col("index_date")))

# Filter visits that occurred between 7 and 84 days after index_date
filtered_visits = df_combined.filter(
    (col("date_difference") >= 7) &
    (col("date_difference") <= 84)
)

filtered_visits.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC start sensitivity analysis

# COMMAND ----------

from pyspark.sql import functions as F

# Assuming your DataFrame is named df
filtered_visits = filtered_visits.withColumn(
    "semester", 
    F.when(F.month(F.col("SRVC_BGN_DT")).between(1, 6), 1).otherwise(2)
)

from pyspark.sql import functions as F

# Perform value count for the 'semester' column
semester_count = filtered_visits.groupBy("semester").count()

# Count the total number of rows in the dataset
total_count = filtered_visits.count()

# Calculate the percentage for each semester value
semester_percentage = semester_count.withColumn(
    "percentage", (F.col("count") / total_count) * 100
)

# Show the result with count and percentage
semester_percentage.show()

# COMMAND ----------

# MAGIC %md
# MAGIC end sensitivity analysis

# COMMAND ----------

# Select distinct rows based on beneID, state, and prenatal
postpartum_final = filtered_visits.select("beneID", "state", "postpartum").distinct()

# Show the result
postpartum_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC COMBINE

# COMMAND ----------

final = earliest_records_df.join(prenatal_final, on=['beneID','state'], how='left').fillna(0)
final.show(100)

# COMMAND ----------

final = final.join(postpartum_final, on=['beneID','state'], how='left').fillna(0)
final.show()

# COMMAND ----------

print(final.count())

#160276

# COMMAND ----------

final1 = final.select('beneID','state','prenatal','postpartum')
final1.show()

# COMMAND ----------

denom = final1

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("prenatal") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("postpartum") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC final data

# COMMAND ----------

final_data = spark.table("dua_058828_spa240.paper_4_demo_file_01_12_months")
final_data = final_data.select("beneID","state")
final_data = final_data.join(final1, on=["beneID","state"], how="inner")
print(final_data.count())

#160276
#160276

# COMMAND ----------

final_data.write.saveAsTable("dua_058828_spa240.paper_4_post_partum_care_12_months_new", mode='overwrite')

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_post_partum_care_12_months_new")
print(denom.count())

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("prenatal") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("postpartum") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

