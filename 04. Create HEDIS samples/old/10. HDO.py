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
# MAGIC EXCLUSIONS
# MAGIC
# MAGIC - CANCER 
# MAGIC - SICKLE CELL
# MAGIC - DIED DURING MEASUREMENT PERIOD

# COMMAND ----------

outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")
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

cancer_codes = [
    "C000", "C001", "C002", "C003", "C004", "C005", "C006", "C008", "C009", 
    "C010", "C020", "C021", "C022", "C023", "C024", "C028", "C029", "C030", 
    "C031", "C039", "C040", "C041", "C048", "C049", "C050", "C051", "C052", 
    "C058", "C059", "C060", "C061", "C062", "C0680", "C0689", "C069", "C070", 
    "C080", "C081", "C089", "C090", "C091", "C098", "C099", "C100", "C101", 
    "C102", "C103", "C104", "C108", "C109", "C110", "C111", "C112", "C113", 
    "C118", "C119", "C120", "C130", "C131", "C132", "C138", "C139", "C140", 
    "C142", "C148", "C153", "C154", "C155", "C158", "C159", "C160", "C161", 
    "C162", "C163", "C164", "C165", "C166", "C168", "C169", "C170", "C171", 
    "C172", "C178", "C179", "C180", "C181", "C182", "C183", "C184", "C185", 
    "C186", "C187", "C188", "C189", "C190", "C200", "C210", "C920", "C9290", 
    "C9392", "C93Z0", "C93Z1", "C93Z2", "C9400", "C9401", "C9402", "C9420", 
    "C9421", "C9422", "C9430", "C9431", "C9432", "C9440", "C9441", "C9442", 
    "C9460", "C9480", "C9481", "C9482", "C9500", "C9501", "C9502", "C9510", 
    "C9511", "C9512", "C9590", "C9591", "C9592", "C960", "C962", "C9620", 
    "C9621", "C9622", "C9629", "C964", "C965", "C966", "C969", "C96A", "C96Z"
]

other_neoplasm_codes = [
    "D0000", "D0001", "D0008", "D001", "D002", "D010", "D011", "D012", 
    "D013", "D0140", "D0149", "D015", "D017", "D019", "D020", "D021", 
    "D0220", "D0221", "D0222", "D023", "D024", "D030", "D0310", "D0311", 
    "D03111", "D03112", "D03121", "D03122", "D0320", "D0321", "D0322", 
    "D0330", "D0339", "D034", "D0351", "D0352", "D0359", "D0360", "D0361", 
    "D0362", "D0370", "D0371", "D0372", "D038", "D039", "D040", "D0410", 
    "D0411", "D04111", "D04112", "D0412", "D04121", "D04122", "D0420", 
    "D0421", "D0422", "D0430", "D0439", "D044", "D045", "D0460", "D0461", 
    "D0462", "D0470", "D0471", "D0472", "D048", "D049", "D0500", "D0501", 
    "D0502", "D0510", "D0511", "D0512", "D0580", "D0581", "D0582", "D0590", 
    "D0591", "D0592", "D060", "D061", "D067", "D069", "D070", "D071", 
    "D072", "D0730", "D0739", "D074", "D075", "D0760", "D0761", "D0769", 
    "D090", "D0910", "D0919", "D0920", "D0921", "D0922", "D093", "D098", 
    "D099", "D3701", "D3702", "D37030", "D37031", "D37032", "D37039", 
    "D3704", "D3705", "D3709", "D371", "D376", "D378", "D379", "D380", 
    "D386", "D390", "D3910", "D3911", "D3912", "D392", "D398", "D399", 
    "D400", "D4010", "D4011", "D4012", "D408", "D409", "D4100", "D4101", 
    "D4102", "D4110", "D4111", "D4112", "D413", "D414", "D418", "D419", 
    "D420", "D421", "D429", "D430", "D434", "D438", "D439", "D440", 
    "D4410", "D4411", "D4412", "D442", "D443", "D447", "D449", "D450", 
    "D461", "D4620", "D4621", "D4622", "D464", "D469", "D46A", "D46B", 
    "D46C", "D46Z", "D470", "D4701", "D4702", "D4709", "D471", "D472", 
    "D474", "D479", "D47Z1", "D47Z2", "D47Z9", "D480", "D481", "D482", 
    "D483", "D484", "D485", "D4860", "D4861", "D4862", "D487", "D489", 
    "D490", "D491", "D492", "D493", "D494", "D495", "D49511", "D49512", 
    "D49519", "D4959", "D496", "D497", "D4981", "D4989", "D499"
]

history_of_malignant_neoplasm_codes = [
    "Z8500", "Z8501", "Z85020"
]

other_malignant_neoplasm_of_skin_codes = [
    "C4400", "C4401", "C4402"
]

sickle_cell = ["D570", "D5700", "D5701", "D5702", "D571", "D5710", "D5711", "D5712", "D572", "D5720", "D5721", "D574", "D5740", "D5741", "D578", "D5780", "D5781"]

all_codes = cancer_codes + other_neoplasm_codes + history_of_malignant_neoplasm_codes + other_malignant_neoplasm_of_skin_codes + sickle_cell

# Filter the DataFrame
all_claims = all_claims.withColumn("exclude", when(col("DGNS_CD_1").isin(all_codes), 1).otherwise(0))

# Show the result
all_claims.show(500)

# COMMAND ----------

# Count occurrences of each value in the dgns_cd column
value_counts = all_claims.groupBy("exclude").count()

# Show the result
value_counts.show()

# COMMAND ----------

# Filter rows where exclude is not 0
filtered_claims = all_claims.filter(all_claims.exclude != 0)

# Select distinct values for beneID, state, and exclude
distinct_claims = filtered_claims.select("beneID", "state", "exclude").distinct()

# Show the result
print(distinct_claims.count())
distinct_claims.show()

# COMMAND ----------

eligible_population = eligible_population.join(distinct_claims, on=['beneID','state'] ,how='left').fillna(0)
eligible_population.show()

# COMMAND ----------

print(eligible_population.count())
eligible_population = eligible_population.filter("exclude != 1")
print(eligible_population.count())

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY MEMBERS WITH AT LEAST TWO OPIIOID MEDS ON DIFFERENT DAYS

# COMMAND ----------

import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#rx_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df.show()

# COMMAND ----------

import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#betos_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df = rx_df[rx_df['rxDcDrugName'].isin(['butorphanol',
'codeine',
'codeine | phenylephrine | promethazine',
'codeine | promethazine',
'fentanyl',
'fentanyl [Abstral]',
'fentanyl [Actiq]',
'fentanyl [Duragesic]',
'fentanyl [Fentora]',
'fentanyl [Lazanda]',
'fentanyl [Subsys]',
'hydrocodone',
'hydrocodone [Hysingla]',
'hydrocodone [Zohydro]', 
'hydrocodone | ibuprofen',
'hydrocodone | ibuprofen [Vicoprofen]',
'hydromorphone',
'hydromorphone [Dilaudid]',
'hydromorphone [Exalgo]',
'levorphanol',
'meperidine',
'meperidine [Demerol]',
'methadone',
'methadone [Diskets]',
'methadone [Dolophine]',
'methadone [Methadose]',
'morphine',
'morphine [Duramorph]',
'morphine [Infumorph]',
'morphine [Kadian]',
'morphine [MS Contin]',
'morphine | naltrexone [Embeda]',
'opium',
'oxycodone',
'oxycodone [Oxaydo]',
'oxycodone [Oxycontin]',
'oxycodone [Roxicodone]',
'oxycodone [Xtampza]',
'oxymorphone',
'oxymorphone [Opana]',
'tapentadol [Nucynta]',
'tramadol',
'tramadol [ConZip]',
'tramadol [Qdolo]',
'tramadol [Ultram]'])]

rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
print(rx_df.count())
rx_df.show(1000)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad

# Pad the values with zeros to ensure they are 11 digits long
rx_df = rx_df.withColumn("ndc", lpad(rx_df["ndc"], 11, "0"))

# Show the results
rx_df.show(1000)

# COMMAND ----------

# import pandas as pd
# rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
# #betos_df.head()
# rx_df = spark.createDataFrame(rx_df)
# rx_df = rx_df[rx_df['rxDcTherapeuticClass'].isin(['Anilides | Opioid Agonist',
# 'Opioid Agonist',
# 'Partial Opioid Agonist',
# 'Opioid Antagonist | Partial Opioid Agonist',
# 'Opioid Agonist Antagonist',
# 'Cholinergic Muscarinic Antagonist | Opioid Agonist',
# 'Opioid Antagonist | Partial Opioid Agonist Antagonist',
# 'Nonsteroidal Anti-inflammatory Drug | Opioid Agonist | Platelet Aggregation Inhibitor',
# 'Kappa Opioid Receptor Agonists',
# 'Opioid Agonist | Opioid Antagonist',
# 'Mu-Opioid Receptor Agonist'])]
# rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
# rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
# print(rx_df.count())
# rx_df.show(1000)

# COMMAND ----------

pharm2018 = spark.table("dua_058828_spa240.pharm2018_HEDIS_PAPER")
# Renaming columns
pharm2018 = pharm2018.withColumnRenamed("BENE_ID", "beneID") \
               .withColumnRenamed("STATE_CD", "state") 
pharm2018 = pharm2018.join(eligible_population, on=["beneID","state"], how="inner")
pharm2018.show()

# COMMAND ----------

pharm2018 = pharm2018.join(eligible_population, on=["beneID","state"], how="inner")

# COMMAND ----------

pharm2018a = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
print(pharm2018.count())
print(pharm2018a.count())

# COMMAND ----------

pharm = pharm2018
pharm = pharm.select("beneID", "state", "RX_FILL_DT", "DAYS_SUPPLY", "NDC", "NDC_UOM_CD", "NDC_QTY", "MTRC_DCML_QTY", "NDC_QTY_ALOWD")
#pharm.show()

# COMMAND ----------

# MAGIC %md
# MAGIC - MERGE MEDICARE DATA FOR MEDICATION
# MAGIC - DO MISSING CHECK

# COMMAND ----------

table_name = f"extracts.pdch2020.drug_char_2020_extract"
ndc2020 = spark.table(table_name).select("ndc", "str").distinct()
ndc2020.show()

table_name = f"extracts.pdch2019.drug_char_2019_extract"
ndc2019 = spark.table(table_name).select("ndc", "str").distinct()
ndc2019.show()

table_name = f"extracts.pdch2018.drug_char_2018_extract"
ndc2018 = spark.table(table_name).select("ndc", "str").distinct()
ndc2018.show()

table_name = f"extracts.pdch2017.drug_char_2017_extract"
ndc2017 = spark.table(table_name).select("ndc", "str").distinct()
ndc2017.show()

ndc = ndc2020.union(ndc2019).union(ndc2018).union(ndc2017)
ndc.show()

# COMMAND ----------

# Remove duplicate NDC values
ndc_unique = ndc.dropDuplicates(["ndc"])

# Show the results
ndc_unique.show()

# COMMAND ----------

from pyspark.sql.functions import split, regexp_extract, col

# Split the column into two parts
# Extract the numeric part
ndc_unique = ndc_unique.withColumn("number", regexp_extract(col("str"), r'(\d+)', 1).cast("int")) \
             .withColumn("unit", regexp_extract(col("str"), r'([A-Za-z]+)', 1))


# Show the results
ndc_unique.show(200)

# COMMAND ----------

pharm = pharm.join(ndc_unique, on="ndc", how="left")
pharm.show()

# COMMAND ----------

# Specify the column to check
column_to_check = "str"

df =  pharm

total_rows = pharm.count()

# Calculate the percentage of null values for the specified column
null_percentage = df.select((col(column_to_check).isNull().cast("int").alias(column_to_check)))
null_percentage = null_percentage.agg((100 * sum(col(column_to_check)) / total_rows).alias(column_to_check))

# Show the percentage of null values for the specified column
null_percentage.show()

# COMMAND ----------

opioid_meds = pharm.join(rx_df, on="ndc", how="inner")
print(opioid_meds.count())
opioid_meds.show()

# COMMAND ----------

# Specify the column to check
column_to_check = "str"

df =  opioid_meds

total_rows = opioid_meds.count()

# Calculate the percentage of null values for the specified column
null_percentage = df.select((col(column_to_check).isNull().cast("int").alias(column_to_check)))
null_percentage = null_percentage.agg((100 * sum(col(column_to_check)) / total_rows).alias(column_to_check))

# Show the percentage of null values for the specified column
null_percentage.show()

# COMMAND ----------

# Count occurrences of each value in the dgns_cd column
value_counts = opioid_meds.groupBy("unit").count()

# Show the result
value_counts.show()

# COMMAND ----------

# Filter the DataFrame to keep only rows where NDC_UOM_CD is in ["ME", "MG", "GR"]
filtered_df = opioid_meds.filter(col('unit').isin(["MCG", "MG"]))

# Count occurrences of each value in the dgns_cd column
value_counts = filtered_df.groupBy("unit").count()

# Show the result
value_counts.show()

# COMMAND ----------

# Specify the column to check
column_to_check = "str"

df =  filtered_df

total_rows = filtered_df.count()

# Calculate the percentage of null values for the specified column
null_percentage = df.select((col(column_to_check).isNull().cast("int").alias(column_to_check)))
null_percentage = null_percentage.agg((100 * sum(col(column_to_check)) / total_rows).alias(column_to_check))

# Show the percentage of null values for the specified column
null_percentage.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ADD CONVERSION CODESE

# COMMAND ----------

# List unique values in column "rxDCDrugName"

unique_values = filtered_df.select("rxDCDrugName").distinct().orderBy("rxDCDrugName")

# Collect unique values into a list
unique_values_list = unique_values.rdd.map(lambda row: row[0]).collect()

print(unique_values_list)

# COMMAND ----------

opioid_conversion_factors = {
    "butorphanol": 7,
    "codeine": 0.15,
    "codeine | phenylephrine | promethazine": 0.15,
    "codeine | promethazine": 0.15,
    "fentanyl": 0.13,
    "fentanyl [Duragesic]": 2.4,
    "fentanyl [Fentora]": 0.13,
    "fentanyl [Subsys]": 0.36,
    "hydrocodone [Hysingla]": 1,
    "hydrocodone [Zohydro]": 1,
    "hydrocodone | ibuprofen": 1,
    "hydromorphone": 1,
    "'hydromorphone [Dilaudid]": 1,
    "'hydromorphone [Exalgo]": 1,
    "levorphanol": 11,
    "meperidine": 0.1,
    "meperidine [Demerol]": 0.1,
    "methadone":1,
    "methadone [Methadose]":1,
    "morphine": 1,
    "morphine [Duramorph]": 1,
    "morphine [Infumorph]": 1,
    "morphine [Kadian]": 1,
    "morphine [MS Contin]": 1,
    "morphine | naltrexone [Embeda]": 1, 
    "opium": 1,
    'oxycodone':1.5 , 
    'oxycodone [Oxaydo]':1.5 , 
    'oxycodone [Oxycontin]':1.5, 
    'oxycodone [Roxicodone]':1.5, 
    'oxycodone [Xtampza]':1.5, 
    'oxymorphone':3, 
    'oxymorphone [Opana]':3, 
    'tapentadol [Nucynta]':0.4, 
    'tramadol':0.1 , 
    'tramadol [ConZip]':0.1 , 
    'tramadol [Ultram]': 0.1
}


# COMMAND ----------

# Convert the dictionary to a broadcast variable
broadcast_opioid_conversion_factors = spark.sparkContext.broadcast(opioid_conversion_factors)

# Function to get the conversion factor
def get_conversion_factor(opioid):
    return broadcast_opioid_conversion_factors.value.get(opioid, None)

# Register the function as a UDF
from pyspark.sql.functions import udf
get_conversion_factor_udf = udf(get_conversion_factor, StringType())

# Add the conversion_factor column
filtered_df = filtered_df.withColumn("conversion_factor", get_conversion_factor_udf(col("rxDCDrugName")))

filtered_df.show()

# COMMAND ----------

# Specify the column to check
column_to_check = "conversion_factor"

df =  filtered_df

total_rows = filtered_df.count()

# Calculate the percentage of null values for the specified column
null_percentage = df.select((col(column_to_check).isNull().cast("int").alias(column_to_check)))
null_percentage = null_percentage.agg((100 * sum(col(column_to_check)) / total_rows).alias(column_to_check))

# Show the percentage of null values for the specified column
null_percentage.show()

# COMMAND ----------



# COMMAND ----------

filtered_df.write.saveAsTable("dua_058828_spa240.paper_10_hdo_rx_dpisode_12_months_new2", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC - ONLY KEEP PEOPLE WITH 2 RX ON DIFF DAYS
# MAGIC - AT LEAST 15 DAYS SUPPLY

# COMMAND ----------

filtered_df = spark.table("dua_058828_spa240.paper_10_hdo_total_rx_12_months_new2")
filtered_df.show()

# COMMAND ----------

filtered_df.registerTempTable("connections")

agg_med = spark.sql('''

SELECT DISTINCT beneID, state, RX_FILL_DT, NDC, DAYS_SUPPLY

FROM connections;
''')

agg_med = agg_med.filter(col("NDC").isNotNull())
agg_med.show(200)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

# Group by beneID, state and perform the aggregations
result_df = agg_med.groupBy('beneID', 'state') \
                   .agg(
                        count('*').alias('total_rx'),
                        spark_sum('DAYS_SUPPLY').alias('total_days_supply')
                   )

# Show the results to verify correct application
result_df.show()

# COMMAND ----------

# Filter the DataFrame to keep only rows where total_rx is 2 or more and total_days_supply is 15 or more
filtered_result_df = result_df.filter((col('total_rx') >= 2) & (col('total_days_supply') >= 15))

# Show the results to verify correct application
filtered_result_df.show(100)

# COMMAND ----------

print(eligible_population.count())
denominator = eligible_population.join(result_df, on=["beneID","state"], how="inner")
print(denominator.count())

# COMMAND ----------

# Count the total number of unique beneID, state combinations
unique_combinations_count = denominator.select('beneID', 'state').distinct().count()

# Print the count
print("Total number of unique beneID, state combinations:", unique_combinations_count)

# COMMAND ----------

denominator.write.saveAsTable("dua_058828_spa240.paper_10_hdo_denom_12_months_new3", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC save med for opioid

# COMMAND ----------

meds = filtered_df.join(denominator, on=["beneID","state"], how="inner")
print(meds.count())

# COMMAND ----------

meds.write.saveAsTable("dua_058828_spa240.paper_10_hdo_all_meds_12_months_new2", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC START HERE

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_10_hdo_denom_12_months_new3")
rx = spark.table("dua_058828_spa240.paper_10_hdo_all_meds_12_months_new2")

# COMMAND ----------

print(denom.count())
print(rx.count())

# COMMAND ----------

rx.show()

# COMMAND ----------

rx_select = rx.select("beneID","state","ndc","rxDcDrugName","RX_FILL_DT","DAYS_SUPPLY","NDC_QTY", "number", "unit")
rx_select.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Convert RX_FILL_DT to DateType if it's not already
rx_select = rx_select.withColumn("RX_FILL_DT", col("RX_FILL_DT").cast("date"))

# Convert DAYS_SUPPLY to IntegerType if it's not already
rx_select = rx_select.withColumn("DAYS_SUPPLY", col("DAYS_SUPPLY").cast("int"))

# Create StartDate and EndDate columns
rx_select = rx_select.withColumn("StartDate", col("RX_FILL_DT")) \
                     .withColumn("EndDate", expr("date_add(RX_FILL_DT, DAYS_SUPPLY - 1)"))

# Show the result
rx_select.show()

# COMMAND ----------

df = rx_select.withColumn("StartDate", col("StartDate").cast("date"))
df = df.withColumn("EndDate", col("EndDate").cast("date"))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum as cumsum, when, row_number
from pyspark.sql.window import Window

def episodesOfCare(df):
    # Define window specifications for calculating lag values, cumulative sum, and row number
    beneID_state_window = Window.partitionBy("beneID", "state","ndc").orderBy("StartDate", "EndDate")
    beneID_state_window_cumsum = Window.partitionBy("beneID", "state","ndc").orderBy("StartDate", "EndDate").rowsBetween(Window.unboundedPreceding, 0)

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
df = df.withColumn("StartDate", col("StartDate").cast("date"))
df = df.withColumn("EndDate", col("EndDate").cast("date"))

# Apply the episodesOfCare function
result_df = episodesOfCare(df)

# Sort the DataFrame by beneID, state, StartDate, and EndDate
result_df = result_df.orderBy("beneID", "state", "rxDcDrugName", "StartDate", "EndDate")

# Show the result
result_df.show(200)

# COMMAND ----------

from pyspark.sql import functions as F

# Aggregating by beneID, state, episode
aggregated_df = result_df.groupBy("beneID", "state", "ndc", "episode").agg(
    F.min("StartDate").alias("min_StartDate"),
    F.max("EndDate").alias("max_EndDate"),
    F.sum("DAYS_SUPPLY").alias("total_days_supply"),
    F.sum("NDC_QTY").alias("total_ndc_supply"),
    F.mean("number").alias("mean_number")
)

aggregated_df.show()

# COMMAND ----------

# Assuming the DataFrame is named rx_select

# Sort by beneID, state, and episode in descending order
sorted_df = aggregated_df.orderBy(col("beneID").desc(), col("state").desc(), col("ndc").asc(), col("episode").asc())

# Show the result
sorted_df.show(200)

# COMMAND ----------

# MAGIC %md
# MAGIC conversion table

# COMMAND ----------

aggregated_df = aggregated_df.join(rx_df, on="ndc", how="left")
aggregated_df.show()

# COMMAND ----------

opioid_conversion_factors = {
    "butorphanol": 7,
    "codeine": 0.15,
    "codeine | phenylephrine | promethazine": 0.15,
    "codeine | promethazine": 0.15,
    "fentanyl": 0.13,
    "fentanyl [Duragesic]": 2.4,
    "fentanyl [Fentora]": 0.13,
    "fentanyl [Subsys]": 0.36,
    "hydrocodone [Hysingla]": 1,
    "hydrocodone [Zohydro]": 1,
    "hydrocodone | ibuprofen": 1,
    "hydromorphone": 1,
    "'hydromorphone [Dilaudid]": 1,
    "'hydromorphone [Exalgo]": 1,
    "levorphanol": 11,
    "meperidine": 0.1,
    "meperidine [Demerol]": 0.1,
    "methadone":1,
    "methadone [Methadose]":1,
    "morphine": 1,
    "morphine [Duramorph]": 1,
    "morphine [Infumorph]": 1,
    "morphine [Kadian]": 1,
    "morphine [MS Contin]": 1,
    "morphine | naltrexone [Embeda]": 1, 
    "opium": 1,
    'oxycodone':1.5 , 
    'oxycodone [Oxaydo]':1.5 , 
    'oxycodone [Oxycontin]':1.5, 
    'oxycodone [Roxicodone]':1.5, 
    'oxycodone [Xtampza]':1.5, 
    'oxymorphone':3, 
    'oxymorphone [Opana]':3, 
    'tapentadol [Nucynta]':0.4, 
    'tramadol':0.1 , 
    'tramadol [ConZip]':0.1 , 
    'tramadol [Ultram]': 0.1
}

# Convert the dictionary to a broadcast variable
broadcast_opioid_conversion_factors = spark.sparkContext.broadcast(opioid_conversion_factors)

# Function to get the conversion factor
def get_conversion_factor(opioid):
    return broadcast_opioid_conversion_factors.value.get(opioid, None)

# Register the function as a UDF
from pyspark.sql.functions import udf
get_conversion_factor_udf = udf(get_conversion_factor, StringType())

# Add the conversion_factor column
aggregated_df = aggregated_df.withColumn("conversion_factor", get_conversion_factor_udf(col("rxDCDrugName")))

# Show the results
aggregated_df.show()

# COMMAND ----------

# Specify the column to check
column_to_check = "conversion_factor"

df =  aggregated_df

total_rows = aggregated_df.count()

# Calculate the percentage of null values for the specified column
null_percentage = df.select((col(column_to_check).isNull().cast("int").alias(column_to_check)))
null_percentage = null_percentage.agg((100 * sum(col(column_to_check)) / total_rows).alias(column_to_check))

# Show the percentage of null values for the specified column
null_percentage.show()

# COMMAND ----------

aggregated_df = aggregated_df.filter(col("conversion_factor").isNotNull())
aggregated_df.show()

# COMMAND ----------

# Specify the column to check
column_to_check = "conversion_factor"

df =  aggregated_df

total_rows = aggregated_df.count()

# Calculate the percentage of null values for the specified column
null_percentage = df.select((col(column_to_check).isNull().cast("int").alias(column_to_check)))
null_percentage = null_percentage.agg((100 * sum(col(column_to_check)) / total_rows).alias(column_to_check))

# Show the percentage of null values for the specified column
null_percentage.show()

# COMMAND ----------

# MAGIC %md
# MAGIC CONVERSION

# COMMAND ----------

aggregated_df.show()

# COMMAND ----------

# Create a new column 'date_diff' which is the difference between date1 and date2

from pyspark.sql.functions import datediff
aggregated_df = aggregated_df.withColumn("date_diff", datediff(col("max_EndDate"), col("min_StartDate")))

# Show the result
aggregated_df.show()

# COMMAND ----------

ndc_dose = spark.table("dua_058828_spa240.paper_10_hdo_total_rx_12_months_new2")

ndc_dose.registerTempTable("connections")

ndc_dose = spark.sql('''

SELECT DISTINCT ndc, number

FROM connections;
''')

ndc_dose.show()

# COMMAND ----------

aggregated_df = aggregated_df.join(ndc_dose, on='ndc', how='left')
aggregated_df = aggregated_df.drop("mean_number")
aggregated_df.show()

# COMMAND ----------

# Create the new column based on the equation

from pyspark.sql.functions import col, round

aggregated_df = aggregated_df.withColumn(
    "total_mme",
    round((col("total_ndc_supply") / col("total_days_supply")) * 
    col('number') * 
    col('conversion_factor') *
    (col('total_ndc_supply') / col('date_diff')), 3))


# Show the result
aggregated_df.show()

# COMMAND ----------

# Perform aggregation
aggregated_df = aggregated_df.groupBy("beneID", "state") \
                  .agg(
                      mean("total_mme").alias("mean_mme")                  )

# Show result
aggregated_df.show()

# COMMAND ----------

# Create the flag column
final = aggregated_df.withColumn("flag", when(col("mean_mme") >= 90, 1).otherwise(0))

# Calculate the total number of rows and the number of flagged rows
total_count = final.count()
flagged_count = final.filter(col("flag") == 1).count()

# Calculate the percentage of rows that are flagged
percentage_flagged = (flagged_count / total_count) * 100

# Show the result
final.show()
print(f"Percentage of rows with mean_mme >= 90: {percentage_flagged:.2f}%")

# COMMAND ----------

from pyspark.sql.functions import max

# Group by beneID and state, and calculate the mean of the mme column
final_agg = final.groupBy("beneID", "state").agg(max("flag").alias("mme_yes"))

# Show the result
final_agg.show()

# COMMAND ----------

# Calculate the total number of rows and the number of flagged rows
total_count = final_agg.count()
flagged_count = final_agg.filter(col("mme_yes") == 1).count()

# Calculate the percentage of rows that are flagged
percentage_flagged = (flagged_count / total_count) * 100

# Show the result
print(f"Percentage of rows with mean_mme >= 90: {percentage_flagged:.2f}%")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Create the new column using the given equation
sorted_df = sorted_df.withColumn("equation", col("total_days_supply") * col("mean_ndc_supply") / col("duration_days"))

# Show the result
sorted_df.show()

# COMMAND ----------


# Filter the DataFrame where equation is greater than or equal to 90
filtered_df = sorted_df.filter(col('equation') >= 90)

# Select distinct values in the 'rxDcDrugName' column
unique_drug_names = filtered_df.select('rxDcDrugName').distinct()

# Collect and print the unique values
unique_drug_names_list = unique_drug_names.collect()
for row in unique_drug_names_list:
    print(row['rxDcDrugName'])

# COMMAND ----------

# Create the high_mme column based on the condition
sorted_df = sorted_df.withColumn("high_mme", when(col("equation") >= 90, 1).otherwise(0))

# Calculate the value counts for high_mme
high_mme_counts = sorted_df.groupBy("high_mme").count()

# Calculate the total number of rows
total_count = sorted_df.count()

# Calculate the percentage
high_mme_counts = high_mme_counts.withColumn("percentage", (col("count") / total_count) * 100)

# Show the result
high_mme_counts.show()

# COMMAND ----------

from pyspark.sql.functions import max

# Group by beneID and state, and take the maximum value for high_mme
df_max_high_mme = sorted_df.groupBy("beneID", "state").agg(max(col("high_mme")).alias("high_mme"))

# Show the result
df_max_high_mme.show()

# COMMAND ----------

# Calculate the value counts for high_mme
high_mme_counts = df_max_high_mme.groupBy("high_mme").count()

# Calculate the total number of rows
total_count = df_max_high_mme.count()

# Calculate the percentage
high_mme_counts = high_mme_counts.withColumn("percentage", (col("count") / total_count) * 100)

# Show the result
high_mme_counts.show()

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_10_hdo_denom_12_months_new1")
denom = denom.drop("exclude","total_rx","total_days_supply")
denom.show()

# COMMAND ----------

final = denom.join(df_max_high_mme, on=["beneID","state"], how="left").fillna(0)
final.show()

# COMMAND ----------

# Calculate the value counts for high_mme
high_mme_counts = final.groupBy("high_mme").count()

# Calculate the total number of rows
total_count = final.count()

# Calculate the percentage
high_mme_counts = high_mme_counts.withColumn("percentage", (col("count") / total_count) * 100)

# Show the result
high_mme_counts.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F

# Aggregating by beneID, state, episode
aggregated_df_2 = aggregated_df.groupBy("beneID", "state").agg(
    F.sum("total_days_supply").alias("total_supply"),
    F.sum("total_ndc_supply").alias("total_ndc_supply"),
    F.sum("duration").alias("total_duration")
)

aggregated_df_2 = aggregated_df_2.withColumn("total_supply_per_duration", col("total_supply") / col("total_duration"))
aggregated_df_2.show()

# COMMAND ----------

distribution = aggregated_df_2.select(
    when(col("total_supply_per_duration") > 1, 1).otherwise(0).alias("greater_than_one")
).groupBy("greater_than_one").count()

# Show the distribution
distribution.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

dep_meds = dep_meds.select("beneID", "state", "RX_FILL_DT").distinct()
print(dep_meds.count())

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


# Define the window specification
window_spec = Window.partitionBy("beneID", "state").orderBy("RX_FILL_DT")

# Add a row number column to identify the earliest date
df_with_row_num = dep_meds.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the earliest date for each unique beneID, state combination
final_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

# Show the final DataFrame
print(dep_meds.count())
print(final_df.count())
final_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, date_sub

index_df = final_df.withColumn("RX_FILL_DT", col("RX_FILL_DT").cast("date"))

# Add start_date and end_date columns
index_df = index_df.withColumn('start_date', date_sub(col('RX_FILL_DT'), 60)) \
       .withColumn('end_date', date_add(col('RX_FILL_DT'), 60))

# Show the results to verify correct application
index_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC REMOVE PEOPLE WITH NO MAJOR DEP DISCORDER DIAGNOSIS

# COMMAND ----------

outpat2017 = spark.table("dua_058828_spa240.paper_4_otherservices2017_12_months")
outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")
inpat2017 = spark.table("dua_058828_spa240.paper_4_inpatient2017_12_months_new")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

# COMMAND ----------

outpat2017 = outpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD", "POS_CD")
outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD", "POS_CD")
outpat2019 = outpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD", "POS_CD")

inpat2017 = inpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1","PRCDR_CD_1")
inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1","PRCDR_CD_1")
inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1","PRCDR_CD_1")

inpat2017 = inpat2017.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2019 = inpat2019.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

inpat2017 = inpat2017.withColumn('POS_CD', lit(None))
inpat2018 = inpat2018.withColumn('POS_CD', lit(None))
inpat2019 = inpat2019.withColumn('POS_CD', lit(None))

# Use reduce to apply union to all DataFrames in the list
all_claims = outpat2018.union(outpat2019).union(inpat2018).union(inpat2019).union(outpat2017).union(inpat2017)
all_claims.show()

# COMMAND ----------

# Define lists for the ICD-10-CM, CPT, HCPCS, and POS codes
icd10_codes = [
    'F320', 'F321', 'F322', 'F323', 'F324', 'F329', 'F330', 'F331', 'F332', 'F333', 'F3341', 'F3342', 'F339', 'F341'
]

bh_outpatient_cpt_codes = [
    '98960', '98961', '98962', '99078', '99202', '99203', '99204', '99205', '99211', '99212', '99213', '99214', '99215',
    '99242', '99243', '99244', '99245', '99341', '99342', '99343', '99344', '99345', '99347', '99348', '99349', '99350',
    '99381', '99382', '99383', '99384', '99385', '99386', '99387', '99391', '99392', '99393', '99394', '99395', '99396', '99397',
    '99401', '99402', '99403', '99404', '99411', '99412', '99483', '99492', '99493', '99494', '99510'
]

bh_outpatient_hcpcs_codes = [
    'G0155', 'G0176', 'G0177', 'G0409', 'G0463', 'G0512', 'H0002', 'H0004', 'H0031', 'H0034', 'H0036', 'H0037', 'H0039', 
    'H0040', 'H2000', 'H2010', 'H2011', 'H2013', 'H2014', 'H2015', 'H2016', 'H2017', 'H2018', 'H2019', 'H2020', 'T1015'
]

pos_codes = ['02', '10', '52', '53']
telehealth_pos_codes = ['10']
telehealth_cpt_codes = ['98966', '98967', '98968', '99441', '99442', '99443']
ed_visit_cpt_codes = ['99281', '99282', '99283', '99284', '99285']
online_assessment_cpt_codes = [
    '98970', '98971', '98972', '98980', '98981', '99421', '99422', '99423', '99457', '99458'
]
online_assessment_hcpcs_codes = ['G0071', 'G2010', 'G2012', 'G2250', 'G2251', 'G2252']

# Filter the DataFrame based on the specified conditions
filtered_df = all_claims.filter(
    (col('DGNS_CD_1').isin(icd10_codes)) &
    (
        col('LINE_PRCDR_CD').isin(bh_outpatient_cpt_codes + bh_outpatient_hcpcs_codes) & col('POS_CD').isin(pos_codes) |
        col('LINE_PRCDR_CD').isin(telehealth_cpt_codes) & col('POS_CD').isin(telehealth_pos_codes) |
        col('LINE_PRCDR_CD').isin(ed_visit_cpt_codes) |
        col('LINE_PRCDR_CD').isin(online_assessment_cpt_codes + online_assessment_hcpcs_codes)
    )
)

# Show the results to verify correct application
filtered_df.show()

# COMMAND ----------

# Select distinct rows based on beneID, state, and date
distinct_df = filtered_df.select('beneID', 'state', 'SRVC_BGN_DT').distinct()

# Add a column with value 1 called 'dep_visit'
final_df = distinct_df.withColumn('dep_visit', lit(1))

# Show the results to verify correct application
final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY VISITS in 120 day period of index medication

# COMMAND ----------

combined = final_df.join(index_df, on=["beneID","state"], how="inner")
print(final_df.count())
print(combined.count())
combined.show()

# COMMAND ----------

# Add mdd_visit column based on whether rx_date is between start_date and end_date
combined = combined.withColumn(
    'mdd_visit',
    when((col('SRVC_BGN_DT') >= col('start_date')) & (col('SRVC_BGN_DT') <= col('end_date')), 1).otherwise(0)
)

# Show the results to verify correct application
combined.show()

# COMMAND ----------

from pyspark.sql.functions import col, date_add, date_sub, when, max as spark_max

# Group by beneID, state, RX_DATE and calculate max(mdd_visit)
combined = combined.groupBy('beneID', 'state', 'RX_FILL_DT') \
              .agg(spark_max('mdd_visit').alias('mdd_yes'))

# Show the results to verify correct application
combined.show()

# COMMAND ----------

values = combined.groupBy("mdd_yes").count()
values.show()

# COMMAND ----------

print(combined.count())

# COMMAND ----------

# Drop rows where mdd_visit is not 1
filtered_df = combined.filter(col('mdd_yes') == 1)
print(filtered_df.count())

# COMMAND ----------

filtered_df.write.saveAsTable("dua_058828_spa240.paper_4_amm_denom_12_months_new", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC Capture medication andherence 

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_amm_denom_12_months_new")
denom = denom.withColumnRenamed('RX_FILL_DT', 'index_date')
print(denom.count())

# COMMAND ----------

denom.show()

# COMMAND ----------

import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#betos_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df = rx_df[rx_df['rxDcDrugName'].isin(['bupropion','bupropion [Aplenzin]','bupropion [Forfivo]','bupropion [Wellbutrin]','bupropion | naltrexone [Contrave]','Vilazodone','vortioxetine [Trintellix]','isocarboxazid [Marplan]','phenelzine','phenelzine [Nardil]','selegiline','selegiline [Emsam]','selegiline [Zelapar]','tranylcypromine','tranylcypromine [Parnate]','nefazodone','trazodone','amitriptyline | chlordiazepoxide','amitriptyline | perphenazine','fluoxetine | olanzapine','fluoxetine | olanzapine [Symbyax]','desvenlafaxine','desvenlafaxine [Pristiq]','duloxetine','duloxetine [Cymbalta]','duloxetine [Drizalma]','duloxetine [Irenka]','levomilnacipran','levomilnacipran [Fetzima]','venlafaxine','venlafaxine [Effexor]','citalopram','citalopram [Celexa]','escitalopram','escitalopram [Lexapro]','fluoxetine','fluoxetine [Prozac]','fluvoxamine','paroxetine','paroxetine [Brisdelle]','paroxetine [Paxil]','paroxetine [Pexeva]','sertraline','sertraline [Zoloft]','mirtazapine','mirtazapine [Remeron]','amitriptyline','amitriptyline [Elavil]','amoxapine','clomipramine','clomipramine [Anafranil]','desipramine','desipramine [Norpramin]','doxepin','doxepin [Prudoxin]','doxepin [Silenor]','doxepin [Zonalon]','Imipramine','nortriptyline','nortriptyline [Pamelor]','protriptyline','trimipramine'])]
rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
print(rx_df.count())
rx_df.show(1000)

# COMMAND ----------

pharm2018 = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
pharm2019 = spark.table("dua_058828_spa240.paper_4_pharm2019_12_months")
pharm = pharm2018.union(pharm2019)
#print(pharm.count())
pharm = pharm.select("beneID", "state", "RX_FILL_DT", "DAYS_SUPPLY", "NDC")
pharm.show()

# COMMAND ----------

pharm.registerTempTable("connections")

pharm = spark.sql('''

SELECT DISTINCT beneID, state, RX_FILL_DT, NDC, DAYS_SUPPLY

FROM connections;
''')

pharm = pharm.filter(col("NDC").isNotNull())
#pharm.show(200)

# COMMAND ----------

from pyspark.sql.functions import col, substring

dep_meds = pharm.join(
    rx_df,
    on='ndc',
    how='inner'
)

#print(pharm.count())
#print(dep_meds.count())
#dep_meds.show()

# COMMAND ----------

dep_meds = dep_meds.join(denom, on=["beneID","state"], how="inner")
#dep_meds.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 84 days

# COMMAND ----------

# Add a column 'index_date_plus_84' which is 'index_date' plus 84 days
dep_meds = dep_meds.withColumn('index_plus_84', date_add(col('index_date'), 84))

# Show the results to verify correct application
dep_meds =dep_meds.drop("mdd_yes","ndc")
#dep_meds.show()

# COMMAND ----------

# Filter the DataFrame where rx_fill_dt is less than index_date_plus_84
dep_meds = dep_meds.filter((col('RX_FILL_DT') < col('index_plus_84')) & (col('index_date') <= col('RX_FILL_DT')))
dep_meds = dep_meds.orderBy(['beneID', 'state', 'RX_FILL_DT'], ascending=[True, True, True])

# Show the results to verify correct application
dep_meds.show(150)

# COMMAND ----------

distinct = dep_meds.select("beneID","state", "RX_FILL_DT", "DAYS_SUPPLY").distinct()

# COMMAND ----------

# Sum days_supply by beneID and state
from pyspark.sql.functions import col, date_add, sum as spark_sum
distinct_agg = distinct.groupBy('beneID', 'state').agg(spark_sum('DAYS_SUPPLY').alias('outcome1'))

# COMMAND ----------

# Add outcome1_yes column based on the condition
distinct_agg = distinct_agg.withColumn('outcome1_yes', when(col('outcome1') >= 84, 1).otherwise(0))
distinct_agg.show()

# COMMAND ----------

denom = denom.join(distinct_agg, on=["beneID","state"], how="left").fillna(0)
print(denom.count())

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

denom.show()

# COMMAND ----------

denom.write.saveAsTable("dua_058828_spa240.paper_4_amm_outcome1_months_new", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC 180 days

# COMMAND ----------

# Add a column 'index_date_plus_84' which is 'index_date' plus 84 days
from pyspark.sql.functions import col, date_add, sum as spark_sum

dep_meds = dep_meds.withColumn('index_plus_180', date_add(col('index_date'), 180))

# Show the results to verify correct application
dep_meds =dep_meds.drop("mdd_yes","ndc")
#dep_meds.show()

# COMMAND ----------

# Filter the DataFrame where rx_fill_dt is less than index_date_plus_84
dep_meds = dep_meds.filter((col('RX_FILL_DT') < col('index_plus_180')) & (col('index_date') <= col('RX_FILL_DT')))
dep_meds = dep_meds.orderBy(['beneID', 'state', 'RX_FILL_DT'], ascending=[True, True, True])

# Show the results to verify correct application
dep_meds.show(150)

# COMMAND ----------

distinct = dep_meds.select("beneID","state", "RX_FILL_DT", "DAYS_SUPPLY").distinct()

# COMMAND ----------

# Sum days_supply by beneID and state
from pyspark.sql.functions import col, date_add, sum as spark_sum
distinct_agg = distinct.groupBy('beneID', 'state').agg(spark_sum('DAYS_SUPPLY').alias('outcome2'))

# COMMAND ----------

# Add outcome1_yes column based on the condition
distinct_agg = distinct_agg.withColumn('outcome2_yes', when(col('outcome2') >= 180, 1).otherwise(0))
distinct_agg.show()

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_amm_outcome1_months_new")
denom = denom.join(distinct_agg, on=["beneID","state"], how="left").fillna(0)
print(denom.count())

# COMMAND ----------

value_outcome1 = denom.groupBy("outcome2_yes").count()
value_outcome1.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("outcome2_yes") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

denom.show()

# COMMAND ----------

denom.write.saveAsTable("dua_058828_spa240.paper_4_amm_both_outcomes_months_new", mode='overwrite')